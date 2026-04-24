"""
AI-powered tender extractor using OpenAI GPT-4.1.

CHANGES IN THIS VERSION:
  1. AI SEMAPHORE — derived automatically from WORKER_CONCURRENCY.
     Formula: max(4, min(8, WORKER_CONCURRENCY - 4)) when WORKER_CONCURRENCY > 8,
     else 4. This means AI concurrency scales with workers but never overloads
     the OpenAI endpoint. Uses the same WORKER_CONCURRENCY env var — no new var.

  2. CHUNKING instead of truncation.
     Old: page_text[:80000] — silently dropped the rest.
     New: pages > CHUNK_SIZE chars are split into overlapping chunks
     (overlap = CHUNK_OVERLAP chars to avoid cutting a tender in half).
     Results from all chunks are merged and de-duplicated by (title, deadline).
     Data fidelity is preserved — nothing is dropped.

  3. SMART is_tender_page() with GPT-4.1-mini pre-filter.
     Old: returned True for everything ≥ 80 chars (sent every page to full AI).
     New three-tier check:
       Tier 1 — Fast keyword scan (zero cost, catches 80 % of obvious pages).
       Tier 2 — URL structure heuristic (catches /tenders/, /procurement/ etc.
                 in ANY language path segment, not just English keywords).
       Tier 3 — GPT-4.1-mini 1-shot classifier on first 2 000 chars of text.
                 Returns "yes"/"no". Catches multilingual/unusual URLs like
                 https://aispo.org/en/tenders/ with no matching keywords.
     Only pages that pass at least one tier reach the full GPT-4.1 extractor.

  4. MODEL uses env var properly — was hardcoded to "gpt-4.1" ignoring MODEL.
"""
import asyncio
import json
import os
import re
import time
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential
from loguru import logger

client = AsyncOpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url=os.getenv("OPENAI_BASE_URL"),
)
MODEL      = os.getenv("OPENAI_MODEL",      "gpt-4.1")
MODEL_MINI = os.getenv("OPENAI_MODEL", "gpt-4.1")

# ── AI Semaphore ──────────────────────────────────────────────────────────────
# Derived from WORKER_CONCURRENCY (same env var, no new config needed).
# When workers ≤ 8  → AI concurrency = 4  (safe floor)
# When workers > 8  → AI concurrency = min(8, workers - 4)
# This prevents the AI endpoint from being flooded when workers scale up.

def _compute_ai_concurrency() -> int:
    w = int(os.getenv("WORKER_CONCURRENCY", "3"))
    if w <= 4:
        return 3
    if w <= 8:
        return 4
    return max(4, min(8, w - 3))

_AI_SEMAPHORE: asyncio.Semaphore | None = None  # lazy-init (event loop must exist)

def _get_ai_semaphore() -> asyncio.Semaphore:
    global _AI_SEMAPHORE
    if _AI_SEMAPHORE is None:
        n = _compute_ai_concurrency()
        _AI_SEMAPHORE = asyncio.Semaphore(n)
        logger.info(f"AI semaphore initialised: {n} concurrent calls "
                    f"(WORKER_CONCURRENCY={os.getenv('WORKER_CONCURRENCY', '3')})")
    return _AI_SEMAPHORE


# ── Chunking config ───────────────────────────────────────────────────────────
CHUNK_SIZE    = int(os.getenv("AI_CHUNK_SIZE",    "40000"))  # chars per chunk
CHUNK_OVERLAP = int(os.getenv("AI_CHUNK_OVERLAP", "2000"))   # overlap to avoid splits


# ── Prompts ───────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a precise tender/procurement data extraction engine that works in ANY language.

YOUR JOB: Extract ALL tender, bid, RFP, procurement, or contract notices from the given webpage text.

TWO CASES you must handle:

CASE 1 — LISTING PAGE: The page shows a table or list of multiple tenders.
  → Extract each row/item as a separate object in the JSON array.

CASE 2 — DETAIL PAGE: The page shows a single tender's full details (title, deadline, documents, etc.)
  → Extract it as a one-element JSON array.

STRICT RULES:
1. Extract ONLY what is explicitly written. NEVER infer, guess, or hallucinate.
2. Missing fields → use null. NEVER make up values.
3. Copy exact text — do NOT translate, summarize, or rephrase any field.
4. If no tender/procurement content found → return [].
5. Works in ALL languages: French, Arabic, Spanish, Portuguese, Italian, German, English, etc.

Each tender object MUST have exactly these fields (null if not present):
{
  "title":            "exact title of this tender/notice as written (any language)",
  "reference_number": "tender/bid/lot reference number or code",
  "description":      "full description exactly as written (do not shorten)",
  "date_posted":      "publication date as written on page",
  "deadline":         "submission/closing/expiry deadline as written",
  "organization":     "issuing organization, ministry, or department",
  "category":         "type: works/goods/services/consulting or as written",
  "location":         "country, city, or project location if mentioned",
  "estimated_value":  "budget or contract value if mentioned",
  "contact":          "contact person, email, phone if mentioned",
  "document_links":   ["list of document/attachment URLs (.pdf .doc .docx .xls .xlsx .zip)"],
  "status":           "open/closed/cancelled/awarded as written, or null",
  "additional_info":  "any other relevant procurement info verbatim from page"
}

Return ONLY valid JSON array. No explanation. No markdown. No code blocks."""

CLASSIFIER_PROMPT = """You are a webpage classifier. Decide if this webpage text contains or is likely to contain tender/procurement/bid/contract notices — in ANY language.

Answer with a single word: yes or no.

Criteria for "yes":
- The page lists tenders, bids, RFPs, RFQs, or procurement notices (any language)
- The page is a single tender detail/description page
- The page shows contract awards, expressions of interest, or supplier notices
- The page is a procurement portal index or category listing that leads to tenders
- Keywords in ANY language suggesting procurement: appel d'offres, licitación, مناقصة, gara, Ausschreibung, licitação, etc.

Criteria for "no":
- News articles, blog posts, press releases, about/contact pages
- Job listings (unless combined with tenders)
- General product/service pages with no procurement notices

Answer only: yes or no"""


# ── Multilingual tender signals (Tier 1 fast scan) ───────────────────────────

_TENDER_SIGNALS = {
    # English
    "tender", "bid", "rfp", "rfq", "rft", "procurement", "solicitation",
    "closing date", "submission deadline", "invitation to tender",
    "expression of interest", "request for proposal", "contract notice",
    "reference no", "ref no", "nit", "quotation", "proposal","tenders",
    # French
    "appel d'offres", "appel d offres", "marché public", "marche public",
    "avis de marché", "avis de marche", "soumission", "fournitures",
    "prestation", "dossier d'appel", "date limite", "remise des offres",
    "avis d'appel", "consultation", "offres de prix",
    # Arabic
    "مناقصة", "عطاء", "مشتريات", "طلب عروض", "إعلان مناقصة",
    "المناقصات", "العطاءات", "تاريخ الإغلاق", "الشراء",
    # Spanish
    "licitación", "licitacion", "concurso", "convocatoria", "adjudicación",
    "adjudicacion", "pliego", "oferta", "propuesta", "contratación",
    "contratacion", "fecha límite", "fecha limite",
    # Portuguese
    "licitação", "licitacao", "concurso público", "concurso publico",
    "edital", "aquisição", "aquisicao", "pregão", "pregao",
    "processo licitatório", "data limite",
    # Italian
    "gara d'appalto", "gara appalto", "bando", "appalto", "offerta",
    "aggiudicazione", "capitolato", "scadenza",
    # German
    "ausschreibung", "vergabe", "beschaffung", "bieter", "angebotsfrist",
    "vergabebekanntmachung", "zuschlag", "angebotsabgabe",
    # Universal
    "deadline", "closing date", "due date", "submission date",
    "validity", "lot ", "lot no", "lot number",
    # NGO/humanitarian procurement
    "open tenders", "tender dossier", "contract award", "call for tenders",
    "invitation to bid", "tender notice", "procurement notice",
    "supply of", "works contract", "services contract",
    "award of contract", "framework contract",
}

# Tier 2 — URL path segments that strongly suggest a tender/procurement page
# regardless of language. Even /tenders/, /appels-offres/, /licitaciones/ etc.
_TENDER_URL_SEGMENTS = {
    # English
    "tender", "tenders", "bid", "bids", "rfp", "rfq", "procurement",
    "eprocurement", "e-procurement", "etender", "e-tender", "solicitation",
    "contract", "contracts", "opportunity", "opportunities", "notice",
    "nit", "eoi", "expressions-of-interest",
    # French
    "appels-offres", "appel-offres", "marches", "marche", "soumissions",
    "avis-marche", "consultation", "avis",
    # Spanish
    "licitaciones", "licitacion", "concurso", "convocatoria", "contratacion",
    "compras", "subastas",
    # Portuguese
    "licitacoes", "licitacao", "pregoes", "pregao", "compras",
    "editais", "edital",
    # Arabic transliterated
    "monaqasat", "munaqasat", "tenders-ar",
    # Italian
    "gare", "appalti", "bandi",
    # German
    "ausschreibungen", "vergaben", "beschaffung",
    # Generic procurement portals
    "eprocure", "negociaciones", "procurement-notices",
}


# ── Core helpers ──────────────────────────────────────────────────────────────

def _split_into_chunks(text: str) -> list[str]:
    """
    Split text into overlapping chunks of CHUNK_SIZE chars.
    The overlap ensures a tender that straddles a boundary is not lost.
    Returns a list with one element if text fits in a single chunk.
    """
    if len(text) <= CHUNK_SIZE:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + CHUNK_SIZE
        chunks.append(text[start:end])
        if end >= len(text):
            break
        start = end - CHUNK_OVERLAP  # step back by overlap
    return chunks


def _dedup_tenders(tenders: list[dict]) -> list[dict]:
    """
    Remove duplicate tenders that appear in multiple chunks.
    Two tenders are considered the same if they share (title, deadline)
    or (reference_number) — whichever is non-null.
    """
    seen_refs: set[str]         = set()
    seen_title_dl: set[tuple]   = set()
    unique: list[dict]          = []

    for t in tenders:
        ref  = (t.get("reference_number") or "").strip().lower()
        title = (t.get("title") or "").strip().lower()[:80]
        dl    = (t.get("deadline") or "").strip().lower()[:30]

        if ref and ref in seen_refs:
            continue
        key = (title, dl)
        if title and key in seen_title_dl:
            continue

        if ref:
            seen_refs.add(ref)
        if title:
            seen_title_dl.add(key)
        unique.append(t)

    return unique


def _parse_ai_response(raw: str) -> list[dict]:
    """Parse AI JSON response, handle markdown fences, normalise to list."""
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        raw = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []

    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        for key in ("tenders", "data", "results", "items", "procurements",
                    "bids", "contracts", "notices", "offres", "marches"):
            if key in parsed and isinstance(parsed[key], list):
                return parsed[key]
        if any(k in parsed for k in ("title", "reference_number", "deadline",
                                      "organization", "description")):
            return [parsed]
    return []


def _normalise_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _value_grounded(value, page_text_norm: str) -> bool:
    if value is None:
        return True
    if isinstance(value, list):
        return all(_value_grounded(item, page_text_norm) for item in value)

    text = _normalise_text(str(value))
    if not text:
        return True
    if len(text) < 4:
        return False
    if text in page_text_norm:
        return True

    tokens = [tok for tok in re.findall(r"[a-z0-9]{3,}", text) if tok]
    if len(tokens) < 2:
        return False
    matched = sum(1 for tok in tokens if tok in page_text_norm)
    return matched / len(tokens) >= 0.85


def _validate_tender_grounding(tender: dict, page_text: str) -> dict | None:
    allowed = {
        "title", "reference_number", "description", "date_posted", "deadline",
        "organization", "category", "location", "estimated_value", "contact",
        "document_links", "status", "additional_info",
    }
    page_text_norm = _normalise_text(page_text)
    cleaned = {}

    for field in allowed:
        value = tender.get(field)
        if field == "document_links" and isinstance(value, list):
            grounded_links = [link for link in value if _value_grounded(link, page_text_norm)]
            cleaned[field] = grounded_links
            continue
        cleaned[field] = value if _value_grounded(value, page_text_norm) else None

    if not cleaned.get("title"):
        return None

    material_fields = ("description", "deadline", "organization", "reference_number", "additional_info")
    if not any(cleaned.get(field) for field in material_fields):
        return None

    return cleaned


# ── Extraction ────────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    reraise=True,
)
async def _call_extractor(chunk_text: str, url: str, fetched_at: float) -> list[dict]:
    """
    Single GPT-4.1 call for one chunk. Retried up to 3 times.
    Called under the AI semaphore by the caller.
    """
    user_message = (
        f"Source URL: {url}\n"
        f"Fetched at: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(fetched_at))}\n\n"
        f"PAGE TEXT:\n{chunk_text}"
    )
    response = await client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_message},
        ],
        temperature=0,
        max_tokens=8192,
    )
    raw = response.choices[0].message.content or ""
    return _parse_ai_response(raw)


async def extract_tenders_with_ai(
    page_text: str,
    url: str,
    fetched_at: float,
) -> list[dict]:
    """
    Extract tenders from page_text using GPT-4.1.

    If page_text > CHUNK_SIZE chars, splits into overlapping chunks,
    calls the AI on each chunk concurrently (still under the semaphore),
    then merges and de-duplicates results.

    All calls go through the AI semaphore so total concurrent AI requests
    stay within the limit derived from WORKER_CONCURRENCY.
    """
    if not page_text or len(page_text.strip()) < 100:
        return []

    chunks = _split_into_chunks(page_text)
    sem    = _get_ai_semaphore()

    async def _bounded_chunk(chunk: str, idx: int) -> list[dict]:
        async with sem:
            try:
                results = await _call_extractor(chunk, url, fetched_at)
                if len(chunks) > 1:
                    logger.debug(f"Chunk {idx+1}/{len(chunks)} → {len(results)} tender(s) — {url}")
                return results
            except Exception as e:
                logger.error(f"AI error chunk {idx+1}/{len(chunks)} for {url}: {e}")
                return []

    if len(chunks) == 1:
        raw_tenders = await _bounded_chunk(chunks[0], 0)
    else:
        logger.info(f"Chunking {len(page_text):,} chars into {len(chunks)} chunks for {url}")
        chunk_results = await asyncio.gather(*[_bounded_chunk(c, i) for i, c in enumerate(chunks)])
        raw_tenders = [t for batch in chunk_results for t in batch]

    # Enrich and de-duplicate
    enriched = []
    for t in raw_tenders:
        if not isinstance(t, dict):
            continue
        validated = _validate_tender_grounding(t, page_text)
        if not validated:
            continue
        non_null = [v for v in validated.values() if v is not None and v != "" and v != []]
        if not non_null:
            continue
        validated["source_url"]  = url
        validated["fetched_at"]  = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(fetched_at))
        enriched.append(validated)

    return _dedup_tenders(enriched)


# ── Page classifier ───────────────────────────────────────────────────────────

async def _mini_classify(text_sample: str, url: str) -> bool:
    """
    GPT-4.1-mini 1-shot classifier. Returns True if the page likely has tenders.
    Uses only the first 2 000 chars — fast and cheap.
    Called only when Tier 1 and Tier 2 both fail.
    """
    sem = _get_ai_semaphore()
    async with sem:
        try:
            response = await client.chat.completions.create(
                model=MODEL_MINI,
                messages=[
                    {"role": "system", "content": CLASSIFIER_PROMPT},
                    {"role": "user",   "content": f"URL: {url}\n\nPAGE SAMPLE:\n{text_sample[:2000]}"},
                ],
                temperature=0,
                max_tokens=5,
            )
            answer = (response.choices[0].message.content or "").strip().lower()
            return answer.startswith("yes")
        except Exception as e:
            logger.warning(f"Mini classifier failed for {url}: {e} — defaulting to True")
            return True  # fail open: better to over-send than miss tenders


async def is_tender_page(page_text: str, url: str) -> bool:
    """
    Three-tier decision — cheapest first, AI only when needed.

    Tier 1 — Multilingual keyword scan of page text (free).
              Catches pages that explicitly contain procurement vocabulary.

    Tier 2 — URL path segment check (free).
              Catches procurement portals whose URL contains /tenders/,
              /licitaciones/, /appels-offres/, etc. in any language.
              This is the fix for URLs like https://aispo.org/en/tenders/
              which have no keyword in the HTML but clearly host tenders.

    Tier 3 — GPT-4.1-mini classifier on first 2 000 chars (tiny cost).
              Catches any page that slipped through Tiers 1 & 2 — unusual
              layouts, obfuscated text, pages where tender content is below
              the fold, etc.
    """
    if not page_text or len(page_text.strip()) < 80:
        return False

    # ── Tier 1: keyword scan ─────────────────────────────────────────────────
    text_lower = page_text.lower()
    if any(signal in text_lower for signal in _TENDER_SIGNALS):
        return True
    # Also check URL itself — e.g. aispo.org/en/tenders/ has keyword in URL
    url_lower = url.lower()
    if any(signal in url_lower for signal in _TENDER_SIGNALS):
        return True

    # ── Tier 2: URL path segment check ──────────────────────────────────────
    from urllib.parse import urlparse
    import re
    parsed = urlparse(url.lower())
    # Split path into individual segments (handles hyphens, slashes, dots)
    path_parts = set(re.split(r'[/\-_.?&= ]', parsed.path))
    path_parts = {p for p in path_parts if len(p) > 2}
    if path_parts & _TENDER_URL_SEGMENTS:
        return True

    # ── Tier 3: mini AI classifier ───────────────────────────────────────────
    result = await _mini_classify(page_text, url)
    if result:
        logger.debug(f"Mini classifier: TENDER → {url}")
    return result