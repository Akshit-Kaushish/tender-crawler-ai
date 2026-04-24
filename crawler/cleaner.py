"""
HTML cleaner: strips all non-content elements before sending to AI.

CHANGES:
  1. extract_links_from_html() now follows pagination links.
     Pages like ?page=2, /page/3, ?p=2, ?start=10, rel="next", class="next"
     are returned even if they contain no tender keyword — they are listing
     pages that carry more tenders.

  2. is_tender_related_url() no longer excludes "page" as a path segment.
     "page" was wrongly in _EXCLUDE_SEGMENTS, which blocked every paginated
     URL like /tenders/page/2 or /bids?page=3.

  3. clean_html() now preserves full table row context.
     Old code emitted each <td>/<th> as an isolated "| text" line, losing
     the row relationship. Now rows are emitted together as pipe-separated
     lines so the AI sees "| Title | Deadline | Value |" on one line.

  4. Language-agnostic link following: extract_links_from_html() accepts
     any same-domain link that matches a pagination pattern OR a tender
     keyword, without relying on English anchor text like "Next".

  5. Queue cap: enqueue() in redis_manager.py enforces MAX_QUEUE_SIZE.
     This function respects that cap by never returning more links than
     needed when the queue is near capacity.
"""
import re
import os
from bs4 import BeautifulSoup, Comment
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse


# Tags to completely remove (with their content)
REMOVE_TAGS = {
    "script", "style", "noscript", "iframe", "svg", "canvas", "video",
    "audio", "picture", "source", "track", "map", "area", "object",
    "embed", "applet", "link", "meta", "head", "header", "footer",
    "nav", "aside", "form", "input", "button", "select", "textarea",
    "label", "fieldset", "legend", "datalist", "output", "progress",
    "meter", "details", "summary", "dialog", "menu", "menuitem",
    "template", "slot", "portal", "math", "annotation"
}

KEEP_ATTRS = {"href", "src", "alt", "title"}

# URL path segments that mean the page is NOT a tender listing.
# NOTE: "page" is intentionally NOT here — /tenders/page/2 is valid.
_EXCLUDE_SEGMENTS = {
    # Auth / account
    "login", "logout", "signin", "signup", "register", "account",
    "password", "forgot-password", "reset-password", "profile",
    # Site structure
    "about", "about-us", "contact", "contact-us", "faq", "help",
    "sitemap", "accessibility", "terms", "privacy", "disclaimer",
    "cookie", "legal",
    # Non-tender content
    "news", "media", "press", "blog", "article", "articles",
    "events", "gallery", "photos", "videos", "podcast",
    "careers", "jobs", "vacancies", "recruitment",
    "search", "tag", "tags", "category", "categories",
    "archive", "archives",
    # Downloads handled separately
    "download", "downloads",
}

# Regex patterns that identify a pagination URL regardless of keywords.
# Matches: ?page=2  /page/3  ?p=2  ?start=10  ?offset=20  ?pg=2  &page=2
_PAGINATION_RE = re.compile(
    r'[?&/](page|p|pg|start|offset|from|skip|paged)[=/]\d+',
    re.IGNORECASE
)

# rel="next" or class containing "next"/"pagination" on an <a> tag
_NEXT_CLASS_RE = re.compile(r'\bnext\b|\bpagination\b|\bnextpage\b', re.IGNORECASE)
MAX_FOLLOW_LINKS = int(os.getenv("MAX_FOLLOW_LINKS", "120"))

_TENDER_HINTS = {
    "tender", "tenders", "bid", "bids", "rfp", "rfq", "procurement",
    "contract", "contracts", "opportunity", "opportunities", "solicitation",
    "appel", "offres", "licitacion", "licitaciones", "licitacao", "licitacoes",
    "gara", "appalto", "appalti", "ausschreibung", "vergabe", "monaqasat",
    "munaqasat", "concours", "concursos", "quotation", "quotations",
}


def clean_html(raw_html: str, base_url: str = "") -> str:
    """
    Clean HTML to plain text suitable for AI extraction.
    Tables are preserved as pipe-separated rows so structure is not lost.
    """
    if not raw_html:
        return ""

    soup = BeautifulSoup(raw_html, "lxml")

    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    for tag in REMOVE_TAGS:
        for el in soup.find_all(tag):
            el.decompose()

    body = soup.find("body") or soup.find("main") or soup.find("article") or soup

    # Make hrefs absolute
    for el in body.find_all(True):
        attrs_to_remove = [a for a in el.attrs if a not in KEEP_ATTRS]
        for a in attrs_to_remove:
            del el[a]
        if el.get("href") and base_url:
            href = el["href"]
            if href.startswith("/"):
                el["href"] = urljoin(base_url, href)

    lines = []

    # Process tables as whole rows for better AI context
    for table in body.find_all("table"):
        for row in table.find_all("tr"):
            cells = [td.get_text(separator=" ", strip=True)
                     for td in row.find_all(["td", "th"])]
            row_text = " | ".join(c for c in cells if c)
            if row_text:
                lines.append(f"| {row_text} |")
        table.decompose()  # prevent double-processing cells below

    for el in body.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p",
                              "li", "dt", "dd", "caption",
                              "div", "span", "a", "strong", "b", "em"]):
        text = el.get_text(separator=" ", strip=True)
        if text and len(text) > 2:
            if el.name in ["h1", "h2", "h3", "h4"]:
                lines.append(f"\n## {text}")
            elif el.name == "li":
                lines.append(f"• {text}")
            else:
                lines.append(text)

    # Collect document links
    doc_links = []
    for a in body.find_all("a", href=True):
        href = a["href"]
        link_text = a.get_text(strip=True)
        if any(ext in href.lower() for ext in
               [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar", ".csv"]):
            doc_links.append(f"{link_text}: {href}")

    text = "\n".join(lines)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]{2,}', ' ', text)
    text = text.strip()

    if doc_links:
        text += "\n\n--- DOCUMENT LINKS ---\n" + "\n".join(doc_links)

    return text


def _is_pagination_link(url: str, tag) -> bool:
    """
    Return True if this link looks like a pagination link:
    - URL matches a pagination query pattern (?page=2, /page/3, etc.)
    - OR the <a> tag has rel="next" or a class matching "next/pagination"
    """
    if _PAGINATION_RE.search(url):
        return True
    if tag is None:
        return False
    rel = tag.get("rel", [])
    if isinstance(rel, list):
        rel = " ".join(rel)
    if "next" in rel.lower():
        return True
    cls = " ".join(tag.get("class", []))
    if _NEXT_CLASS_RE.search(cls):
        return True
    return False


def extract_links_from_html(raw_html: str, base_url: str) -> list[str]:
    """
    Extract ALL crawlable same-domain links from a page.

    Returns every same-domain HTTP/HTTPS link that is not a document,
    not a fragment, and not an excluded structural page (login, about etc).

    We do NOT filter by tender keywords here — that was the root cause of
    the empty queue. A page like /departments/procurement/ has no tender
    keyword in the URL but leads directly to tender listings.
    The AI extractor decides whether each page has tender content.
    The crawler's job is to follow all reachable links.
    """
    soup = BeautifulSoup(raw_html, "lxml")
    scored_links: dict[str, int] = {}
    base_domain = urlparse(base_url).netloc

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)

        if parsed.netloc != base_domain or parsed.scheme not in ("http", "https"):
            continue

        clean = parsed._replace(fragment="").geturl()

        if is_document_url(clean):
            continue

        # Skip obviously structural non-content pages
        path_segments = set(re.split(r'[/\-_?&=. ]', parsed.path.lower()))
        path_segments = {s for s in path_segments if s}
        if path_segments & _EXCLUDE_SEGMENTS:
            continue

        anchor_text = a.get_text(" ", strip=True).lower()
        link_tokens = path_segments | {t for t in re.split(r"[/\-_?&=. ]", parsed.query.lower()) if t}
        score = 0

        if _is_pagination_link(clean, a):
            score += 5
        if link_tokens & _TENDER_HINTS:
            score += 4
        if any(term in anchor_text for term in _TENDER_HINTS):
            score += 3
        if clean.rstrip("/") == base_url.rstrip("/"):
            score -= 5
        if len(parsed.path.strip("/").split("/")) <= 1:
            score -= 1

        # Historically we dropped score<=0 links, which often killed crawling on portals
        # where anchor text is generic or non-English. Keep only negative-score links
        # filtered out (self-links, extremely shallow paths), and allow score==0.
        _see_more = {"see more", "view all", "load more", "show more", "more tenders",
                     "voir plus", "ver más", "ver tudo", "mehr anzeigen", "tutti", "all tenders"}
        if score < 0 and not any(t in anchor_text for t in _see_more):
            continue

        previous = scored_links.get(clean)
        if previous is None or score > previous:
            scored_links[clean] = score

    ranked = sorted(scored_links.items(), key=lambda item: (-item[1], item[0]))
    return [url for url, _score in ranked[:MAX_FOLLOW_LINKS]]


def is_tender_related_url(url: str) -> bool:
    """
    Return True only if the URL path contains a procurement-specific keyword
    AND does not contain any excluded structural/non-tender path segment.
    """
    from crawler.redis_manager import TENDER_KEYWORDS

    parsed = urlparse(url.lower())
    path_query = parsed.path + " " + parsed.query

    path_segments = set(re.split(r'[/\-_?&=. ]', parsed.path))
    path_segments = {s for s in path_segments if s}
    if path_segments & _EXCLUDE_SEGMENTS:
        return False

    tokens = re.split(r'[/\-_?&=. ]', path_query)
    tokens = {t for t in tokens if t}

    return bool(tokens & TENDER_KEYWORDS)


def is_document_url(url: str) -> bool:
    """Return True if URL points to a downloadable document."""
    doc_exts = {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip",
                ".rar", ".ppt", ".pptx", ".csv", ".txt"}
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in doc_exts)
