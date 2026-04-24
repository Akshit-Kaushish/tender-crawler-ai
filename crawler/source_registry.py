"""
source_registry.py — Source/credential registry backed by Azure Key Vault + Redis.

Uses  (same as auth_manager) — the only approach that
works in a plain Docker container without Azure CLI or managed identity.

stray-quote fix: _clean_env() strips all quoting variants from env var values.
"""

import json
import os
import re
import time
from typing import Optional
from urllib.parse import urlparse

from loguru import logger

HASH_SOURCES = "tender:sources"


# ── Env var cleaning ──────────────────────────────────────────────────────────

def _clean_env(name: str, required: bool = True) -> str:
    """Strip whitespace and ALL stray quote characters from an env var value."""
    val = os.environ.get(name, "").strip().strip('"').strip("'").strip("`").strip()
    if required and not val:
        raise RuntimeError(f"Required env var {name!r} is not set or is empty.")
    return val


# ── URL helpers ───────────────────────────────────────────────────────────────

def normalize_url(url: str) -> str:
    url = (url or "").strip().strip('"').strip("'")
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def _enforce_https(url: str, label: str = "URL") -> str:
    if url.startswith("http://"):
        upgraded = "https://" + url[7:]
        logger.warning(f"[source_registry] {label} upgraded http→https: {upgraded}")
        return upgraded
    return url


def make_auth_id(name: str, base_url: str) -> str:
    seed = name or urlparse(base_url).netloc or "source"
    slug = re.sub(r"[^a-z0-9]+", "-", seed.lower()).strip("-")
    return slug[:80] or "source"


# ── Key Vault client ──────────────────────────────────────────────────────────

def _get_kv_client():
    """
    Build a Key Vault SecretClient using .
    Requires AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET in .env.
    """
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient

    kv_url        = _clean_env("AZURE_KEY_VAULT_URL")
    # tenant_id     = _clean_env("AZURE_TENANT_ID")
    # client_id     = _clean_env("AZURE_CLIENT_ID")
    # client_secret = _clean_env("AZURE_CLIENT_SECRET")

    kv_url = _enforce_https(kv_url, "AZURE_KEY_VAULT_URL")
    if not kv_url.startswith("https://"):
        raise RuntimeError(
            f"AZURE_KEY_VAULT_URL must be https://. Got: {kv_url!r}\n"
            f"Raw value in env: {os.environ.get('AZURE_KEY_VAULT_URL', '')!r}"
        )

    # credential = ClientSecretCredential(
    #     tenant_id=tenant_id,
    #     client_id=client_id,
    #     client_secret=client_secret,
    # )
    credential = DefaultAzureCredential(exclude_environment_credential=True)
    return SecretClient(vault_url=kv_url, credential=credential)


# ── Permission hint ───────────────────────────────────────────────────────────

def _kv_permission_hint(exc: Exception, action: str) -> str:
    text = str(exc).lower()
    if "forbidden" not in text and "not authorized" not in text:
        return ""
    client_id  = _clean_env("AZURE_CLIENT_ID", required=False)
    vault_url  = _clean_env("AZURE_KEY_VAULT_URL", required=False)
    vault_name = vault_url.replace("https://", "").replace(".vault.azure.net/", "")
    return (
        f"\nKey Vault denied '{action}' for {client_id}.\n"
        f"Fix: az keyvault set-policy --name {vault_name} "
        f"--spn {client_id} --secret-permissions get set list delete"
    )


# ── Secret persistence ────────────────────────────────────────────────────────

async def _save_secret(secret_name: str, payload: dict) -> None:
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        client = _get_kv_client()
        await loop.run_in_executor(
            None, lambda: client.set_secret(secret_name, json.dumps(payload))
        )
        logger.info(f"[source_registry] Secret '{secret_name}' saved to Key Vault ✓")
    except Exception as exc:
        hint = _kv_permission_hint(exc, "setSecret")
        logger.error(
            f"[source_registry] Failed to save secret '{secret_name}': {exc}{hint}"
        )
        raise RuntimeError(f"Key Vault write failed for '{secret_name}': {exc}") from exc


# ── Source record builders ────────────────────────────────────────────────────

def _build_source_record(payload: dict) -> dict:
    base_url = normalize_url(payload.get("base_url") or payload.get("url") or "")
    parsed   = urlparse(base_url) if base_url else None
    auth_id  = payload.get("auth_id") or make_auth_id(payload.get("name", ""), base_url)
    return {
        "auth_id":       auth_id,
        "name":          (payload.get("name") or (parsed.netloc if parsed else "") or auth_id).strip(),
        "base_url":      base_url,
        "domain":        (parsed.netloc if parsed else "").lower(),
        "auth_required": bool(payload.get("auth_required", True)),
        "method":        "form_login",
        "created_at":    payload.get("created_at") or time.time(),
        "updated_at":    time.time(),
        "notes":         (payload.get("notes") or "").strip(),
    }


def _build_secret_payload(payload: dict, base_url: str) -> dict:
    """
    Minimum required: login_url, username, password.
    Selectors are optional — left empty here triggers auto-detection in auth_manager.
    """
    login_url = normalize_url(payload.get("login_url") or base_url or "")
    login_url = _enforce_https(login_url, "login_url")
    return {
        "method":               "form_login",
        "login_url":            login_url,
        "username":             payload.get("username", "").strip(),
        "password":             payload.get("password", ""),
        # Leave empty → auto-detected by auth_manager at login time
        "username_selector":    payload.get("username_selector", "").strip(),
        "password_selector":    payload.get("password_selector", "").strip(),
        "submit_selector":      payload.get("submit_selector", "").strip(),
        "success_url_fragment": payload.get("success_url_fragment", "").strip(),
        "session_ttl":          int(payload.get("session_ttl") or 3600),
    }


# ── Public CRUD ───────────────────────────────────────────────────────────────

async def register_source(redis, payload: dict) -> dict:
    source         = _build_source_record(payload)
    secret_payload = _build_secret_payload(payload, source["base_url"])
    if source["auth_required"]:
        await _save_secret(f"auth-{source['auth_id']}", secret_payload)
    await redis.hset(HASH_SOURCES, source["auth_id"], json.dumps(source))
    logger.info(f"[source_registry] Registered '{source['name']}' (auth_id={source['auth_id']})")
    return source


async def list_sources(redis) -> list[dict]:
    raw     = await redis.hgetall(HASH_SOURCES)
    sources = []
    for value in raw.values():
        try:
            sources.append(json.loads(value))
        except Exception:
            continue
    return sorted(sources, key=lambda s: s.get("name", "").lower())


async def get_source(redis, auth_id: str) -> Optional[dict]:
    raw = await redis.hget(HASH_SOURCES, auth_id)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


async def match_source_for_url(redis, url: str) -> Optional[dict]:
    normalized = normalize_url(url)
    domain     = urlparse(normalized).netloc.lower()
    if not domain:
        return None
    best_match = None
    for source in await list_sources(redis):
        sd = (source.get("domain") or "").lower()
        if sd and (domain == sd or domain.endswith("." + sd)):
            if best_match is None or len(sd) > len(best_match.get("domain", "")):
                best_match = source
    return best_match