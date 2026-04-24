"""
Azure Blob Storage client — FINAL VERSION.

Single module-level BlobServiceClient, created once and reused.
Uses ContentSettings object (not dict) — fixes cache_control AttributeError.
"""
import os
from datetime import datetime, timezone, timedelta
from loguru import logger
from azure.storage.blob import (
    BlobServiceClient,
    generate_blob_sas,
    BlobSasPermissions,
    ContentSettings,
)

CONN_STR    = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER   = os.getenv("AZURE_BLOB_CONTAINER", "tender-results")
ACCOUNT     = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

# Single client reused across all calls — no new HTTP connections per write
_service_client: BlobServiceClient = None


def _get_service() -> BlobServiceClient:
    global _service_client
    if _service_client is None and CONN_STR:
        _service_client = BlobServiceClient.from_connection_string(CONN_STR)
    return _service_client


def _ensure_container():
    try:
        svc = _get_service()
        if svc:
            svc.get_container_client(CONTAINER).create_container()
    except Exception:
        pass  # already exists


def upload_blob(blob_name: str, data: bytes, content_type: str = "application/x-ndjson") -> str:
    svc = _get_service()
    if not svc:
        return blob_name
    _ensure_container()
    blob = svc.get_blob_client(CONTAINER, blob_name)
    blob.upload_blob(
        data,
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )
    return blob_name


def read_blob(blob_name: str) -> str:
    """Download and return blob text. Returns empty string if not found."""
    try:
        svc = _get_service()
        if not svc:
            return ""
        blob = svc.get_blob_client(CONTAINER, blob_name)
        return blob.download_blob().readall().decode("utf-8")
    except Exception:
        return ""


def generate_sas_url(blob_name: str, expires_minutes: int = 10) -> str:
    expiry = datetime.now(timezone.utc) + timedelta(minutes=expires_minutes)
    sas = generate_blob_sas(
        account_name=ACCOUNT,
        container_name=CONTAINER,
        blob_name=blob_name,
        account_key=ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=expiry,
    )
    return f"https://{ACCOUNT}.blob.core.windows.net/{CONTAINER}/{blob_name}?{sas}"


def delete_all_blobs() -> int:
    """
    Delete all blobs in the container using batch delete.
    Much faster than deleting one-by-one — single HTTP request for up to 256 blobs.
    """
    deleted = 0
    try:
        svc = _get_service()
        if not svc:
            return 0
        container = svc.get_container_client(CONTAINER)
        # Collect all blob names first
        blob_names = [b.name for b in container.list_blobs()]
        if not blob_names:
            return 0
        # Batch delete in chunks of 256 (Azure limit per batch)
        chunk_size = 256
        for i in range(0, len(blob_names), chunk_size):
            chunk = blob_names[i:i + chunk_size]
            try:
                container.delete_blobs(*chunk)
                deleted += len(chunk)
            except Exception:
                # Fallback: delete one by one if batch fails
                for name in chunk:
                    try:
                        container.delete_blob(name)
                        deleted += 1
                    except Exception:
                        pass
    except Exception as e:
        logger.warning(f"delete_all_blobs error: {e}")
    return deleted