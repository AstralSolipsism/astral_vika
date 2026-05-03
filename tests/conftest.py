from __future__ import annotations

import os
import re
import asyncio
import logging
from typing import List, Optional, Tuple
import pytest

logger = logging.getLogger(__name__)

# Robust regex for IDs in workbench URL
DST_RE = re.compile(r"(dst[0-9A-Za-z]+)")
VIW_RE = re.compile(r"(viw[0-9A-Za-z]+)")


def _extract_ids(url: str) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(url, str):
        return None, None
    dst: Optional[str] = None
    viw: Optional[str] = None
    m1 = DST_RE.search(url)
    if m1:
        dst = m1.group(1)
    m2 = VIW_RE.search(url)
    if m2:
        viw = m2.group(1)
    return dst, viw


@pytest.fixture(scope="session")
def api_token():
    token = os.getenv("VIKA_API_TOKEN")
    if not token:
        pytest.skip("VIKA_API_TOKEN is not set; skipping Vika integration tests.")
    return token


@pytest.fixture(scope="session")
def workbench_url():
    url = os.getenv("VIKA_WORKBENCH_URL")
    if not url:
        pytest.skip("VIKA_WORKBENCH_URL is not set; skipping Vika integration tests.")
    return url


@pytest.fixture(scope="session")
def datasheet_id(workbench_url: str):
    dst, _ = _extract_ids(workbench_url)
    if not dst:
        pytest.skip("Failed to parse datasheetId (dst...) from VIKA_WORKBENCH_URL.")
    return dst


@pytest.fixture(scope="session")
def view_id(workbench_url: str) -> Optional[str]:
    _, viw = _extract_ids(workbench_url)
    # View may be absent; tests that require it will skip conditionally.
    return viw


@pytest.fixture(scope="session")
def created_record_ids(api_token: str, datasheet_id: str):
    """
    Session-scoped accumulator of created record IDs.
    On teardown, bulk-delete residual records to avoid pollution.
    """
    ids: List[str] = []
    yield ids

    if not ids:
        return

    # Deduplicate while preserving order
    unique_ids = list(dict.fromkeys(ids))
    logger.info("Teardown: deleting %d temporary records", len(unique_ids))

    async def _cleanup():
        # Import here to avoid side effects during test collection
        from astral_vika import Vika
        vika = Vika(api_token)
        try:
            ds = vika.datasheet(datasheet_id)
            try:
                await ds.records.adelete(unique_ids)
                logger.info("Teardown: deleted records: %s", ",".join(unique_ids))
            except Exception as e:
                logger.warning("Teardown deletion failed for some records: %s", e)
        finally:
            try:
                await vika.aclose()
            except Exception:
                pass

    try:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_cleanup())
        finally:
            loop.close()
    except Exception as e:
        logger.warning("Teardown: event loop cleanup failed: %s", e)
