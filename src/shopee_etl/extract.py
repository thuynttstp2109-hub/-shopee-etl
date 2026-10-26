"""EXTRACT: gọi API (hoặc mock) và lưu nguyên bản JSON vào vùng raw của data lake.

Đường dẫn: raw/product_offers/snapshot_date=YYYY-MM-DD/keyword=<slug>/page=NNN.json
Giữ nguyên dữ liệu gốc để có thể chạy lại transform bất cứ lúc nào.
"""
from __future__ import annotations

import json
import logging
import re
import unicodedata
from datetime import date, datetime, timezone

from .config import Settings
from .mock_data import iter_mock_offers
from .shopee_client import ShopeeAffiliateClient
from .storage import LakeStorage

log = logging.getLogger(__name__)


def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).replace("đ", "d").replace("Đ", "D")
    text = text.encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def raw_prefix(snapshot_date: date) -> str:
    return f"raw/product_offers/snapshot_date={snapshot_date.isoformat()}/"


def run_extract(settings: Settings, storage: LakeStorage, snapshot_date: date) -> int:
    use_mock = settings.mock or not (settings.app_id and settings.app_secret)
    if use_mock:
        log.warning("Đang chạy chế độ MOCK (dữ liệu giả lập)")
    else:
        client = ShopeeAffiliateClient(
            settings.app_id, settings.app_secret, settings.api_url, settings.request_delay
        )

    total = 0
    for keyword in settings.keywords:
        pages = (
            iter_mock_offers(keyword, snapshot_date, settings.page_limit, min(settings.max_pages, 2))
            if use_mock
            else client.iter_product_offers(
                keyword, settings.sort_type, settings.page_limit, settings.max_pages
            )
        )
        for result in pages:
            page_no = result["pageInfo"]["page"]
            record = {
                "source": "mock" if use_mock else "shopee_affiliate_api",
                "keyword": keyword,
                "snapshot_date": snapshot_date.isoformat(),
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                "page_info": result["pageInfo"],
                "nodes": result["nodes"],
            }
            key = f"{raw_prefix(snapshot_date)}keyword={slugify(keyword)}/page={page_no:03d}.json"
            storage.put_bytes(key, json.dumps(record, ensure_ascii=False).encode("utf-8"))
            total += len(result["nodes"])

    log.info("Extract xong: %d bản ghi cho ngày %s", total, snapshot_date)
    return total
