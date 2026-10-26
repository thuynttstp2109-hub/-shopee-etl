"""Client cho Shopee Affiliate Open API (GraphQL).

Xác thực: mỗi request ký bằng SHA256(app_id + timestamp + payload + secret),
trong đó payload là đúng chuỗi JSON gửi trong body. Header có dạng:
    Authorization: SHA256 Credential={app_id}, Timestamp={ts}, Signature={sig}
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Iterator

import requests

log = logging.getLogger(__name__)

PRODUCT_FIELDS = """
    itemId
    productName
    price
    priceMin
    priceMax
    priceDiscountRate
    sales
    ratingStar
    commissionRate
    shopId
    shopName
    shopType
    productCatIds
    imageUrl
    productLink
    offerLink
"""

PRODUCT_OFFER_QUERY = (
    "query ($keyword: String, $sortType: Int, $page: Int, $limit: Int) {"
    "  productOfferV2(keyword: $keyword, sortType: $sortType, page: $page, limit: $limit) {"
    "    nodes {" + " ".join(PRODUCT_FIELDS.split()) + "}"
    "    pageInfo { page limit hasNextPage }"
    "  }"
    "}"
)


class ShopeeAPIError(RuntimeError):
    pass


def build_signature(app_id: str, timestamp: int, payload: str, secret: str) -> str:
    raw = f"{app_id}{timestamp}{payload}{secret}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class ShopeeAffiliateClient:
    def __init__(
        self,
        app_id: str,
        app_secret: str,
        api_url: str,
        request_delay: float = 1.5,
        max_retries: int = 4,
        timeout: int = 30,
    ):
        if not app_id or not app_secret:
            raise ValueError("Thiếu SHOPEE_APP_ID hoặc SHOPEE_APP_SECRET")
        self.app_id = app_id
        self.app_secret = app_secret
        self.api_url = api_url
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()

    def _post(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        # separators gọn để chuỗi ký và chuỗi gửi đi giống hệt nhau
        payload = json.dumps({"query": query, "variables": variables}, separators=(",", ":"))

        for attempt in range(1, self.max_retries + 1):
            ts = int(time.time())
            sig = build_signature(self.app_id, ts, payload, self.app_secret)
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"SHA256 Credential={self.app_id}, Timestamp={ts}, Signature={sig}",
            }
            try:
                resp = self.session.post(
                    self.api_url, data=payload.encode("utf-8"), headers=headers, timeout=self.timeout
                )
            except requests.RequestException as exc:
                self._backoff(attempt, f"lỗi mạng: {exc}")
                continue

            if resp.status_code == 429 or resp.status_code >= 500:
                self._backoff(attempt, f"HTTP {resp.status_code}")
                continue
            resp.raise_for_status()

            body = resp.json()
            if body.get("errors"):
                err = body["errors"][0]
                code = (err.get("extensions") or {}).get("code")
                # Lỗi giới hạn tần suất thì thử lại, lỗi khác (sai key, sai query) thì dừng
                if "limit" in str(err.get("message", "")).lower():
                    self._backoff(attempt, f"rate limit ({code})")
                    continue
                raise ShopeeAPIError(f"Shopee API lỗi {code}: {err.get('message')}")
            return body["data"]

        raise ShopeeAPIError(f"Thất bại sau {self.max_retries} lần thử")

    def _backoff(self, attempt: int, reason: str) -> None:
        wait = min(60, 2 ** attempt)
        log.warning("Request lỗi (%s), thử lại lần %d sau %ds", reason, attempt, wait)
        time.sleep(wait)

    def iter_product_offers(
        self, keyword: str, sort_type: int = 2, limit: int = 50, max_pages: int = 5
    ) -> Iterator[dict[str, Any]]:
        """Trả về từng trang kết quả productOfferV2 cho một từ khóa."""
        for page in range(1, max_pages + 1):
            data = self._post(
                PRODUCT_OFFER_QUERY,
                {"keyword": keyword, "sortType": sort_type, "page": page, "limit": limit},
            )
            result = data["productOfferV2"]
            log.info("keyword=%r page=%d -> %d sản phẩm", keyword, page, len(result["nodes"]))
            yield result
            if not result["pageInfo"].get("hasNextPage"):
                break
            time.sleep(self.request_delay)
