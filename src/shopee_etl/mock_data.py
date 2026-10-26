"""Sinh dữ liệu giả có cùng cấu trúc với productOfferV2.

Dùng để chạy thử toàn bộ pipeline trong lúc chờ Shopee duyệt tài khoản affiliate.
Dữ liệu đổi nhẹ theo ngày để bạn có thể thử phân tích biến động giá.
"""
from __future__ import annotations

import random
import zlib
from datetime import date
from typing import Any, Iterator

_EPOCH = date(2026, 1, 1)  # mốc để tính lượt bán tích lũy

_SHOPS = [
    (100001, "Tech Zone Official", [1]),
    (100002, "Phu Kien Gia Re", []),
    (100003, "Audio Pro Store", [2]),
    (100004, "Gear Hub VN", []),
]


def iter_mock_offers(
    keyword: str, snapshot_date: date, limit: int = 50, max_pages: int = 2
) -> Iterator[dict[str, Any]]:
    base_rng = random.Random(keyword)  # cố định danh sách sản phẩm theo từ khóa
    day_rng = random.Random(f"{keyword}-{snapshot_date.isoformat()}")  # biến động theo ngày

    total = limit * max_pages
    for page in range(1, max_pages + 1):
        nodes = []
        for i in range((page - 1) * limit, min(page * limit, total)):
            shop_id, shop_name, shop_type = base_rng.choice(_SHOPS)
            base_price = base_rng.randrange(99_000, 2_500_000, 1_000)
            price = int(base_price * day_rng.uniform(0.85, 1.05)) // 1000 * 1000
            item_id = 20_000_000_000 + zlib.crc32(keyword.encode()) % 1_000_000 * 1000 + i
            nodes.append(
                {
                    "itemId": item_id,
                    "productName": f"{keyword.title()} mẫu {i + 1}",
                    "price": str(price),
                    "priceMin": str(price),
                    "priceMax": str(int(price * 1.1)),
                    "priceDiscountRate": base_rng.randint(0, 40),
                    # Lượt bán tích lũy: chỉ tăng theo thời gian, như dữ liệu thật
                    "sales": base_rng.randint(0, 20_000)
                    + base_rng.randint(0, 60) * max(0, (snapshot_date - _EPOCH).days),
                    "ratingStar": f"{base_rng.uniform(3.5, 5.0):.1f}",
                    "commissionRate": f"{base_rng.choice([0.02, 0.03, 0.05, 0.08]):.2f}",
                    "shopId": shop_id,
                    "shopName": shop_name,
                    "shopType": shop_type,
                    "productCatIds": [100535, 100636, base_rng.choice([101550, 101551, 101552])],
                    "imageUrl": "https://example.com/image.jpg",
                    "productLink": f"https://shopee.vn/product/{shop_id}/{item_id}",
                    "offerLink": f"https://s.shopee.vn/mock{item_id}",
                }
            )
        yield {
            "nodes": nodes,
            "pageInfo": {"page": page, "limit": limit, "hasNextPage": page < max_pages},
        }
