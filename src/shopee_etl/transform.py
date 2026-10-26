"""TRANSFORM: raw JSON -> bảng sạch dạng Parquet ở vùng processed.

Các bước:
  1. Làm phẳng các trang JSON thành một DataFrame
  2. Ép kiểu (giá, rating, hoa hồng đang là chuỗi -> số)
  3. Chuẩn hóa: tên sản phẩm, danh mục lá, loại shop
  4. Bỏ trùng (một sản phẩm có thể xuất hiện ở nhiều từ khóa)
  5. Kiểm tra chất lượng dữ liệu, dừng pipeline nếu dữ liệu hỏng
"""
from __future__ import annotations

import json
import logging
from datetime import date

import pandas as pd

from .extract import raw_prefix
from .storage import LakeStorage

log = logging.getLogger(__name__)

# shopType trong API: 1 = Shopee Mall, 2 = Preferred (Yêu thích), 4 = Preferred+
SHOP_TYPE_LABELS = {1: "mall", 2: "preferred", 4: "preferred_plus"}

OUTPUT_COLUMNS = [
    "snapshot_date", "item_id", "product_name", "shop_id", "shop_name", "shop_type",
    "category_id", "category_path", "price", "price_min", "price_max",
    "discount_rate", "sales", "rating_star", "commission_rate",
    "image_url", "product_link", "offer_link", "keyword", "source",
]


class DataQualityError(RuntimeError):
    pass


def processed_key(snapshot_date: date) -> str:
    return f"processed/product_snapshot/snapshot_date={snapshot_date.isoformat()}/data.parquet"


def flatten_raw(records: list[dict]) -> pd.DataFrame:
    rows = []
    for rec in records:
        for node in rec["nodes"]:
            rows.append({**node, "keyword": rec["keyword"],
                         "snapshot_date": rec["snapshot_date"], "source": rec["source"]})
    return pd.DataFrame(rows)


def _shop_type_label(value) -> str:
    if not isinstance(value, (list, tuple)) or len(value) == 0:
        return "normal"
    return SHOP_TYPE_LABELS.get(int(value[0]), "normal")


def clean(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    out = pd.DataFrame()
    out["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    out["item_id"] = pd.to_numeric(df["itemId"], errors="coerce").astype("Int64")
    out["product_name"] = df["productName"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    out["shop_id"] = pd.to_numeric(df["shopId"], errors="coerce").astype("Int64")
    out["shop_name"] = df["shopName"].astype(str).str.strip()
    out["shop_type"] = df["shopType"].apply(_shop_type_label)

    cats = df["productCatIds"].apply(lambda v: list(v) if isinstance(v, (list, tuple)) else [])
    out["category_id"] = cats.apply(lambda v: int(v[-1]) if v else None).astype("Int64")
    out["category_path"] = cats.apply(lambda v: "/".join(str(c) for c in v))

    for src, dst in [("price", "price"), ("priceMin", "price_min"), ("priceMax", "price_max")]:
        out[dst] = pd.to_numeric(df[src], errors="coerce").astype(float)
    out["discount_rate"] = pd.to_numeric(df["priceDiscountRate"], errors="coerce").fillna(0).astype(int)
    out["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0).astype(int)
    out["rating_star"] = pd.to_numeric(df["ratingStar"], errors="coerce").round(2)
    out["commission_rate"] = pd.to_numeric(df["commissionRate"], errors="coerce")

    out["image_url"] = df["imageUrl"]
    out["product_link"] = df["productLink"]
    out["offer_link"] = df["offerLink"]
    out["keyword"] = df["keyword"]
    out["source"] = df["source"]

    before = len(out)
    out = out.dropna(subset=["item_id", "price"])
    out = out.drop_duplicates(subset=["snapshot_date", "item_id"], keep="first")
    log.info("Transform: %d dòng thô -> %d dòng sạch", before, len(out))
    return out[OUTPUT_COLUMNS].reset_index(drop=True)


def check_quality(df: pd.DataFrame, min_rows: int = 1) -> None:
    problems = []
    if len(df) < min_rows:
        problems.append(f"chỉ có {len(df)} dòng (tối thiểu {min_rows})")
    if df["item_id"].duplicated().any():
        problems.append("item_id bị trùng")
    if (df["price"] <= 0).any():
        problems.append("có giá <= 0")
    if not df["rating_star"].dropna().between(0, 5).all():
        problems.append("rating nằm ngoài khoảng 0-5")
    if not df["discount_rate"].between(0, 100).all():
        problems.append("tỷ lệ giảm giá nằm ngoài khoảng 0-100")
    if problems:
        raise DataQualityError("; ".join(problems))


def run_transform(storage: LakeStorage, snapshot_date: date) -> int:
    keys = [k for k in storage.list_keys(raw_prefix(snapshot_date)) if k.endswith(".json")]
    if not keys:
        raise DataQualityError(f"Không có dữ liệu raw cho ngày {snapshot_date}")

    records = [json.loads(storage.get_bytes(k)) for k in keys]
    df = clean(flatten_raw(records))
    check_quality(df)
    location = storage.put_parquet(processed_key(snapshot_date), df)
    log.info("Đã ghi %d dòng vào %s", len(df), location)
    return len(df)
