"""LOAD: Parquet ở vùng processed -> star schema trong PostgreSQL.

Dùng UPSERT (INSERT ... ON CONFLICT) nên chạy lại cùng một ngày nhiều lần
vẫn cho kết quả giống nhau (idempotent), không sinh dòng trùng.
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from .storage import LakeStorage
from .transform import processed_key

log = logging.getLogger(__name__)

UPSERT_DATE = """
INSERT INTO dim_date (date_key, year, month, day, day_of_week, is_weekend)
VALUES ({p}, {p}, {p}, {p}, {p}, {p})
ON CONFLICT (date_key) DO NOTHING
"""

UPSERT_SHOP = """
INSERT INTO dim_shop (shop_id, shop_name, shop_type, first_seen, last_seen)
VALUES ({p}, {p}, {p}, {p}, {p})
ON CONFLICT (shop_id) DO UPDATE SET
    shop_name = excluded.shop_name,
    shop_type = excluded.shop_type,
    last_seen = excluded.last_seen
"""

UPSERT_PRODUCT = """
INSERT INTO dim_product (item_id, product_name, shop_id, category_id, category_path,
                         image_url, product_link, offer_link, first_seen, last_seen)
VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
ON CONFLICT (item_id) DO UPDATE SET
    product_name  = excluded.product_name,
    category_id   = excluded.category_id,
    category_path = excluded.category_path,
    image_url     = excluded.image_url,
    product_link  = excluded.product_link,
    offer_link    = excluded.offer_link,
    last_seen     = excluded.last_seen
"""

UPSERT_FACT = """
INSERT INTO fact_product_daily (snapshot_date, item_id, shop_id, keyword, price, price_min,
                                price_max, discount_rate, sales, rating_star, commission_rate)
VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
ON CONFLICT (snapshot_date, item_id) DO UPDATE SET
    keyword         = excluded.keyword,
    price           = excluded.price,
    price_min       = excluded.price_min,
    price_max       = excluded.price_max,
    discount_rate   = excluded.discount_rate,
    sales           = excluded.sales,
    rating_star     = excluded.rating_star,
    commission_rate = excluded.commission_rate
"""


def _py(v):
    """Đổi kiểu pandas/numpy sang kiểu Python thuần để driver DB hiểu được."""
    if v is None or (not isinstance(v, (list, str)) and pd.isna(v)):
        return None
    return v.item() if hasattr(v, "item") else v


def _rows(df: pd.DataFrame, cols: list[str]) -> list[tuple]:
    return [tuple(_py(v) for v in row) for row in df[cols].itertuples(index=False, name=None)]


def load_dataframe(conn, df: pd.DataFrame, snapshot_date: date, placeholder: str = "%s") -> int:
    """Ghi một DataFrame đã transform vào kho. `conn` là kết nối DB-API bất kỳ."""
    p = placeholder
    d = snapshot_date
    shops = df.drop_duplicates("shop_id").assign(first_seen=d, last_seen=d)
    products = df.drop_duplicates("item_id").assign(first_seen=d, last_seen=d)

    with conn:  # một transaction: lỗi giữa chừng thì rollback toàn bộ
        cur = conn.cursor()
        cur.execute(UPSERT_DATE.format(p=p),
                    (d, d.year, d.month, d.day, d.isoweekday(), d.isoweekday() >= 6))
        cur.executemany(UPSERT_SHOP.format(p=p),
                        _rows(shops, ["shop_id", "shop_name", "shop_type", "first_seen", "last_seen"]))
        cur.executemany(UPSERT_PRODUCT.format(p=p), _rows(products, [
            "item_id", "product_name", "shop_id", "category_id", "category_path",
            "image_url", "product_link", "offer_link", "first_seen", "last_seen"]))
        cur.executemany(UPSERT_FACT.format(p=p), _rows(df, [
            "snapshot_date", "item_id", "shop_id", "keyword", "price", "price_min",
            "price_max", "discount_rate", "sales", "rating_star", "commission_rate"]))
    log.info("Load xong: %d shop, %d sản phẩm, %d dòng fact", len(shops), len(products), len(df))
    return len(df)


def run_load(storage: LakeStorage, dw_dsn: str, snapshot_date: date) -> int:
    import psycopg2

    df = storage.get_parquet(processed_key(snapshot_date))
    conn = psycopg2.connect(dw_dsn)
    try:
        return load_dataframe(conn, df, snapshot_date)
    finally:
        conn.close()
