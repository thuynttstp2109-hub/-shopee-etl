# Databricks notebook source
# MAGIC %md
# MAGIC # Cấu hình chung
# MAGIC Các notebook khác nạp file này bằng `%run ./_config`. Đổi `CATALOG` nếu workspace của bạn không dùng catalog `workspace` (Free Edition mặc định là `workspace`).

# COMMAND ----------

import os
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

CATALOG = "workspace"
SCHEMA = "shopee"
VOLUME = "lake"
LAKE_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"  # data lake: thay cho MinIO
LOCAL_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def T(name: str) -> str:
    """Tên bảng đầy đủ: catalog.schema.table"""
    return f"{CATALOG}.{SCHEMA}.{name}"


# Dùng lại code Python của project (thư mục src/) để không phải viết lại phần gọi API.
# Notebook nằm trong shopee-etl/databricks/, nên src/ ở thư mục cha.
PROJECT_ROOT = Path(os.getcwd()).parent
if not (PROJECT_ROOT / "src" / "shopee_etl").exists():
    raise RuntimeError(
        f"Không thấy {PROJECT_ROOT}/src/shopee_etl. Hãy đưa cả thư mục shopee-etl lên workspace "
        "(Git folder hoặc databricks bundle deploy), đừng chỉ tải riêng các notebook."
    )
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))


def resolve_snapshot_date(value: str) -> date:
    """Ô trống = hôm nay theo giờ Việt Nam."""
    value = (value or "").strip()
    return date.fromisoformat(value) if value else datetime.now(LOCAL_TZ).date()


def raw_dir(d: date) -> str:
    return f"{LAKE_ROOT}/raw/product_offers/snapshot_date={d.isoformat()}"


# Cấu trúc một sản phẩm trong productOfferV2. Khai báo sẵn thay vì để Spark tự đoán:
# đọc nhanh hơn và không bị đổi kiểu khi một ngày nào đó API trả thiếu trường.
NODE_SCHEMA = (
    "itemId BIGINT, productName STRING, price STRING, priceMin STRING, priceMax STRING, "
    "priceDiscountRate INT, sales BIGINT, ratingStar STRING, commissionRate STRING, "
    "shopId BIGINT, shopName STRING, shopType ARRAY<INT>, productCatIds ARRAY<BIGINT>, "
    "imageUrl STRING, productLink STRING, offerLink STRING"
)
RAW_FILE_SCHEMA = (
    f"source STRING, keyword STRING, snapshot_date STRING, extracted_at STRING, "
    f"nodes ARRAY<STRUCT<{NODE_SCHEMA}>>"
)
