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
# Không dựa vào os.getcwd(): trên serverless, notebook được gọi bằng dbutils.notebook.run
# có thể có thư mục làm việc khác (ví dụ .conversations). Thử lần lượt nhiều chỗ.
WORKSPACE_ROOT = Path("/Workspace")


def _find_project_root() -> Path:
    candidates = []
    try:  # 1. Từ đường dẫn của chính notebook đang chạy
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        nb_dir = Path(nb if nb.startswith(str(WORKSPACE_ROOT)) else f"{WORKSPACE_ROOT}{nb}").parent
        candidates += [nb_dir.parent, nb_dir]
    except Exception:
        pass
    cwd = Path(os.getcwd())  # 2. Từ thư mục làm việc
    candidates += [cwd.parent, cwd]
    for c in candidates:
        if (c / "src" / "shopee_etl").is_dir():
            return c
    try:  # 3. Tìm trong thư mục cá nhân (Git folder hoặc bundle deploy)
        user = spark.sql("SELECT current_user()").first()[0]
        home = WORKSPACE_ROOT / "Users" / user
        for pattern in ("*/src/shopee_etl", ".bundle/*/*/files/src/shopee_etl"):
            hits = sorted(p for p in home.glob(pattern) if p.is_dir())
            if hits:
                return hits[0].parent.parent
    except Exception:
        pass
    raise RuntimeError(
        "Không tìm thấy thư mục src/shopee_etl. Hãy đưa cả project lên workspace "
        "(Git folder có đủ databricks/, src/, sql/), đừng chỉ tải riêng các notebook."
    )


PROJECT_ROOT = _find_project_root()
print("Project:", PROJECT_ROOT)
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
