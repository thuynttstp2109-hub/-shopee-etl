# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Extract → Bronze
# MAGIC 1. Lấy dữ liệu và lưu JSON gốc vào volume `raw/product_offers/snapshot_date=.../`
# MAGIC 2. Đọc JSON bằng Spark, tách mỗi sản phẩm thành một dòng, ghi vào bảng `bronze_product_offers`
# MAGIC
# MAGIC **Chế độ (`mode`):**
# MAGIC - `mock`: dữ liệu giả lập, chạy được ngay
# MAGIC - `api`: gọi Shopee Affiliate API (cần secret, xem README; Free Edition có thể chặn kết nối ra ngoài)
# MAGIC - `files`: không gọi gì, chỉ nạp các file JSON bạn đã tải lên volume cho ngày đó

# COMMAND ----------

dbutils.widgets.text("snapshot_date", "", "Ngày (YYYY-MM-DD, trống = hôm nay)")
dbutils.widgets.dropdown("mode", "mock", ["mock", "api", "files"], "Chế độ")
dbutils.widgets.text("keywords", "tai nghe bluetooth,ban phim co,chuot gaming", "Từ khóa")

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import logging
import shutil

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", force=True)

snapshot = resolve_snapshot_date(dbutils.widgets.get("snapshot_date"))
mode = dbutils.widgets.get("mode")
keywords = [k.strip() for k in dbutils.widgets.get("keywords").split(",") if k.strip()]
print(f"Ngày: {snapshot} | Chế độ: {mode} | Từ khóa: {keywords}")

# COMMAND ----------

# MAGIC %md ## 1. Extract: lưu JSON gốc vào volume

# COMMAND ----------

from shopee_etl.config import Settings
from shopee_etl.extract import run_extract
from shopee_etl.storage import LakeStorage

if mode == "files":
    print("Bỏ qua bước gọi API, dùng file đã có trong", raw_dir(snapshot))
else:
    if mode == "api":
        app_id = dbutils.secrets.get("shopee", "app_id")
        app_secret = dbutils.secrets.get("shopee", "app_secret")
    else:
        app_id = app_secret = ""

    settings = Settings(
        mock=(mode == "mock"),
        app_id=app_id,
        app_secret=app_secret,
        keywords=keywords,
        storage_backend="local",  # volume được gắn như thư mục thường
        local_root=LAKE_ROOT,
    )
    # Xóa file cũ của ngày này để chạy lại không bị lẫn dữ liệu cũ
    shutil.rmtree(raw_dir(snapshot), ignore_errors=True)
    n = run_extract(settings, LakeStorage(settings), snapshot)
    print(f"Đã lưu {n} sản phẩm")

# COMMAND ----------

# MAGIC %md ## 2. Bronze: JSON → bảng Delta

# COMMAND ----------

from pyspark.sql import functions as F

if not os.path.isdir(raw_dir(snapshot)):
    raise FileNotFoundError(
        f"Chưa có dữ liệu ở {raw_dir(snapshot)}. Ở chế độ files, hãy tải thư mục JSON của ngày này lên volume trước."
    )

raw = (
    spark.read.schema(RAW_FILE_SCHEMA)
    # Tắt tự nhận thư mục keyword=... thành cột, vì file JSON đã có trường keyword
    .option("recursiveFileLookup", "true")
    .json(raw_dir(snapshot))
)

bronze = raw.select(
    F.lit(snapshot).cast("date").alias("snapshot_date"),
    "keyword",
    "source",
    F.to_timestamp("extracted_at").alias("extracted_at"),
    F.col("_metadata.file_path").alias("source_file"),
    F.current_timestamp().alias("ingested_at"),
    F.explode("nodes").alias("node"),
)

count = bronze.count()
if count == 0:
    raise ValueError(f"Không có sản phẩm nào trong {raw_dir(snapshot)}")

# Chỉ ghi đè đúng ngày này: chạy lại bao nhiêu lần cũng không sinh dòng trùng
(
    bronze.write.mode("overwrite")
    .option("replaceWhere", f"snapshot_date = '{snapshot}'")
    .saveAsTable(T("bronze_product_offers"))
)
print(f"Bronze: {count} dòng cho ngày {snapshot}")

# COMMAND ----------

display(spark.table(T("bronze_product_offers")).where(F.col("snapshot_date") == snapshot).limit(5))

# COMMAND ----------

# Chuyển ngày cho các task sau trong Lakeflow Job (chạy tay thì lệnh này không làm gì)
try:
    dbutils.jobs.taskValues.set(key="snapshot_date", value=snapshot.isoformat())
except Exception:
    pass  # không chạy trong job (chạy tay hoặc được notebook khác gọi)
