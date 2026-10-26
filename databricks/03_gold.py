# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver → Gold
# MAGIC - Bảng dim cập nhật bằng **MERGE** (có thì sửa, chưa có thì thêm) và lưu ngày xuất hiện đầu/cuối
# MAGIC - Bảng fact ghi đè đúng ngày đang chạy bằng **replaceWhere**
# MAGIC
# MAGIC Cả hai cách đều idempotent: chạy lại một ngày bao nhiêu lần cũng cho cùng kết quả.

# COMMAND ----------

dbutils.widgets.text("snapshot_date", "", "Ngày (YYYY-MM-DD, trống = hôm nay)")

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

snapshot = resolve_snapshot_date(dbutils.widgets.get("snapshot_date"))
d = snapshot.isoformat()

src = spark.table(T("silver_product_snapshot")).where(F.col("snapshot_date") == snapshot)
if src.isEmpty():
    raise ValueError(f"Silver chưa có dữ liệu ngày {d}. Hãy chạy 01 và 02 trước.")
src.createOrReplaceTempView("src")

# COMMAND ----------

# MAGIC %md ## dim_date

# COMMAND ----------

spark.sql(f"""
MERGE INTO {T('dim_date')} t
USING (SELECT DATE'{d}' AS date_key) s
ON t.date_key = s.date_key
WHEN NOT MATCHED THEN INSERT (date_key, year, month, day, day_of_week, is_weekend)
VALUES (s.date_key, year(s.date_key), month(s.date_key), dayofmonth(s.date_key),
        weekday(s.date_key) + 1, weekday(s.date_key) >= 5)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_shop và dim_product
# MAGIC Tên và thông tin chỉ được cập nhật khi ngày đang nạp mới hơn hoặc bằng `last_seen`,
# MAGIC để chạy lại một ngày cũ không ghi đè thông tin mới hơn.

# COMMAND ----------

spark.sql(f"""
MERGE INTO {T('dim_shop')} t
USING (
    SELECT shop_id,
           max_by(shop_name, sales) AS shop_name,
           max_by(shop_type, sales) AS shop_type
    FROM src GROUP BY shop_id
) s
ON t.shop_id = s.shop_id
WHEN MATCHED THEN UPDATE SET
    t.shop_name  = CASE WHEN DATE'{d}' >= t.last_seen THEN s.shop_name ELSE t.shop_name END,
    t.shop_type  = CASE WHEN DATE'{d}' >= t.last_seen THEN s.shop_type ELSE t.shop_type END,
    t.first_seen = least(t.first_seen, DATE'{d}'),
    t.last_seen  = greatest(t.last_seen, DATE'{d}')
WHEN NOT MATCHED THEN INSERT (shop_id, shop_name, shop_type, first_seen, last_seen)
VALUES (s.shop_id, s.shop_name, s.shop_type, DATE'{d}', DATE'{d}')
""")

spark.sql(f"""
MERGE INTO {T('dim_product')} t
USING src s
ON t.item_id = s.item_id
WHEN MATCHED THEN UPDATE SET
    t.product_name  = CASE WHEN DATE'{d}' >= t.last_seen THEN s.product_name  ELSE t.product_name  END,
    t.shop_id       = CASE WHEN DATE'{d}' >= t.last_seen THEN s.shop_id       ELSE t.shop_id       END,
    t.category_id   = CASE WHEN DATE'{d}' >= t.last_seen THEN s.category_id   ELSE t.category_id   END,
    t.category_path = CASE WHEN DATE'{d}' >= t.last_seen THEN s.category_path ELSE t.category_path END,
    t.image_url     = CASE WHEN DATE'{d}' >= t.last_seen THEN s.image_url     ELSE t.image_url     END,
    t.product_link  = CASE WHEN DATE'{d}' >= t.last_seen THEN s.product_link  ELSE t.product_link  END,
    t.offer_link    = CASE WHEN DATE'{d}' >= t.last_seen THEN s.offer_link    ELSE t.offer_link    END,
    t.first_seen    = least(t.first_seen, DATE'{d}'),
    t.last_seen     = greatest(t.last_seen, DATE'{d}')
WHEN NOT MATCHED THEN INSERT (item_id, product_name, shop_id, category_id, category_path,
                              image_url, product_link, offer_link, first_seen, last_seen)
VALUES (s.item_id, s.product_name, s.shop_id, s.category_id, s.category_path,
        s.image_url, s.product_link, s.offer_link, DATE'{d}', DATE'{d}')
""")

# COMMAND ----------

# MAGIC %md ## fact_product_daily

# COMMAND ----------

fact = src.select(
    "snapshot_date", "item_id", "shop_id", "keyword", "price", "price_min", "price_max",
    "discount_rate", "sales", "rating_star", "commission_rate",
    F.current_timestamp().alias("loaded_at"),
)
(
    fact.write.mode("overwrite")
    .option("replaceWhere", f"snapshot_date = '{d}'")
    .saveAsTable(T("fact_product_daily"))
)

# COMMAND ----------

display(spark.sql(f"""
SELECT 'dim_date' AS bang, COUNT(*) AS so_dong FROM {T('dim_date')}
UNION ALL SELECT 'dim_shop', COUNT(*) FROM {T('dim_shop')}
UNION ALL SELECT 'dim_product', COUNT(*) FROM {T('dim_product')}
UNION ALL SELECT 'fact_product_daily', COUNT(*) FROM {T('fact_product_daily')}
"""))
