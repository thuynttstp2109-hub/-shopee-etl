# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Khởi tạo
# MAGIC Chạy **một lần** để tạo schema, volume (data lake) và các bảng Delta.
# MAGIC
# MAGIC | Tầng | Bảng | Nội dung |
# MAGIC |---|---|---|
# MAGIC | Bronze | `bronze_product_offers` | Dữ liệu gốc từ API, mỗi dòng một sản phẩm, giữ nguyên kiểu |
# MAGIC | Silver | `silver_product_snapshot` | Đã làm sạch, ép kiểu, bỏ trùng, kiểm tra chất lượng |
# MAGIC | Gold | `dim_date`, `dim_shop`, `dim_product`, `fact_product_daily`, `v_price_change` | Star schema để phân tích |

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

# MAGIC %md ## Bronze và Silver

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T('bronze_product_offers')} (
    snapshot_date DATE,
    keyword       STRING,
    source        STRING,
    extracted_at  TIMESTAMP,
    source_file   STRING,
    ingested_at   TIMESTAMP,
    node          STRUCT<{NODE_SCHEMA}>
)
CLUSTER BY (snapshot_date)
COMMENT 'Dữ liệu gốc productOfferV2, một dòng một sản phẩm'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T('silver_product_snapshot')} (
    snapshot_date   DATE,
    item_id         BIGINT,
    product_name    STRING,
    shop_id         BIGINT,
    shop_name       STRING,
    shop_type       STRING,
    category_id     BIGINT,
    category_path   STRING,
    price           DOUBLE,
    price_min       DOUBLE,
    price_max       DOUBLE,
    discount_rate   INT,
    sales           BIGINT,
    rating_star     DOUBLE,
    commission_rate DOUBLE,
    image_url       STRING,
    product_link    STRING,
    offer_link      STRING,
    keyword         STRING,
    source          STRING
)
CLUSTER BY (snapshot_date)
COMMENT 'Sản phẩm đã làm sạch, mỗi (ngày, sản phẩm) một dòng'
""")

# COMMAND ----------

# MAGIC %md ## Gold: star schema

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T('dim_date')} (
    date_key    DATE,
    year        INT,
    month       INT,
    day         INT,
    day_of_week INT,      -- 1 = Thứ Hai ... 7 = Chủ Nhật
    is_weekend  BOOLEAN
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T('dim_shop')} (
    shop_id    BIGINT,
    shop_name  STRING,
    shop_type  STRING,
    first_seen DATE,
    last_seen  DATE
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T('dim_product')} (
    item_id       BIGINT,
    product_name  STRING,
    shop_id       BIGINT,
    category_id   BIGINT,
    category_path STRING,
    image_url     STRING,
    product_link  STRING,
    offer_link    STRING,
    first_seen    DATE,
    last_seen     DATE
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T('fact_product_daily')} (
    snapshot_date   DATE,
    item_id         BIGINT,
    shop_id         BIGINT,
    keyword         STRING,
    price           DOUBLE,
    price_min       DOUBLE,
    price_max       DOUBLE,
    discount_rate   INT,
    sales           BIGINT,
    rating_star     DOUBLE,
    commission_rate DOUBLE,
    loaded_at       TIMESTAMP
)
CLUSTER BY (snapshot_date)
COMMENT 'Mỗi dòng = trạng thái một sản phẩm trong một ngày'
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {T('v_price_change')} AS
SELECT
    f.snapshot_date,
    f.item_id,
    p.product_name,
    s.shop_name,
    f.price,
    f.sales,
    f.commission_rate,
    LAG(f.price) OVER (PARTITION BY f.item_id ORDER BY f.snapshot_date)           AS prev_price,
    f.price - LAG(f.price) OVER (PARTITION BY f.item_id ORDER BY f.snapshot_date) AS price_diff,
    f.sales - LAG(f.sales) OVER (PARTITION BY f.item_id ORDER BY f.snapshot_date) AS sales_gained
FROM {T('fact_product_daily')} f
JOIN {T('dim_product')} p ON p.item_id = f.item_id
JOIN {T('dim_shop')}    s ON s.shop_id = f.shop_id
""")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))
