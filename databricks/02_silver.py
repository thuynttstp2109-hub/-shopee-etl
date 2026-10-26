# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze → Silver
# MAGIC Làm sạch bằng PySpark: ép kiểu, chuẩn hóa tên, gắn nhãn loại shop, bỏ trùng, rồi kiểm tra chất lượng.
# MAGIC Dữ liệu hỏng thì notebook dừng, job không chạy tiếp sang gold.

# COMMAND ----------

dbutils.widgets.text("snapshot_date", "", "Ngày (YYYY-MM-DD, trống = hôm nay)")

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

snapshot = resolve_snapshot_date(dbutils.widgets.get("snapshot_date"))
bronze = spark.table(T("bronze_product_offers")).where(F.col("snapshot_date") == snapshot)
print(f"Ngày {snapshot}: {bronze.count()} dòng bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Làm sạch
# MAGIC Dùng `try_cast` và `try_element_at` thay cho `cast` / `element_at`: compute serverless bật chế độ ANSI,
# MAGIC nên một giá trị lỗi (ví dụ giá rỗng) sẽ làm hỏng cả job nếu dùng hàm thường. Với `try_*`, giá trị lỗi thành NULL rồi bị lọc ở bước sau.

# COMMAND ----------

# shopType trong API: 1 = Shopee Mall, 2 = Yêu thích, 4 = Yêu thích+
first_shop_type = F.expr("try_element_at(node.shopType, 1)")
shop_type = (
    F.when(first_shop_type == 1, "mall")
    .when(first_shop_type == 2, "preferred")
    .when(first_shop_type == 4, "preferred_plus")
    .otherwise("normal")
)

silver = (
    bronze.select(
        "snapshot_date",
        F.col("node.itemId").alias("item_id"),
        F.regexp_replace(F.trim("node.productName"), r"\s+", " ").alias("product_name"),
        F.col("node.shopId").alias("shop_id"),
        F.trim("node.shopName").alias("shop_name"),
        shop_type.alias("shop_type"),
        F.expr("try_element_at(node.productCatIds, -1)").alias("category_id"),  # danh mục lá
        F.concat_ws("/", F.col("node.productCatIds").cast("array<string>")).alias("category_path"),
        F.expr("try_cast(node.price AS DOUBLE)").alias("price"),
        F.expr("try_cast(node.priceMin AS DOUBLE)").alias("price_min"),
        F.expr("try_cast(node.priceMax AS DOUBLE)").alias("price_max"),
        F.coalesce("node.priceDiscountRate", F.lit(0)).alias("discount_rate"),
        F.coalesce("node.sales", F.lit(0)).alias("sales"),
        F.round(F.expr("try_cast(node.ratingStar AS DOUBLE)"), 2).alias("rating_star"),
        F.expr("try_cast(node.commissionRate AS DOUBLE)").alias("commission_rate"),
        F.col("node.imageUrl").alias("image_url"),
        F.col("node.productLink").alias("product_link"),
        F.col("node.offerLink").alias("offer_link"),
        "keyword",
        "source",
    )
    .where(F.col("item_id").isNotNull() & F.col("price").isNotNull())
    # Một sản phẩm có thể xuất hiện ở nhiều từ khóa: giữ một dòng
    .dropDuplicates(["snapshot_date", "item_id"])
)

# COMMAND ----------

# MAGIC %md ## Kiểm tra chất lượng

# COMMAND ----------

q = silver.agg(
    F.count("*").alias("rows"),
    F.countDistinct("item_id").alias("distinct_items"),
    F.sum((F.col("price") <= 0).cast("int")).alias("bad_price"),
    F.sum((~F.col("rating_star").between(0, 5)).cast("int")).alias("bad_rating"),
    F.sum((~F.col("discount_rate").between(0, 100)).cast("int")).alias("bad_discount"),
).first()

problems = []
if q.rows == 0:
    problems.append("không có dòng nào")
if q.rows != q.distinct_items:
    problems.append("item_id bị trùng")
if q.bad_price:
    problems.append(f"{q.bad_price} dòng có giá <= 0")
if q.bad_rating:
    problems.append(f"{q.bad_rating} dòng có rating ngoài 0-5")
if q.bad_discount:
    problems.append(f"{q.bad_discount} dòng có giảm giá ngoài 0-100%")
if problems:
    raise ValueError("Dữ liệu không đạt: " + "; ".join(problems))

print(f"Đạt kiểm tra: {q.rows} sản phẩm")

# COMMAND ----------

(
    silver.write.mode("overwrite")
    .option("replaceWhere", f"snapshot_date = '{snapshot}'")
    .saveAsTable(T("silver_product_snapshot"))
)
display(spark.table(T("silver_product_snapshot")).where(F.col("snapshot_date") == snapshot).limit(10))
