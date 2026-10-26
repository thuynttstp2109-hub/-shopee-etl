# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Phân tích
# MAGIC Mỗi bảng kết quả có nút **+ → Visualization** để vẽ biểu đồ ngay trong notebook,
# MAGIC hoặc ghim vào dashboard (**Add to dashboard**).
# MAGIC
# MAGIC Cần ít nhất 2 ngày dữ liệu để thấy biến động giá. Chưa có thì chạy `05_backfill_mock`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

latest = spark.sql(f"SELECT MAX(snapshot_date) AS d FROM {T('fact_product_daily')}").first().d
print("Ngày gần nhất:", latest)

# COMMAND ----------

# MAGIC %md ## Sản phẩm giảm giá mạnh nhất

# COMMAND ----------

display(spark.sql(f"""
SELECT product_name, shop_name, prev_price, price, price_diff,
       ROUND(-price_diff / prev_price * 100, 1) AS giam_pct
FROM {T('v_price_change')}
WHERE snapshot_date = DATE'{latest}' AND price_diff < 0
ORDER BY giam_pct DESC
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sản phẩm đáng làm video
# MAGIC Bán chạy trong 7 ngày, rating từ 4,7, giá 50k–300k, xếp theo hoa hồng dự kiến mỗi đơn (giá × tỷ lệ hoa hồng).
# MAGIC Đây là danh sách cho buổi chọn sản phẩm sáng Thứ Hai trong kế hoạch kênh.

# COMMAND ----------

display(spark.sql(f"""
WITH recent AS (
    SELECT item_id, SUM(sales_gained) AS ban_them_7_ngay
    FROM {T('v_price_change')}
    WHERE snapshot_date > date_sub(DATE'{latest}', 7)
    GROUP BY item_id
)
SELECT p.product_name,
       s.shop_name,
       f.price,
       ROUND(f.price * f.commission_rate) AS hoa_hong_moi_don,
       r.ban_them_7_ngay,
       f.rating_star,
       p.offer_link
FROM {T('fact_product_daily')} f
JOIN {T('dim_product')} p ON p.item_id = f.item_id
JOIN {T('dim_shop')}    s ON s.shop_id = f.shop_id
JOIN recent             r ON r.item_id = f.item_id
WHERE f.snapshot_date = DATE'{latest}'
  AND f.price BETWEEN 50000 AND 300000
  AND f.rating_star >= 4.7
ORDER BY r.ban_them_7_ngay * f.price * f.commission_rate DESC NULLS LAST
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md ## Giá trung bình theo loại shop

# COMMAND ----------

display(spark.sql(f"""
SELECT s.shop_type,
       ROUND(AVG(f.price))          AS gia_tb,
       ROUND(AVG(f.rating_star), 2) AS rating_tb,
       COUNT(*)                     AS so_sp
FROM {T('fact_product_daily')} f
JOIN {T('dim_shop')} s ON s.shop_id = f.shop_id
WHERE f.snapshot_date = DATE'{latest}'
GROUP BY s.shop_type
ORDER BY gia_tb DESC
"""))

# COMMAND ----------

# MAGIC %md ## Biến động giá của 5 sản phẩm bán chạy nhất (chọn Visualization → Line, trục X = snapshot_date)

# COMMAND ----------

display(spark.sql(f"""
WITH top AS (
    SELECT item_id FROM {T('fact_product_daily')}
    WHERE snapshot_date = DATE'{latest}'
    ORDER BY sales DESC LIMIT 5
)
SELECT v.snapshot_date, LEFT(v.product_name, 35) AS san_pham, v.price
FROM {T('v_price_change')} v JOIN top USING (item_id)
ORDER BY v.snapshot_date
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta time travel
# MAGIC Mỗi lần ghi tạo một phiên bản mới của bảng. Xem lịch sử và đọc lại dữ liệu ở phiên bản cũ:

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {T('fact_product_daily')}").select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS so_dong_o_phien_ban_1 FROM {T('fact_product_daily')} VERSION AS OF 1"))
