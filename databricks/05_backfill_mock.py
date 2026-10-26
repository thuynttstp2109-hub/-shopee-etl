# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Tạo lịch sử giả lập
# MAGIC Chạy pipeline mock cho N ngày gần nhất để có dữ liệu phân tích biến động giá.
# MAGIC Chỉ dùng với chế độ mock: API thật chỉ trả dữ liệu hiện tại, không lấy lại được giá của ngày đã qua.

# COMMAND ----------

dbutils.widgets.text("days", "7", "Số ngày")

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from datetime import timedelta

days = int(dbutils.widgets.get("days"))
today = resolve_snapshot_date("")

for i in range(days, -1, -1):
    d = (today - timedelta(days=i)).isoformat()
    dbutils.notebook.run("./01_extract_bronze", 600, {"snapshot_date": d, "mode": "mock"})
    dbutils.notebook.run("./02_silver", 600, {"snapshot_date": d})
    dbutils.notebook.run("./03_gold", 600, {"snapshot_date": d})
    print("Xong", d)

# COMMAND ----------

display(spark.sql(f"""
SELECT snapshot_date, COUNT(*) AS so_san_pham
FROM {T('fact_product_daily')} GROUP BY snapshot_date ORDER BY snapshot_date
"""))
