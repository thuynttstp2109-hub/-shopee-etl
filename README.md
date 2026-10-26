# Shopee ETL

Pipeline thu thập dữ liệu sản phẩm Shopee hằng ngày qua **Shopee Affiliate Open API**, lưu vào data lake (MinIO), làm sạch và nạp vào kho dữ liệu PostgreSQL dạng star schema, điều phối bằng Airflow và trực quan hóa bằng Metabase.

```
Shopee Affiliate API ──► MinIO raw/ (JSON) ──► MinIO processed/ (Parquet) ──► PostgreSQL ──► Metabase
      extract                                transform                        load
└──────────────────────── Airflow chạy lúc 7h sáng mỗi ngày ────────────────────────┘
```

## 1. Đăng ký Shopee Affiliate (làm một lần)

1. Đăng ký tại https://affiliate.shopee.vn và chờ duyệt (thường vài ngày).
2. Sau khi duyệt, vào mục **Open API** trên dashboard để lấy **App ID** và **App Secret**.
3. Đối chiếu endpoint GraphQL trong tài liệu Open API trên dashboard với `SHOPEE_API_URL` trong `.env`.

Trong lúc chờ duyệt, cứ để `SHOPEE_MOCK=true`: pipeline chạy bằng dữ liệu giả lập có cùng cấu trúc với API thật, nên bạn làm được hết các phần còn lại.

## 2. Chạy toàn bộ bằng Docker

```bash
cp .env.example .env        # rồi điền App ID / Secret nếu đã có
docker compose up -d
```

Metabase không chạy mặc định để tiết kiệm RAM. Khi cần dashboard: `docker compose --profile dashboard up -d`.

Lần đầu Airflow mất vài phút để cài thư viện. Sau đó:

| Dịch vụ | Địa chỉ | Đăng nhập |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Metabase | http://localhost:3000 | tự tạo lúc mở lần đầu |
| PostgreSQL (kho) | localhost:5433 | dw / dw, database `shopee_dw` |

Trong Airflow, bật DAG `shopee_daily_etl` và bấm **Trigger** để chạy ngay.

Khi có key thật: sửa `.env` thành `SHOPEE_MOCK=false`, rồi `docker compose restart airflow`.

### Chạy trên GitHub Codespaces (khi máy yếu)

1. Đẩy project lên một repo GitHub.
2. Trên trang repo: **Code > Codespaces > Create codespace** (chọn máy 2-core). Môi trường tự cài Python, Docker và thư viện.
3. Trong terminal của Codespace: `docker compose up -d`. Mở tab **Ports** để vào Airflow (8080) và MinIO (9001).
4. Dừng Codespace khi không dùng (**Stop codespace**) để không tốn giờ miễn phí.

### Chạy trên Databricks

Xem [databricks/README.md](databricks/README.md): cùng pipeline viết bằng PySpark, bảng Delta theo kiến trúc bronze/silver/gold, lên lịch bằng Lakeflow Job.

### Chạy trên Jupyter / Google Colab (không cần Docker)

Mở `shopee_etl.ipynb` và chạy lần lượt từng cell. Notebook dùng thư mục `data_lake/` thay cho MinIO và SQLite thay cho PostgreSQL, còn code ETL dùng chung với bản Docker. Trên Colab, chạy cell đầu tiên rồi tải `shopee-etl.zip` lên khi được hỏi.

## 3. Chạy không cần Airflow (để debug)

```bash
pip install -r requirements.txt
export $(grep -v '^#' .env | xargs)
export STORAGE_BACKEND=local                                  # lưu vào ./data_lake
export DW_DSN=postgresql://dw:dw@localhost:5433/shopee_dw     # nếu đã chạy postgres-dw
python scripts/run_local.py                 # extract + transform + load
python scripts/run_local.py --skip-load     # bỏ bước load
python scripts/run_local.py --date 2026-10-20
pytest -q                                   # test, không cần mạng hay database
```

## 4. Cấu trúc project

```
src/shopee_etl/
  config.py         đọc cấu hình từ biến môi trường
  shopee_client.py  gọi GraphQL, ký SHA256, phân trang, retry khi lỗi tạm thời
  mock_data.py      dữ liệu giả lập
  storage.py        đọc/ghi MinIO (S3) hoặc thư mục local
  extract.py        API -> raw/product_offers/snapshot_date=.../keyword=.../page=NNN.json
  transform.py      raw -> processed/product_snapshot/snapshot_date=.../data.parquet
  load.py           Parquet -> PostgreSQL (upsert, chạy lại không bị trùng)
dags/               DAG Airflow
sql/schema.sql      star schema + view biến động giá
tests/              test chạy ở chế độ mock
```

## 5. Mô hình dữ liệu

- `fact_product_daily`: mỗi dòng là trạng thái một sản phẩm trong một ngày (giá, lượt bán, rating, hoa hồng)
- `dim_product`, `dim_shop`, `dim_date`: thông tin mô tả
- `v_price_change`: so sánh giá và lượt bán với ngày trước đó

Một số truy vấn để bắt đầu:

```sql
-- Sản phẩm giảm giá mạnh nhất hôm nay
SELECT product_name, shop_name, prev_price, price, price_diff
FROM v_price_change
WHERE snapshot_date = CURRENT_DATE AND price_diff < 0
ORDER BY price_diff LIMIT 20;

-- Sản phẩm tăng lượt bán nhanh nhất
SELECT product_name, SUM(sales_gained) AS sold_7d
FROM v_price_change
WHERE snapshot_date >= CURRENT_DATE - 7
GROUP BY product_name ORDER BY sold_7d DESC NULLS LAST LIMIT 20;

-- Giá trung bình theo loại shop
SELECT s.shop_type, ROUND(AVG(f.price)) AS avg_price, COUNT(*) AS n
FROM fact_product_daily f JOIN dim_shop s USING (shop_id)
WHERE f.snapshot_date = CURRENT_DATE
GROUP BY s.shop_type;
```

Kết nối Metabase vào kho: chọn PostgreSQL, host `postgres-dw`, port `5432`, database `shopee_dw`, user/pass `dw`/`dw`.

## 6. Hướng mở rộng

- Thay Pandas bằng **PySpark** ở bước transform khi dữ liệu lớn
- Thêm **dbt** để quản lý các bảng tổng hợp và test chất lượng bằng SQL
- Lưu lịch sử thay đổi tên/danh mục sản phẩm theo kiểu **SCD Type 2**
- Gửi cảnh báo (email, Telegram) khi DAG lỗi hoặc khi có sản phẩm giảm giá sâu

## Lưu ý

- Tuân thủ điều khoản Shopee Affiliate và giới hạn tần suất của API. Tăng `SHOPEE_REQUEST_DELAY` nếu gặp lỗi rate limit.
- Không commit file `.env` (đã có trong `.gitignore`).
- Bản community của MinIO không còn được cập nhật; để học vẫn dùng ổn, hoặc đổi image sang `pgsty/minio`.
