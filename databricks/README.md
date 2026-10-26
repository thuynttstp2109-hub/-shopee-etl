# Shopee ETL trên Databricks

Phiên bản Databricks của pipeline, theo kiến trúc **medallion**:

```
Shopee API / mock / file tải lên
        │
        ▼
Volume  /Volumes/workspace/shopee/lake/raw/...      JSON gốc (thay MinIO)
        │  01_extract_bronze   PySpark đọc JSON, tách mỗi sản phẩm một dòng
        ▼
Bronze  bronze_product_offers                       dữ liệu gốc dạng bảng Delta
        │  02_silver           ép kiểu, làm sạch, bỏ trùng, kiểm tra chất lượng
        ▼
Silver  silver_product_snapshot
        │  03_gold             MERGE vào dim, replaceWhere vào fact
        ▼
Gold    dim_date · dim_shop · dim_product · fact_product_daily · v_price_change
        │
        ▼
04_analysis / dashboard Databricks SQL

Lakeflow Job chạy 01 → 02 → 03 lúc 7h sáng mỗi ngày
```

| Bản Docker | Bản Databricks |
|---|---|
| MinIO | Unity Catalog volume |
| Pandas | PySpark |
| Parquet + PostgreSQL | Bảng Delta |
| UPSERT trong PostgreSQL | `MERGE INTO` và `replaceWhere` |
| Airflow DAG | Lakeflow Job (`databricks.yml`) |
| Metabase | Visualization / dashboard trong Databricks |

Code gọi API trong `src/shopee_etl` được dùng lại nguyên vẹn.

## 1. Tạo tài khoản

Đăng ký **Databricks Free Edition** (miễn phí, không cần thẻ). Free Edition chỉ có compute serverless, có hạn mức sử dụng và **không được dùng cho mục đích thương mại**: phù hợp để học và làm project CV.

## 2. Đưa code lên workspace

Cần đưa **cả thư mục `shopee-etl`**, vì notebook dùng code trong `src/`.

**Cách A: Git folder (dễ nhất).** Đẩy project lên GitHub. Trong Databricks: **Workspace → Create → Git folder**, dán URL repo. Mở thư mục `databricks/` để thấy các notebook.

**Cách B: Databricks CLI.** Cài CLI, đăng nhập rồi triển khai. Cách này tạo luôn Lakeflow Job:

```bash
databricks auth login --host <địa chỉ workspace của bạn>
cd shopee-etl
databricks bundle deploy
```

## 3. Chạy lần đầu

1. `00_setup`: tạo schema `workspace.shopee`, volume và các bảng (chạy một lần)
2. `05_backfill_mock`: chạy pipeline mock cho 8 ngày gần nhất để có lịch sử
3. `04_analysis`: xem kết quả; mỗi bảng có nút **+ → Visualization** để vẽ biểu đồ

Muốn chạy từng bước: `01_extract_bronze` → `02_silver` → `03_gold`, mỗi notebook có ô nhập ngày ở trên cùng (để trống = hôm nay).

## 4. Lên lịch chạy hằng ngày

- Nếu dùng cách B: job `shopee_daily_etl` đã được tạo. Ở chế độ development, lịch chạy bị tạm dừng; vào **Jobs & Pipelines** để bật hoặc bấm **Run now**.
- Nếu dùng cách A: vào **Jobs & Pipelines → Create job**, tạo 3 task loại Notebook theo thứ tự `01_extract_bronze` → `02_silver` → `03_gold`. Ở task 02 và 03 thêm tham số `snapshot_date` = `{{tasks.extract_bronze.values.snapshot_date}}` (thay `extract_bronze` bằng tên bạn đặt cho task đầu). Thêm lịch 7:00 múi giờ Asia/Ho_Chi_Minh.

## 5. Dữ liệu thật

Free Edition chỉ cho kết nối ra ngoài tới một số domain tin cậy, nên **có thể không gọi được Shopee API** từ Databricks. Có hai cách:

**Cách 1: Lấy dữ liệu ở ngoài, tải lên volume (dùng được với Free Edition).** Chạy phần extract trên máy, Colab hoặc Codespaces (bản Docker/notebook đã có), rồi tải thư mục JSON lên:

```bash
# Trên máy: chỉ lấy dữ liệu, lưu vào ./data_lake
STORAGE_BACKEND=local SHOPEE_MOCK=false SHOPEE_APP_ID=... SHOPEE_APP_SECRET=... \
  python scripts/run_local.py --skip-load

# Tải lên volume
databricks fs cp -r data_lake/raw dbfs:/Volumes/workspace/shopee/lake/raw --overwrite
```

Sau đó chạy job (hoặc notebook 01) với `mode = files`.

**Cách 2: Gọi API trực tiếp (workspace trả phí, hoặc nếu domain Shopee được cho phép).** Lưu key vào secret rồi chạy với `mode = api`:

```bash
databricks secrets create-scope shopee
databricks secrets put-secret shopee app_id
databricks secrets put-secret shopee app_secret
```

## Kiến thức Databricks học được qua project

- Unity Catalog: catalog → schema → bảng / volume
- Đọc JSON với schema khai báo sẵn, `explode` mảng lồng nhau, cột `_metadata.file_path`
- `try_cast` / `try_element_at` để không vỡ job khi compute bật chế độ ANSI
- Ghi Delta idempotent bằng `replaceWhere`, cập nhật dim bằng `MERGE INTO`
- Liquid clustering (`CLUSTER BY`), Delta time travel (`DESCRIBE HISTORY`, `VERSION AS OF`)
- Lakeflow Job nhiều task, truyền giá trị giữa các task bằng task values
- Databricks Asset Bundles để quản lý job bằng code
