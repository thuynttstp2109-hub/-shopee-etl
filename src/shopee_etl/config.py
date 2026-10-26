"""Đọc cấu hình từ biến môi trường (file .env được docker-compose nạp sẵn)."""
import os
from dataclasses import dataclass, field


def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "y")


def _list(name: str, default: str) -> list[str]:
    return [x.strip() for x in os.getenv(name, default).split(",") if x.strip()]


@dataclass
class Settings:
    # --- Shopee Affiliate API ---
    app_id: str = field(default_factory=lambda: os.getenv("SHOPEE_APP_ID", ""))
    app_secret: str = field(default_factory=lambda: os.getenv("SHOPEE_APP_SECRET", ""))
    api_url: str = field(
        default_factory=lambda: os.getenv(
            "SHOPEE_API_URL", "https://open-api.affiliate.shopee.vn/graphql"
        )
    )
    # Chạy bằng dữ liệu giả lập khi chưa có App ID / Secret
    mock: bool = field(default_factory=lambda: _bool("SHOPEE_MOCK", True))

    # --- Phạm vi lấy dữ liệu ---
    keywords: list[str] = field(
        default_factory=lambda: _list("SHOPEE_KEYWORDS", "tai nghe bluetooth,ban phim co")
    )
    page_limit: int = field(default_factory=lambda: int(os.getenv("SHOPEE_PAGE_LIMIT", "50")))
    max_pages: int = field(default_factory=lambda: int(os.getenv("SHOPEE_MAX_PAGES", "5")))
    sort_type: int = field(default_factory=lambda: int(os.getenv("SHOPEE_SORT_TYPE", "2")))
    request_delay: float = field(
        default_factory=lambda: float(os.getenv("SHOPEE_REQUEST_DELAY", "1.5"))
    )

    # --- Data lake ---
    # "s3" = MinIO / S3, "local" = thư mục trên máy (tiện để chạy thử)
    storage_backend: str = field(default_factory=lambda: os.getenv("STORAGE_BACKEND", "s3"))
    local_root: str = field(default_factory=lambda: os.getenv("LOCAL_LAKE_DIR", "./data_lake"))
    s3_endpoint: str = field(default_factory=lambda: os.getenv("S3_ENDPOINT", "http://minio:9000"))
    s3_access_key: str = field(default_factory=lambda: os.getenv("S3_ACCESS_KEY", "minioadmin"))
    s3_secret_key: str = field(default_factory=lambda: os.getenv("S3_SECRET_KEY", "minioadmin"))
    s3_bucket: str = field(default_factory=lambda: os.getenv("S3_BUCKET", "shopee-lake"))

    # --- Kho dữ liệu (PostgreSQL) ---
    dw_dsn: str = field(
        default_factory=lambda: os.getenv(
            "DW_DSN", "postgresql://dw:dw@postgres-dw:5432/shopee_dw"
        )
    )


def get_settings() -> Settings:
    return Settings()
