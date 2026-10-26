"""Lớp lưu trữ data lake: MinIO/S3 hoặc thư mục local, cùng một giao diện."""
from __future__ import annotations

import io
from pathlib import Path

from .config import Settings


class LakeStorage:
    def __init__(self, settings: Settings):
        self.backend = settings.storage_backend
        if self.backend == "s3":
            import boto3  # chỉ cần khi dùng MinIO/S3

            self.bucket = settings.s3_bucket
            self.s3 = boto3.client(
                "s3",
                endpoint_url=settings.s3_endpoint,
                aws_access_key_id=settings.s3_access_key,
                aws_secret_access_key=settings.s3_secret_key,
                region_name="us-east-1",
            )
            self._ensure_bucket()
        elif self.backend == "local":
            self.root = Path(settings.local_root)
            self.root.mkdir(parents=True, exist_ok=True)
        else:
            raise ValueError(f"STORAGE_BACKEND không hợp lệ: {self.backend}")

    def _ensure_bucket(self) -> None:
        existing = [b["Name"] for b in self.s3.list_buckets().get("Buckets", [])]
        if self.bucket not in existing:
            self.s3.create_bucket(Bucket=self.bucket)

    def put_bytes(self, key: str, data: bytes) -> str:
        if self.backend == "s3":
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)
            return f"s3://{self.bucket}/{key}"
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return str(path)

    def get_bytes(self, key: str) -> bytes:
        if self.backend == "s3":
            return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return (self.root / key).read_bytes()

    def list_keys(self, prefix: str) -> list[str]:
        if self.backend == "s3":
            keys: list[str] = []
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                keys += [obj["Key"] for obj in page.get("Contents", [])]
            return sorted(keys)
        base = self.root / prefix
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*") if p.is_file())

    def put_parquet(self, key: str, df) -> str:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        return self.put_bytes(key, buf.getvalue())

    def get_parquet(self, key: str):
        import pandas as pd

        return pd.read_parquet(io.BytesIO(self.get_bytes(key)))
