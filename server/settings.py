"""Application settings loaded from environment variables."""

from __future__ import annotations

import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "A2D_"}

    # CORS
    cors_origins: list[str] = ["http://localhost:5173", "http://localhost:3000"]
    cors_allow_methods: list[str] = ["GET", "POST", "OPTIONS"]
    cors_allow_headers: list[str] = ["Content-Type", "Authorization"]

    # Upload limits
    max_upload_size_bytes: int = 50 * 1024 * 1024  # 50 MB
    max_batch_files: int = 50
    allowed_extensions: set[str] = {".yxmd"}

    # Job management
    job_ttl_seconds: int = 3600  # 1 hour

    # Server
    host: str = "0.0.0.0"
    port: int = int(os.environ.get("PORT", "8000"))
    log_level: str = "info"

    # Database (optional — history disabled when empty)
    database_url: str = ""


settings = Settings()
