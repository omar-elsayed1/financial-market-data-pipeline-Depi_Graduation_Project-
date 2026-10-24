"""Environment-backed settings and logging configuration."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "pipeline.log"


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    api_provider: str
    api_base_url: str
    api_key: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str


def configure_logging() -> None:
    """Configure application logging to write into logs/pipeline.log."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(),
        ],
    )


def load_settings() -> Settings:
    """Load typed settings from environment variables.

    Returns:
        Settings instance.

    Raises:
        ValueError: When required variables are missing.
    """
    required = {
        "API_BASE_URL": os.getenv("API_BASE_URL", "https://api.twelvedata.com"),
        "API_KEY": os.getenv("API_KEY", ""),
        "DB_HOST": os.getenv("DB_HOST", ""),
        "DB_PORT": os.getenv("DB_PORT", "5432"),
        "DB_NAME": os.getenv("DB_NAME", ""),
        "DB_USER": os.getenv("DB_USER", ""),
        "DB_PASSWORD": os.getenv("DB_PASSWORD", ""),
    }

    missing = [key for key, value in required.items() if value == ""]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return Settings(
        api_provider=os.getenv("API_PROVIDER", "twelve_data"),
        api_base_url=required["API_BASE_URL"],
        api_key=required["API_KEY"],
        db_host=required["DB_HOST"],
        db_port=int(required["DB_PORT"]),
        db_name=required["DB_NAME"],
        db_user=required["DB_USER"],
        db_password=required["DB_PASSWORD"],
    )
