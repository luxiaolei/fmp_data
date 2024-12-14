"""Configuration settings for FMP Data client."""

import os
from typing import TYPE_CHECKING, Protocol

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

if TYPE_CHECKING:
    import pydantic

class ConfigModel(Protocol):
    """Protocol for configuration model functionality."""
    def dict(self) -> dict: ...

try:
    import yaml
    from pydantic import BaseModel, Field
except ImportError:
    raise ImportError(
        "Required dependencies not found. Please install: "
        "pip install pyyaml pydantic"
    )


class APIConfig(BaseModel if 'BaseModel' in locals() else ConfigModel):  # type: ignore
    """API configuration settings."""
    
    key: str = Field(..., description="FMP API key")
    base_url: str = Field(
        "https://financialmodelingprep.com/api/v3",
        description="FMP API base URL"
    )
    rate_limit: int = Field(
        750,
        description="API rate limit (requests per minute)"
    )


class StorageConfig(BaseModel):
    """Storage configuration."""
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_db: str = "fmp_data"
    mongodb_user: str = ""
    mongodb_pass: str = ""
    redis_url: str = "redis://localhost:6379"
    redis_password: str = ""


class Settings(BaseModel):
    """Global settings."""
    
    api: APIConfig
    storage: StorageConfig


def load_settings() -> Settings:
    """Load settings from environment."""
    load_dotenv()
    
    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    mongodb_user = os.getenv("MONGODB_USER", "")
    mongodb_pass = os.getenv("MONGODB_PASS", "")
    mongodb_db = os.getenv("MONGODB_DB", "fmp_data")
    
    # Log connection details (without password)
    logger.debug(f"MongoDB URI: {mongodb_uri}")
    logger.debug(f"MongoDB User: {mongodb_user}")
    logger.debug(f"MongoDB DB: {mongodb_db}")
    
    return Settings(
        api=APIConfig(
            key=os.getenv("FMP_API_KEY", "")
        ),
        storage=StorageConfig(
            mongodb_uri=mongodb_uri,
            mongodb_db=mongodb_db,
            mongodb_user=mongodb_user,
            mongodb_pass=mongodb_pass,
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"),
            redis_password=os.getenv("REDIS_PASSWORD", "")
        )
    ) 