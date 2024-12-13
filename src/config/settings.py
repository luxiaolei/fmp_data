"""Configuration settings for FMP Data client."""

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Protocol

from dotenv import load_dotenv

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
    """Storage configuration settings."""
    
    mongodb_uri: str = Field(..., description="MongoDB connection URI")
    mongodb_db: str = Field(..., description="MongoDB database name")
    redis_url: str = Field(..., description="Redis connection URL")
    redis_password: str = Field(..., description="Redis password")


class Settings(BaseModel):
    """Global settings."""
    
    api: APIConfig
    storage: StorageConfig


def load_config(config_path: Optional[Path] = None) -> Settings:
    """Load configuration from file and/or environment variables.
    
    Parameters
    ----------
    config_path : Optional[Path]
        Path to YAML config file. If not provided, will look for config.yaml
        in the current directory.
        
    Returns
    -------
    Settings
        Configuration settings
        
    Raises
    ------
    ValueError
        If required settings are missing
    """
    # Default config
    config: Dict[str, Any] = {
        "api": {
            "key": os.getenv("FMP_API_KEY", ""),
            "base_url": "https://financialmodelingprep.com/api/v3",
            "rate_limit": 750
        },
        "storage": {
            "mongodb_uri": os.getenv("MONGO_URI", ""),
            "mongodb_db": os.getenv("MONGO_DB_NAME", ""),
            "redis_url": os.getenv("REDIS_URL", ""),
            "redis_password": os.getenv("REDIS_PASSWORD", "")
        }
    }
    
    # Load from file if provided
    if config_path is None:
        config_path = Path("config.yaml")
        
    if config_path.exists():
        with open(config_path) as f:
            file_config = yaml.safe_load(f)
            if file_config:
                config.update(file_config)
                
    return Settings(**config) 