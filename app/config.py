"""Application configuration for Hydra Engine."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment-driven runtime settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/hydra",
        description="Async SQLAlchemy database URL.",
    )
    REDIS_URL: str = Field(
        default="redis://localhost:6379/1",
        description="Redis URL used for durable task queueing.",
    )
    MAX_STEP_RETRIES: int = Field(default=3, ge=0)
    BACKOFF_FACTOR: float = Field(default=2.0, gt=0.0)
    CONTEXT_TOKEN_THRESHOLD: int = Field(default=4000, gt=0)
    REDIS_QUEUE_NAME: str = Field(default="hydra:workflow:queue")
    REDIS_DELAYED_QUEUE_NAME: str = Field(default="hydra:workflow:delayed")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached application settings instance."""

    return Settings()


settings = get_settings()
