"""
configuration module for the distributed key-value store.
loads settings from environment variables using pydantic-settings.
"""

from functools import lru_cache
from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """application settings loaded from environment variables."""

    # node configuration
    role: Literal["leader", "follower"] = "follower"
    node_name: str = "node"

    # replication settings (leader only)
    quorum: int = 2  # number of acks required for write success
    delay_min: int = 0  # minimum replication delay in milliseconds
    delay_max: int = 1000  # maximum replication delay in milliseconds
    followers: str = ""  # comma-separated list of follower urls

    # server configuration
    host: str = "0.0.0.0"
    port: int = 8000

    @field_validator("quorum")
    @classmethod
    def validate_quorum(cls, v: int) -> int:
        """ensure quorum is between 1 and 5."""
        if v < 1 or v > 5:
            raise ValueError("quorum must be between 1 and 5")
        return v

    @field_validator("delay_min", "delay_max")
    @classmethod
    def validate_delay(cls, v: int) -> int:
        """ensure delay is non-negative."""
        if v < 0:
            raise ValueError("delay must be non-negative")
        return v

    @property
    def follower_urls(self) -> list[str]:
        """parse followers string into list of urls."""
        if not self.followers:
            return []
        return [url.strip() for url in self.followers.split(",") if url.strip()]

    @property
    def is_leader(self) -> bool:
        """check if this node is the leader."""
        return self.role == "leader"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    """get cached settings instance."""
    return Settings()
