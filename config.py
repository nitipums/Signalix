from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Google Cloud
    gcp_project_id: str = ""

    # LINE Messaging API
    line_channel_access_token: str = ""
    line_channel_secret: str = ""

    # SET Trade Open API
    settrade_app_id: str = ""
    settrade_app_secret: str = ""
    settrade_broker_id: str = ""
    settrade_app_code: str = ""

    # Internal security — Cloud Scheduler passes this header to /scan
    scan_secret: str = "dev-secret"

    # App
    environment: str = "development"
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
