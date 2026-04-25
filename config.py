from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Google Cloud
    gcp_project_id: str = ""
    bq_dataset: str = "signalix"

    # LINE Messaging API
    line_channel_access_token: str = ""
    line_channel_secret: str = ""

    # SET Trade Open API
    settrade_app_id: str = ""
    settrade_app_secret: str = ""
    settrade_broker_id: str = ""
    settrade_app_code: str = ""
    settrade_account_no: str = ""
    settrade_pin: str = ""

    # Trading switches
    trading_enabled: bool = False
    trading_mode: str = "paper"  # "live" or "paper"

    # Risk parameters
    min_strength_score: int = 70
    allowed_patterns: str = "breakout,ath_breakout,vcp,vcp_low_cheat"
    risk_per_trade_pct: float = 0.01
    reward_r_multiple: float = 2.5
    max_position_size_thb: float = 5000.0
    max_open_positions: int = 1
    max_daily_loss_thb: float = 2000.0
    entry_price_type: str = "Limit"  # "Limit" or "Market"
    board_lot: int = 100

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
