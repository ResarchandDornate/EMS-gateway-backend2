from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    env: str = "dev"
    database_url: str
    influx_url: str
    influx_token: str
    influx_org: str
    influx_bucket_raw: str
    influx_bucket_long: str
    influx_bucket_test: str = "telemetry_test"
    redis_url: str
    jwt_secret: str

    # 9b-ii-c live-ingestion gate (default OFF; record contract still PROVISIONAL)
    ingest_enabled: bool = False

    # auth / OTP
    otp_provider: str = "mock"            # "mock" now; "msg91" once the account is live
    msg91_auth_key: str | None = None
    msg91_base_url: str | None = None     # confirm from MSG91 docs — do not guess
    msg91_sms_template_id: str | None = None
    msg91_sender_id: str | None = None
    msg91_email_from: str | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()