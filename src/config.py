from functools import lru_cache

from pydantic import Field, field_validator
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
    influx_bucket_3y: str = "telemetry_3y"
    influx_bucket_3y_retention_seconds: int = Field(default=94_608_000, gt=0)  # 1095 d (PROPOSED, not prod-approved)
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

    @field_validator("influx_bucket_3y")
    @classmethod
    def _influx_bucket_3y_nonblank(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("influx_bucket_3y must not be blank")
        return v


@lru_cache
def get_settings() -> Settings:
    return Settings()