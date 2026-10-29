from functools import lru_cache

from pydantic import Field, field_validator, model_validator
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

    # --- Rate limiting (MECHANISM only; enforcement OFF by default). ---
    # NONE of the numbers below are approved operational policy; they are unset
    # placeholders pending gateway/ops confirmation. The limiter is a hard no-op
    # unless rate_limit_enabled=True AND a policy's capacity+refill are configured.
    rate_limit_enabled: bool = False          # master switch (default OFF)
    rate_limit_enforce: bool = False          # False => SHADOW (never blocks / no 429)
    rate_limit_redis_timeout_seconds: float = 0.05        # bounded per-check (fail-open)
    rate_limit_redis_connect_timeout_seconds: float = 0.25
    rate_limit_redis_socket_timeout_seconds: float = 0.25
    token_rate_capacity: int | None = None                # UNAPPROVED placeholder
    token_rate_refill_per_sec: float | None = None        # UNAPPROVED placeholder
    ingest_rate_capacity: int | None = None               # UNAPPROVED placeholder
    ingest_rate_refill_per_sec: float | None = None       # UNAPPROVED placeholder
    # Fallback-bucket MEMORY bounds (mechanism sizing, NOT a rate/burst/replay policy):
    rate_limit_local_fallback_max_keys: int = 10_000
    rate_limit_local_fallback_idle_seconds: float = 300.0

    # auth / OTP
    otp_provider: str = "mock"            # "mock" now; "msg91" once the account is live
    msg91_auth_key: str | None = None
    msg91_base_url: str | None = None     # confirm from MSG91 docs — do not guess
    msg91_sms_template_id: str | None = None
    msg91_sender_id: str | None = None
    msg91_email_from: str | None = None

    # --- OTP auth rate limiting (F1) + verification cap (F2). Inert by default. ---
    # Independent of the gateway rate_limit_* flags; NONE of these numbers are approved
    # operational policy. F1 is a hard no-op unless otp_rate_limit_enabled AND a policy
    # capacity+refill are set; F2 is inert unless otp_verify_cap is set.
    otp_rate_limit_enabled: bool = False       # master switch for OTP limiting (default OFF)
    otp_rate_limit_enforce: bool = False       # False => SHADOW (log only; never 429/lock)
    otp_request_ip_capacity: int | None = None
    otp_request_ip_refill_per_sec: float | None = None
    otp_request_user_capacity: int | None = None
    otp_request_user_refill_per_sec: float | None = None
    otp_verify_ip_capacity: int | None = None
    otp_verify_ip_refill_per_sec: float | None = None
    otp_verify_user_capacity: int | None = None
    otp_verify_user_refill_per_sec: float | None = None
    # F2 cumulative verification cap (DB-backed; independent of Redis).
    otp_verify_cap: int | None = None                  # max verifies per user per window
    otp_verify_cap_window_seconds: int = 900           # rolling window (>= 3x the 5-min TTL)
    # F5 expired-challenge cleanup (on-write, per-user). Inert by default.
    otp_challenge_cleanup_enabled: bool = False        # default OFF -> no behavior change
    otp_challenge_retention_seconds: int = 86400       # must be >= otp_verify_cap_window_seconds

    @field_validator("influx_bucket_3y")
    @classmethod
    def _influx_bucket_3y_nonblank(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("influx_bucket_3y must not be blank")
        return v

    @field_validator("token_rate_capacity", "ingest_rate_capacity")
    @classmethod
    def _rl_capacity_nonneg(cls, v):
        if v is not None and v < 0:
            raise ValueError("rate capacity must be >= 0 or null")
        return v

    @field_validator("token_rate_refill_per_sec", "ingest_rate_refill_per_sec")
    @classmethod
    def _rl_refill_nonneg(cls, v):
        if v is not None and v < 0:
            raise ValueError("rate refill must be >= 0 or null")
        return v

    @field_validator("rate_limit_redis_timeout_seconds",
                     "rate_limit_redis_connect_timeout_seconds",
                     "rate_limit_redis_socket_timeout_seconds")
    @classmethod
    def _rl_timeout_positive(cls, v):
        if v is None or v <= 0:
            raise ValueError("rate-limit timeout seconds must be > 0")
        return v

    @field_validator("rate_limit_local_fallback_max_keys")
    @classmethod
    def _rl_maxkeys_positive(cls, v):
        if v < 1:
            raise ValueError("rate_limit_local_fallback_max_keys must be >= 1")
        return v

    @field_validator("rate_limit_local_fallback_idle_seconds")
    @classmethod
    def _rl_idle_nonneg(cls, v):
        if v < 0:
            raise ValueError("rate_limit_local_fallback_idle_seconds must be >= 0")
        return v

    @field_validator("otp_request_ip_capacity", "otp_request_user_capacity",
                     "otp_verify_ip_capacity", "otp_verify_user_capacity")
    @classmethod
    def _otp_capacity_nonneg(cls, v):
        if v is not None and v < 0:
            raise ValueError("OTP rate capacity must be >= 0 or null")
        return v

    @field_validator("otp_request_ip_refill_per_sec", "otp_request_user_refill_per_sec",
                     "otp_verify_ip_refill_per_sec", "otp_verify_user_refill_per_sec")
    @classmethod
    def _otp_refill_nonneg(cls, v):
        if v is not None and v < 0:
            raise ValueError("OTP rate refill must be >= 0 or null")
        return v

    @field_validator("otp_verify_cap")
    @classmethod
    def _otp_cap_positive(cls, v):
        if v is not None and v < 1:
            raise ValueError("otp_verify_cap must be >= 1 or null")
        return v

    @field_validator("otp_verify_cap_window_seconds")
    @classmethod
    def _otp_window_positive(cls, v):
        if v < 1:
            raise ValueError("otp_verify_cap_window_seconds must be >= 1")
        return v

    @field_validator("otp_challenge_retention_seconds")
    @classmethod
    def _otp_retention_positive(cls, v):
        if v < 1:
            raise ValueError("otp_challenge_retention_seconds must be >= 1")
        return v

    @model_validator(mode="after")
    def _otp_retention_covers_window(self):
        # cleanup must never delete rows still counted by the F2 window
        if self.otp_challenge_retention_seconds < self.otp_verify_cap_window_seconds:
            raise ValueError(
                "otp_challenge_retention_seconds must be >= otp_verify_cap_window_seconds")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()