from pydantic import BaseModel


class LoginRequest(BaseModel):
    identifier: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserMe(BaseModel):
    id: str
    email: str | None = None
    phone: str | None = None
    organization_id: str | None = None


class OtpRequestIn(BaseModel):
    identifier: str          # registered phone (email later)


class OtpRequestOut(BaseModel):
    status: str = "otp_sent"


class OtpVerifyIn(BaseModel):
    identifier: str
    otp: str