from abc import ABC, abstractmethod

from src.config import get_settings


class OtpDeliveryProvider(ABC):
    @abstractmethod
    async def send_sms(self, mobile: str, code: str) -> None: ...
    @abstractmethod
    async def send_email(self, email: str, code: str) -> None: ...


class MockOtpProvider(OtpDeliveryProvider):
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, str]] = []

    async def send_sms(self, mobile: str, code: str) -> None:
        self.sent.append(("sms", mobile, code))

    async def send_email(self, email: str, code: str) -> None:
        self.sent.append(("email", email, code))


class Msg91OtpProvider(OtpDeliveryProvider):
    """PLACEHOLDER — complete after MSG91 is live (Auth Key, DLT template/sender,
    exact endpoints/params). Will use httpx + get_settings().msg91_*."""
    async def send_sms(self, mobile: str, code: str) -> None:
        raise NotImplementedError("MSG91 SMS pending account setup")
    async def send_email(self, email: str, code: str) -> None:
        raise NotImplementedError("MSG91 email pending account setup")


def get_otp_provider() -> OtpDeliveryProvider:
    if get_settings().otp_provider == "msg91":
        return Msg91OtpProvider()
    return MockOtpProvider()