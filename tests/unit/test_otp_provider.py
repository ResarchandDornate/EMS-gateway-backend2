"""Provider-selection + placeholder-guard tests (no network, no DB).

Locks the OtpDeliveryProvider factory contract and guarantees the MSG91 provider
stays a hard NotImplementedError until its API contract + account are supplied,
so the stub can never be shipped as if it delivered OTPs.

The MSG91-stub checks drive the coroutine with stdlib asyncio.run so they assert
the same behavior whether or not pytest-asyncio is installed in the interpreter.
"""
import asyncio
import types

import pytest

import src.auth.otp_provider as mod
from src.auth.otp_provider import MockOtpProvider, Msg91OtpProvider, get_otp_provider


def _provider(monkeypatch, value):
    monkeypatch.setattr(mod, "get_settings",
                        lambda: types.SimpleNamespace(otp_provider=value))


def test_factory_returns_mock_for_mock(monkeypatch):
    _provider(monkeypatch, "mock")
    assert isinstance(get_otp_provider(), MockOtpProvider)


def test_factory_returns_msg91_for_msg91(monkeypatch):
    _provider(monkeypatch, "msg91")
    assert isinstance(get_otp_provider(), Msg91OtpProvider)


def test_msg91_send_sms_still_blocked():
    with pytest.raises(NotImplementedError):
        asyncio.run(Msg91OtpProvider().send_sms("919999999999", "123456"))


def test_msg91_send_email_still_blocked():
    with pytest.raises(NotImplementedError):
        asyncio.run(Msg91OtpProvider().send_email("user@example.com", "123456"))


def test_settings_reject_unknown_otp_provider():
    from pydantic import ValidationError
    from src.config import Settings, get_settings
    base = get_settings().model_dump()
    Settings.model_validate({**base, "otp_provider": "mock"})
    Settings.model_validate({**base, "otp_provider": "msg91"})
    with pytest.raises(ValidationError):
        Settings.model_validate({**base, "otp_provider": "MSG91"})
