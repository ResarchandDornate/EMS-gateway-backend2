from src.auth.security import (create_access_token, decode_access_token,
                               hash_password, verify_password)


def test_password_hash_and_verify():
    h = hash_password("secret123")
    assert h != "secret123"
    assert verify_password("secret123", h) is True
    assert verify_password("wrong", h) is False


def test_jwt_round_trip():
    tok = create_access_token("abc-123")
    assert decode_access_token(tok) == "abc-123"
    assert decode_access_token("garbage") is None