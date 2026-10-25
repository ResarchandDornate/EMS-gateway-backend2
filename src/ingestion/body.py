import zlib

MAX_COMPRESSED_BYTES = 5 * 1024 * 1024       # 5 MiB
MAX_DECOMPRESSED_BYTES = 50 * 1024 * 1024    # 50 MiB


class BodyError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


def _gunzip_limited(raw: bytes, cap: int) -> bytes:
    """Decompress a single gzip member with a HARD output cap.

    Never produces or buffers more than cap+1 bytes: input is fed in bounded
    steps and the output is checked after each step, so a zip bomb is stopped
    at cap+1. No unbounded flush() is used. Rejects output over cap (413),
    truncated streams (400), and trailing/concatenated data (400)."""
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)      # gzip header
    out = bytearray()
    data = raw
    while True:
        want = cap + 1 - len(out)
        if want <= 0:                                # already at cap, more remains
            raise BodyError(413, "decompressed body too large")
        chunk = d.decompress(data, want)
        out += chunk
        if len(out) > cap:
            raise BodyError(413, "decompressed body too large")
        if d.eof:
            break
        data = d.unconsumed_tail
        if not chunk and not data:                   # no progress, stream not ended
            raise BodyError(400, "truncated gzip body")
    if d.unused_data:                                # bytes beyond the first member
        raise BodyError(400, "trailing data after gzip stream")
    return bytes(out)


def decode_body(raw: bytes, content_encoding: str | None) -> bytes:
    """Validate Content-Encoding and size; return decompressed bytes.
    Safe (no bypass) on truncated, concatenated, or malformed gzip."""
    if len(raw) > MAX_COMPRESSED_BYTES:
        raise BodyError(413, "request body too large")
    enc = (content_encoding or "identity").strip().lower()
    if enc in ("", "identity"):
        if len(raw) > MAX_DECOMPRESSED_BYTES:
            raise BodyError(413, "body too large")
        return raw
    if enc != "gzip":
        raise BodyError(415, f"unsupported Content-Encoding: {enc!r}")
    try:
        return _gunzip_limited(raw, MAX_DECOMPRESSED_BYTES)
    except BodyError:
        raise
    except (OSError, EOFError, zlib.error):
        raise BodyError(400, "malformed gzip body")
