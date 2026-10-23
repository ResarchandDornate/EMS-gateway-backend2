import math

from src.telemetry_errors import InvalidValue, UnsupportedDataType

QUALITY_SUFFIX = "_q"

# Only the data types actually present in signal_registry + the frozen contract.
_FLOAT = {"FLOAT32", "Float"}
_INT_RANGE = {
    "U16": (0, 65535),
    "S16": (-32768, 32767),
    "U32": (0, 4294967295),
}

# Documented quality states: telemetry shows "good"; the S5-S6 delta defines
# GOOD/STALE/OFFLINE/ERROR. Case-insensitive on input, stored lowercase.
_QUALITY = {"good", "stale", "offline", "error"}


def field_type(data_type: str) -> str:
    if data_type in _FLOAT:
        return "float"
    if data_type in _INT_RANGE:
        return "int"
    raise UnsupportedDataType(f"Unsupported registry data_type: {data_type!r}")


def coerce(data_type: str, raw):
    """Typed value (float|int) or raise. Never clamps/rounds/substitutes.
    floats reject NaN/±Inf; ints reject fractional values and enforce documented range."""
    ft = field_type(data_type)
    if raw is None or isinstance(raw, bool):
        raise InvalidValue(f"invalid value {raw!r} for {data_type}")
    try:
        f = float(raw)
    except (TypeError, ValueError) as e:
        raise InvalidValue(f"cannot parse {raw!r} as a number ({data_type})") from e
    if math.isnan(f) or math.isinf(f):
        raise InvalidValue(f"NaN/Inf not allowed ({data_type})")
    if ft == "float":
        return f
    if not f.is_integer():
        raise InvalidValue(f"non-integer value {raw!r} for integer type {data_type}")
    v = int(f)
    lo, hi = _INT_RANGE[data_type]
    if not (lo <= v <= hi):
        raise InvalidValue(f"{v} out of range for {data_type} [{lo}, {hi}]")
    return v


def validate_quality(q):
    """Validate against the documented quality set; return normalized lowercase or None."""
    if q is None:
        return None
    if not isinstance(q, str):
        raise InvalidValue(f"quality must be a string, got {type(q).__name__}")
    ql = q.strip().lower()
    if ql not in _QUALITY:
        raise InvalidValue(f"unsupported quality {q!r}; allowed {sorted(_QUALITY)}")
    return ql


def quality_collisions(by_measurement: dict) -> list:
    """[(measurement, signal_key)] where <signal_key>+QUALITY_SUFFIX would overwrite
    a real signal in the same measurement."""
    out = []
    for meas, keys in by_measurement.items():
        ks = set(keys)
        for k in ks:
            if k + QUALITY_SUFFIX in ks:
                out.append((meas, k))
    return out