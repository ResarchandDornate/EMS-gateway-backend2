from dataclasses import dataclass


@dataclass(frozen=True)
class SoCMapping:
    measurement: str
    signal_key: str
    scope: str          # "per_asset" | "single"


# NOT production-confirmed. Do NOT populate from display_soc / PCS soc / ems_system soc
# until the gateway/EMS team confirms the authoritative signal and its asset semantics.
OVERALL_SOC: SoCMapping | None = None
RACK_SOC: SoCMapping | None = None

_UNCONFIGURED = {
    "status": "unconfigured",
    "detail": "SoC source not confirmed by gateway/EMS team; no value is guessed, "
              "duplicated across racks, or averaged.",
}


def resolve_overall_soc() -> dict:
    if OVERALL_SOC is None:
        return dict(_UNCONFIGURED, which="overall_soc")
    return {"status": "configured", "mapping": OVERALL_SOC}


def resolve_rack_soc() -> dict:
    if RACK_SOC is None:
        return dict(_UNCONFIGURED, which="rack_soc")
    return {"status": "configured", "mapping": RACK_SOC}