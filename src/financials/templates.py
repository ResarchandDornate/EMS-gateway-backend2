"""State ToD tariff templates — the starting point a site fills from.

A template carries the SLOTS and their modifiers (regulatory, public), not the
base energy charge, which is the site's own bill. `verified` marks the states
whose slots come from a published order/review (the ToD note of 24 Nov 2026);
every other state carries the central framework default — an eight-hour solar
rebate and an evening peak surcharge — flagged so it is confirmed before use.

Modifier is a percent of the base EC, except where a state sets a flat paise
amount (Gujarat), which uses modifier_rs instead. These are reference data, so
they live in code and are served read-only; a site's own saved values live in
the revenue_config table.
"""

_FRAMEWORK_SLOTS = [
    {"name": "Solar", "start": "09:00", "end": "17:00", "modifier_pct": -20},
    {"name": "Peak", "start": "18:00", "end": "22:00", "modifier_pct": 20},
    {"name": "Off-Peak", "start": "22:00", "end": "06:00", "modifier_pct": -10},
    {"name": "Normal", "start": "06:00", "end": "09:00", "modifier_pct": 0},
]
_FRAMEWORK_NOTE = (
    "Standard ToD framework — confirm the slots and percentages against your "
    "DISCOM's current tariff order."
)


def _framework(state: str, base_ec: float = 8.0, solar: float = 2.5) -> dict:
    return {
        "state": state, "financial_year": "FY27", "verified": False,
        "base_energy_charge_rs": base_ec, "solar_charge_value_rs": solar,
        "slots": [dict(s) for s in _FRAMEWORK_SLOTS], "note": _FRAMEWORK_NOTE,
    }


STATE_TARIFFS: list[dict] = [
    # --- Verified from the ToD note (24 Nov 2026) ---
    {
        "state": "Maharashtra", "discom": "MSEDCL", "financial_year": "FY27", "verified": True,
        "base_energy_charge_rs": 8.0, "solar_charge_value_rs": 2.5,
        "note": "MERC continues the 9 AM–5 PM solar-hours concession for C&I; peak surcharge applies evening onward. Confirm the exact percentages in the MERC order.",
        "slots": [
            {"name": "Solar", "start": "09:00", "end": "17:00", "modifier_pct": -25},
            {"name": "Peak", "start": "17:00", "end": "24:00", "modifier_pct": 25},
            {"name": "Normal", "start": "06:00", "end": "09:00", "modifier_pct": 0},
            {"name": "Night", "start": "00:00", "end": "06:00", "modifier_pct": 0},
        ],
    },
    {
        "state": "Gujarat", "discom": "GERC", "financial_year": "FY27", "verified": True,
        "base_energy_charge_rs": 7.5, "solar_charge_value_rs": 2.5,
        "note": "FY27 replaces the night concession with a flat 30 paise/unit solar-hours rebate (11 AM–3 PM) for certain HT categories; peak is licensee-specific. Flat paise, not a percentage.",
        "slots": [
            {"name": "Solar", "start": "11:00", "end": "15:00", "modifier_rs": -0.30},
            {"name": "Peak", "start": "17:00", "end": "23:00", "modifier_pct": 20},
            {"name": "Normal", "start": "06:00", "end": "11:00", "modifier_pct": 0},
        ],
    },
    {
        "state": "Uttarakhand", "discom": "UPCL", "financial_year": "FY27", "verified": True,
        "base_energy_charge_rs": 7.0, "solar_charge_value_rs": 2.5,
        "note": "22.5% solar-hours rebate (9 AM–5 PM) for LT/HT industrial above 25 kW; 25% off-peak rebate and 30% peak surcharge.",
        "slots": [
            {"name": "Solar", "start": "09:00", "end": "17:00", "modifier_pct": -22.5},
            {"name": "Peak", "start": "18:00", "end": "23:00", "modifier_pct": 30},
            {"name": "Off-Peak", "start": "23:00", "end": "06:00", "modifier_pct": -25},
            {"name": "Normal", "start": "06:00", "end": "09:00", "modifier_pct": 0},
        ],
    },
    {
        "state": "Odisha", "discom": "TPCODL / OERC", "financial_year": "FY27", "verified": True,
        "base_energy_charge_rs": 7.0, "solar_charge_value_rs": 2.5,
        "note": "From 1 Jul 2026, smart-metered consumers above 10 kW: 10% lower in solar hours, 10% higher in peak.",
        "slots": [
            {"name": "Solar", "start": "08:00", "end": "16:00", "modifier_pct": -10},
            {"name": "Peak", "start": "18:00", "end": "24:00", "modifier_pct": 10},
            {"name": "Normal", "start": "06:00", "end": "08:00", "modifier_pct": 0},
        ],
    },
    {
        "state": "Kerala", "discom": "KSEB", "financial_year": "FY27", "verified": True,
        "base_energy_charge_rs": 7.5, "solar_charge_value_rs": 2.5,
        "note": "HT schedule is night-based: normal 06–18, peak 18–22, off-peak 22–06. No solar-hours rebate slot; confirm the peak/off-peak percentages in the KSERC order.",
        "slots": [
            {"name": "Peak", "start": "18:00", "end": "22:00", "modifier_pct": 10},
            {"name": "Off-Peak", "start": "22:00", "end": "06:00", "modifier_pct": -10},
            {"name": "Normal", "start": "06:00", "end": "18:00", "modifier_pct": 0},
        ],
    },
    {
        "state": "Jammu & Kashmir", "discom": "JPDCL / KPDCL", "financial_year": "FY27", "verified": True,
        "base_energy_charge_rs": 7.0, "solar_charge_value_rs": 2.5,
        "note": "20% solar-hours rebate for consumers above 10 kW; 20% peak surcharge for industrial and commercial.",
        "slots": [
            {"name": "Solar", "start": "09:00", "end": "17:00", "modifier_pct": -20},
            {"name": "Peak", "start": "18:00", "end": "22:00", "modifier_pct": 20},
            {"name": "Normal", "start": "06:00", "end": "09:00", "modifier_pct": 0},
        ],
    },
    {
        "state": "Delhi", "discom": "BRPL / BYPL / TPDDL", "financial_year": "FY27", "verified": True,
        "base_energy_charge_rs": 8.5, "solar_charge_value_rs": 2.5,
        "note": "Reported 20% off-peak rebate and 20% evening peak surcharge; verify against the DERC order.",
        "slots": [
            {"name": "Solar", "start": "09:00", "end": "17:00", "modifier_pct": -20},
            {"name": "Peak", "start": "17:00", "end": "23:00", "modifier_pct": 20},
            {"name": "Off-Peak", "start": "23:00", "end": "06:00", "modifier_pct": -20},
            {"name": "Normal", "start": "06:00", "end": "09:00", "modifier_pct": 0},
        ],
    },
    {
        "state": "Uttar Pradesh", "discom": "UPPCL", "financial_year": "FY27", "verified": True,
        "base_energy_charge_rs": 8.0, "solar_charge_value_rs": 2.5,
        "note": "Reported 15% evening peak surcharge (5–11 PM); solar-hours rebate per the framework. Verify against the UPERC order.",
        "slots": [
            {"name": "Solar", "start": "09:00", "end": "17:00", "modifier_pct": -15},
            {"name": "Peak", "start": "17:00", "end": "23:00", "modifier_pct": 15},
            {"name": "Normal", "start": "06:00", "end": "09:00", "modifier_pct": 0},
        ],
    },

    # --- Framework default (confirm against the state's order) ---
    _framework("Andhra Pradesh", 8.0),
    _framework("Arunachal Pradesh", 7.0),
    _framework("Assam", 7.5),
    _framework("Bihar", 8.0),
    _framework("Chhattisgarh", 7.0),
    _framework("Goa", 6.5),
    _framework("Haryana", 8.0),
    _framework("Himachal Pradesh", 6.5),
    _framework("Jharkhand", 7.5),
    _framework("Karnataka", 8.0),
    _framework("Madhya Pradesh", 8.0),
    _framework("Manipur", 7.0),
    _framework("Meghalaya", 7.0),
    _framework("Mizoram", 7.0),
    _framework("Nagaland", 7.0),
    _framework("Punjab", 8.0),
    _framework("Rajasthan", 8.0),
    _framework("Sikkim", 7.0),
    _framework("Tamil Nadu", 8.0),
    _framework("Telangana", 8.0),
    _framework("Tripura", 7.0),
    _framework("West Bengal", 8.0),
    _framework("Chandigarh", 7.0),
    _framework("Puducherry", 7.5),
    _framework("Ladakh", 7.0),
]
