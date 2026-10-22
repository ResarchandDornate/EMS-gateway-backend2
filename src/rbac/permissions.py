# The full permission catalog
PERMISSIONS = [
    "dashboard:view", "telemetry:read", "soc:read", "solis:read",
    "alarms:read", "alarms:ack", "anomaly:read", "history:read",
    "config:read", "config:write", "command:write", "data:export",
    "user:manage", "site:manage", "org:manage",
]

ROLES = ["super_admin", "org_admin", "site_operator"]

# Which permissions each role holds by default
ROLE_PERMISSIONS = {
    "super_admin": PERMISSIONS,  # everything
    "org_admin": [
        "dashboard:view", "telemetry:read", "soc:read", "solis:read",
        "alarms:read", "alarms:ack", "anomaly:read", "history:read",
        "config:read", "config:write", "command:write", "data:export",
        "user:manage",
    ],
    "site_operator": [
        "dashboard:view", "telemetry:read", "soc:read", "solis:read",
        "alarms:read", "alarms:ack", "anomaly:read", "history:read",
        "config:read",
    ],
}

# Default allowed state when seeding an org: reads ON, sensitive writes OFF
DEFAULT_ORG_PERMISSION_ALLOWED = {p: True for p in PERMISSIONS}
for _p in ("config:write", "command:write", "data:export",
           "site:manage", "org:manage"):
    DEFAULT_ORG_PERMISSION_ALLOWED[_p] = False