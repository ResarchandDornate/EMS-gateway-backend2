"""Characterization: the ORM metadata table/column set registered by the app.

This test pins the EXACT set of tables (and each table's column names) that the
application registers on ``Base.metadata`` today, by importing ``src.main`` (the real
app entrypoint, which pulls in every model the app uses).

Purpose: it is a safety net for the planned refactor. Moving model classes between
modules (Phase 2) or restructuring per-stream packages (Phase 3) must NOT change the
registered schema. If a move drops a table, renames a column, or fails to re-register a
model on the single declarative ``Base``, this test fails loudly — satisfying the
"preserve SQLAlchemy metadata registration" requirement without needing a database.

The snapshot was captured from tarun-DB @ d4eca47 on 2026-11-04. It is behaviour-locking,
not aspirational: if a FUTURE change intentionally alters the schema, update GOLDEN in the
same PR (and ship the matching Alembic migration) so the change is explicit and reviewed.
"""

# table_name -> sorted tuple of column names, as registered on Base.metadata today.
GOLDEN: dict[str, tuple[str, ...]] = {
    "admin_audit": ("action", "actor_user_id", "created_at", "id", "is_active", "is_deleted",
                    "new_value", "old_value", "target_id", "target_type", "updated_at"),
    "alarm_event_records": ("correlation_id", "created_at", "dedupe_key", "event_id", "event_type",
                            "gateway_id", "id", "is_active", "is_deleted", "message_id", "occurred_at",
                            "organization_id", "payload", "severity", "site_id", "state", "updated_at"),
    "auth_tokens": ("created_at", "expires_at", "id", "is_active", "is_deleted", "kind",
                    "token_hash", "updated_at", "used_at", "user_id"),
    "command_result_records": ("command_id", "command_type", "correlation_id", "created_at", "final_status",
                               "gateway_id", "gateway_received_at_utc", "id", "is_active", "is_deleted",
                               "message_id", "organization_id", "payload", "readback_value", "record_index",
                               "requested_at_utc", "requested_value", "site_id", "stored_at", "updated_at"),
    "configuration_audit_records": ("actor", "actor_role", "audit_id", "change_source", "correlation_id",
                                    "created_at", "gateway_id", "id", "is_active", "is_deleted", "message_id",
                                    "new_value", "occurred_at", "old_value", "organization_id", "payload",
                                    "received_at_utc", "revision", "setting_path", "site_id", "updated_at"),
    "controller_state_records": ("controller_state", "created_at", "decision", "gateway_id", "healthy", "id",
                                 "is_active", "is_deleted", "message_id", "mode", "organization_id", "payload",
                                 "previous_state", "received_at_utc", "record_index", "sample_time_utc",
                                 "site_id", "updated_at", "version"),
    "edge_ai_inference_records": ("created_at", "gateway_id", "id", "is_active", "is_deleted", "message_id",
                                  "organization_id", "payload", "received_at_utc", "record_index",
                                  "sample_time_utc", "site_id", "updated_at"),
    "fast_bess_records": ("bess_id", "created_at", "gateway_id", "id", "is_active", "is_deleted", "message_id",
                          "organization_id", "payload", "record_index", "recorded_at", "site_id", "source_id",
                          "substream", "updated_at"),
    "gateway_health_records": ("created_at", "gateway_id", "id", "is_active", "is_deleted", "message_id",
                               "organization_id", "payload", "record_index", "recorded_at", "site_id",
                               "substream", "updated_at"),
    "gateways": ("client_id", "client_secret_hash", "created_at", "enabled", "gateway_key", "id", "is_active",
                 "is_deleted", "last_seen_at", "site_id", "software_version", "updated_at"),
    "ingest_messages": ("created_at", "gateway_id", "id", "is_active", "is_deleted", "lease_expires_at",
                        "lease_owner", "message_id", "payload_hash", "record_count", "sequence", "status",
                        "stored_at", "stream", "substream", "updated_at"),
    "organization_permissions": ("allowed", "created_at", "id", "is_active", "is_deleted", "organization_id",
                                 "permission", "updated_at", "updated_by"),
    "organizations": ("created_at", "id", "is_active", "is_deleted", "name", "org_key", "status", "updated_at"),
    "otp_challenges": ("attempts", "channel", "consumed_at", "created_at", "destination", "expires_at", "id",
                       "is_active", "is_deleted", "otp_hash", "updated_at", "user_id"),
    "permissions": ("created_at", "id", "is_active", "is_deleted", "name", "updated_at"),
    "role_permissions": ("permission_id", "role_id"),
    "roles": ("created_at", "id", "is_active", "is_deleted", "name", "updated_at"),
    "signal_registry": ("asset_type", "cadence_sec", "category", "created_at", "data_type", "deadband",
                        "display_name", "enabled", "event_eligible", "id", "is_active", "is_deleted", "point_id",
                        "policy_group", "policy_mode", "primary_stream", "protocol_ref", "retention_class", "rw",
                        "signal_key", "source", "source_ref", "substream", "unit", "updated_at"),
    "sites": ("created_at", "id", "is_active", "is_deleted", "name", "organization_id", "site_key",
              "timezone", "updated_at"),
    "solis_telemetry_records": ("created_at", "gateway_id", "id", "is_active", "is_deleted", "message_id",
                                "organization_id", "payload", "received_at_utc", "record_index",
                                "sample_time_utc", "site_id", "updated_at"),
    "stream_records": ("created_at", "gateway_id", "id", "is_active", "is_deleted", "message_id",
                       "organization_id", "payload", "record_index", "recorded_at", "site_id", "source_id",
                       "stream", "substream", "updated_at"),
    "user_roles": ("role_id", "user_id"),
    "user_site_access": ("site_id", "user_id"),
    "users": ("created_at", "email", "id", "is_active", "is_deleted", "organization_id", "password_hash",
              "phone", "updated_at"),
}


def _live_snapshot() -> dict[str, tuple[str, ...]]:
    import src.main  # noqa: F401  (import for its side effect: registers every model used by the app)
    from src.models import Base
    return {name: tuple(sorted(c.name for c in table.columns))
            for name, table in Base.metadata.tables.items()}


def test_registered_table_set_is_unchanged():
    live = _live_snapshot()
    assert set(live) == set(GOLDEN), (
        "Registered table set changed.\n"
        f"  added:   {sorted(set(live) - set(GOLDEN))}\n"
        f"  removed: {sorted(set(GOLDEN) - set(live))}"
    )


def test_each_table_columns_are_unchanged():
    live = _live_snapshot()
    diffs = []
    for name, cols in GOLDEN.items():
        got = live.get(name, ())
        if got != cols:
            diffs.append(f"{name}: added {sorted(set(got) - set(cols))}, "
                         f"removed {sorted(set(cols) - set(got))}")
    assert not diffs, "Column set changed on:\n  " + "\n  ".join(diffs)


def test_single_declarative_base():
    # The refactor must not split models across two MetaData registries.
    import src.main  # noqa: F401
    from src.models import Base
    assert len(Base.metadata.tables) == len(GOLDEN)
