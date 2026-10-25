"""Phase 9b-ii-a: pure record -> registry -> deterministic Influx point mapping.

NO I/O. No Influx writes, no ledger, no endpoint changes. This module only turns a
validated envelope plus the authenticated gateway's DB-derived identity into a list of
(bucket, influxdb_client.Point) using the existing registry validators in src.influx.

Record keys are PROVISIONAL (source_id, asset_id, signal_key, value, quality,
timestamp) and their timestamp semantics are NOT gateway-confirmed.
"""
from src.influx import TelemetryPoint, build_point, measurement_for, parse_contract_timestamp


def build_message_points(envelope, identity, index, settings):
    """Validate EVERY record and build EVERY (bucket, Point) before returning.

    A single invalid record raises (UnknownSignal / SignalMismatch / InvalidValue /
    InvalidTimestamp / UnsafeQueryInput / RetentionNotConfigured / UnresolvedMetadata /
    UnknownRetentionClass), so the whole message is rejected and nothing is produced.
    The function performs NO I/O; the caller (9b-ii-c) writes only if this returns.

    identity: DB-derived {"organization_id","site_id","gateway_id"} from the
    authenticated gateway. Client-supplied envelope IDs are NEVER used for tags.
    """
    measurement = measurement_for(envelope.stream, envelope.substream)
    org = str(identity["organization_id"])
    site = str(identity["site_id"])
    gw = str(identity["gateway_id"])
    built = []
    for rec in envelope.records:
        point = TelemetryPoint(
            measurement=measurement,
            organization_id=org,
            site_id=site,
            gateway_id=gw,
            source_id=rec.source_id,
            asset_id=rec.asset_id,
            substream=envelope.substream,
            signal_key=rec.signal_key,
            value=rec.value,
            quality=rec.quality,
            timestamp=parse_contract_timestamp(rec.timestamp),
            asset_type=None,     # registry asset_type is authoritative (build_point)
        )
        built.append(build_point(index, point, settings))
    return built
