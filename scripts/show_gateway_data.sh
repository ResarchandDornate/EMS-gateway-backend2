#!/bin/sh
# What has the backend received from gateways?
#   sh scripts/show_gateway_data.sh            # messages + latest value of every parameter (last 24h)
#   sh scripts/show_gateway_data.sh 1h         # same, other time window (15m, 6h, 7d ...)
#   sh scripts/show_gateway_data.sh live       # follow gateway requests as they arrive
P=ems-gateway-backend2
WINDOW=${1:-24h}

if [ "$1" = "live" ]; then
  docker logs -f --tail 20 $P-api-1 2>&1 | grep --line-buffered "/api/v1/gateway/"
  exit
fi

echo "=== Last 20 messages received (Postgres: ingest_messages) ==="
docker exec $P-postgres-1 psql -U ems -d ems -c "
  select g.gateway_key, m.stream, m.sequence, m.record_count, m.status,
         to_char(m.created_at at time zone 'Asia/Kolkata', 'YYYY-MM-DD HH24:MI:SS') as received_ist
  from ingest_messages m join gateways g on g.id = m.gateway_id
  order by m.created_at desc limit 20"

for BUCKET in telemetry_raw_90d telemetry_1y; do
  echo "=== Latest value of every parameter, last $WINDOW (Influx: $BUCKET) ==="
  docker exec $P-influxdb-1 influx query --org unityess --token ems_dev_token "
    from(bucket: \"$BUCKET\")
      |> range(start: -$WINDOW)
      |> last()
      |> keep(columns: [\"_time\", \"_measurement\", \"gateway_id\", \"asset_id\", \"_field\", \"_value\"])
      |> group()
      |> sort(columns: [\"_measurement\", \"_field\"])"
done
