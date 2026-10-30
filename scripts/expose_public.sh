#!/bin/sh
# Adds ems-gateway-api.unityess.cloud -> localhost:8040 to the main tunnel.
# Run with: sudo sh scripts/expose_public.sh
set -e
CFG=/etc/cloudflared/config.yml
HOST=ems-gateway-api.unityess.cloud

if grep -q "hostname: $HOST" "$CFG"; then
  echo "$HOST already in $CFG"; exit 0
fi

cp "$CFG" "$CFG.bak.$(date +%Y%m%d-%H%M%S)"
# insert just above the catch-all 404 rule
awk -v host="$HOST" '
  /^  - service: http_status:404/ { print "  - hostname: " host; print "    service: http://localhost:8040" }
  { print }
' "$CFG" > "$CFG.new"
/opt/homebrew/bin/cloudflared tunnel --config "$CFG.new" ingress validate
mv "$CFG.new" "$CFG"

launchctl kickstart -k system/com.cloudflare.cloudflared
echo "restarted cloudflared; check: curl https://$HOST/health"
