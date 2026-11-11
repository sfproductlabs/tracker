#!/bin/bash
set -uo pipefail
cd /opt/tracker
export STAMP=$(date +%s)
systemctl stop tracker
cp -a tracker tracker.old-$STAMP 2>/dev/null || true
cp /opt/tracker-src/tracker-test /opt/tracker/tracker
chmod +x /opt/tracker/tracker
python3 - <<'PY'
import json, os
p="/opt/tracker/config.json"
c=json.load(open(p))
c["UseCHCerts"]=True
c["CHCertDB"]="sfpla"; c["CHCertTable"]="mcerts"; c["CHCertRefreshSeconds"]=30
c["UseTelemetryGate"]=False  # defense-in-depth; enable per-tenant once registered in site_configs
c["ProxyPort"]=":80"; c["ProxyPortRedirect"]=""
json.dump(c,open(p,"w"),indent=2)
print("config updated: UseCHCerts=True ProxyPort=:80 backup=tracker.old-%s"%os.environ["STAMP"])
PY
systemctl start tracker
sleep 8
echo "=== status ==="; systemctl is-active tracker
echo "=== cert manager / errors ==="
journalctl -u tracker --no-pager -n 60 2>&1 | grep -iE "CH cert manager|cert manager failed|Fatal|panic|Serving TLS|Serving HTTP|Disabling GeoIP" | tail -12
echo "=== ports ==="; ss -tlnp 2>/dev/null | grep tracker | awk '{print $4}' | sort
