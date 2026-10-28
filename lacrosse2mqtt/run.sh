#!/bin/sh
set -e

echo "[run.sh] starting lacrosse2mqtt daemon..."
exec python3 /lacrosse2mqtt.py
