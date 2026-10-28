#!/usr/bin/with-contenv bashio
set -e

bashio::log.info "Starting lacrosse2mqtt daemon..."
exec python3 /lacrosse2mqtt.py
