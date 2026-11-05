#!/usr/bin/env python3
# Docker-Healthcheck für lacrosse2mqtt (v1.1.2)
# Der Daemon schreibt alle 30 s /tmp/daemon_heartbeat (tmpfs — kein Flash-Verschleiss).
# Der Healthcheck schlägt fehl, wenn der Herzschlag aelter als 10 min ist
# (=> Container wird 'unhealthy', sichtbar in docker ps).

import os
import time

HEARTBEAT = "/tmp/daemon_heartbeat"
MAX_AGE_S = 600

if not os.path.exists(HEARTBEAT):
    print("kein Herzschlag vorhanden")
    raise SystemExit(1)

age = time.time() - os.path.getmtime(HEARTBEAT)
if age > MAX_AGE_S:
    print(f"Herzschlag alt: {int(age)}s (Limit {MAX_AGE_S}s)")
    raise SystemExit(1)

print(f"ok (Herzschlag {int(age)}s)")
