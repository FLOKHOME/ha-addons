# Changelog

## 1.1.0 (2026-10-28)

- **Gateway-Watchdog:** new entities via Discovery — `Datenalter` sensor (seconds
  since last sensor frame, updated every 30 s) and `Keine Sensordaten` binary
  sensor (`problem`, goes `on` when no sensor data for `data_timeout_min` minutes,
  default 60). HA automations can trigger directly on it.
- **Half-dead reconnect:** if the LGW TCP connection stays open but the gateway
  sends nothing for >180 s, the add-on forces a reconnect (previously it would
  hang forever).
- New option `data_timeout_min` (int, default 60).

## 1.0.4 (2026-10-28)

- Validity guard: implausible temperature (< -50 / > 60 °C) dropped; humidity
  > 99 (non-decodable frame type) → temperature-only publish (as FHEM does).
- Unknown-sensor log cooldown: max 1 log line per address per 10 minutes.

## 1.0.3 (2026-10-28)

- Own `ENTRYPOINT` in Dockerfile — the s6-overlay of the HA base image
  (`s6-overlay-suexec: fatal: can only run as pid 1`) broke container startup.

## 1.0.0 (2026-10-28)

- Initial release
- LaCrosse IT+ decode (identical to FHEM `36_LaCrosse.pm`): temperature, humidity,
  battery-low and new-battery flags
- MQTT publish with retain + HA-Discovery (temperature, humidity, battery)
- `replaceBatteryForSec` command for battery-change re-mapping
- `rename`, `delete`, `list` commands
- unknown-sensor event topic
- gateway KV status passthrough
- LWT availability topic
