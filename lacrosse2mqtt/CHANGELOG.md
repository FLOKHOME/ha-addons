# Changelog

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
