# LaCrosse 2 MQTT — Home Assistant Add-on

**LaCrosse IT+ Sensors (via LaCrosseGateway) → MQTT with HA-Discovery — without FHEM.**

Reads the RF data stream from a [LaCrosseGateway](https://github.com/rinie/LaCrosseITPlusReader)
(DataPort2), decodes LaCrosse IT+ frames exactly like FHEM's `36_LaCrosse.pm` module and
publishes them to your MQTT broker with automatic Home Assistant discovery.

## Features

- **Parallel to FHEM**: connect to a second data port of the gateway (e.g. DataPort2 = 82) while
  FHEM keeps reading DataPort1 — no cutover needed
- **Temperature, humidity, battery** per sensor (retained MQTT topics)
- **HA-Discovery**: sensors appear automatically (temperature, humidity, battery low)
- **`replaceBatteryForSec`**: re-map a sensor with a new radio address after battery change,
  without losing name/history/entities (trigger via MQTT)
- **Unknown sensors**: new IDs are reported on an `unknownSensor` event topic, not silently dropped
- **Gateway status**: KV status frames (`OK VALUES LGW ...`) published to
  `lacrosse2mqtt/gateway/...`
- **LWT**: availability topic `lacrosse2mqtt/status` (`online`/`offline`, retained)
- Sensor mapping persists in `/data/state.json` (survives add-on restarts and host reboots)

## Setup

1. **Gateway**: open the gateway web UI (`http://<lgw-ip>/`, port 80), set *DataPort2* to a free
   port (e.g. `82`), save and restart. Port 81 is typically used by FHEM (single session).
2. **MQTT**: your Mosquitto broker must be reachable (if it is the HA Mosquitto Add-on, the
   default `core-mosquitto` works). Enter your MQTT user/password in the add-on options.
3. **Install**: Add this repository to your Home Assistant add-on store, then install
   *LaCrosse 2 MQTT*.
4. **Sensors**: either let the add-on discover new sensors (they appear on
   `lacrosse/events/unknownSensor`), then rename them via MQTT, or pre-fill the
   `sensors` option map: `{"Sensor01": 58, "Sensor02": 4, ...}` (decimal radio addresses).

## MQTT topics

| Topic | Value |
|---|---|
| `lacrosse/<Sensor>/temperature` | °C (retained) |
| `lacrosse/<Sensor>/humidity` | % (retained) |
| `lacrosse/<Sensor>/battery` | `ok` / `low` (retained) |
| `lacrosse/events/batteryReplaced` | `{"name","old_addr","new_addr"}` |
| `lacrosse/events/unknownSensor` | `{"addr","temperature","humidity"}` |
| `lacrosse2mqtt/status` | `online` / `offline` (LWT) |
| `lacrosse2mqtt/gateway/LGW/<id>/status` | gateway KV status (JSON) |
| `lacrosse2mqtt/raw/frames` | unparseable frames (debug) |

## Commands (publish, QoS 1)

| Topic | Payload |
|---|---|
| `lacrosse2mqtt/set/replaceBatteryForSec` | `{"name":"Sensor01","sec":60}` |
| `lacrosse2mqtt/set/rename` | `{"addr":58,"name":"Wohnzimmer"}` |
| `lacrosse2mqtt/set/delete` | `{"addr":58}` |
| `lacrosse2mqtt/set/list` | `{}` → reply on `lacrosse2mqtt/set/ack` |

Replace-battery procedure: publish the first command, then pull the sensor's batteries
(or wait for its first frame with the new-battery flag) — the first matching frame is
re-mapped and the window closes.

## Frame format

```
OK 9 <addr> <b1> <temp_msb> <temp_lsb> <hum_raw>
  b1      & 0x80  -> new battery flag
  b1      & 0x70  -> sensor type
  b1      & 0x0F  -> channel
  temp    = (temp_msb*256 + temp_lsb - 1000) / 10
  hum     = hum_raw & 0x7F
  bat low = hum_raw & 0x80
```

## Watchdog & Telegram

- `binary_sensor.lacrosse2mqtt_gw_timeout` (discovery) goes `on` when no sensor
  data has arrived for `data_timeout_min` minutes (default 60, configurable in
  the add-on options). At that moment the daemon itself triggers a soft-reboot
  of the gateway (HTTP GET `http://<lgw_host>/reset`).
- `sensor.lacrossegateway_datenalter` shows the data age in seconds.
- For notifications, see [automation-example.yaml](automation-example.yaml) —
  a ready-to-use Telegram automation (replace the placeholder chat id).

## License

MIT — see [LICENSE](../LICENSE).
