#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
lacrosse2mqtt — Home Assistant Add-on.

Liest den RF-Stream vom LaCrosseGateway (DataPort2), decodiert IT+-Frames
(wie FHEMs 36_LaCrosse.pm) und publisht nach MQTT mit HA-Discovery.

  lacrosse/<Name>/temperature|humidity|battery|battery_low  (retained)
  homeassistant/.../lacrosse2mqtt_<sensor>_<id>/config       (Discovery, retained)
  lacrosse/events/batteryReplaced  {"name","old_addr","new_addr"}
  lacrosse2mqtt/set/replaceBatteryForSec  {"name":"Sensor01","sec":60,"ignore_battery":false}
  lacrosse2mqtt/set/rename  {"addr":58,"name":"Bad EG"}   | set/delete | set/list
  lacrosse2mqtt/status  online/offline (LWT)

Konfiguration über Add-on-Optionen (HA-UI), Zustand (Sensor-Map, Pairing-Fenster)
persistiert in /data/state.json (Add-on-Backups).
"""
import socket, json, time, os, sys, struct, threading, logging, re

OPTIONS_PATH = "/data/options.json"
STATE_PATH = "/data/state.json"
CLIENT_ID = "lacrosse2mqtt"
LWT_TOPIC = "lacrosse2mqtt/status"
KEEPALIVE = 30

log = logging.getLogger("lacrosse2mqtt")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    stream=sys.stdout)


def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=1)
    os.replace(tmp, path)


# ---------- MQTT (nur stdlib) ----------

def _s(x):
    b = x.encode() if isinstance(x, str) else x
    return struct.pack(">H", len(b)) + b


def _l(n):
    o = b""
    while True:
        b = n % 128
        n //= 128
        if n:
            b |= 0x80
        o += bytes([b])
        if not n:
            return o


class MQTTClient:
    def __init__(self, host, port, user, passwd, on_message=None):
        self.host, self.port = host, port
        self.user, self.passwd = user, passwd
        self.on_message = on_message
        self.sock = None
        self.lock = threading.Lock()
        self.cache = {}          # topic -> (payload, retain) — republish nach Reconnect
        self.connected = False

    def connect(self):
        s = socket.create_connection((self.host, self.port), timeout=15)
        s.settimeout(3)
        flags = 0xEE  # clean session + will(QoS1, retain) + user/pass
        body = b"\x00\x04MQTT" + bytes([0x04, flags]) + struct.pack(">H", KEEPALIVE)
        body += _s(CLIENT_ID) + _s(LWT_TOPIC) + _s("offline")
        body += _s(self.user) + _s(self.passwd)
        s.sendall(b"\x10" + _l(len(body)) + body)
        resp = s.recv(4)
        if len(resp) < 4 or resp[0] >> 4 != 2 or resp[3] != 0:
            raise RuntimeError(f"MQTT CONNACK rc={resp[3] if len(resp) > 3 else '?'}")
        self.sock = s
        self.connected = True
        log.info("MQTT connected %s:%s", self.host, self.port)
        self._send_pub(LWT_TOPIC, "online", True)
        self._send_sub([("lacrosse2mqtt/set/#", 1)])
        for t, (p, r) in list(self.cache.items()):
            if t != LWT_TOPIC:
                self._send_pub(t, p, r)

    def _send_pub(self, topic, payload, retain):
        body = _s(topic) + (payload.encode() if isinstance(payload, str) else payload)
        header = 0x30 | (0x01 if retain else 0x00)
        with self.lock:
            self.sock.sendall(bytes([header]) + _l(len(body)) + body)

    def _send_sub(self, subs):
        self.pid = (getattr(self, "pid", 0) % 65535) + 1
        body = struct.pack(">H", self.pid)
        for t, q in subs:
            body += _s(t) + bytes([q])
        with self.lock:
            self.sock.sendall(b"\x82" + _l(len(body)) + body)

    def publish(self, topic, payload, retain=True):
        self.cache[topic] = (payload, retain)
        if self.connected:
            try:
                self._send_pub(topic, payload, retain)
            except Exception as e:
                log.warning("publish failed (%s) — cached", e)
                self.connected = False

    def loop(self):
        last_ping = 0.0
        buf = b""
        while True:
            try:
                if not self.connected:
                    buf = b""
                    time.sleep(5)
                    self.connect()
                    last_ping = time.time()
                    continue
                now = time.time()
                if now - last_ping > KEEPALIVE - 5:
                    with self.lock:
                        self.sock.sendall(b"\xc0\x00")
                    last_ping = now
                try:
                    data = self.sock.recv(65536)
                except socket.timeout:
                    continue
                if not data:
                    raise ConnectionError("closed by broker")
                buf += data
                while True:
                    if len(buf) < 2:
                        break
                    ln, i, mult = 0, 1, 1
                    while i < len(buf):
                        b = buf[i]
                        ln += (b & 0x7F) * mult
                        mult *= 128
                        i += 1
                        if not (b & 0x80):
                            break
                    if len(buf) < i + ln:
                        break
                    hb = buf[0]
                    pkt = buf[i:i + ln]
                    buf = buf[i + ln:]
                    t4 = hb >> 4
                    if t4 == 3:  # PUBLISH
                        tl = struct.unpack(">H", pkt[:2])[0]
                        topic = pkt[2:2 + tl].decode("utf-8", "replace")
                        idx = 2 + tl
                        qos = (hb >> 1) & 0x03
                        if qos:
                            pid = struct.unpack(">H", pkt[idx:idx + 2])[0]
                            idx += 2
                            with self.lock:
                                self.sock.sendall(b"\x40" + _l(2) + struct.pack(">H", pid))
                        payload = pkt[idx:].decode("utf-8", "replace")
                        if self.on_message:
                            try:
                                self.on_message(topic, payload)
                            except Exception:
                                log.exception("on_message error")
            except Exception as e:
                log.warning("MQTT loop: %s — reconnect in 5s", e)
                self.connected = False
                if self.sock:
                    try:
                        self.sock.close()
                    except Exception:
                        pass
                self.sock = None


# ---------- Decoder (Logik = FHEM 36_LaCrosse.pm) ----------

class Decoder:
    def __init__(self, state, opts, mqtt):
        self.state = state
        self.cfg = opts
        self.mqtt = mqtt
        self.prefix = opts.get("topic_prefix", "lacrosse")
        self.last_data = None   # Zeitstempel des letzten Sensor-Frames (OK 9 ...)
        self.last_any = None    # Zeitstempel der letzten Zeile vom LGW (KV-Status alle 10s)

    def handle_line(self, line):
        self.last_any = time.time()
        armed = self.state.get("armed")
        if armed and time.time() > armed["until"]:
            log.info("replaceBattery window expired for %s", armed["name"])
            self.state["armed"] = None
            save_json(STATE_PATH, self.state)

        if line.startswith("OK VALUES "):
            self.handle_kv(line)
            return
        if line == "" or line.startswith("["):
            return  # Gateway-Banner/Leerzeilen
        if line == "LGW ALIVE":
            self.mqtt.publish("lacrosse2mqtt/gateway/alive", time.strftime("%Y-%m-%dT%H:%M:%S"), retain=False)
            return
        if not line.startswith("OK 9 "):
            self.mqtt.publish("lacrosse2mqtt/raw/frames", line, retain=False)
            return

        parts = line.split()
        if len(parts) != 7 or not all(p.isdigit() for p in parts[2:]):
            self.mqtt.publish("lacrosse2mqtt/raw/frames", line, retain=False)
            return
        self.last_data = time.time()
        addr, b1, t_msb, t_lsb, hum_raw = (int(p) for p in parts[2:7])
        battery_new = (b1 & 0x80) >> 7
        temperature = (t_msb * 256 + t_lsb - 1000) / 10.0
        humidity = hum_raw & 0x7F
        battery_low = (hum_raw & 0x80) >> 7

        key = str(addr)
        sensors = self.state["sensors"]
        entry = sensors.get(key)
        if entry and entry.get("name"):
            if not (-50.0 <= temperature <= 60.0):
                log.debug("Unplaussible Temperatur von %s: %.1f (Frame verworfen)", entry["name"], temperature)
                return
            if not (0 <= humidity <= 99):
                # H > 99 (z. B. 106) = Frame-Typ, den wir nicht sauber dekodieren
                # -> nur Temperatur publishen, Feuchte auslassen (wie FHEM: ungueltig ignorieren)
                log.info("%s: H=%d ungueltig -> nur Temperatur published (%.1f)", entry["name"], humidity, temperature)
                self.pub_values(entry["name"], battery_low, temperature, None)
            else:
                self.pub_values(entry["name"], battery_low, temperature, humidity)
            if battery_new:
                self.mqtt.publish(f"{self.prefix}/{entry['name']}/newBattery", "yes", retain=False)
            return

        if armed and (battery_new or armed.get("ignore_battery")):
            old = next((a for a, v in sensors.items() if v.get("name") == armed["name"]), None)
            if old is not None:
                del sensors[old]
            sensors[key] = {"name": armed["name"]}
            self.state["armed"] = None
            save_json(STATE_PATH, self.state)
            log.info("Batterie gewechselt: %s %s -> %s", armed["name"], old, addr)
            self.mqtt.publish(f"{self.prefix}/events/batteryReplaced",
                              json.dumps({"name": armed["name"], "old_addr": old, "new_addr": addr}),
                              retain=False)
            self.pub_discovery()
            self.pub_values(armed["name"], battery_low, temperature, humidity)
            return

        now_ts = time.time()
        last_logged = getattr(self, "_unknown_last_log", {})
        if now_ts - last_logged.get(key, 0) > 600:  # max 1x/10 min pro addr ins Log
            log.info("UNBEKANTER Sensor addr=%s T=%.1f H=%d (replaceBatteryForSec oder rename moeglich)",
                     addr, temperature, humidity)
            last_logged[key] = now_ts
        self.state.setdefault("unknown", {})[key] = time.strftime("%Y-%m-%d %H:%M")
        save_json(STATE_PATH, self.state)
        self.mqtt.publish(f"{self.prefix}/events/unknownSensor",
                          json.dumps({"addr": addr, "temperature": temperature, "humidity": humidity}),
                          retain=False)

    def handle_kv(self, line):
        # "OK VALUES LGW 264788 UpTimeSeconds=...,..."
        m = re.match(r"OK VALUES (\S+) (\S+) (.*)", line)
        if not m:
            return
        tag, ident, rest = m.groups()
        d = dict(kv.split("=", 1) for kv in rest.split(",") if "=" in kv)
        self.mqtt.publish(f"lacrosse2mqtt/gateway/{tag}/{ident}/status",
                          json.dumps(d, indent=0))

    def pub_values(self, name, battery_low, temperature, humidity):
        base = f"{self.prefix}/{name}"
        self.mqtt.publish(f"{base}/temperature", f"{temperature:.1f}")
        if humidity is not None:
            self.mqtt.publish(f"{base}/humidity", str(int(humidity)))
        self.mqtt.publish(f"{base}/battery", "low" if battery_low else "ok")
        self.mqtt.publish(f"{base}/battery_low", "low" if battery_low else "ok")

    def pub_discovery(self):
        for key, v in self.state["sensors"].items():
            name = v.get("name")
            if not name:
                continue
            dev = {"identifiers": [f"lacrosse2mqtt_{name}"], "name": name,
                   "manufacturer": "LaCrosse", "model": "IT+ (LaCrosseGateway)"}
            avail = {"availability_topic": LWT_TOPIC, "payload_available": "online",
                     "payload_not_available": "offline"}
            base = f"{self.prefix}/{name}"
            cfgs = [
                ("sensor", f"{name}_temp", {
                    "name": "Temperatur", "device_class": "temperature", "state_class": "measurement",
                    "state_topic": f"{base}/temperature", "unit_of_measurement": "°C",
                    "unique_id": f"lacrosse2mqtt_{name}_temp", "device": dev, **avail}),
                ("sensor", f"{name}_hum", {
                    "name": "Feuchte", "device_class": "humidity", "state_class": "measurement",
                    "state_topic": f"{base}/humidity", "unit_of_measurement": "%",
                    "unique_id": f"lacrosse2mqtt_{name}_hum", "device": dev, **avail}),
                ("binary_sensor", f"{name}_batt", {
                    "name": "Batterie schwach", "device_class": "battery",
                    "state_topic": f"{base}/battery_low",
                    "payload_on": "low", "payload_off": "ok",
                    "unique_id": f"lacrosse2mqtt_{name}_batt", "device": dev, **avail}),
            ]
            for kind, uid, payload in cfgs:
                self.mqtt.publish(f"homeassistant/{kind}/lacrosse2mqtt_{uid}/config",
                                  json.dumps(payload))

    def pub_gateway_discovery(self):
        """Discovery-Entities: Gateway-Datenalter + Timeout-Alarm."""
        dev = {"identifiers": ["lacrosse2mqtt_gw"], "name": "LaCrosseGateway",
               "manufacturer": "LaCrosse", "model": "LaCrosseGateway"}
        avail = {"availability_topic": LWT_TOPIC, "payload_available": "online",
                 "payload_not_available": "offline"}
        cfgs = [
            ("sensor", "gw_data_age", {
                "name": "Datenalter", "state_topic": "lacrosse2mqtt/gateway/data_age_s",
                "unit_of_measurement": "s", "icon": "mdi:timer-outline",
                "unique_id": "lacrosse2mqtt_gw_data_age", "device": dev, **avail}),
            ("binary_sensor", "gw_timeout", {
                "name": "Keine Sensordaten", "device_class": "problem",
                "state_topic": "lacrosse2mqtt/gateway/timeout",
                "payload_on": "on", "payload_off": "off",
                "unique_id": "lacrosse2mqtt_gw_timeout", "device": dev, **avail}),
        ]
        for kind, uid, payload in cfgs:
            self.mqtt.publish(f"homeassistant/{kind}/lacrosse2mqtt_{uid}/config",
                              json.dumps(payload))

    def on_command(self, topic, payload):
        sub = topic.split("/set/", 1)[-1]
        try:
            data = json.loads(payload) if payload.strip() else {}
        except Exception:
            data = {}
        ack = {"cmd": sub, "ok": False}
        if sub == "replaceBatteryForSec":
            name = data.get("name")
            sec = int(data.get("sec", 60))
            if name:
                self.state["armed"] = {"name": name, "until": time.time() + sec,
                                       "ignore_battery": bool(data.get("ignore_battery", False))}
                save_json(STATE_PATH, self.state)
                log.info("replaceBattery armiert: %s fuer %ds", name, sec)
                ack["ok"] = True
        elif sub == "rename":
            addr, name = data.get("addr"), data.get("name")
            if addr is not None and name:
                key = str(addr)
                old_name = self.state["sensors"].get(key, {}).get("name")
                self.state["sensors"][key] = {"name": name}
                if old_name and old_name != name:
                    for kind, uid in (("sensor", "temp"), ("sensor", "hum"), ("binary_sensor", "batt")):
                        self.mqtt.publish(f"homeassistant/{kind}/lacrosse2mqtt_{old_name}_{uid}/config", "")
                save_json(STATE_PATH, self.state)
                self.pub_discovery()
                ack["ok"] = True
        elif sub == "delete":
            addr = data.get("addr")
            if addr is not None:
                old_name = self.state["sensors"].pop(str(addr), {}).get("name")
                if old_name:
                    for kind, uid in (("sensor", "temp"), ("sensor", "hum"), ("binary_sensor", "batt")):
                        self.mqtt.publish(f"homeassistant/{kind}/lacrosse2mqtt_{old_name}_{uid}/config", "")
                save_json(STATE_PATH, self.state)
                self.pub_discovery()
                ack["ok"] = True
        elif sub == "list":
            ack["ok"] = True
            ack["sensors"] = self.state["sensors"]
        self.mqtt.publish("lacrosse2mqtt/set/ack", json.dumps(ack), retain=False)


# ---------- LGW-Reader ----------

def lgw_loop(decoder, opts):
    host, port = opts["lgw_host"], int(opts["lgw_port"])
    while True:
        try:
            s = socket.create_connection((host, port), timeout=15)
            s.settimeout(1.0)
            log.info("LGW verbunden %s:%s", host, port)
            buf = b""
            while True:
                try:
                    buf += s.recv(65536)
                except socket.timeout:
                    # Keepalive-Gefühl: LGW sendet KV-Status alle 10 s
                    # Half-dead-Erkennung: TCP haelt, aber LGW schweigt >180 s -> Reconnect
                    if decoder.last_any and time.time() - decoder.last_any > 180:
                        log.warning("LGW schweigt seit >180 s (TCP offen, keine Zeilen) — erzwinge Reconnect")
                        s.close()
                        break
                    continue
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    decoder.handle_line(line.decode("utf-8", "replace").strip())
        except Exception as e:
            log.warning("LGW: %s — reconnect in 5s", e)
            time.sleep(5)


def main():
    opts = load_json(OPTIONS_PATH, {})
    if not opts.get("lgw_host"):
        log.error("options.json unvollstaendig: lgw_host fehlt")
        sys.exit(1)
    state = load_json(STATE_PATH, {"sensors": {}, "armed": None, "unknown": {}})
    # Sensoren aus Optionen uebernehmen (HA-UI-Liste "Name=Adresse")
    for entry in opts.get("sensors") or []:
        if "=" in entry:
            name, _, addr = entry.partition("=")
            state["sensors"][str(addr.strip())] = {"name": name.strip()}
    save_json(STATE_PATH, state)

    mqtt_host = opts.get("mqtt_host", "core-mosquitto")
    mqtt_port = int(opts.get("mqtt_port", 1883))
    mqtt_user = opts.get("mqtt_user", "")
    mqtt_pass = opts.get("mqtt_pass", "")
    mqtt = MQTTClient(mqtt_host, mqtt_port, mqtt_user, mqtt_pass)
    decoder = Decoder(state, opts, mqtt)
    mqtt.on_message = decoder.on_command
    threading.Thread(target=mqtt.loop, daemon=True).start()
    threading.Thread(target=lgw_loop, args=(decoder, opts), daemon=True).start()

    def discovery_loop():
        had = set()
        while True:
            time.sleep(10)
            names = {v.get("name") for v in state["sensors"].values() if v.get("name")}
            if names and names != had and mqtt.connected:
                decoder.pub_discovery()
                had = names

    def watchdog_loop():
        # Gateway-Daten-Watchdog: Alter des letzten Sensor-Frames + Timeout-Alarm
        # (wird auch als HA-Entity via Discovery gespiegelt — siehe pub_gateway_discovery)
        timeout_s = int(opts.get("data_timeout_min", 60)) * 60
        decoder.pub_gateway_discovery()
        last_state = None
        while True:
            time.sleep(30)
            if not mqtt.connected:
                continue
            age = (time.time() - decoder.last_data) if decoder.last_data else None
            mqtt.publish("lacrosse2mqtt/gateway/data_age_s",
                         str(int(age)) if age is not None else "-1", retain=True)
            timeout = age is None or age > timeout_s
            if timeout != last_state:
                mqtt.publish("lacrosse2mqtt/gateway/timeout",
                             "on" if timeout else "off", retain=True)
                if timeout:
                    log.warning("GATEWAY-ALARM: keine Sensordaten seit %s",
                                f"{int(age)}s" if age is not None else "Add-on-Start")
                    try:
                        import urllib.request as _ur
                        _ur.urlopen(f"http://{opts.get('lgw_host','172.16.0.11')}/reset", timeout=10)
                        log.info("Gateway /reset ausgeloesst (Timeout-Wache)")
                    except Exception as _e:
                        log.warning("Gateway /reset fehlgeschlagen: %s", _e)
                else:
                    log.info("Gateway-Daten fliessen wieder (Alter %ds)", int(age or 0))
                last_state = timeout

    threading.Thread(target=discovery_loop, daemon=True).start()
    threading.Thread(target=watchdog_loop, daemon=True).start()
    log.info("lacrosse2mqtt gestartet (LGW %s:%s -> MQTT %s:%s, %d Sensoren, Timeout %d min)",
             opts["lgw_host"], opts["lgw_port"], mqtt_host, mqtt_port,
             len(state["sensors"]), int(opts.get("data_timeout_min", 60)))
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
