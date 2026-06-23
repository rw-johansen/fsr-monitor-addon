#!/usr/bin/env python3
"""
FireServiceRota WebSocket Monitor  –  med MQTT til Home Assistant
===================================================================
Lytter på alle 3 WebSocket-kanaler fra FireServiceRota API.
Viser beskeder live på skærmen (3-panel layout) og skriver
til separate daglige logfiler pr. kanal.

Publicerer incident-status og per-person respons til MQTT,
så Home Assistant kan vise udkaldsstatus på stationen.

MQTT-emner der publiceres:
  fsr/incident/state              → JSON med aktiv hændelse (eller tom)
  fsr/incident/response/<user_id> → JSON med en persons status

Installation:
    pip install websockets rich pyfireservicerota paho-mqtt

Kørsel:
    python fsr_monitor_mqtt.py
"""

import asyncio
import json
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# Dansk tidszone – håndterer automatisk sommer-/vintertid (CEST/CET)
LOCAL_TZ = ZoneInfo("Europe/Copenhagen")

# Reference til hoved-event-loop, sat i main() – bruges til at
# kalde async refresh-funktionen fra MQTT-knappens callback-tråd
_main_loop = None

import paho.mqtt.client as mqtt
import websockets
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.text import Text

from pyfireservicerota import (
    FireServiceRota,
    InvalidAuthError,
    ExpiredTokenError,
    InvalidTokenError,
)

# ═══════════════════════════════════════════════════════════
#  KONFIGURATION
#  Læses fra environment variables (HA add-on) hvis tilgængelige,
#  ellers bruges standardværdierne herunder som fallback ved
#  kørsel udenfor HA.
# ═══════════════════════════════════════════════════════════

import os

EMAIL    = os.environ.get("FSR_EMAIL",    "DIN_EMAIL_HER")
PASSWORD = os.environ.get("FSR_PASSWORD", "DIT_PASSWORD_HER")
GROUP_ID = os.environ.get("FSR_GROUP_ID", "DIT_GROUP_ID_HER")

BASE_URL     = "www.fireservicerota.co.uk"
BASE_WSS_URL = "wss://www.fireservicerota.co.uk/cable"

# ── MQTT ───────────────────────────────────────────────────
MQTT_HOST     = os.environ.get("FSR_MQTT_HOST",     "core-mosquitto")
MQTT_PORT     = int(os.environ.get("FSR_MQTT_PORT", "1883"))
MQTT_USER     = os.environ.get("FSR_MQTT_USER",     "")
MQTT_PASSWORD = os.environ.get("FSR_MQTT_PASSWORD", "")
MQTT_PREFIX   = os.environ.get("FSR_MQTT_PREFIX",   "fsr")

# ── Logfiler ───────────────────────────────────────────────
# I HA add-on skrives logs til /data (persistent storage)
_log_base = "/data" if os.path.isdir("/data") else "."
LOG_DIR   = Path(_log_base) / "fsr_logs"
MAX_LINES = 40

TOKEN_REFRESH_BUFFER_SEC = 300

# ═══════════════════════════════════════════════════════════
#  KANALER
# ═══════════════════════════════════════════════════════════

CHANNELS = [
    {
        "id":    "incidents",
        "label": "🚨  HÆNDELSER",
        "color": "red",
        "params": {
            "channel":  "IncidentNotificationsChannel",
            "group_id": str(GROUP_ID),
        },
    },
    {
        "id":    "schedules",
        "label": "📅  VAGTPLANER",
        "color": "green",
        "params": {
            "channel":  "CombinedSchedulesNotificationsChannel",
            "group_id": str(GROUP_ID),
        },
    },
    {
        "id":    "availability",
        "label": "🚒  KØRETØJSSTATUS",
        "color": "yellow",
        "params": {
            "channel":  "AvailabilityRequirementNotificationsChannel",
            "group_id": str(GROUP_ID),
        },
    },
]

# ───────────────────────────────────────────────────────────
# MQTT-KLIENT
# ───────────────────────────────────────────────────────────

def _build_mqtt_client() -> mqtt.Client:
    def _on_message(client, userdata, msg):
        if msg.topic == f"{MQTT_PREFIX}/command/refresh_availability":
            print("[REFRESH] Manuel genopfriskning anmodet via knap", flush=True)
            if _main_loop is not None:
                asyncio.run_coroutine_threadsafe(_do_manual_refresh(), _main_loop)

    try:
        # paho-mqtt >= 2.0
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id="fsr_monitor",
            clean_session=True,
        )
    except AttributeError:
        # paho-mqtt < 2.0 fallback
        client = mqtt.Client(client_id="fsr_monitor", clean_session=True)
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.will_set(
        f"{MQTT_PREFIX}/status",
        payload="offline",
        retain=True,
    )
    client.on_connect = lambda c, *_: c.publish(
        f"{MQTT_PREFIX}/status", "online", retain=True
    )
    client.on_message = _on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    # Vent til forbindelsen er etableret og publiser online synkront,
    # så HA ikke når at markere sensorer som utilgængelige ved genstart.
    import time
    for _ in range(20):
        if client.is_connected():
            client.publish(f"{MQTT_PREFIX}/status", "online", retain=True)
            client.subscribe(f"{MQTT_PREFIX}/command/refresh_availability")
            break
        time.sleep(0.1)

    return client


try:
    mqtt_client = _build_mqtt_client()
    mqtt_status = f"Forbundet til {MQTT_HOST}:{MQTT_PORT}"
except Exception as e:
    mqtt_client = None
    mqtt_status = f"MQTT FEJL: {e}"


def mqtt_publish(topic: str, payload: dict, retain: bool = False) -> None:
    """Publicer JSON-payload. Fejler lydløst hvis klient ikke er klar."""
    if mqtt_client is None:
        return
    try:
        mqtt_client.publish(
            f"{MQTT_PREFIX}/{topic}",
            json.dumps(payload, ensure_ascii=False),
            retain=retain,
        )
    except Exception:
        pass


# ───────────────────────────────────────────────────────────
# MQTT AUTO-DISCOVERY
# ───────────────────────────────────────────────────────────

# Hold styr på hvilke user_ids der allerede har fået en discovery-besked
_discovered_users: set[int] = set()

# Cache: user_id → navn (udfyldes fra incident-responses)
_user_names: dict[int, str] = {}

_HA_DISCOVERY_PREFIX = "homeassistant"

_DEVICE = {
    "identifiers": [f"fsr_station_{GROUP_ID}"],
    "name":         f"FireServiceRota Station {GROUP_ID}",
    "manufacturer": "FireServiceRota",
    "model":        "WebSocket Monitor",
}


def publish_discovery_incident() -> None:
    """Registrer hændelses-sensoren i HA via auto-discovery."""
    if mqtt_client is None:
        return
    payload = {
        "name":                    "FSR Hændelse",
        "unique_id":               f"fsr_{GROUP_ID}_incident_state",
        "state_topic":             f"{MQTT_PREFIX}/incident/state",
        "json_attributes_topic":   f"{MQTT_PREFIX}/incident/state",
        "value_template":          "{{ value_json.state }}",
        "icon":                    "mdi:fire-truck",
        "availability_topic":      f"{MQTT_PREFIX}/status",
        "payload_available":       "online",
        "payload_not_available":   "offline",
        "device":                  _DEVICE,
    }
    topic = f"{_HA_DISCOVERY_PREFIX}/sensor/fsr_{GROUP_ID}_incident/config"
    try:
        mqtt_client.publish(topic, json.dumps(payload, ensure_ascii=False), retain=True)
        log_write("_token", "Auto-discovery: hændelses-sensor registreret")
    except Exception:
        pass


def publish_discovery_person(user_id: int, name: str, nickname: str = "") -> None:
    """Registrer én persons sensor i HA via auto-discovery (første gang vi ser dem)."""
    if mqtt_client is None or user_id in _discovered_users:
        return
    _discovered_users.add(user_id)

    display = nickname if nickname else name
    payload = {
        "name":                    display,
        "unique_id":               f"fsr_{GROUP_ID}_response_{user_id}",
        "state_topic":             f"{MQTT_PREFIX}/incident/response/{user_id}",
        "json_attributes_topic":   f"{MQTT_PREFIX}/incident/response/{user_id}",
        "value_template":          "{{ value_json.status_label }}",
        "icon":                    "mdi:account-hard-hat",
        "availability_topic":      f"{MQTT_PREFIX}/status",
        "payload_available":       "online",
        "payload_not_available":   "offline",
        "device":                  _DEVICE,
    }
    topic = (
        f"{_HA_DISCOVERY_PREFIX}/sensor/"
        f"fsr_{GROUP_ID}_response_{user_id}/config"
    )
    try:
        mqtt_client.publish(topic, json.dumps(payload, ensure_ascii=False), retain=True)
        log_write("_token", f"Auto-discovery: {display} ({user_id}) registreret")
    except Exception:
        pass


# ───────────────────────────────────────────────────────────
# INCIDENT → MQTT
# ───────────────────────────────────────────────────────────

# Statusord der vises i HA (oversættes til dansk)
_STATUS_LABEL = {
    "acknowledged": "Bekræftet",
    "pending":      "Afventer",
    "rejected":     "Afvist",
}

_CHANNEL_LABEL = {
    "pager":          "Pager",
    "smartphone_app": "App",
    "unknown":        "Ukendt",
}

# ───────────────────────────────────────────────────────────
# AVAILABILITY → MQTT
# ───────────────────────────────────────────────────────────

_LEVEL_LABEL = {
    "above_buffer": "OK",
    "at_buffer":    "På grænsen",
    "below_buffer": "Undermandet",
    "critical":     "Kritisk",
}

def publish_discovery_availability(ar_id: int, ar_name: str) -> None:
    """Registrer én station/køretøjsstatus-sensor i HA via auto-discovery."""
    if mqtt_client is None:
        return
    payload = {
        "name":                    f"FSR Bemanding – {ar_name}",
        "unique_id":               f"fsr_{GROUP_ID}_availability_{ar_id}",
        "state_topic":             f"{MQTT_PREFIX}/availability/{ar_id}",
        "json_attributes_topic":   f"{MQTT_PREFIX}/availability/{ar_id}",
        "value_template":          "{{ value_json.level_label }}",
        "icon":                    "mdi:fire-station",
        "availability_topic":      f"{MQTT_PREFIX}/status",
        "payload_available":       "online",
        "payload_not_available":   "offline",
        "device":                  _DEVICE,
    }
    topic = (
        f"{_HA_DISCOVERY_PREFIX}/sensor/"
        f"fsr_{GROUP_ID}_availability_{ar_id}/config"
    )
    try:
        mqtt_client.publish(topic, json.dumps(payload, ensure_ascii=False), retain=True)
        log_write("_token", f"Auto-discovery: bemanding {ar_name} ({ar_id}) registreret")
    except Exception:
        pass


_discovered_availability: set[int] = set()
_discovered_user_availability: set[int] = set()


def publish_discovery_refresh_button() -> None:
    """Registrer en knap i HA der trigger manuel genopfriskning af availability."""
    if mqtt_client is None:
        return
    payload = {
        "name":                  "FSR – Genopfrisk tilgængelighed",
        "unique_id":             f"fsr_{GROUP_ID}_refresh_availability",
        "command_topic":         f"{MQTT_PREFIX}/command/refresh_availability",
        "payload_press":         "PRESS",
        "icon":                  "mdi:refresh",
        "availability_topic":    f"{MQTT_PREFIX}/status",
        "payload_available":     "online",
        "payload_not_available": "offline",
        "device":                _DEVICE,
    }
    topic = f"{_HA_DISCOVERY_PREFIX}/button/fsr_{GROUP_ID}_refresh_availability/config"
    try:
        mqtt_client.publish(topic, json.dumps(payload, ensure_ascii=False), retain=True)
        log_write("_token", "Auto-discovery: refresh-knap registreret")
    except Exception:
        pass


async def _do_manual_refresh() -> None:
    """
    Køres når 'Genopfrisk tilgængelighed'-knappen trykkes i HA.
    Henter et frisk live snapshot direkte via REST – ingen cache,
    ingen WebSocket-gen-abonnement nødvendig.
    """
    print("[REFRESH] Henter frisk availability-snapshot via REST...", flush=True)
    try:
        await fetch_current_availability()
    except Exception as e:
        print(f"[REFRESH] Fejl: {e}", flush=True)


async def availability_periodic_refresh_loop(interval_sec: int = 60) -> None:
    """
    Henter automatisk et frisk REST-snapshot hvert minut, så status
    er aktuel uden at brugeren skal trykke på genopfriskningsknappen.
    """
    while True:
        await asyncio.sleep(interval_sec)
        try:
            await fetch_current_availability()
        except Exception as e:
            print(f"[AVAILABILITY] Periodisk opdatering fejlede: {e}", flush=True)


def publish_discovery_user_availability(user_id: int, name: str) -> None:
    """Registrer én brugers tilgængeligheds-sensor i HA via auto-discovery."""
    if mqtt_client is None or user_id in _discovered_user_availability:
        return
    _discovered_user_availability.add(user_id)
    payload = {
        "name":                    f"{name} – Tilgængelighed",
        "unique_id":               f"fsr_{GROUP_ID}_user_{user_id}_availability",
        "state_topic":             f"{MQTT_PREFIX}/user/{user_id}/availability",
        "json_attributes_topic":   f"{MQTT_PREFIX}/user/{user_id}/availability",
        "value_template":          "{{ value_json.status_label }}",
        "icon":                    "mdi:account-clock",
        "availability_topic":      f"{MQTT_PREFIX}/status",
        "payload_available":       "online",
        "payload_not_available":   "offline",
        "device":                  _DEVICE,
    }
    topic = f"{_HA_DISCOVERY_PREFIX}/sensor/fsr_{GROUP_ID}_user_{user_id}_availability/config"
    try:
        mqtt_client.publish(topic, json.dumps(payload, ensure_ascii=False), retain=True)
    except Exception:
        pass


def publish_all_discoveries() -> None:
    """
    Publicerer auto-discovery for ALLE kendte brugere ved opstart.
    Kræver at _user_names er fyldt via prefill_user_names() først.
    """
    count_incident = 0
    count_avail    = 0
    for uid, name in _user_names.items():
        # Incident response-sensor
        publish_discovery_person(uid, name=name)
        count_incident += 1
        # Availability-sensor
        publish_discovery_user_availability(uid, name)
        count_avail += 1
        # Publicer initial "ikke tilgængelig" så sensoren ikke er tom
        mqtt_publish(
            f"user/{uid}/availability",
            {
                "user_id":      uid,
                "name":         name,
                "available":    False,
                "status_label": "Ikke tilgængelig",
                "skill_ids":    [],
                "availability_code": "",
            },
            retain=True,
        )
    log_write("_token", f"Startup discovery: {count_incident} incident + {count_avail} availability sensorer publiceret")
    print(f"  → {count_incident} sensorer oprettet for udkald og tilgængelighed", flush=True)


def publish_incident(msg: dict) -> None:
    """
    Kaldes ved hvert incident-besked.
    Publicerer:
      fsr/incident/state                – overordnet hændelse
      fsr/incident/response/<user_id>   – én sensor pr. person

    Registrerer automatisk nye personer i HA via auto-discovery.
    """
    incident_id = msg.get("id")
    state       = msg.get("state", "unknown")
    body        = msg.get("body", "").replace("\r\n", " / ")
    location    = msg.get("location", "")
    prio        = msg.get("prio", "")

    # Overordnet hændelse
    mqtt_publish(
        "incident/state",
        {
            "incident_id": incident_id,
            "state":       state,
            "active":      state not in ("finished", "closed"),
            "body":        body,
            "location":    location,
            "prio":        prio,
            "updated_at":  datetime.now().isoformat(timespec="seconds"),
        },
        retain=True,
    )

    # Per-person status
    for r in msg.get("incident_responses", []):
        user_id  = r.get("user_id")
        raw_stat = r.get("status", "pending")

        # Gem navn i cache – bruges til at berige availability-data
        if user_id and r.get("user_name"):
            _user_names[user_id] = r["user_name"]

        # Registrer personen i HA første gang vi ser dem
        publish_discovery_person(
            user_id,
            name=r.get("user_name", ""),
            nickname=r.get("user_nickname", ""),
        )

        mqtt_publish(
            f"incident/response/{user_id}",
            {
                "user_id":       user_id,
                "name":          r.get("user_name", ""),
                "nickname":      r.get("user_nickname", ""),
                "status":        raw_stat,
                "status_label":  _STATUS_LABEL.get(raw_stat, raw_stat),
                "reported":      r.get("reported_status", ""),
                "channel":       _CHANNEL_LABEL.get(r.get("channel", ""), r.get("channel", "")),
                "on_duty":       r.get("on_duty", False),
                "responded_at":  r.get("responded_at", ""),
                "incident_id":   incident_id,
                "active":        state not in ("finished", "closed"),
            },
            retain=True,
        )




# ───────────────────────────────────────────────────────────
# TOKEN-MANAGER
# ───────────────────────────────────────────────────────────

class TokenManager:
    def __init__(self):
        self._api:        FireServiceRota | None = None
        self._token_info: dict                   = {}
        self._lock                               = asyncio.Lock()
        self._expires_at: float                  = 0.0
        self.status:      str                    = "Ikke logget ind"

    def _build_api(self, token_info: dict | None = None) -> FireServiceRota:
        if token_info:
            return FireServiceRota(base_url=BASE_URL, token_info=token_info)
        return FireServiceRota(base_url=BASE_URL, username=EMAIL, password=PASSWORD)

    def _store(self, token_info: dict) -> None:
        self._token_info = token_info
        expires_in       = int(token_info.get("expires_in", 7200))
        self._expires_at = asyncio.get_event_loop().time() + expires_in
        self.status      = f"Token OK (udløber om {expires_in // 60} min)"
        log_write("_token", f"Nyt token – udløber om {expires_in}s")

    async def ensure_valid(self) -> str:
        async with self._lock:
            now = asyncio.get_event_loop().time()
            if (self._token_info.get("access_token")
                    and self._expires_at - now > TOKEN_REFRESH_BUFFER_SEC):
                return self._token_info["access_token"]

            loop = asyncio.get_event_loop()

            if self._token_info:
                try:
                    self.status = "Fornyer token..."
                    api  = self._build_api(self._token_info)
                    info = await loop.run_in_executor(None, api.refresh_tokens)
                    self._api = api
                    self._store(info)
                    return info["access_token"]
                except (ExpiredTokenError, InvalidTokenError, InvalidAuthError) as e:
                    self.status = f"Refresh fejlede: {e}"
                    log_write("_token", f"REFRESH FEJL: {e}")

            try:
                self.status = "Logger ind..."
                api  = self._build_api()
                info = await loop.run_in_executor(None, api.request_tokens)
                self._api = api
                self._store(info)
                return info["access_token"]
            except InvalidAuthError as e:
                self.status = f"Login FEJL: {e}"
                log_write("_token", f"LOGIN FEJL: {e}")
                raise

    async def background_refresh_loop(self) -> None:
        while True:
            now      = asyncio.get_event_loop().time()
            sleep_for = max(30.0, self._expires_at - now - TOKEN_REFRESH_BUFFER_SEC)
            await asyncio.sleep(sleep_for)
            try:
                await self.ensure_valid()
            except Exception:
                await asyncio.sleep(60)


token_mgr = TokenManager()

# ───────────────────────────────────────────────────────────
# Interne datastrukturer
# ───────────────────────────────────────────────────────────

buffers  = {ch["id"]: deque(maxlen=MAX_LINES) for ch in CHANNELS}
statuses = {ch["id"]: "⏳ Venter..." for ch in CHANNELS}


# ───────────────────────────────────────────────────────────
# LOGFIL
# ───────────────────────────────────────────────────────────

def log_path(channel_id: str) -> Path:
    date = datetime.now().strftime("%Y-%m-%d")
    d    = LOG_DIR / channel_id
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{channel_id}_{date}.log"


def log_write(channel_id: str, text: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(log_path(channel_id), "a", encoding="utf-8") as f:
            f.write(f"[{ts}]  {text}\n")
    except Exception:
        pass


# ───────────────────────────────────────────────────────────
# RICH SKÆRM-LAYOUT
# ───────────────────────────────────────────────────────────

def build_layout() -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=3),
    )
    layout["body"].split_row(
        Layout(name="p0"),
        Layout(name="p1"),
        Layout(name="p2"),
    )
    return layout


def render_panel(ch: dict) -> Panel:
    cid   = ch["id"]
    col   = ch["color"]
    lines = list(buffers[cid])
    body  = Text()
    for ts, line in lines:
        body.append(f"{ts} ", style="dim")
        body.append(line + "\n", style="white")
    title = Text()
    title.append(ch["label"], style=f"bold {col}")
    title.append(f"  [{statuses[cid]}]", style="dim")
    return Panel(body, title=title, border_style=col, expand=True)


def render_header() -> Panel:
    now = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    return Panel(
        Text(
            f"FireServiceRota Monitor  •  {now}  •  {token_mgr.status}  •  MQTT: {mqtt_status}",
            justify="center", style="bold white",
        ),
        style="blue",
    )


def render_footer() -> Panel:
    parts = [
        f"[{ch['color']}]{ch['id']}[/{ch['color']}] -> {log_path(ch['id'])}"
        for ch in CHANNELS
    ]
    return Panel(
        Text.from_markup("  |  ".join(parts), justify="center"),
        title="[dim]Logfiler (ny fil pr. dato)[/dim]",
        style="blue",
    )


# ───────────────────────────────────────────────────────────
# BESKED-OPSUMMERING
# ───────────────────────────────────────────────────────────

def summarise(channel_id: str, msg: dict) -> str:
    inner = msg.get("message", msg)
    if not isinstance(inner, dict):
        return str(inner)[:120]
    if channel_id == "incidents":
        trig   = inner.get("trigger", "?")
        body   = inner.get("message_to_speech",
                 inner.get("body", inner.get("message", "")))
        inc_id = inner.get("id", inner.get("incident_id", "?"))
        return f"[{trig}] #{inc_id}  {str(body)[:75]}"
    if channel_id == "schedules":
        kind  = inner.get("type", inner.get("schedule_type", "?"))
        start = str(inner.get("start_time", "?"))[:16]
        end   = str(inner.get("end_time", "?"))[:16]
        mem   = inner.get("membership", {})
        name  = mem.get("full_name", "") if isinstance(mem, dict) else ""
        return f"[{kind}] {name}  {start} -> {end}"
    if channel_id == "availability":
        ar = inner.get("availability_requirement", {})
        wl = inner.get("warning_level", "?")
        sl = inner.get("service_level", "?")
        return f"[{ar.get('name','?')}]  warn={wl}  svc={sl}"
    return str(list(inner.keys())[:5])


# ───────────────────────────────────────────────────────────
# WEBSOCKET-LYTTER
# ───────────────────────────────────────────────────────────

async def listen(ch: dict, reconnect_delay: int = 5) -> None:
    cid = ch["id"]

    while True:
        try:
            token = await token_mgr.ensure_valid()
            url   = f"{BASE_WSS_URL}?access_token={token}"
            sub   = json.dumps({
                "command":    "subscribe",
                "identifier": json.dumps(ch["params"]),
            })

            statuses[cid] = "Forbinder..."
            async with websockets.connect(
                url,
                ping_interval=30,
                ping_timeout=10,
                max_size=10 * 1024 * 1024,   # 10 MB – store incident-payloads
            ) as ws:
                statuses[cid] = "Forbundet"
                log_write(cid, f"=== FORBUNDET – {ch['params']['channel']} ===")
                await ws.send(sub)

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        msg = {"_raw": raw}

                    mtype = msg.get("type", "")
                    if mtype in ("ping", "welcome"):
                        continue
                    if mtype == "confirm_subscription":
                        statuses[cid] = "Subscribed OK"
                        log_write(cid, "Subscription bekraeftet")
                        continue

                    log_write(cid, json.dumps(msg, ensure_ascii=False))
                    ts = datetime.now().strftime("%H:%M:%S")
                    buffers[cid].append((ts, summarise(cid, msg)))

                    # ── MQTT: publicer kun for incidents-kanalen ──
                    # (availability håndteres nu via periodisk REST-polling,
                    #  se fetch_current_availability() – WebSocket-kanalen
                    #  for availability bruges kun til log/visning herover)
                    if cid == "incidents":
                        inner = msg.get("message", {})
                        if isinstance(inner, dict) and inner.get("id"):
                            publish_incident(inner)

        except (websockets.ConnectionClosed, ConnectionRefusedError, OSError) as exc:
            statuses[cid] = "Afbrudt – genforbinder..."
            log_write(cid, f"AFBRUDT: {exc}")
            await asyncio.sleep(reconnect_delay)
        except Exception as exc:
            statuses[cid] = f"FEJL: {exc}"
            log_write(cid, f"FEJL: {exc}")
            await asyncio.sleep(reconnect_delay)


# ───────────────────────────────────────────────────────────
# BRUGERNAVN-PREFILL  (REST API)
# ───────────────────────────────────────────────────────────

async def prefill_user_names() -> None:
    """
    Henter alle medlemmer af gruppen via FireServiceRota REST API
    og fylder _user_names-cachen, så navne er tilgængelige med
    det samme – også inden det første udkald.

    Bruger requests-biblioteket (samme som pyfireservicerota) for at
    undgå at Cloudflare blokerer User-Agent.

    Prøver endpoints i rækkefølge og logger rå svar til logfilen
    ved fejl, så endpoint og feltnavn nemt kan justeres.
    """
    import requests as req_lib

    token = await token_mgr.ensure_valid()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept":        "application/json",
    }

    # /api/v2/users returnerer first_name, last_name og nickname direkte.
    # /api/v2/memberships har user_id men ingen navnefelter – bruges som fallback
    # hvis users-endpoint ikke er tilgængeligt.
    endpoints = [
        f"https://{BASE_URL}/api/v2/users?group_id={GROUP_ID}&per_page=500",
        f"https://{BASE_URL}/api/v2/memberships?group_id={GROUP_ID}&per_page=500",
    ]

    loop = asyncio.get_event_loop()

    for url in endpoints:
        def _fetch(u=url, h=headers):
            resp = req_lib.get(u, headers=h, timeout=10)
            return resp.status_code, resp.text

        try:
            status, raw = await loop.run_in_executor(None, _fetch)

            if status == 404:
                log_write("_token", f"Prefill 404 (endpoint findes ikke): {url}")
                continue

            if status != 200:
                log_write("_token", f"Prefill HTTP {status} fra {url}: {raw[:300]}")
                print(f"  → HTTP {status} for {url}")
                continue

            data = json.loads(raw)

            # Log rå struktur til fejlfinding
            preview = json.dumps(data, ensure_ascii=False)[:500]
            log_write("_token", f"Prefill svar fra {url}: {preview}")

            # Håndter både liste og dict
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = (
                    data.get("memberships")
                    or data.get("employees")
                    or data.get("users")
                    or data.get("data")
                    or []
                )
                if not items:
                    log_write("_token", f"Prefill: ingen items – nøgler: {list(data.keys())}")
                    continue
            else:
                continue

            count = 0
            for item in items:
                uid = item.get("user_id") or item.get("id")
                name = (
                    item.get("nickname")
                    or item.get("full_name")
                    or item.get("name")
                    or item.get("user_name")
                    or (
                        f"{item.get('first_name', '')} {item.get('last_name', '')}".strip()
                        or None
                    )
                )
                if uid and name:
                    _user_names[uid] = name
                    count += 1

            if count:
                log_write("_token", f"Prefill: {count} brugernavne hentet fra {url}")
                print(f"  → {count} brugernavne hentet fra API")
                return
            else:
                sample = json.dumps(items[0], ensure_ascii=False)[:300] if items else "tom"
                log_write("_token", f"Prefill: ingen uid+navn-par fundet. Første item: {sample}")

        except Exception as e:
            log_write("_token", f"Prefill fejl for {url}: {e}")
            print(f"  → Fejl: {e}")

    log_write("_token", "Prefill: ingen endpoints virkede – tjek logfil for rå svar")
    print("  → Kunne ikke hente brugernavne – tjek fsr_logs/_token/ for detaljer")
    print("     (navne udfyldes løbende fra udkald i mellemtiden)")


# ───────────────────────────────────────────────────────────
# AVAILABILITY VIA REST  (bekræftet virkende endpoints – ingen
# tidszone-matching nødvendig, da vi spørger om "nu" direkte)
# ───────────────────────────────────────────────────────────

# Kendte requirement-ID'er, navne og skill-minimumskrav
_known_ar_ids: set[int] = set()
_ar_names: dict[int, str] = {}
_ar_skill_minimums: dict[int, dict[int, int]] = {}


async def fetch_ar_definitions() -> None:
    """
    Henter bemandingskravenes definitioner (navn + minimum pr. skill).

    Bekræftet endpoint: GET /api/v2/availability_requirements?group_id=X
    Returnerer KUN definitioner (ingen live status) – bruges til at
    berige det live snapshot fra fetch_current_availability() med
    minimumskrav pr. skill, så vi kan vise "above_buffer"/"below_buffer".

    Bemærk: feltet "assigned" i single_skill_availability_requirements
    er forvirrende navngivet – det er det KONFIGUREREDE MINIMUM,
    ikke en live optælling.
    """
    import requests as req_lib

    token = await token_mgr.ensure_valid()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept":        "application/json",
    }
    url    = f"https://{BASE_URL}/api/v2/availability_requirements"
    params = {"group_id": GROUP_ID}

    loop = asyncio.get_event_loop()

    def _fetch():
        resp = req_lib.get(url, headers=headers, params=params, timeout=10)
        return resp.status_code, resp.text

    try:
        status, raw = await loop.run_in_executor(None, _fetch)
        if status != 200:
            print(f"[AR] HTTP {status}: {raw[:200]}", flush=True)
            return

        data = json.loads(raw)
        for entry in data:
            ar_id = entry.get("id")
            if not ar_id:
                continue
            _ar_names[ar_id] = entry.get("name", str(ar_id))
            _known_ar_ids.add(ar_id)

            mins: dict[int, int] = {}
            for ss in entry.get("single_skill_availability_requirements", []):
                skill_id = ss.get("skill_id")
                minimum  = ss.get("assigned", 0)
                if skill_id is not None:
                    mins[skill_id] = minimum
            _ar_skill_minimums[ar_id] = mins

        names = ", ".join(_ar_names.values())
        print(f"  → {len(data)} bemandingskrav hentet ({names})", flush=True)

    except Exception as e:
        print(f"[AR] Fejl ved hentning af krav-definitioner: {e}", flush=True)


async def fetch_current_availability() -> bool:
    """
    Henter et live availability-snapshot via REST – bekræftet endpoint:

        GET /api/v2/memberships/combined_schedules
            ?group_ids=<GROUP_ID>&start_time=<nu, ISO8601 med offset>

    Når start_time=nu, returnerer API'et for hver person ÉT interval
    der dækker netop nu – ingen klient-side tidszone-matching er altså
    nødvendig, modsat den gamle WebSocket-baserede tilgang.

    Bruges: ved opstart, hvert minut i baggrunden, og ved tryk på
    "Genopfrisk tilgængelighed"-knappen.
    """
    import requests as req_lib

    token = await token_mgr.ensure_valid()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept":        "application/json",
    }

    now = datetime.now(LOCAL_TZ).replace(microsecond=0)
    url = f"https://{BASE_URL}/api/v2/memberships/combined_schedules"
    params = {
        "group_ids":  GROUP_ID,
        "start_time": now.isoformat(),
    }

    loop = asyncio.get_event_loop()

    def _fetch():
        resp = req_lib.get(url, headers=headers, params=params, timeout=10)
        return resp.status_code, resp.text

    try:
        status, raw = await loop.run_in_executor(None, _fetch)
    except Exception as e:
        print(f"[AVAILABILITY] REST-fejl: {e}", flush=True)
        return False

    if status != 200:
        print(f"[AVAILABILITY] HTTP {status}: {raw[:300]}", flush=True)
        log_write("_token", f"combined_schedules HTTP {status}: {raw[:500]}")
        return False

    try:
        data = json.loads(raw)
    except Exception as e:
        print(f"[AVAILABILITY] Kunne ikke parse JSON: {e}", flush=True)
        return False

    if not isinstance(data, list):
        print(f"[AVAILABILITY] Uventet svar-type: {type(data)}", flush=True)
        return False

    # Filtrér til hovedgruppen – andre group_id'er i svaret er Hold 1-4
    own_entries = [e for e in data if str(e.get("group_id")) == str(GROUP_ID)]

    available_people: list[dict] = []
    per_skill_names: dict[int, list[str]] = {}

    for entry in own_entries:
        uid       = entry.get("user_id")
        intervals = entry.get("intervals", [])
        if not uid or not intervals:
            continue

        iv           = intervals[0]   # Matcher altid forespurgt start_time = nu
        is_available = bool(iv.get("available"))
        skill_ids    = iv.get("skill_ids", [])
        name         = _user_names.get(uid, f"bruger_{uid}")

        publish_discovery_user_availability(uid, name)
        mqtt_publish(
            f"user/{uid}/availability",
            {
                "user_id":               uid,
                "name":                  name,
                "available":             is_available,
                "status_label":          "Tilgængelig" if is_available else "Ikke tilgængelig",
                "skill_ids":             skill_ids,
                "assigned_function_ids": iv.get("assigned_function_ids", []),
                "valid_until":           iv.get("end_time", ""),
                "updated_at":            datetime.now().isoformat(timespec="seconds"),
            },
            retain=True,
        )

        if is_available:
            available_people.append({"user_id": uid, "name": name})
            for sk in skill_ids:
                per_skill_names.setdefault(sk, []).append(name)

    # Byg gruppe-niveau opsummering pr. kendt bemandingskrav
    for ar_id in (_known_ar_ids or {1274}):
        ar_name  = _ar_names.get(ar_id, str(ar_id))
        minimums = _ar_skill_minimums.get(ar_id, {})

        skill_summary = []
        for skill_id, minimum in minimums.items():
            names = per_skill_names.get(skill_id, [])
            if len(names) >= minimum:
                level = "above_buffer"
            elif names:
                level = "at_buffer"
            else:
                level = "below_buffer"
            skill_summary.append({
                "skill_id": skill_id,
                "assigned": len(names),
                "minimum":  minimum,
                "level":    level,
                "names":    names,
            })

        overall_ok = all(s["assigned"] >= s["minimum"] for s in skill_summary) if skill_summary else True

        if ar_id not in _discovered_availability:
            _discovered_availability.add(ar_id)
            publish_discovery_availability(ar_id, ar_name)

        mqtt_publish(
            f"availability/{ar_id}",
            {
                "ar_id":            ar_id,
                "name":             ar_name,
                "level":            "above_buffer" if overall_ok else "below_buffer",
                "level_label":      "OK" if overall_ok else "Undermandet",
                "available_count":  len(available_people),
                "available_people": available_people,
                "skill_statuses":   skill_summary,
                "updated_at":       datetime.now().isoformat(timespec="seconds"),
            },
            retain=True,
        )

    print(
        f"[AVAILABILITY] REST-snapshot: {len(available_people)} tilgængelige "
        f"af {len(own_entries)} medlemmer",
        flush=True,
    )
    return True


async def main() -> None:
    global _main_loop
    _main_loop = asyncio.get_event_loop()

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    print("Henter access token via pyfireservicerota...")
    try:
        await token_mgr.ensure_valid()
        print(f"OK: {token_mgr.status}")
    except Exception as exc:
        print(f"\nFEJL ved login: {exc}")
        print("Tjek EMAIL og PASSWORD i toppen af scriptet.")
        return

    print("Henter brugernavne fra API...")
    await prefill_user_names()

    print("Opretter sensorer for alle brugere...")
    publish_all_discoveries()

    # Registrer hændelses-sensoren og refresh-knappen i HA med det samme
    publish_discovery_incident()
    publish_discovery_refresh_button()

    print("Henter bemandingskrav (skills/minimum)...")
    await fetch_ar_definitions()

    print("Henter frisk availability-snapshot via REST...")
    got_snapshot = await fetch_current_availability()
    if got_snapshot:
        print("  → Snapshot hentet og publiceret")
    else:
        print("  → Snapshot fejlede – tjek log ovenfor")
        print("     (opdateres automatisk hvert minut og ved manuel genopfriskning)")

    layout  = build_layout()
    console = Console()

    tasks = [
        asyncio.create_task(token_mgr.background_refresh_loop()),
        asyncio.create_task(availability_periodic_refresh_loop()),
        *[asyncio.create_task(listen(ch)) for ch in CHANNELS],
    ]

    with Live(layout, refresh_per_second=2, screen=True, console=console):
        try:
            while True:
                layout["header"].update(render_header())
                layout["footer"].update(render_footer())
                for i, ch in enumerate(CHANNELS):
                    layout["body"][f"p{i}"].update(render_panel(ch))
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStoppet (Ctrl+C).")
