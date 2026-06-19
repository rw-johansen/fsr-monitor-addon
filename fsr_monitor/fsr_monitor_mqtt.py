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
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# Dansk tidszone – håndterer automatisk sommer-/vintertid (CEST/CET)
LOCAL_TZ = ZoneInfo("Europe/Copenhagen")

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
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    # Vent til forbindelsen er etableret og publiser online synkront,
    # så HA ikke når at markere sensorer som utilgængelige ved genstart.
    import time
    for _ in range(20):
        if client.is_connected():
            client.publish(f"{MQTT_PREFIX}/status", "online", retain=True)
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


def _parse_fsr_time(ts: str):
    """
    Parser FSR's ISO8601-tidsstempler til et tidszone-bevidst datetime-objekt.
    Håndterer både 'Z'-suffiks (UTC) og tidsstempler uden tidszone-info
    (antages da at være dansk lokal tid).
    Returnerer None hvis tidsstemplet ikke kan tolkes.
    """
    if not ts:
        return None
    try:
        cleaned = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=LOCAL_TZ)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def publish_availability(msg: dict) -> None:
    """
    Publicerer bemandingsstatus til MQTT.

    Faktisk beskedstruktur fra FSR:
    {
      "availability_requirement": { "id": 1274, "name": "Nyborg", ... },
      "start_time": "...",
      "end_time": "...",
      "intervals": [
        {
          "start_time": "...",
          "end_time": "...",
          "warning_level": "above_buffer",
          "service_level": "in_service",
          "available_memberships": [...],
          "skill_statuses": [...]
        },
        ...
      ]
    }

    Finder det interval der er aktivt nu, og publicerer det til MQTT.
    MQTT-emne: fsr/availability/<requirement_id>
    """
    ar      = msg.get("availability_requirement", {})
    ar_id   = ar.get("id")
    ar_name = ar.get("name", str(ar_id))

    if not ar_id:
        print(f"[AVAILABILITY] Ingen ar_id – nøgler: {list(msg.keys())}", flush=True)
        return

    # Auto-discovery første gang
    if ar_id not in _discovered_availability:
        _discovered_availability.add(ar_id)
        publish_discovery_availability(ar_id, ar_name)

    # Find det aktive interval (start <= nu < slut) – tidszone-bevidst sammenligning
    now_dt    = datetime.now(timezone.utc)
    intervals = msg.get("intervals", [])
    interval  = None

    for iv in intervals:
        start_dt = _parse_fsr_time(iv.get("start_time", ""))
        end_dt   = _parse_fsr_time(iv.get("end_time", ""))
        if start_dt and end_dt and start_dt <= now_dt <= end_dt:
            interval = iv
            break

    # Fallback: brug det interval hvis intet matcher præcist (kan ske ved data-gaps)
    if interval is None and intervals:
        interval = intervals[0]
        print(
            f"[AVAILABILITY] Intet interval matchede {now_dt.isoformat()} – "
            f"bruger fallback (første interval i listen)",
            flush=True,
        )

    if interval is None:
        print(f"[AVAILABILITY] Ingen intervals i besked for {ar_name}", flush=True)
        return

    # Midlertidig log – bekræfter at det rigtige interval vælges
    print(
        f"[AVAILABILITY] {ar_name}: interval {interval.get('start_time')} "
        f"-> {interval.get('end_time')} (nu UTC={now_dt.isoformat()}), "
        f"{len(interval.get('available_memberships', []))} tilgængelige",
        flush=True,
    )

    warning_level   = interval.get("warning_level", "")
    service_level   = interval.get("service_level", "")
    memberships     = interval.get("available_memberships", [])
    available_count = len(memberships)

    # Navneliste fra cache + publicer per-bruger availability
    available_user_ids = {m.get("user_id") for m in memberships}
    available_people   = []

    for m in memberships:
        uid   = m.get("user_id")
        name  = _user_names.get(uid, f"bruger_{uid}")
        code  = m.get("availability_code", "")
        funcs = m.get("assigned_functions", [])
        skills = m.get("skill_ids", [])

        available_people.append({
            "user_id":           uid,
            "name":              name,
            "availability_code": code,
            "assigned_functions": funcs,
            "skill_ids":         skills,
        })

        # Publicer individuel tilgængeligheds-sensor
        publish_discovery_user_availability(uid, name)
        mqtt_publish(
            f"user/{uid}/availability",
            {
                "user_id":            uid,
                "name":               name,
                "available":          True,
                "status_label":       "Tilgængelig",
                "availability_code":  code,
                "assigned_functions": funcs,
                "skill_ids":          skills,
            },
            retain=True,
        )

    # Sæt alle IKKE-tilgængelige brugere til "Ikke tilgængelig"
    for uid, name in _user_names.items():
        if uid not in available_user_ids:
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

    # Skill-status med navne
    skill_summary = []
    for ss in interval.get("skill_statuses", []):
        assigned_names = [
            _user_names.get(sm.get("user_id"), f"bruger_{sm.get('user_id')}")
            for sm in ss.get("available_memberships", [])
        ]
        skill_summary.append({
            "skill_id": ss.get("skill_id"),
            "assigned": ss.get("assigned_count", 0),
            "minimum":  ss.get("minimum", 0),
            "level":    ss.get("level", ""),
            "names":    assigned_names,
        })

    mqtt_publish(
        f"availability/{ar_id}",
        {
            "ar_id":            ar_id,
            "name":             ar_name,
            "level":            warning_level,
            "level_label":      _LEVEL_LABEL.get(warning_level, warning_level),
            "service_level":    service_level,
            "available_count":  available_count,
            "available_people": available_people,
            "skill_statuses":   skill_summary,
            "interval_start":   interval.get("start_time", ""),
            "interval_end":     interval.get("end_time", ""),
            "updated_at":       datetime.now().isoformat(timespec="seconds"),
        },
        retain=True,
    )

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

                    # Log interval-struktur (fjernes når det virker)
                    if cid == "availability":
                        inner_msg  = msg.get("message", {})
                        intervals  = inner_msg.get("intervals", [])
                        print(f"[AV] intervals count={len(intervals)}", flush=True)
                        if intervals:
                            iv = intervals[0]
                            print(f"[AV] INTERVAL[0] KEYS: {sorted(iv.keys())}", flush=True)
                            print(f"[AV] INTERVAL[0] warning_level={iv.get('warning_level','–')}", flush=True)
                            am = iv.get("available_memberships", [])
                            print(f"[AV] INTERVAL[0] available_memberships count={len(am)}", flush=True)
                            if am:
                                print(f"[AV] FIRST MEMBER KEYS: {sorted(am[0].keys())}", flush=True)

                    # ── MQTT: publicer kun for incidents-kanalen ──
                    if cid == "incidents":
                        inner = msg.get("message", {})
                        if isinstance(inner, dict) and inner.get("id"):
                            publish_incident(inner)

                    # ── MQTT: publicer for availability-kanalen ──
                    elif cid == "availability":
                        inner = msg.get("message", msg)
                        if isinstance(inner, (dict, list)):
                            try:
                                publish_availability(inner)
                            except Exception as e:
                                log_write(cid, f"MQTT availability fejl: {e}")
                                print(f"[AVAILABILITY FEJL] {e}", flush=True)
                        else:
                            print(f"[AVAILABILITY] Uventet type: {type(inner)} – {str(inner)[:200]}", flush=True)
                            log_write(cid, f"Uventet type: {type(inner)} – {str(inner)[:200]}")

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
# MAIN
# ───────────────────────────────────────────────────────────

async def main() -> None:
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

    # Registrer hændelses-sensoren i HA med det samme
    publish_discovery_incident()

    layout  = build_layout()
    console = Console()

    tasks = [
        asyncio.create_task(token_mgr.background_refresh_loop()),
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
