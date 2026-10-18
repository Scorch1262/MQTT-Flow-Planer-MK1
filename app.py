"""
MQTT-Ablauf- und Verschaltungsplaner
=====================================
Ein lokal laufendes Web-Tool, um MQTT-gestuetzte Automatisierungen als
Blockschaltbild zu entwerfen - aehnlich wie in Node-RED oder im
"Ablaufplan"-Programm: Bloecke (Ereignis, Aktion, Wartezeit, Bedingung)
werden per Drag & Drop auf einer Flaeche platziert und durch Linien zu
einem Ablauf verschaltet.

Block-Typen:
  - MQTT Empfangen (Ereignis/Trigger): abonniert ein Topic bei einem
    Broker und startet den nachfolgenden Ablauf, sobald eine passende
    Nachricht ankommt.
  - MQTT Senden (Aktion): veroeffentlicht eine Nachricht auf einem
    Topic bei einem Broker. "{payload}" im Text wird durch die zuletzt
    empfangene Nachricht des ausloesenden Ereignisses ersetzt.
  - Wartezeit: pausiert den Ablauf fuer eine einstellbare Dauer.
  - Bedingung: prueft entweder die zuletzt empfangene Nachricht
    (Payload-Vergleich), die Uhrzeit, den Sonnenstand (aufgegangen/
    untergegangen), den Wochentag oder das Datum und verzweigt den
    Ablauf nach "Ja" oder "Nein".

  - Merker: speichert einen Wert (Text oder Zahl, wie ein Merker in einer
    SPS). Laeuft ein Ablauf in den Block, wird seine Aktion ausgefuehrt
    (setzen, umschalten, hoch-/herunterzaehlen, zuruecksetzen). Bedingungen
    koennen den Wert auswerten, Wartezeiten ihn als Dauer verwenden. Auf
    Wunsch bleibt der Wert nach einem Neustart erhalten (markers.json).

  - UND / ODER: verknuepfen mehrere Bedingungen. Die Ergebnis-Ausgaenge
    von Bedingungen (oder anderen UND/ODER-Elementen) werden ueber
    gestrichelte Datenlinien mit dem Eingang des UND/ODER-Elements
    verbunden. Erreicht der Ablauf das Element, werden alle Eingaenge
    ausgewertet; der Ablauf geht bei UND ueber "Ja" weiter, wenn alle
    Eingaenge Ja sind, bei ODER, wenn mindestens einer Ja ist - sonst
    ueber "Nein".

Die komplette Verschaltung (Broker, Bloecke, Verbindungen, Ansicht)
wird menschenlesbar in "config.json" gespeichert, die im selben
Verzeichnis wie das Skript / die exe liegt - Handbearbeitung bleibt
damit jederzeit moeglich.

Start (Entwicklung):   python app.py
Erreichbar unter:      http://<IP-DES-PCS>:8020  (im gesamten LAN)
"""

# Versionsnummer (semantische Versionierung: MAJOR.MINOR.PATCH).
# Einzige Quelle der Wahrheit fuer die Version - wird vom GitHub-Actions-
# Workflow per Regex ausgelesen, um Release-Tag und Dateinamen zu
# erzeugen, und zusaetzlich auf der Webseite angezeigt (/api/version).
# Bei jeder ausgelieferten Aenderung hier erhoehen:
#   PATCH (x.x.+1) -> Bugfix, keine neuen Funktionen
#   MINOR (x.+1.0) -> neue Funktion, abwaertskompatibel
#   MAJOR (+1.0.0) -> Breaking Change (z. B. Config-Format aendert sich)
APP_VERSION = "1.8.0"

import os
import sys
import json
import ssl
import time
import copy
import uuid
import math
import re
import threading
from datetime import datetime, timedelta, timezone

from flask import Flask, request, jsonify, Response

try:
    import urllib.request as urlrequest
except ImportError:  # pragma: no cover - manche OpenWrt-Installationen: opkg install python3-urllib
    urlrequest = None
try:
    import certifi  # aktuelle CA-Zertifikate (v. a. fuer die macOS-App noetig)
except ImportError:  # pragma: no cover
    certifi = None

try:
    import paho.mqtt.client as mqtt
    import paho.mqtt.publish as mqtt_publish
    MQTT_AVAILABLE = True
except ImportError:  # pragma: no cover
    mqtt = None
    mqtt_publish = None
    MQTT_AVAILABLE = False


# --------------------------------------------------------------------------
# Pfade (wichtig fuer PyInstaller --onefile / .app)
# --------------------------------------------------------------------------
def base_dir() -> str:
    """Verzeichnis, in dem die exe/app liegt (bzw. das Skript im
    Dev-Betrieb). Hier wird die config.json gespeichert."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


CONFIG_PATH = os.path.join(base_dir(), "config.json")
MARKERS_PATH = os.path.join(base_dir(), "markers.json")  # remanente Merker-Werte
LOCK = threading.RLock()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


DEFAULT_CONFIG = {
    "_kommentar": "Diese Datei wird vom MQTT-Ablaufplaner gelesen und "
                  "geschrieben. Manuelle Aenderungen sind moeglich, aber "
                  "bitte gueltiges JSON beibehalten.",
    "server": {"host": "0.0.0.0", "port": 8020},
    "location": {"lat": 51.1657, "lon": 10.4515},
    "pages": [{"id": "page_1", "name": "Seite 1"}],
    "weather": {"interval_min": 10},
    "brokers": [],
    "nodes": [],
    "connections": []
}


def load_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        save_config(DEFAULT_CONFIG)
        return copy.deepcopy(DEFAULT_CONFIG)
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as exc:  # noqa: BLE001
        print(f"[WARNUNG] config.json konnte nicht gelesen werden ({exc}). "
              f"Erzeuge neue Standardkonfiguration.")
        save_config(DEFAULT_CONFIG)
        return copy.deepcopy(DEFAULT_CONFIG)

    cfg.setdefault("server", copy.deepcopy(DEFAULT_CONFIG["server"]))
    cfg.setdefault("brokers", [])
    cfg.setdefault("nodes", [])
    cfg.setdefault("connections", [])
    cfg.setdefault("location", copy.deepcopy(DEFAULT_CONFIG["location"]))
    for c in cfg["connections"]:
        c.setdefault("from_port", "out")
        c.setdefault("to_port", "in")   # "data" = Datenlinie zu einem UND/ODER-Element
    if not isinstance(cfg.get("weather"), dict):
        cfg["weather"] = {"interval_min": 10}
    cfg["weather"].setdefault("interval_min", 10)
    # Seiten: jedes Element gehoert zu genau einer Seite (aeltere Dateien: Seite 1)
    if not isinstance(cfg.get("pages"), list) or not cfg["pages"]:
        cfg["pages"] = [{"id": "page_1", "name": "Seite 1"}]
    for p in cfg["pages"]:
        p.setdefault("id", new_id("page"))
        p.setdefault("name", "Seite")
    valid_pages = {p["id"] for p in cfg["pages"]}
    for n in cfg["nodes"]:
        if n.get("page") not in valid_pages:
            n["page"] = cfg["pages"][0]["id"]

    for b in cfg["brokers"]:
        b.setdefault("port", 1883)
        b.setdefault("username", "")
        b.setdefault("password", "")
        b.setdefault("tls", False)

    for n in cfg["nodes"]:
        n.setdefault("x", 40)
        n.setdefault("y", 40)
        n.setdefault("label", "")
        n.setdefault("broker_id", "")
        n.setdefault("topic", "")
        n.setdefault("payload", "")
        n.setdefault("qos", 0)
        n.setdefault("retain", False)
        n.setdefault("delay_sec", 1.0)
        n.setdefault("operator", "eq")
        n.setdefault("compare_value", "")
        n.setdefault("cond_type", "payload")
        n.setdefault("time_mode", "between")
        n.setdefault("time_from", "08:00")
        n.setdefault("time_to", "18:00")
        n.setdefault("sun_state", "up")
        n.setdefault("sun_offset_min", 0)
        n.setdefault("weekdays", [0, 1, 2, 3, 4])
        n.setdefault("date_mode", "equal")
        n.setdefault("date_from", "")
        n.setdefault("date_to", "")
        n.setdefault("date_yearly", False)
        n.setdefault("marker_initial", "0")
        n.setdefault("marker_action", "set")
        n.setdefault("marker_value", "1")
        n.setdefault("marker_retain", False)
        n.setdefault("marker_id", "")
        n.setdefault("marker_op", "eq")
        n.setdefault("marker_cmp", "")
        n.setdefault("delay_source", "fixed")
        n.setdefault("delay_marker_id", "")
        n.setdefault("delay_marker_unit", "s")
        n.setdefault("weather_metric", "temperature")
        n.setdefault("weather_op", "gt")
        n.setdefault("weather_value", "25")
        n.setdefault("weather_state", "rain")
        n.setdefault("weather_mode", "rise")
        n.setdefault("weather_fire_start", False)

    return cfg


def save_config(cfg: dict) -> None:
    with LOCK:
        tmp_path = CONFIG_PATH + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, CONFIG_PATH)


# --------------------------------------------------------------------------
# Zeit-/Sonnen-/Datums-Bedingungen
# --------------------------------------------------------------------------
COND_LABELS = {"payload": "Payload", "time": "Uhrzeit", "sun": "Sonnenstand",
               "weekday": "Wochentag", "date": "Datum", "marker": "Merker", "weather": "Wetter"}


def _parse_hhmm(text):
    try:
        h, m = str(text).strip().split(":")[:2]
        h, m = int(h), int(m)
        if 0 <= h < 24 and 0 <= m < 60:
            return h * 60 + m
    except Exception:
        pass
    return None


def _parse_date(text):
    try:
        return datetime.strptime(str(text).strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def sun_times(day, lat, lon):
    """Sonnenauf-/-untergang fuer ein Datum (NOAA-Naeherung, rein lokal
    berechnet - keine Internetverbindung noetig). Gibt (aufgang, untergang,
    polar) zurueck; aufgang/untergang sind zeitzonenbewusste datetime-Werte
    in der lokalen Zeitzone. polar ist None, "day" (Mitternachtssonne) oder
    "night" (Polarnacht); dann sind aufgang/untergang None."""
    lat = max(-89.9, min(89.9, float(lat)))
    n = day.timetuple().tm_yday
    g = 2 * math.pi / 365.0 * (n - 1)
    eqtime = 229.18 * (0.000075 + 0.001868 * math.cos(g) - 0.032077 * math.sin(g)
                       - 0.014615 * math.cos(2 * g) - 0.040849 * math.sin(2 * g))
    decl = (0.006918 - 0.399912 * math.cos(g) + 0.070257 * math.sin(g)
            - 0.006758 * math.cos(2 * g) + 0.000907 * math.sin(2 * g)
            - 0.002697 * math.cos(3 * g) + 0.00148 * math.sin(3 * g))
    lat_r = math.radians(lat)
    cos_ha = (math.cos(math.radians(90.833)) / (math.cos(lat_r) * math.cos(decl))
              - math.tan(lat_r) * math.tan(decl))
    if cos_ha > 1:
        return None, None, "night"
    if cos_ha < -1:
        return None, None, "day"
    ha = math.degrees(math.acos(cos_ha))
    base = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    rise = (base + timedelta(minutes=720 - 4 * (lon + ha) - eqtime)).astimezone()
    sett = (base + timedelta(minutes=720 - 4 * (lon - ha) - eqtime)).astimezone()
    return rise, sett, None


def is_sun_up(now, lat, lon, offset_min=0):
    """True, wenn die Sonne zum Zeitpunkt `now` (zeitzonenbewusst) aufgegangen
    ist. offset_min verschiebt Auf- und Untergang (z. B. +30 = 30 Minuten
    spaeter, -30 = 30 Minuten frueher)."""
    off = timedelta(minutes=offset_min)
    polar = None
    for delta in (-1, 0, 1):
        rise, sett, pol = sun_times((now + timedelta(days=delta)).date(), lat, lon)
        if delta == 0:
            polar = pol
        if rise is None:
            continue
        if rise + off <= now < sett + off:
            return True
    return polar == "day"


_PLACEHOLDER = re.compile(r"\{payload(?:\.([^{}]+))?\}")


# ---- Wetter aus dem Internet (Open-Meteo: kostenlos, ohne Anmeldung) ----
WEATHER_API = os.environ.get("ABLAUF_WEATHER_API", "https://api.open-meteo.com/v1/forecast")
WEATHER_FIELDS = ("temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,"
                  "rain,showers,snowfall,weather_code,cloud_cover,wind_speed_10m,wind_gusts_10m")
RAIN_CODES = {51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82, 95, 96, 99}
SNOW_CODES = {71, 73, 75, 77, 85, 86}
WMO_TEXT = {
    0: "klar", 1: "überwiegend klar", 2: "teils bewölkt", 3: "bedeckt", 45: "Nebel", 48: "Nebel mit Reif",
    51: "leichter Nieselregen", 53: "Nieselregen", 55: "starker Nieselregen",
    56: "leichter gefrierender Nieselregen", 57: "gefrierender Nieselregen",
    61: "leichter Regen", 63: "Regen", 65: "starker Regen", 66: "leichter gefrierender Regen",
    67: "gefrierender Regen", 71: "leichter Schneefall", 73: "Schneefall", 75: "starker Schneefall",
    77: "Schneegriesel", 80: "leichte Regenschauer", 81: "Regenschauer", 82: "heftige Regenschauer",
    85: "leichte Schneeschauer", 86: "starke Schneeschauer", 95: "Gewitter",
    96: "Gewitter mit leichtem Hagel", 99: "Gewitter mit starkem Hagel",
}
WEATHER_METRICS = {"temperature": "temperature", "apparent_temperature": "apparent", "humidity": "humidity",
                   "wind": "wind", "gusts": "gusts", "precipitation": "precipitation", "cloud": "cloud"}


def weather_interval_sec(cfg):
    try:
        minutes = int((cfg.get("weather") or {}).get("interval_min", 10))
    except (TypeError, ValueError):
        minutes = 10
    return max(5, min(120, minutes)) * 60


def _ssl_context():
    if certifi:
        try:
            return ssl.create_default_context(cafile=certifi.where())
        except Exception:  # noqa: BLE001
            pass
    return ssl.create_default_context()


GEOCODE_API = os.environ.get("ABLAUF_GEOCODE_API", "https://geocoding-api.open-meteo.com/v1/search")


def geocode_city(query, count=6, timeout=8):
    """Sucht Orte nach Namen (Open-Meteo-Geocoding, kein Schluessel noetig).
    Gibt eine Liste mit name, label, lat, lon zurueck."""
    if urlrequest is None:
        raise RuntimeError("urllib.request fehlt (OpenWrt: opkg install python3-urllib)")
    from urllib.parse import quote
    url = f"{GEOCODE_API}?name={quote(query.strip())}&count={int(count)}&language=de&format=json"
    request_obj = urlrequest.Request(url, headers={"User-Agent": "MQTT-Ablaufplaner"})
    kwargs = {"timeout": timeout}
    if url.startswith("https"):
        kwargs["context"] = _ssl_context()
    with urlrequest.urlopen(request_obj, **kwargs) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    out = []
    for r in payload.get("results") or []:
        try:
            lat, lon = float(r["latitude"]), float(r["longitude"])
        except (KeyError, TypeError, ValueError):
            continue
        parts = [r.get("name") or "", r.get("admin1") or "", r.get("country") or ""]
        label = ", ".join(p for p in parts if p)
        out.append({"name": r.get("name") or label, "label": label, "lat": round(lat, 4), "lon": round(lon, 4)})
    return out


def fetch_weather(lat, lon, timeout=8):
    """Ruft das aktuelle Wetter fuer eine Position ab (Open-Meteo, JSON)."""
    if urlrequest is None:
        raise RuntimeError("urllib.request fehlt (OpenWrt: opkg install python3-urllib)")
    url = (f"{WEATHER_API}?latitude={lat:.4f}&longitude={lon:.4f}&current={WEATHER_FIELDS}"
           f"&timezone=auto&wind_speed_unit=kmh")
    request_obj = urlrequest.Request(url, headers={"User-Agent": "MQTT-Ablaufplaner"})
    kwargs = {"timeout": timeout}
    if url.startswith("https"):
        kwargs["context"] = _ssl_context()
    with urlrequest.urlopen(request_obj, **kwargs) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    cur = payload.get("current")
    if not isinstance(cur, dict):
        raise ValueError("Antwort ohne aktuelle Wetterdaten")

    def num(key):
        value = cur.get(key)
        return float(value) if isinstance(value, (int, float)) else None

    code = cur.get("weather_code")
    code = int(code) if isinstance(code, (int, float)) else -1
    flags = {
        "rain": code in RAIN_CODES or (num("rain") or 0) > 0 or (num("showers") or 0) > 0,
        "snow": code in SNOW_CODES or (num("snowfall") or 0) > 0,
        "thunder": code in (95, 96, 99),
        "fog": code in (45, 48),
        "clear": code in (0, 1),
        "cloudy": code in (2, 3),
    }
    return {"temperature": num("temperature_2m"), "apparent": num("apparent_temperature"),
            "humidity": num("relative_humidity_2m"), "wind": num("wind_speed_10m"),
            "gusts": num("wind_gusts_10m"), "precipitation": num("precipitation"),
            "cloud": num("cloud_cover"), "code": code, "text": WMO_TEXT.get(code, "unbekannt"),
            "flags": flags, "is_day": bool(cur.get("is_day")), "time": cur.get("time"),
            "lat": lat, "lon": lon, "fetched": time.time()}


def evaluate_weather(spec, data):
    """Wertet eine Wetter-Angabe (Bedingung bzw. Ausloeser) gegen die aktuellen
    Wetterdaten aus. Gibt (trifft_zu, Wert_als_Text) zurueck."""
    metric = spec.get("weather_metric", "temperature")
    if metric == "state":
        has = bool(data["flags"].get(spec.get("weather_state", "rain"), False))
        return (not has if spec.get("weather_op") == "not" else has), data["text"]
    value = data.get(WEATHER_METRICS.get(metric, "temperature"))
    if value is None:
        return False, ""
    return _compare(spec.get("weather_op", "gt"), value, spec.get("weather_value", "")), _fmt_num(float(value))


def extract_json_field(payload_text, path):
    """Liest ein Feld aus einer JSON-Nachricht. path: "temperature",
    "sensor.temp" oder "werte.0" (Listenindex). Gibt (True, Text) oder
    (False, Fehlergrund) zurueck."""
    try:
        data = json.loads(payload_text)
    except Exception:
        return False, "Nachricht ist kein JSON"
    cur = data
    for part in [p for p in str(path).strip().split(".") if p != ""]:
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        elif (isinstance(cur, list) and part.lstrip("-").isdigit()
              and -len(cur) <= int(part) < len(cur)):
            cur = cur[int(part)]
        else:
            return False, f"Feld '{path}' nicht gefunden"
    if isinstance(cur, bool):
        return True, "true" if cur else "false"
    if cur is None:
        return True, ""
    if isinstance(cur, (dict, list)):
        return True, json.dumps(cur, ensure_ascii=False)
    return True, str(cur)


def fill_payload(template, payload_ctx):
    """Ersetzt {payload} durch die empfangene Nachricht und {payload.feld}
    durch ein Feld aus einer JSON-Nachricht (fehlt das Feld: leerer Text)."""
    def sub(match):
        path = match.group(1)
        if not path:
            return payload_ctx
        ok, value = extract_json_field(payload_ctx, path)
        return value if ok else ""
    return _PLACEHOLDER.sub(sub, str(template))


TRUE_WORDS = ("1", "true", "on", "an", "ein", "ja", "yes", "wahr")


def is_truthy(value):
    """Merker-Wert als Wahrheitswert (1 / true / on / an / ein / ja / yes / wahr)."""
    return str(value).strip().lower() in TRUE_WORDS


def _to_float(value):
    """Zahl aus Text (Komma oder Punkt als Dezimaltrenner) oder None."""
    try:
        number = float(str(value).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _fmt_num(number):
    if number == int(number):
        return str(int(number))
    return ("%f" % number).rstrip("0").rstrip(".")


def _compare(op, left, right):
    """Vergleicht zwei Werte als Text bzw. (bei Zahlen) numerisch."""
    left, right = str(left), str(right)
    a, b = _to_float(left), _to_float(right)
    both = a is not None and b is not None
    if op == "eq":
        return left == right or (both and a == b)
    if op == "neq":
        return not (left == right or (both and a == b))
    if op == "contains":
        return right in left
    if op in ("gt", "gte", "lt", "lte"):
        if not both:
            return False
        return {"gt": a > b, "gte": a >= b, "lt": a < b, "lte": a <= b}[op]
    return False


def evaluate_condition(node, payload_ctx, location, now=None, markers=None, weather=None):
    """Wertet einen Bedingungs-Block aus und gibt True ("Ja") oder False
    ("Nein") zurueck."""
    try:
        now = now or datetime.now().astimezone()
        ctype = node.get("cond_type", "payload")

        if ctype == "time":
            cur = now.hour * 60 + now.minute
            a = _parse_hhmm(node.get("time_from"))
            b = _parse_hhmm(node.get("time_to"))
            mode = node.get("time_mode", "between")
            if a is None:
                return False
            if mode == "after":
                return cur >= a
            if mode == "before":
                return cur < a
            if b is None:
                return False
            if a <= b:
                return a <= cur < b
            return cur >= a or cur < b  # Zeitfenster ueber Mitternacht

        if ctype == "sun":
            lat = float((location or {}).get("lat", 51.1657))
            lon = float((location or {}).get("lon", 10.4515))
            off = int(float(node.get("sun_offset_min", 0) or 0))
            up = is_sun_up(now, lat, lon, off)
            return up if node.get("sun_state", "up") == "up" else not up

        if ctype == "weekday":
            days = [int(d) for d in (node.get("weekdays") or [])]
            return now.weekday() in days  # 0 = Montag ... 6 = Sonntag

        if ctype == "date":
            d1 = _parse_date(node.get("date_from"))
            d2 = _parse_date(node.get("date_to"))
            yearly = bool(node.get("date_yearly"))
            today = now.date()
            if d1 is None:
                return False
            if node.get("date_mode", "equal") == "equal":
                if yearly:
                    return (today.month, today.day) == (d1.month, d1.day)
                return today == d1
            if d2 is None:
                return False
            if yearly:
                t = (today.month, today.day)
                a = (d1.month, d1.day)
                b = (d2.month, d2.day)
                return a <= t <= b if a <= b else (t >= a or t <= b)
            return d1 <= today <= d2

        if ctype == "weather":
            data = weather() if callable(weather) else weather
            if not data:
                return False
            return evaluate_weather(node, data)[0]

        if ctype == "marker":
            values = markers or {}
            mid = node.get("marker_id")
            if mid not in values:
                return False
            current = values[mid]
            op = node.get("marker_op", "eq")
            if op == "true":
                return is_truthy(current)
            if op == "false":
                return not is_truthy(current)
            return _compare(op, current, node.get("marker_cmp", ""))

        # Standard: Vergleich mit der zuletzt empfangenen Nachricht
        return _compare(node.get("operator", "eq"), payload_ctx, node.get("compare_value", ""))
    except Exception:
        return False


# --------------------------------------------------------------------------
# MQTT: einmaliges Veroeffentlichen (kurzlebige Verbindung pro Aufruf, damit
# gleichzeitig laufende Ablaeufe sich nicht gegenseitig blockieren koennen)
# --------------------------------------------------------------------------
def evaluate_gate(node, payload_ctx, location, nodes_by_id, connections, markers, _stack=None, trace=None, weather=None):
    """Wertet ein UND/ODER-Element aus. Gibt (ergebnis, details) zurueck;
    details ist eine Liste aus (Name, Ja/Nein) je verbundenem Eingang.
    Eingaenge sind Bedingungen oder weitere UND/ODER-Elemente, die ueber
    Datenlinien (to_port == "data") angeschlossen sind. Ohne Eingaenge ist
    das Ergebnis Nein; Kreise zwischen Elementen werden als Nein gewertet."""
    stack = set(_stack or ()) | {node["id"]}
    details = []
    for c in connections:
        if c.get("to") != node["id"] or c.get("to_port") != "data":
            continue
        src = nodes_by_id.get(c.get("from"))
        if not src or src["id"] in stack:
            if src:
                details.append((src.get("label") or "Kreis", False))
            continue
        if src["type"] == "condition":
            name = src.get("label") or ("Bedingung " + COND_LABELS.get(src.get("cond_type", "payload"), ""))
            result = evaluate_condition(src, payload_ctx, location, markers=markers, weather=weather)
        elif src["type"] in ("and", "or"):
            name = src.get("label") or ("UND" if src["type"] == "and" else "ODER")
            result, _sub = evaluate_gate(src, payload_ctx, location, nodes_by_id, connections, markers, stack, trace, weather)
        else:
            continue
        if trace:
            trace(c.get("id", ""), result)
        details.append((name, result))
    if not details:
        return False, []
    values = [r for _name, r in details]
    ok = all(values) if node["type"] == "and" else any(values)
    return ok, details


def make_mqtt_client(client_id):
    """Erzeugt einen MQTT-Client und funktioniert sowohl mit paho-mqtt 2.x
    als auch mit aelteren 1.x-Versionen (z. B. aus den opkg-Paketen von
    OpenWrt/GL.iNet-Routern)."""
    if hasattr(mqtt, "CallbackAPIVersion"):
        return mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id=client_id, protocol=mqtt.MQTTv311)
    return mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)


def mqtt_publish_once(broker: dict, topic: str, payload: str, qos: int = 0,
                       retain: bool = False, timeout: float = 5.0) -> None:
    if not MQTT_AVAILABLE:
        raise RuntimeError(
            "Das Python-Paket 'paho-mqtt' ist nicht installiert. "
            "Bitte 'pip install paho-mqtt' ausfuehren."
        )

    done = threading.Event()
    result = {"error": None}
    client = make_mqtt_client(f"flow-{uuid.uuid4().hex[:8]}")

    if broker.get("username"):
        client.username_pw_set(broker.get("username"), broker.get("password") or None)
    if broker.get("tls"):
        ctx = ssl._create_unverified_context()
        client.tls_set_context(ctx)
        client.tls_insecure_set(True)

    def on_connect(c, userdata, flags, reason_code=None, properties=None):
        if reason_code == 0:
            c.publish(topic, payload, qos=qos, retain=retain)
        else:
            result["error"] = f"Verbindung zum Broker fehlgeschlagen ({reason_code})"
            done.set()

    def on_publish(c, userdata, mid, reason_code=None, properties=None):
        done.set()

    client.on_connect = on_connect
    client.on_publish = on_publish

    try:
        client.connect(broker["host"], int(broker.get("port", 1883)), keepalive=10)
    except Exception as e:
        raise RuntimeError(f"Verbindung zu {broker['host']}:{broker.get('port', 1883)} fehlgeschlagen: {e}")

    client.loop_start()
    finished_in_time = done.wait(timeout)
    client.loop_stop()
    try:
        client.disconnect()
    except Exception:
        pass

    if result["error"]:
        raise RuntimeError(result["error"])
    if not finished_in_time:
        raise RuntimeError("Zeitueberschreitung beim Senden der Nachricht.")


# --------------------------------------------------------------------------
# Laufzeit-Engine: haelt dauerhafte MQTT-Verbindungen fuer alle
# "MQTT Empfangen"-Bloecke offen und fuehrt bei ankommenden Nachrichten den
# angeschlossenen Ablauf aus.
# --------------------------------------------------------------------------
class FlowEngine:
    def __init__(self):
        self._clients = {}     # broker_id -> mqtt.Client
        self._lock = threading.RLock()
        self.last_events = []  # kleine Historie fuer die Oberflaeche
        self._events_lock = threading.RLock()
        self.markers = {}          # Merker-ID -> aktueller Wert (Text)
        self._marker_init = {}     # zuletzt gesehener Startwert je Merker
        self._mlock = threading.RLock()
        self._persisted = self._load_persisted_markers()
        self._tl = threading.local()          # Lauf-ID je Ablauf-Thread
        self._trace_lock = threading.RLock()
        self._trace_events = []               # Ablauf-Protokoll fuer die Anzeige
        self._trace_seq = 0
        self._wlock = threading.RLock()       # Wetter-Zwischenspeicher
        self._wfetch_lock = threading.Lock()  # nur ein Abruf gleichzeitig
        self._weather = None
        self._weather_error = ""
        self._weather_state = {}              # Ausloeser-ID -> letztes Ergebnis
        self._weather_seen = {}               # Ausloeser-ID -> zuletzt gesehener Abruf
        self._weather_kick = threading.Event()

    def log_event(self, text: str) -> None:
        with self._events_lock:
            self.last_events.insert(0, {
                "time": datetime.now().strftime("%H:%M:%S"),
                "text": text
            })
            self.last_events = self.last_events[:50]

    def get_events(self):
        with self._events_lock:
            return list(self.last_events)

    def rebuild(self, cfg: dict) -> None:
        try:
            self.sync_markers(cfg)
        except Exception as exc:  # noqa: BLE001
            self.log_event(f"Merker konnten nicht abgeglichen werden: {exc}")
        self._weather_kick.set()
        with self._lock:
            in_nodes = [n for n in cfg["nodes"] if n["type"] == "mqtt_in" and n.get("broker_id") and n.get("topic")]
            needed_broker_ids = {n["broker_id"] for n in in_nodes}

            for bid in list(self._clients.keys()):
                if bid not in needed_broker_ids:
                    self._stop_client(bid)

            brokers_by_id = {b["id"]: b for b in cfg["brokers"]}
            for bid in needed_broker_ids:
                broker = brokers_by_id.get(bid)
                if not broker:
                    continue
                if bid not in self._clients:
                    try:
                        self._start_client(bid, broker)
                    except Exception as exc:  # noqa: BLE001 - Webseite soll trotzdem starten
                        self.log_event(f"Fehler beim Start der MQTT-Verbindung "
                                       f"'{broker.get('name', bid)}': {exc}")
                else:
                    topics = {n["topic"] for n in in_nodes if n["broker_id"] == bid}
                    for t in topics:
                        try:
                            self._clients[bid].subscribe(t)
                        except Exception:
                            pass

    def _start_client(self, bid: str, broker: dict) -> None:
        if not MQTT_AVAILABLE:
            return
        client = make_mqtt_client(f"engine-{bid}-{uuid.uuid4().hex[:6]}")
        if broker.get("username"):
            client.username_pw_set(broker.get("username"), broker.get("password") or None)
        if broker.get("tls"):
            ctx = ssl._create_unverified_context()
            client.tls_set_context(ctx)
            client.tls_insecure_set(True)

        def on_connect(c, userdata, flags, reason_code=None, properties=None):
            if reason_code == 0:
                cfg = load_config()
                topics = {n["topic"] for n in cfg["nodes"]
                          if n["type"] == "mqtt_in" and n.get("broker_id") == bid and n.get("topic")}
                for t in topics:
                    try:
                        c.subscribe(t)
                    except Exception:
                        pass

        def on_message(c, userdata, msg):
            try:
                payload_str = msg.payload.decode("utf-8", errors="replace")
            except Exception:
                payload_str = ""
            cfg = load_config()
            matches = [n for n in cfg["nodes"]
                       if n["type"] == "mqtt_in" and n.get("broker_id") == bid and n.get("topic") == msg.topic]
            for node in matches:
                required = (node.get("payload") or "").strip()
                if not required or payload_str == required:
                    self.log_event(f"Ereignis '{node.get('label') or node['id']}' ausgeloest ({msg.topic})")
                    threading.Thread(target=self.run_flow_from, args=(node["id"], payload_str, "out"),
                                      daemon=True).start()

        client.on_connect = on_connect
        client.on_message = on_message
        client.reconnect_delay_set(min_delay=1, max_delay=15)
        try:
            client.connect(broker["host"], int(broker.get("port", 1883)), keepalive=30)
            client.loop_start()
            self._clients[bid] = client
        except Exception:
            # Verbindung derzeit nicht moeglich - naechster rebuild() (z. B.
            # nach dem Speichern) versucht es erneut.
            pass

    def _stop_client(self, bid: str) -> None:
        client = self._clients.pop(bid, None)
        if client:
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                pass

    # ---- Wetter -----------------------------------------------------------
    def get_weather(self, cfg=None, force=False):
        """Liefert (Daten oder None, Fehlertext). Die Daten werden hoechstens im
        eingestellten Intervall neu abgerufen; schlaegt ein Abruf fehl, werden
        aeltere Daten (bis 1 Stunde) weiter verwendet."""
        cfg = cfg or load_config()
        loc = cfg.get("location") or {}
        try:
            lat, lon = float(loc.get("lat")), float(loc.get("lon"))
        except (TypeError, ValueError):
            return None, "Standort ungueltig"
        ttl = weather_interval_sec(cfg)

        def fresh(d):
            return d is not None and d["lat"] == lat and d["lon"] == lon and time.time() - d["fetched"] < ttl

        with self._wlock:
            if not force and fresh(self._weather):
                return self._weather, ""
        with self._wfetch_lock:
            with self._wlock:
                if not force and fresh(self._weather):    # ein anderer Thread war schneller
                    return self._weather, ""
            try:
                data = fetch_weather(lat, lon)
            except Exception as exc:  # noqa: BLE001
                message = str(exc) or exc.__class__.__name__
                with self._wlock:
                    self._weather_error = message
                    old = self._weather
                    if old and old["lat"] == lat and old["lon"] == lon and time.time() - old["fetched"] < 3600:
                        return old, message
                return None, message
            with self._wlock:
                self._weather = data
                self._weather_error = ""
            return data, ""

    def weather_data(self):
        """Fuer Bedingungen: Wetterdaten oder None (mit Meldung im Protokoll)."""
        data, err = self.get_weather()
        if data is None:
            self.log_event(f"Wetterdaten nicht verfuegbar: {err}")
        return data

    def weather_summary(self):
        with self._wlock:
            d = self._weather
            return {"ok": d is not None and not self._weather_error, "error": self._weather_error,
                    "data": d, "age": int(time.time() - d["fetched"]) if d else None}

    def start_weather_poller(self):
        threading.Thread(target=self._weather_loop, daemon=True).start()

    def _weather_loop(self):
        logged = None
        while True:
            try:
                cfg = load_config()
                nodes = [n for n in cfg["nodes"] if n["type"] == "weather_in"]
                ids = {n["id"] for n in nodes}
                for nid in list(self._weather_state):
                    if nid not in ids:
                        self._weather_state.pop(nid, None)
                        self._weather_seen.pop(nid, None)
                if nodes:
                    data, err = self.get_weather(cfg)
                    if err and err != logged:
                        self.log_event(f"Wetter: Abruf fehlgeschlagen - {err}")
                    logged = err or None
                    if data:
                        self._check_weather_triggers(nodes, data)
            except Exception as exc:  # noqa: BLE001
                self.log_event(f"Wetter-Abfrage: {exc}")
            self._weather_kick.wait(60)
            self._weather_kick.clear()

    def _check_weather_triggers(self, nodes, data):
        for n in nodes:
            result, value = evaluate_weather(n, data)
            prev = self._weather_state.get(n["id"])
            new_data = self._weather_seen.get(n["id"]) != data["fetched"]
            self._weather_seen[n["id"]] = data["fetched"]
            mode = n.get("weather_mode", "rise")
            if prev is None:                      # erster Abruf: nur auf Wunsch ausloesen
                fire = result and bool(n.get("weather_fire_start"))
            elif mode == "any":
                fire = result != prev
            elif mode == "while":
                fire = result and new_data
            else:
                fire = result and not prev
            self._weather_state[n["id"]] = result
            if fire:
                self.log_event(f"Wetter-Ausloeser '{n.get('label') or 'Wetter'}' ausgeloest (Wert: {value})")
                threading.Thread(target=self.run_flow_from, args=(n["id"], value, "out"), daemon=True).start()

    # ---- Ablauf-Anzeige -------------------------------------------------
    def _trace(self, kind, **data):
        """Haelt fest, was der Ablauf gerade tut (Kante durchlaufen, Block
        betreten/verlassen ...) - die Weboberflaeche zeigt es als wandernden
        Punkt und leuchtenden Block an."""
        with self._trace_lock:
            self._trace_seq += 1
            event = {"seq": self._trace_seq, "t": round(time.time(), 3),
                     "run": getattr(self._tl, "run", ""), "kind": kind}
            event.update(data)
            self._trace_events.append(event)
            if len(self._trace_events) > 400:
                del self._trace_events[:100]

    def get_trace(self, since):
        with self._trace_lock:
            if since < 0:
                return [], self._trace_seq
            if since > self._trace_seq:      # Server wurde neu gestartet
                since = 0
            return [e for e in self._trace_events if e["seq"] > since], self._trace_seq

    # ---- Merker --------------------------------------------------------
    def _load_persisted_markers(self):
        try:
            with open(MARKERS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {str(k): str(v) for k, v in data.items()} if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save_persisted_markers(self):
        try:
            tmp = MARKERS_PATH + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._persisted, f, indent=2, ensure_ascii=False)
            os.replace(tmp, MARKERS_PATH)
        except Exception as exc:  # noqa: BLE001
            self.log_event(f"Merker konnten nicht gespeichert werden: {exc}")

    def sync_markers(self, cfg: dict) -> None:
        """Gleicht die Laufzeit-Werte mit der Konfiguration ab: neue Merker
        bekommen ihren Startwert (bzw. den gespeicherten Wert, wenn remanent),
        gelöschte Merker verschwinden, und eine geaenderte Startwert-Angabe
        wird sofort uebernommen."""
        with self._mlock:
            nodes = {n["id"]: n for n in cfg["nodes"] if n["type"] == "marker"}
            for mid in list(self.markers):
                if mid not in nodes:
                    del self.markers[mid]
                    self._marker_init.pop(mid, None)
            changed = False
            for mid, n in nodes.items():
                init = str(n.get("marker_initial", ""))
                retain = bool(n.get("marker_retain"))
                if mid not in self.markers:
                    self.markers[mid] = self._persisted[mid] if (retain and mid in self._persisted) else init
                elif self._marker_init.get(mid) != init:
                    self.markers[mid] = init
                self._marker_init[mid] = init
                if retain:
                    if self._persisted.get(mid) != self.markers[mid]:
                        self._persisted[mid] = self.markers[mid]
                        changed = True
                elif mid in self._persisted:
                    del self._persisted[mid]
                    changed = True
            for mid in list(self._persisted):
                if mid not in nodes:
                    del self._persisted[mid]
                    changed = True
            if changed:
                self._save_persisted_markers()

    def get_markers(self) -> dict:
        with self._mlock:
            return dict(self.markers)

    def set_marker(self, node: dict, value) -> str:
        value = str(value)
        with self._mlock:
            self.markers[node["id"]] = value
            if node.get("marker_retain"):
                self._persisted[node["id"]] = value
                self._save_persisted_markers()
        return value

    def apply_marker_action(self, node: dict, payload_ctx: str) -> str:
        action = node.get("marker_action", "set")
        with self._mlock:
            current = self.markers.get(node["id"], str(node.get("marker_initial", "")))
            if action == "set":
                new = fill_payload(node.get("marker_value", ""), payload_ctx)
            elif action == "store":
                # Empfangenen Wert (ganze Nachricht oder ein JSON-Feld) speichern
                path = str(node.get("marker_value", "")).strip()
                if path:
                    found, value = extract_json_field(payload_ctx, path)
                    if not found:
                        raise ValueError(f"{value} (Nachricht: {payload_ctx[:60]!r})")
                    new = value
                else:
                    new = payload_ctx
            elif action == "toggle":
                new = "0" if is_truthy(current) else "1"
            elif action in ("inc", "dec"):
                step = _to_float(node.get("marker_value", "1"))
                base = _to_float(current)
                step = 1.0 if step is None else step
                base = 0.0 if base is None else base
                new = _fmt_num(base + step if action == "inc" else base - step)
            elif action == "reset":
                new = str(node.get("marker_initial", ""))
            else:
                new = current
            return self.set_marker(node, new)

    def _delay_seconds(self, node: dict) -> float:
        """Wartezeit in Sekunden - als Festwert oder aus einem Merker-Wert."""
        fixed = max(0.0, float(node.get("delay_sec", 1.0) or 0))
        if node.get("delay_source", "fixed") != "marker":
            return fixed
        self._trace("ref", marker=node.get("delay_marker_id"), target=node["id"])
        with self._mlock:
            raw = self.markers.get(node.get("delay_marker_id"))
        factor = {"ms": 0.001, "s": 1.0, "min": 60.0, "h": 3600.0}.get(
            node.get("delay_marker_unit", "s"), 1.0)
        number = _to_float(raw)
        if number is None or number < 0:
            self.log_event(f"Wartezeit '{node.get('label') or node['id']}': Merker-Wert {raw!r} "
                           f"ist keine gueltige Dauer - verwende Festwert {fixed:g} s")
            return fixed
        return min(number * factor, 7 * 86400.0)  # hoechstens 7 Tage

    # ---- Ablauf-Ausfuehrung -------------------------------------------
    def run_flow_from(self, start_node_id: str, payload_ctx: str, start_port: str) -> None:
        self._tl.run = uuid.uuid4().hex[:6]
        self._trace("start", node=start_node_id)
        cfg = load_config()
        nodes_by_id = {n["id"]: n for n in cfg["nodes"]}
        brokers_by_id = {b["id"]: b for b in cfg["brokers"]}
        visited = set()
        self._walk(start_node_id, start_port, payload_ctx, nodes_by_id, brokers_by_id,
                   cfg["connections"], visited)

    def _walk(self, node_id, from_port, payload_ctx, nodes_by_id, brokers_by_id, connections, visited):
        edges = [c for c in connections if c["from"] == node_id and c.get("from_port", "out") == from_port
                 and c.get("to_port", "in") != "data"]
        for edge in edges:
            target = nodes_by_id.get(edge["to"])
            if not target:
                continue
            guard = (target["id"], payload_ctx)
            if guard in visited:
                continue  # einfache Zyklen-Bremse
            visited.add(guard)
            self._trace("edge", conn=edge.get("id", ""), frm=node_id, port=from_port, to=target["id"])
            self._trace("enter", node=target["id"])
            next_port = self._execute(target, payload_ctx, brokers_by_id, nodes_by_id, connections)
            self._trace("leave", node=target["id"], result=next_port or "stop")
            payload_ctx = self._last_payload if hasattr(self, "_last_payload") else payload_ctx
            if next_port:
                self._walk(target["id"], next_port, payload_ctx, nodes_by_id, brokers_by_id, connections, visited)

    def _execute(self, node: dict, payload_ctx: str, brokers_by_id: dict,
                 nodes_by_id: dict = None, connections: list = None) -> str:
        """Fuehrt einen einzelnen Block aus und gibt den Ausgangs-Port
        zurueck, ueber den der Ablauf weitergehen soll ("out", "true",
        "false") - oder None, um den Zweig zu beenden."""
        ntype = node["type"]
        try:
            if ntype == "mqtt_out":
                broker = brokers_by_id.get(node.get("broker_id"))
                if not broker:
                    self.log_event(f"Fehler: Aktion '{node.get('label') or node['id']}' hat keinen gueltigen Broker.")
                    return None
                text = fill_payload(node.get("payload") or "", payload_ctx)
                mqtt_publish_once(broker, node.get("topic", ""), text,
                                   qos=int(node.get("qos", 0) or 0), retain=bool(node.get("retain")))
                self.log_event(f"Gesendet: {node.get('topic')} = {text!r}")
                return "out"

            if ntype == "delay":
                time.sleep(self._delay_seconds(node))
                return "out"

            if ntype == "condition":
                if node.get("cond_type") == "marker" and node.get("marker_id"):
                    self._trace("ref", marker=node["marker_id"], target=node["id"])
                ok = evaluate_condition(node, payload_ctx, load_config().get("location") or {},
                                        markers=self.get_markers(), weather=self.weather_data)
                ctype = node.get("cond_type", "payload")
                self.log_event(f"Bedingung '{node.get('label') or node['id']}' "
                               f"({COND_LABELS.get(ctype, ctype)}): {'Ja' if ok else 'Nein'}")
                return "true" if ok else "false"

            if ntype in ("and", "or"):
                ok, details = evaluate_gate(node, payload_ctx, load_config().get("location") or {},
                                            nodes_by_id or {}, connections or [], self.get_markers(),
                                            trace=lambda cid, res: self._trace("data", conn=cid, result=res),
                                            weather=self.weather_data)
                title = "UND" if ntype == "and" else "ODER"
                name = node.get("label") or title
                answer = "Ja" if ok else "Nein"
                if details:
                    parts = "; ".join(d_name + "=" + ("Ja" if d_res else "Nein") for d_name, d_res in details)
                    self.log_event(f"{title} '{name}' ({parts}): {answer}")
                else:
                    self.log_event(f"{title} '{name}': keine Eingaenge verbunden - Ergebnis Nein")
                return "true" if ok else "false"

            if ntype == "marker":
                new = self.apply_marker_action(node, payload_ctx)
                self.log_event(f"Merker '{node.get('label') or node['id']}' = {new!r}")
                return "out"

            if ntype == "mqtt_in":
                # Ein Ereignis-Block als Zwischenstation macht inhaltlich
                # keinen Sinn, wird aber sicherheitshalber übersprungen.
                return "out"

        except Exception as exc:  # noqa: BLE001
            self.log_event(f"Fehler in Block '{node.get('label') or node['id']}': {exc}")
            return None

        return None


ENGINE = FlowEngine()


# --------------------------------------------------------------------------
# Flask-App
# --------------------------------------------------------------------------
app = Flask(__name__)


@app.route("/api/version")
def api_version():
    return jsonify({"version": APP_VERSION})


@app.route("/api/config", methods=["GET"])
def api_get_config():
    cfg = load_config()
    cfg["_events"] = ENGINE.get_events()
    cfg["_markers"] = ENGINE.get_markers()
    return jsonify(cfg)


@app.route("/api/config", methods=["POST"])
def api_save_config():
    data = request.get_json(force=True, silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Ungueltige Daten"}), 400
    data.setdefault("server", DEFAULT_CONFIG["server"])
    data.setdefault("brokers", [])
    data.setdefault("nodes", [])
    data.setdefault("connections", [])
    data.setdefault("location", DEFAULT_CONFIG["location"])
    data.setdefault("pages", DEFAULT_CONFIG["pages"])
    data.setdefault("weather", DEFAULT_CONFIG["weather"])
    save_config(data)
    ENGINE.rebuild(data)
    return jsonify({"status": "ok"})


@app.route("/api/mqtt-test", methods=["POST"])
def api_mqtt_test():
    """Sendet einmalig eine Testnachricht - fuer den 'Jetzt senden'-Knopf
    an einem Aktions-Block sowie fuer freie Tests."""
    data = request.get_json(force=True, silent=True) or {}
    broker_id = data.get("broker_id")
    cfg = load_config()
    broker = next((b for b in cfg["brokers"] if b["id"] == broker_id), None)
    if not broker:
        return jsonify({"error": "Broker nicht gefunden."}), 404
    try:
        mqtt_publish_once(broker, data.get("topic", ""), data.get("payload", ""),
                           qos=int(data.get("qos", 0) or 0), retain=bool(data.get("retain")))
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 502
    return jsonify({"status": "ok"})


@app.route("/api/nodes/<node_id>/simulate", methods=["POST"])
def api_simulate(node_id):
    """Simuliert ein ankommendes Ereignis, ohne auf eine echte MQTT-
    Nachricht zu warten - nuetzlich zum Testen eines Ablaufs."""
    data = request.get_json(force=True, silent=True) or {}
    payload_ctx = str(data.get("payload", ""))
    cfg = load_config()
    node = next((n for n in cfg["nodes"] if n["id"] == node_id), None)
    if not node:
        return jsonify({"error": "Block nicht gefunden."}), 404
    if node["type"] == "weather_in" and not payload_ctx:   # aktuellen Wetterwert mitgeben
        weather_now, _err = ENGINE.get_weather(cfg)
        if weather_now:
            payload_ctx = evaluate_weather(node, weather_now)[1]
    ENGINE.log_event(f"Simuliert: '{node.get('label') or node_id}'")
    threading.Thread(target=ENGINE.run_flow_from, args=(node_id, payload_ctx, "out"), daemon=True).start()
    return jsonify({"status": "ok"})


@app.route("/api/geocode")
def api_geocode():
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify({"ok": False, "error": "Bitte mindestens 2 Zeichen eingeben.", "results": []})
    try:
        return jsonify({"ok": True, "error": "", "results": geocode_city(q)})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": f"Ortssuche nicht moeglich: {exc}", "results": []})


@app.route("/api/weather")
def api_weather():
    if request.args.get("refresh"):
        ENGINE.get_weather(force=True)
        ENGINE._weather_kick.set()      # Ausloeser sofort mit den neuen Daten pruefen
    return jsonify(ENGINE.weather_summary())


@app.route("/api/trace")
def api_trace():
    try:
        since = int(request.args.get("since", "-1"))
    except ValueError:
        since = -1
    events, last = ENGINE.get_trace(since)
    return jsonify({"events": events, "last": last})


@app.route("/api/markers", methods=["GET"])
def api_get_markers():
    return jsonify({"markers": ENGINE.get_markers()})


@app.route("/api/markers/<marker_id>", methods=["POST"])
def api_set_marker(marker_id):
    """Setzt den Wert eines Merkers von Hand (z. B. zum Testen)."""
    data = request.get_json(force=True, silent=True) or {}
    cfg = load_config()
    node = next((n for n in cfg["nodes"] if n["id"] == marker_id and n["type"] == "marker"), None)
    if not node:
        return jsonify({"error": "Merker nicht gefunden (noch nicht gespeichert?)."}), 404
    ENGINE.set_marker(node, data.get("value", ""))
    ENGINE.log_event(f"Merker '{node.get('label') or marker_id}' von Hand auf {str(data.get('value', ''))!r} gesetzt")
    return jsonify({"status": "ok"})


@app.route("/api/events")
def api_events():
    return jsonify({"events": ENGINE.get_events(), "markers": ENGINE.get_markers(),
                    "weather": ENGINE.weather_summary()})


@app.route("/")
def index():
    return Response(INDEX_HTML, mimetype="text/html")


# --------------------------------------------------------------------------
# Frontend
# --------------------------------------------------------------------------
INDEX_HTML = r"""
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MQTT-Ablaufplaner</title>
<style>
  :root{
    --bg:#0a0c0f; --panel:#12151a; --panel-2:#171b21; --border:#242a33;
    --text:#e5e8ec; --text-dim:#8891a0;
    --accent:#ff9142; --accent-2:#3ddc97; --accent-3:#b48cff; --danger:#ff5d5d;
    --mono:'JetBrains Mono','Consolas','SFMono-Regular',monospace;
    --sans:'Inter','Segoe UI',system-ui,sans-serif;
  }
  *{box-sizing:border-box;}
  html,body{ height:100%; margin:0; }
  body{ background:var(--bg); color:var(--text); font-family:var(--sans); overflow:hidden; }
  header{
    display:flex; align-items:center; justify-content:space-between;
    padding:14px 22px; border-bottom:1px solid var(--border);
    background:linear-gradient(180deg,#0d1014,#0a0c0f);
  }
  header h1{ font-size:16px; font-weight:600; margin:0; letter-spacing:.5px; text-transform:uppercase; }
  header h1 span{ color:var(--accent); }
  .ver-badge{ font-family:var(--mono); font-size:11px; color:var(--text-dim); margin-left:8px; }
  .header-actions{ display:flex; gap:8px; align-items:center; }
  .btn{ background:var(--accent); color:#12100c; border:none; border-radius:6px; padding:8px 14px;
        font-weight:600; font-size:12.5px; cursor:pointer; }
  .btn:hover{ filter:brightness(1.1); }
  .btn-ghost{ background:transparent; color:var(--text-dim); border:1px solid var(--border);
              border-radius:6px; padding:8px 12px; font-size:12.5px; cursor:pointer; }
  .btn-ghost:hover{ color:var(--text); border-color:#3a4250; }
  .btn-mini{ background:#1b2027; color:var(--text); border:1px solid var(--border); border-radius:5px;
             padding:4px 9px; font-size:11px; cursor:pointer; font-family:var(--mono); }
  .btn-mini:hover{ border-color:var(--accent-2); }

  .layout{ display:flex; height:calc(100% - 53px); }
  .palette{ width:190px; border-right:1px solid var(--border); background:var(--panel);
            padding:14px 12px; overflow-y:auto; flex-shrink:0; }
  .palette h3{ font-size:11px; color:var(--text-dim); text-transform:uppercase; letter-spacing:.5px;
               margin:14px 0 8px; }
  .palette h3:first-child{ margin-top:0; }
  .pal-item{ display:flex; align-items:center; gap:8px; padding:9px 10px; margin-bottom:6px;
             background:var(--panel-2); border:1px solid var(--border); border-radius:7px;
             font-size:12.5px; cursor:pointer; user-select:none; }
  .pal-item:hover{ border-color:var(--accent); }
  .pal-dot{ width:9px; height:9px; border-radius:50%; flex-shrink:0; }
  .dot-in{ background:var(--accent-2); } .dot-out{ background:var(--accent); }
  .dot-delay{ background:var(--text-dim); } .dot-cond{ background:var(--accent-3); }
  .dot-marker{ background:#4cc9f0; } .dot-gate{ background:#f4d35e; } .dot-weather{ background:#7fd1ff; }

  .center{ flex:1; min-width:0; display:flex; flex-direction:column; }
  .tabs{ display:flex; align-items:flex-end; gap:2px; padding:6px 10px 0; background:var(--panel);
         border-bottom:1px solid var(--border); overflow-x:auto; flex-shrink:0; }
  .tab{ display:flex; align-items:center; gap:7px; padding:6px 12px; font-size:12px; color:var(--text-dim);
        border:1px solid transparent; border-bottom:none; border-radius:7px 7px 0 0; cursor:pointer;
        white-space:nowrap; user-select:none; }
  .tab:hover{ color:var(--text); }
  .tab.active{ background:var(--bg); color:var(--text); border-color:var(--border); }
  .tab-x{ color:var(--text-dim); font-size:14px; line-height:1; } .tab-x:hover{ color:var(--danger); }
  .tab-add{ font-weight:700; font-size:14px; } .tab-spacer{ flex:1; }
  .tab-move{ color:var(--accent-2); }
  .tab.run{ color:var(--accent); animation:tabflash .9s; }
  @keyframes tabflash{ 0%,100%{ background:transparent; } 50%{ background:#ff914233; } }
  .canvas-wrap{ flex:1; min-height:0; position:relative; overflow:auto; background:var(--bg); }
  .canvas{ position:relative; width:6000px; height:4000px;
           background:radial-gradient(circle, #1c212a 1px, transparent 1px) 0 0/22px 22px; }
  .flow-node{ transition:box-shadow .15s; }
  .flow-node.running{ box-shadow:0 0 0 2px #ff9142, 0 0 18px #ff914299; }
  .flow-node.pulse, .flow-node.res-true{ box-shadow:0 0 0 2px #3ddc97, 0 0 16px #3ddc9799; }
  .flow-node.res-false{ box-shadow:0 0 0 2px #ff5d5d, 0 0 16px #ff5d5d99; }
  .flow-node.res-out{ box-shadow:0 0 0 2px #3ddc97aa; }
  .flow-node.res-stop{ box-shadow:0 0 0 2px #8891a0; }
  .marker-val{ color:#4cc9f0; font-size:12px; }
  svg.conn-layer{ position:absolute; inset:0; width:100%; height:100%; pointer-events:none; }
  svg.conn-layer path{ pointer-events:stroke; cursor:pointer; }

  .flow-node{ position:absolute; width:190px; background:var(--panel); border:1.5px solid var(--border);
              border-radius:9px; box-shadow:0 4px 14px rgba(0,0,0,.35); user-select:none; }
  .flow-node.selected{ border-color:var(--accent); box-shadow:0 0 0 2px #ff914255; }
  .flow-node.type-mqtt_in{ border-left:4px solid var(--accent-2); }
  .flow-node.type-mqtt_out{ border-left:4px solid var(--accent); }
  .flow-node.type-delay{ border-left:4px solid var(--text-dim); }
  .flow-node.type-condition{ border-left:4px solid var(--accent-3); }
  .flow-node.type-marker{ border-left:4px solid #4cc9f0; }
  .flow-node.type-and, .flow-node.type-or{ border-left:4px solid #f4d35e; }
  .flow-node.type-weather_in{ border-left:4px solid #7fd1ff; }
  .node-head{ display:flex; align-items:center; justify-content:space-between; padding:8px 10px;
              background:var(--panel-2); border-bottom:1px solid var(--border); border-radius:8px 8px 0 0;
              cursor:grab; font-size:12px; font-weight:600; }
  .node-head:active{ cursor:grabbing; }
  .node-head > span:first-child{ flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis;
                                 white-space:nowrap; padding-right:28px; }
  .node-del{ flex:none; }
  .node-del{ color:var(--text-dim); cursor:pointer; font-size:14px; line-height:1; }
  .node-del:hover{ color:var(--danger); }
  .node-body{ padding:8px 10px 10px; font-family:var(--mono); font-size:10.5px; color:var(--text-dim);
              line-height:1.5; cursor:pointer; overflow:hidden; overflow-wrap:anywhere;
              word-break:break-word; box-sizing:border-box; max-width:100%; }
  .node-body b{ color:var(--text); font-weight:500; }
  .node-play{ position:absolute; top:6px; right:24px; font-size:11px; color:var(--text-dim); cursor:pointer; }
  .node-play:hover{ color:var(--accent-2); }

  .port{ position:absolute; width:11px; height:11px; border-radius:50%; background:#1b2027;
         border:2px solid var(--text-dim); cursor:crosshair; }
  .port:hover{ border-color:var(--accent); background:var(--accent); }
  .port-out{ right:-6px; top:50%; margin-top:-5.5px; }
  .port-in{ left:-6px; top:50%; margin-top:-5.5px; }
  .port-true{ right:-6px; top:34%; margin-top:-5.5px; border-color:var(--accent-2); }
  .port-false{ right:-6px; top:66%; margin-top:-5.5px; border-color:var(--danger); }
  .port-label{ position:absolute; font-family:var(--mono); font-size:9px; color:var(--text-dim); }
  .port-label.true{ right:14px; top:28%; color:var(--accent-2); }
  .port-label.false{ right:14px; top:60%; color:var(--danger); }
  .port-data-in, .port-data-out{ border-radius:2px; border-color:#4cc9f0; transform:rotate(45deg); }
  .port-data-in{ left:50%; top:-6px; margin-left:-5.5px; }
  .port-data-out{ left:75%; bottom:-6px; margin-left:-5.5px; }

  .side{ width:270px; border-left:1px solid var(--border); background:var(--panel); padding:14px;
         overflow-y:auto; flex-shrink:0; }
  .side h3{ font-size:11px; color:var(--text-dim); text-transform:uppercase; margin:0 0 10px; }
  .log-item{ font-family:var(--mono); font-size:11px; color:var(--text-dim); padding:6px 0;
             border-bottom:1px dashed var(--border); }
  .log-item .t{ color:var(--accent-2); margin-right:6px; }

  .modal-backdrop{ display:none; position:fixed; inset:0; background:rgba(0,0,0,.6); align-items:center;
                    justify-content:center; z-index:60; padding:20px; }
  .modal-backdrop.show{ display:flex; }
  .modal{ background:var(--panel); border:1px solid var(--border); border-radius:10px; padding:22px;
          width:100%; max-width:420px; max-height:88vh; overflow-y:auto; }
  .modal h2{ margin:0 0 16px; font-size:15px; }
  .modal label{ display:block; font-size:11.5px; color:var(--text-dim); margin:12px 0 5px; }
  .modal input, .modal select, .modal textarea{
    width:100%; background:var(--panel-2); border:1px solid var(--border); border-radius:6px;
    color:var(--text); padding:8px 10px; font-size:13px; font-family:var(--sans);
  }
  .modal textarea{ font-family:var(--mono); min-height:60px; resize:vertical; }
  .modal .row{ display:flex; gap:10px; }
  .modal .row > div{ flex:1; }
  .modal .actions{ display:flex; justify-content:flex-end; gap:8px; margin-top:20px; }
  .modal .danger-zone{ margin-top:16px; }
  .checkbox-line{ display:flex; align-items:center; gap:8px; margin-top:12px; }
  .checkbox-line input{ width:auto; }
  .modal-error{ font-family:var(--mono); font-size:11.5px; color:var(--danger); background:#2a1414;
                border:1px solid #4a1f1f; border-radius:6px; padding:8px 10px; margin-top:12px; display:none; }

  table.broker-table{ width:100%; border-collapse:collapse; font-size:12px; margin-top:10px; }
  table.broker-table th, table.broker-table td{ text-align:left; padding:6px 4px; border-bottom:1px solid var(--border); }
  table.broker-table th{ color:var(--text-dim); font-weight:500; font-size:11px; }

  .canvas{ user-select:none; }
  .marquee{ position:absolute; border:1px dashed var(--accent); background:#ff914222;
            pointer-events:none; z-index:20; }
  .wd-row{ display:flex; gap:8px; flex-wrap:wrap; margin-top:4px; }
  .wd-row label{ display:flex; align-items:center; gap:4px; margin:0; font-size:12px; color:var(--text); }
  .wd-row input{ width:auto; }
  .hint{ font-size:11.5px; color:var(--text-dim); line-height:1.5; margin-top:8px; }
  .toast-container{ position:fixed; top:14px; right:14px; z-index:80; display:flex; flex-direction:column;
                     gap:8px; max-width:320px; }
  .toast{ background:var(--panel); border:1px solid var(--border); border-radius:8px; padding:10px 12px;
          font-size:12.5px; box-shadow:0 6px 18px rgba(0,0,0,.45); }
  .toast.ok{ border-color:#2bb98755; color:var(--accent-2); }
  .toast.err{ border-color:#c0392b55; color:var(--danger); }
</style>
</head>
<body>
<header>
  <h1>MQTT-<span>Ablaufplaner</span><span class="ver-badge" id="verBadge"></span></h1>
  <div class="header-actions">
    <button class="btn-ghost" onclick="openBrokerModal()">Broker</button>
    <button class="btn-ghost" onclick="openLocationModal()">Standort</button>
    <button class="btn-ghost" onclick="openWeatherModal()">Wetter</button>
    <button class="btn" onclick="saveConfig(true)">Speichern</button>
  </div>
</header>

<div class="layout">
  <div class="palette">
    <h3>Bloecke</h3>
    <div class="pal-item" onclick="addNode('mqtt_in')"><span class="pal-dot dot-in"></span>MQTT Empfangen</div>
    <div class="pal-item" onclick="addNode('mqtt_out')"><span class="pal-dot dot-out"></span>MQTT Senden</div>
    <div class="pal-item" onclick="addNode('delay')"><span class="pal-dot dot-delay"></span>Wartezeit</div>
    <div class="pal-item" onclick="addNode('condition')"><span class="pal-dot dot-cond"></span>Bedingung</div>
    <div class="pal-item" onclick="addNode('marker')"><span class="pal-dot dot-marker"></span>Merker</div>
    <div class="pal-item" onclick="addNode('and')"><span class="pal-dot dot-gate"></span>UND</div>
    <div class="pal-item" onclick="addNode('or')"><span class="pal-dot dot-gate"></span>ODER</div>
    <div class="pal-item" onclick="addNode('weather_in')"><span class="pal-dot dot-weather"></span>Wetter (Auslöser)</div>
    <h3>Hinweise</h3>
    <div style="font-size:11.5px; color:var(--text-dim); line-height:1.6;">
      Block ziehen: am Kopf anfassen.<br>
      Mehrere Bloecke: auf leerer Flaeche einen Rahmen aufziehen und die markierten Bloecke gemeinsam am Kopf ziehen. Umschalt/Strg + Klick auf den Kopf fuegt hinzu bzw. entfernt, Strg+A waehlt alle, Esc hebt die Auswahl auf.<br>
      Verbinden: am rechten Punkt ziehen, auf linken Punkt eines anderen Blocks loslassen.<br>
      Bearbeiten: auf den Blockinhalt klicken.<br>
      Merker: speichert einen Wert; Bedingungen und Wartezeiten koennen ihn verwenden. Die Verbindung zeigt eine gestrichelte Linie automatisch.<br>
      UND/ODER: den Ergebnis-Ausgang (&#9670; unten) von Bedingungen auf den Eingang (&#9670; oben) ziehen; der Ablauf geht dann ueber Ja/Nein weiter.<br>
      Wetter (Internet): als Auslöser-Block oder als Bedingung (Typ Wetter); Standort über den Knopf oben rechts einstellen.<br>
      Seiten: oben mit + eine neue Seite anlegen, Doppelklick auf den Reiter benennt um. Merker gelten auf allen Seiten.<br>
      Loeschen: Block/Linie anklicken, dann &times; bzw. Entf.
    </div>
  </div>

  <div class="center">
    <div class="tabs" id="pageTabs"></div>
    <div class="canvas-wrap" id="canvasWrap">
      <div class="canvas" id="canvas">
        <svg class="conn-layer" id="connLayer"></svg>
      </div>
    </div>
  </div>

  <div class="side">
    <h3>Ereignisprotokoll</h3>
    <div id="logList"></div>
  </div>
</div>

<div class="toast-container" id="toasts"></div>

<!-- Node-Editor-Modal -->
<div class="modal-backdrop" id="nodeModal">
  <div class="modal">
    <h2 id="nodeModalTitle">Block bearbeiten</h2>
    <label>Bezeichnung</label>
    <input id="n_label" placeholder="z. B. Taster Wohnzimmer">
    <div id="fields_mqtt_in">
      <label>Broker</label><select id="in_broker"></select>
      <label>Topic</label><input id="in_topic" placeholder="z. B. haus/taster/1">
      <label>Erwartete Payload (leer = jede Nachricht loest aus)</label>
      <input id="in_payload" placeholder="z. B. PRESSED">
    </div>
    <div id="fields_mqtt_out">
      <label>Broker</label><select id="out_broker"></select>
      <label>Topic</label><input id="out_topic" placeholder="z. B. haus/lampe/1/set">
      <label>Payload ({payload} = zuletzt empfangene Nachricht)</label>
      <textarea id="out_payload" placeholder="z. B. ON"></textarea>
      <div class="row">
        <div><label>QoS</label>
          <select id="out_qos"><option value="0">0</option><option value="1">1</option><option value="2">2</option></select>
        </div>
        <div class="checkbox-line" style="margin-top:28px;">
          <input type="checkbox" id="out_retain"><label style="margin:0;">Retain</label>
        </div>
      </div>
      <div class="actions" style="justify-content:flex-start; margin-top:14px;">
        <button class="btn-mini" onclick="fireTest()">&#9654; Jetzt senden (Test)</button>
      </div>
    </div>
    <div id="fields_delay">
      <label>Dauer aus</label>
      <select id="delay_source" onchange="onDelaySourceChange()">
        <option value="fixed">Festwert</option>
        <option value="marker">Merker</option>
      </select>
      <div id="delay_fixed_wrap">
        <label id="delay_sec_lbl">Wartezeit (Sekunden)</label>
        <input id="delay_sec" type="number" min="0" step="0.5">
      </div>
      <div id="delay_marker_wrap">
        <label>Merker (sein Wert ist die Dauer)</label>
        <select id="delay_marker"></select>
        <label>Einheit des Merker-Werts</label>
        <select id="delay_unit">
          <option value="ms">Millisekunden</option>
          <option value="s">Sekunden</option>
          <option value="min">Minuten</option>
          <option value="h">Stunden</option>
        </select>
        <div class="hint">Ist der Merker-Wert keine gültige Zahl, wird stattdessen der Ersatzwert verwendet.</div>
      </div>
    </div>
    <div id="fields_condition">
      <label>Bedingung prüfen auf</label>
      <select id="cond_type" onchange="onCondTypeChange()">
        <option value="payload">Nachrichteninhalt (Payload)</option>
        <option value="time">Uhrzeit</option>
        <option value="sun">Sonne aufgegangen / untergegangen</option>
        <option value="weekday">Wochentag</option>
        <option value="date">Datum</option>
        <option value="marker">Merker</option>
        <option value="weather">Wetter (Internet)</option>
      </select>

      <div id="cond_payload">
        <label>Vergleichsoperator</label>
        <select id="cond_op">
          <option value="eq">ist gleich</option>
          <option value="neq">ist ungleich</option>
          <option value="contains">enthaelt</option>
          <option value="gt">ist groesser als (Zahl)</option>
          <option value="gte">ist groesser oder gleich (Zahl)</option>
          <option value="lt">ist kleiner als (Zahl)</option>
          <option value="lte">ist kleiner oder gleich (Zahl)</option>
        </select>
        <label>Vergleichswert</label>
        <input id="cond_value" placeholder="z. B. 25">
      </div>

      <div id="cond_time">
        <label>Modus</label>
        <select id="time_mode" onchange="onCondTypeChange()">
          <option value="between">zwischen (von - bis)</option>
          <option value="after">ab Uhrzeit</option>
          <option value="before">vor Uhrzeit</option>
        </select>
        <div class="row">
          <div><label id="time_from_lbl">Von</label><input id="time_from" type="time"></div>
          <div id="time_to_wrap"><label>Bis</label><input id="time_to" type="time"></div>
        </div>
        <div class="hint">Zeitfenster über Mitternacht sind möglich (z. B. 22:00 bis 06:00).</div>
      </div>

      <div id="cond_sun">
        <label>Zustand</label>
        <select id="sun_state">
          <option value="up">Sonne ist aufgegangen (Tag)</option>
          <option value="down">Sonne ist untergegangen (Nacht)</option>
        </select>
        <label>Zeitversatz in Minuten (+ später, - früher)</label>
        <input id="sun_offset" type="number" step="1" value="0">
        <div class="hint">Berechnet lokal anhand des Standorts (Knopf „Standort“ oben rechts) und der Zeitzone des Rechners.</div>
      </div>

      <div id="cond_weekday">
        <label>Wochentage, an denen die Bedingung „Ja“ ist</label>
        <div class="wd-row">
          <label><input type="checkbox" value="0">Mo</label>
          <label><input type="checkbox" value="1">Di</label>
          <label><input type="checkbox" value="2">Mi</label>
          <label><input type="checkbox" value="3">Do</label>
          <label><input type="checkbox" value="4">Fr</label>
          <label><input type="checkbox" value="5">Sa</label>
          <label><input type="checkbox" value="6">So</label>
        </div>
      </div>

      <div id="cond_marker">
        <label>Merker</label>
        <select id="cond_marker_sel"></select>
        <label>Bedingung</label>
        <select id="cond_m_op" onchange="onCondTypeChange()">
          <option value="eq">ist gleich</option>
          <option value="neq">ist ungleich</option>
          <option value="gt">ist groesser als (Zahl)</option>
          <option value="gte">ist groesser oder gleich (Zahl)</option>
          <option value="lt">ist kleiner als (Zahl)</option>
          <option value="lte">ist kleiner oder gleich (Zahl)</option>
          <option value="contains">enthaelt</option>
          <option value="true">ist wahr (1 / true / an / ja)</option>
          <option value="false">ist falsch</option>
        </select>
        <div id="cond_m_value_wrap">
          <label>Vergleichswert</label>
          <input id="cond_m_value" placeholder="z. B. 3">
        </div>
      </div>

      <div id="cond_weather"></div>

      <div id="cond_date">
        <label>Modus</label>
        <select id="date_mode" onchange="onCondTypeChange()">
          <option value="equal">an einem bestimmten Datum</option>
          <option value="between">in einem Zeitraum (von - bis)</option>
        </select>
        <div class="row">
          <div><label id="date_from_lbl">Datum</label><input id="date_from" type="date"></div>
          <div id="date_to_wrap"><label>Bis</label><input id="date_to" type="date"></div>
        </div>
        <div class="checkbox-line"><input type="checkbox" id="date_yearly"><label style="margin:0;">jährlich wiederkehrend (Jahr ignorieren)</label></div>
      </div>
    </div>
    <div id="fields_weather_in">
      <div class="hint" style="margin-top:0;">Löst den Ablauf aus, sobald das Wetter laut Internet die Angabe erfüllt. Der Wert (z. B. 26.4 oder „Regen“) steht im Ablauf als {payload} zur Verfügung.</div>
      <div id="wt_spec"></div>
      <label>Auslösen</label>
      <select id="wt_mode">
        <option value="rise">einmal, sobald die Angabe zutrifft</option>
        <option value="any">bei jedem Wechsel (zutreffend / nicht zutreffend)</option>
        <option value="while">bei jedem Abruf, solange sie zutrifft</option>
      </select>
      <div class="checkbox-line"><input type="checkbox" id="wt_start"><label style="margin:0;">auch auslösen, wenn sie beim ersten Abruf schon zutrifft</label></div>
    </div>
    <div id="fields_and"><div class="hint" style="margin-top:0;">UND: Der Ablauf geht über „Ja“ weiter, wenn <b>alle</b> verbundenen Eingänge Ja ergeben, sonst über „Nein“. Verbinden: Ergebnis-Ausgang (&#9670; unten) einer Bedingung auf den Eingang (&#9670; oben) dieses Elements ziehen.</div></div>
    <div id="fields_or"><div class="hint" style="margin-top:0;">ODER: Der Ablauf geht über „Ja“ weiter, wenn <b>mindestens ein</b> verbundener Eingang Ja ergibt, sonst über „Nein“. Verbinden: Ergebnis-Ausgang (&#9670; unten) einer Bedingung auf den Eingang (&#9670; oben) dieses Elements ziehen.</div></div>
    <div id="fields_marker">
      <div class="hint" style="margin-top:0;">Ein Merker speichert einen Wert. Läuft ein Ablauf in diesen Block, wird die Aktion ausgeführt. Bedingungen und Wartezeiten können den Wert verwenden. Die Bezeichnung oben ist der Name des Merkers.</div>
      <label>Aktueller Wert (live)</label>
      <div class="row">
        <div><input id="m_current" oninput="mCurrentDirty = true" placeholder="-"></div>
        <div style="flex:0 0 auto;"><button class="btn-mini" style="margin-top:2px;" onclick="setMarkerNow()">Jetzt setzen</button></div>
      </div>
      <label>Startwert (leer = noch kein Wert)</label>
      <input id="m_initial" placeholder="z. B. 0">
      <label>Aktion, wenn der Ablauf den Block erreicht</label>
      <select id="m_action" onchange="onMarkerActionChange(true)">
        <option value="set">Wert setzen</option>
        <option value="store">Empfangenen Wert speichern (Nachricht / JSON-Feld)</option>
        <option value="toggle">Umschalten (0 / 1)</option>
        <option value="inc">Erhöhen (zählen)</option>
        <option value="dec">Verringern</option>
        <option value="reset">Auf Startwert zurücksetzen</option>
      </select>
      <div id="m_value_wrap">
        <label id="m_value_lbl">Wert</label>
        <input id="m_value" placeholder="z. B. 1">
      </div>
      <div class="checkbox-line"><input type="checkbox" id="m_retain"><label style="margin:0;">Wert nach einem Neustart behalten</label></div>
    </div>
    <div class="modal-error" id="nodeError"></div>
    <div class="actions">
      <button class="btn-ghost" onclick="closeNodeModal()">Abbrechen</button>
      <button class="btn" onclick="submitNode()">Speichern</button>
    </div>
  </div>
</div>

<!-- Broker-Modal -->
<div class="modal-backdrop" id="brokerModal">
  <div class="modal" style="max-width:520px;">
    <h2>MQTT-Broker</h2>
    <table class="broker-table" id="brokerTable"><thead>
      <tr><th>Name</th><th>Host</th><th>Port</th><th></th></tr>
    </thead><tbody></tbody></table>
    <label style="margin-top:18px;">Name</label><input id="b_name" placeholder="z. B. Haus-Broker">
    <div class="row">
      <div><label>Host/IP</label><input id="b_host" placeholder="192.168.1.10"></div>
      <div><label>Port</label><input id="b_port" type="number" value="1883"></div>
    </div>
    <div class="row">
      <div><label>Benutzername</label><input id="b_user"></div>
      <div><label>Passwort</label><input id="b_pass" type="password"></div>
    </div>
    <div class="checkbox-line"><input type="checkbox" id="b_tls"><label style="margin:0;">TLS/SSL</label></div>
    <div class="modal-error" id="brokerError"></div>
    <div class="actions">
      <button class="btn-ghost" onclick="closeBrokerModal()">Schliessen</button>
      <button class="btn" onclick="submitBroker()">Broker hinzufuegen</button>
    </div>
  </div>
</div>

<!-- Wetter -->
<div class="modal-backdrop" id="weatherModal">
  <div class="modal" style="max-width:460px;">
    <h2>Aktuelles Wetter (Internet)</h2>
    <div id="weatherBody"></div>
    <div class="modal-error" id="weatherError"></div>
    <div class="hint">Quelle: Open-Meteo (kostenlos, ohne Anmeldung) für den eingestellten Standort. Die Daten werden höchstens im eingestellten Intervall neu abgerufen.</div>
    <div class="actions">
      <button class="btn-ghost" onclick="closeWeatherModal()">Schließen</button>
      <button class="btn" onclick="refreshWeather(true)">Jetzt abrufen</button>
    </div>
  </div>
</div>

<!-- Auswahl verschieben -->
<div class="modal-backdrop" id="moveModal">
  <div class="modal">
    <h2>Markierte Elemente verschieben nach</h2>
    <div id="moveList"></div>
    <div class="hint">Linien zu Elementen, die auf der alten Seite bleiben, werden entfernt. Merker gelten seitenübergreifend.</div>
    <div class="actions"><button class="btn-ghost" onclick="closeMoveModal()">Abbrechen</button></div>
  </div>
</div>

<!-- Standort-Modal -->
<div class="modal-backdrop" id="locationModal">
  <div class="modal">
    <h2>Standort &amp; Wetter</h2>
    <label>Stadt oder Ort</label>
    <div style="display:flex;gap:8px;">
      <input id="loc_city" type="text" placeholder="z. B. Hamburg" style="flex:1"
             onkeydown="if(event.key==='Enter'){event.preventDefault();searchCity();}">
      <button class="btn-ghost" type="button" onclick="searchCity()">Suchen</button>
    </div>
    <select id="loc_results" size="1" style="display:none;margin-top:8px;width:100%" onchange="pickCity()"></select>
    <div class="hint" id="loc_hint">Ort eingeben und &quot;Suchen&quot; klicken (benötigt Internet) - die Koordinaten werden automatisch eingetragen. Alternativ unten von Hand eingeben.</div>
    <div class="row">
      <div><label>Breitengrad</label><input id="loc_lat" type="number" step="0.0001" min="-90" max="90" oninput="coordsEdited()"></div>
      <div><label>Längengrad</label><input id="loc_lon" type="number" step="0.0001" min="-180" max="180" oninput="coordsEdited()"></div>
    </div>
    <div class="hint">Beispiel Berlin: 52.5200 / 13.4050. Die Zeiten werden lokal berechnet (keine Internetverbindung nötig) und nutzen die Zeitzone des Rechners bzw. Routers.</div>
    <label>Wetterabruf alle (Minuten)</label>
    <input id="loc_interval" type="number" min="5" max="120" step="1">
    <div class="hint">Gilt für Wetter-Auslöser und -Bedingungen (5 bis 120 Minuten). Zu häufige Abrufe belasten den kostenlosen Dienst.</div>
    <div class="modal-error" id="locError"></div>
    <div class="actions">
      <button class="btn-ghost" onclick="closeLocationModal()">Abbrechen</button>
      <button class="btn" onclick="submitLocation()">Speichern</button>
    </div>
  </div>
</div>

<script>
let STATE = { brokers: [], nodes: [], connections: [], location: { lat: 51.1657, lon: 10.4515 },
              pages: [{ id: 'page_1', name: 'Seite 1' }], weather: { interval_min: 10 } };
let selectedIds = new Set(), selectedConnId = null, editingNodeId = null;
let dragGroup = null, marquee = null;
let MARKER_VALUES = {};
let currentPage = 'page_1';
const pageScroll = {};
let dragNode = null, dragOffset = {x:0,y:0};
let drawingConn = null; // {fromId, fromPort, x1, y1}
let saveTimer = null;

function toast(msg, ok=true){
  const c = document.getElementById('toasts');
  const el = document.createElement('div');
  el.className = 'toast ' + (ok ? 'ok' : 'err');
  el.textContent = msg;
  c.appendChild(el);
  setTimeout(()=> el.remove(), 3200);
}
function escapeHtml(s){
  return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

const NODE_META = {
  mqtt_in:    { title: 'MQTT Empfangen', hasIn:false, outs:['out'] },
  mqtt_out:   { title: 'MQTT Senden',    hasIn:true,  outs:['out'] },
  delay:      { title: 'Wartezeit',      hasIn:true,  outs:['out'] },
  condition:  { title: 'Bedingung',      hasIn:true,  outs:['true','false'], dataOut:true },
  marker:     { title: 'Merker',         hasIn:true,  outs:['out'] },
  weather_in: { title: 'Wetter-Auslöser', hasIn:false, outs:['out'] },
  and:        { title: 'UND',            hasIn:true,  outs:['true','false'], dataIn:true, dataOut:true },
  or:         { title: 'ODER',           hasIn:true,  outs:['true','false'], dataIn:true, dataOut:true },
};

async function loadVersion(){
  try{
    const res = await fetch('/api/version');
    const data = await res.json();
    document.getElementById('verBadge').textContent = 'v' + data.version;
  } catch(e){}
}

async function loadConfig(){
  const res = await fetch('/api/config');
  const data = await res.json();
  STATE.brokers = data.brokers; STATE.nodes = data.nodes; STATE.connections = data.connections;
  if (data.pages && data.pages.length) STATE.pages = data.pages;
  if (data.weather) STATE.weather = data.weather;
  let savedPage = null;
  try { savedPage = localStorage.getItem('ablaufPage'); } catch(e) {}
  currentPage = STATE.pages.some(p => p.id === savedPage) ? savedPage : STATE.pages[0].id;
  if (data.location) STATE.location = data.location;
  MARKER_VALUES = data._markers || {};
  renderAll();
  renderPageTabs();
  renderLog(data._events || []);
}

function scheduleSave(){
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => saveConfig(false), 700);
}

async function saveConfig(showToast){
  const res = await fetch('/api/config', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ server: {host:'0.0.0.0', port:8020}, brokers: STATE.brokers,
                           nodes: STATE.nodes, connections: STATE.connections,
                           location: STATE.location, pages: STATE.pages, weather: STATE.weather })
  });
  if (showToast){
    if (res.ok) toast('Gespeichert.'); else toast('Fehler beim Speichern.', false);
  }
}

// ---------------- Bloecke ----------------
function addNode(type){
  const count = STATE.nodes.filter(n => n.page === currentPage).length;
  const node = {
    id: 'node_' + Math.random().toString(16).slice(2,10),
    page: currentPage,
    type, x: 60 + (count % 6) * 40, y: 60 + Math.floor(count/6) * 120,
    label:'', broker_id:'', topic:'', payload:'', qos:0, retain:false,
    delay_sec:1.0, operator:'eq', compare_value:'',
    cond_type:'payload', time_mode:'between', time_from:'08:00', time_to:'18:00',
    sun_state:'up', sun_offset_min:0, weekdays:[0,1,2,3,4],
    date_mode:'equal', date_from:'', date_to:'', date_yearly:false,
    marker_initial:'0', marker_action:'set', marker_value:'1', marker_retain:false,
    marker_id:'', marker_op:'eq', marker_cmp:'',
    delay_source:'fixed', delay_marker_id:'', delay_marker_unit:'s',
    weather_metric:'temperature', weather_op:'gt', weather_value:'25', weather_state:'rain',
    weather_mode:'rise', weather_fire_start:false
  };
  if (type === 'marker') node.label = nextMarkerName();
  STATE.nodes.push(node);
  renderAll(); scheduleSave();
}

function nodeById(id){ return STATE.nodes.find(n => n.id === id); }

function renderAll(){
  updateCanvasSize();
  renderNodes();
  renderConnections();
}

function nodeSummary(n){
  const b = STATE.brokers.find(x => x.id === n.broker_id);
  if (n.type === 'mqtt_in') return `Broker: <b>${escapeHtml(b?b.name:'-')}</b><br>Topic: <b>${escapeHtml(n.topic||'-')}</b>`;
  if (n.type === 'mqtt_out') return `Broker: <b>${escapeHtml(b?b.name:'-')}</b><br>Topic: <b>${escapeHtml(n.topic||'-')}</b><br>Payload: <b>${escapeHtml(n.payload||'-')}</b>`;
  if (n.type === 'delay') return delaySummary(n);
  if (n.type === 'condition') return condSummary(n);
  if (n.type === 'marker') return markerSummary(n);
  if (n.type === 'and' || n.type === 'or') return gateSummary(n);
  if (n.type === 'weather_in') return weatherTriggerSummary(n);
  return '';
}
function opLabel(op){
  return {eq:'=', neq:'&ne;', contains:'enthaelt', gt:'&gt;', lt:'&lt;'}[op] || op;
}

function renderNodes(){
  const canvas = document.getElementById('canvas');
  canvas.querySelectorAll('.flow-node').forEach(el => el.remove());
  STATE.nodes.filter(n => n.page === currentPage).forEach(n => {
    const meta = NODE_META[n.type];
    const div = document.createElement('div');
    div.className = 'flow-node type-' + n.type + (selectedIds.has(n.id) ? ' selected' : '');
    div.style.left = n.x + 'px'; div.style.top = n.y + 'px';
    div.dataset.id = n.id;

    const playBtn = (n.type === 'mqtt_in' || n.type === 'weather_in') ? `<span class="node-play" onclick="simulateNode('${n.id}', event)">&#9654;</span>`
                  : n.type === 'marker' ? `<span class="node-play" title="Wert von Hand setzen" onclick="editMarkerValue('${n.id}', event)">&#9998;</span>` : '';
    div.innerHTML = `
      <div class="node-head" onmousedown="startDrag(event,'${n.id}')">
        <span>${escapeHtml(n.label || meta.title)}</span>
        <span class="node-del" onclick="deleteNode('${n.id}', event)">&times;</span>
      </div>
      ${playBtn}
      <div class="node-body" onclick="openNodeModal('${n.id}')">${nodeSummary(n)}</div>
    `;
    if (meta.hasIn){
      const p = document.createElement('div');
      p.className='port port-in'; p.dataset.node=n.id; p.dataset.port='in';
      p.onmouseup = (e)=>finishConn(n.id, e, 'in');
      div.appendChild(p);
    }
    if (meta.outs.length === 1){
      const p = document.createElement('div');
      p.className='port port-out'; p.dataset.node=n.id; p.dataset.port='out';
      p.onmousedown = (e)=>startConn(n.id, 'out', e);
      div.appendChild(p);
    } else {
      meta.outs.forEach(portName => {
        const p = document.createElement('div');
        p.className = 'port port-' + portName; p.dataset.node=n.id; p.dataset.port=portName;
        p.onmousedown = (e)=>startConn(n.id, portName, e);
        div.appendChild(p);
        const lbl = document.createElement('div');
        lbl.className = 'port-label ' + portName;
        lbl.textContent = portName === 'true' ? 'Ja' : 'Nein';
        div.appendChild(lbl);
      });
    }
    if (meta.dataIn){
      const p = document.createElement('div');
      p.className = 'port port-data-in'; p.dataset.node = n.id; p.dataset.port = 'data';
      p.title = 'Eingänge: Ergebnisse von Bedingungen bzw. UND/ODER-Elementen';
      p.onmouseup = (e) => finishConn(n.id, e, 'data');
      div.appendChild(p);
    }
    if (meta.dataOut){
      const p = document.createElement('div');
      p.className = 'port port-data-out'; p.dataset.node = n.id; p.dataset.port = 'result';
      p.title = 'Ergebnis (Ja/Nein) mit dem Eingang eines UND/ODER-Elements verbinden';
      p.onmousedown = (e) => startConn(n.id, 'result', e);
      div.appendChild(p);
    }
    canvas.appendChild(div);
  });
}

function nodePortPos(nodeId, port){
  const el = document.querySelector(`.flow-node[data-id="${nodeId}"]`);
  if (!el) return {x:0,y:0};
  const w = el.offsetWidth, h = el.offsetHeight;
  const x = el.offsetLeft, y = el.offsetTop;
  if (port === 'data') return {x: x + w*0.5, y: y};
  if (port === 'result') return {x: x + w*0.75, y: y + h};
  if (port === 'in') return {x: x, y: y + h/2};
  if (port === 'true') return {x: x + w, y: y + h*0.34};
  if (port === 'false') return {x: x + w, y: y + h*0.66};
  return {x: x + w, y: y + h/2}; // out
}

function pathBetween(p1, p2){
  const dx = Math.max(40, Math.abs(p2.x - p1.x) / 2);
  return `M ${p1.x} ${p1.y} C ${p1.x+dx} ${p1.y}, ${p2.x-dx} ${p2.y}, ${p2.x} ${p2.y}`;
}

function pathVertical(p1, p2, dir){
  const dy = Math.max(30, Math.abs(p2.y - p1.y) / 2) * (dir || 1);
  return `M ${p1.x} ${p1.y} C ${p1.x} ${p1.y + dy}, ${p2.x} ${p2.y - dy}, ${p2.x} ${p2.y}`;
}
function svgEl(tag, attrs){
  const el = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const k in attrs) el.setAttribute(k, attrs[k]);
  return el;
}
function nodeBox(id){
  const el = document.querySelector(`.flow-node[data-id="${id}"]`);
  return el ? { x: el.offsetLeft, y: el.offsetTop, w: el.offsetWidth, h: el.offsetHeight } : null;
}
// Bedingungen und Wartezeiten, die den Wert eines Merkers auswerten
function markerReferences(){
  const refs = [];
  STATE.nodes.forEach(n => {
    if (n.type === 'condition' && n.cond_type === 'marker' && n.marker_id) refs.push({ marker: n.marker_id, target: n.id });
    if (n.type === 'delay' && n.delay_source === 'marker' && n.delay_marker_id) refs.push({ marker: n.delay_marker_id, target: n.id });
  });
  return refs.filter(r => { const m = nodeById(r.marker); return m && m.type === 'marker'; });
}
function gateSummary(n){
  const cnt = STATE.connections.filter(c => c.to === n.id && c.to_port === 'data').length;
  const rule = n.type === 'and' ? 'Ja, wenn <b>alle</b> Eingänge Ja sind' : 'Ja, wenn <b>mindestens ein</b> Eingang Ja ist';
  return `${rule}<br>Eingänge: <b>${cnt}</b>`;
}
function dataReaches(startId, targetId){
  const seen = new Set(), stack = [startId];
  while (stack.length){
    const cur = stack.pop();
    if (cur === targetId) return true;
    if (seen.has(cur)) continue;
    seen.add(cur);
    STATE.connections.forEach(c => { if (c.to_port === 'data' && c.from === cur) stack.push(c.to); });
  }
  return false;
}

function renderConnections(){
  const svg = document.getElementById('connLayer');
  svg.innerHTML = '';
  const defs = svgEl('defs', {});
  const arrow = svgEl('marker', { id:'arrowData', viewBox:'0 0 10 10', refX:'9', refY:'5',
                                  markerWidth:'7', markerHeight:'7', orient:'auto' });
  arrow.appendChild(svgEl('path', { d:'M 0 1 L 10 5 L 0 9 z', fill:'#4cc9f0' }));
  defs.appendChild(arrow); svg.appendChild(defs);

  // 1) Merker-Bezuege: entstehen automatisch, sobald eine Bedingung/Wartezeit einen Merker auswertet
  markerReferences().forEach(r => {
    const a = nodeBox(r.marker), b = nodeBox(r.target);
    if (!a || !b) return;
    const down = (b.y + b.h / 2) >= (a.y + a.h / 2);
    const p1 = { x: a.x + a.w * 0.5, y: down ? a.y + a.h : a.y };
    const p2 = { x: b.x + b.w * 0.25, y: down ? b.y : b.y + b.h };
    const path = svgEl('path', { d: pathVertical(p1, p2, down ? 1 : -1), fill:'none', stroke:'#4cc9f0aa',
      'stroke-width':'1.6', 'stroke-dasharray':'6,4', 'marker-end':'url(#arrowData)',
      'data-ref': r.marker + '>' + r.target, 'data-key': 'r:' + r.marker + '|' + r.target });
    path.style.pointerEvents = 'none';
    svg.appendChild(path);
  });

  // 2) Ablauflinien (durchgezogen) und Datenlinien zu UND/ODER (gestrichelt)
  STATE.connections.forEach(c => {
    if (!nodeBox(c.from) || !nodeBox(c.to)) return;   // Element liegt auf einer anderen Seite
    const isData = c.to_port === 'data';
    const p1 = nodePortPos(c.from, c.from_port || 'out');
    const p2 = nodePortPos(c.to, isData ? 'data' : 'in');
    const sel = c.id === selectedConnId;
    const path = svgEl('path', { d: isData ? pathVertical(p1, p2, 1) : pathBetween(p1, p2), fill:'none',
      stroke: sel ? '#ff9142' : (isData ? '#4cc9f0cc' : '#3ddc9799'), 'stroke-width': sel ? '2.5' : '2', 'data-key': 'c:' + c.id });
    if (isData){ path.setAttribute('stroke-dasharray', '6,4'); path.setAttribute('marker-end', 'url(#arrowData)'); }
    path.style.pointerEvents = 'stroke';
    path.onclick = () => { selectedConnId = c.id; selectedIds.clear(); renderAll(); };
    svg.appendChild(path);
  });
}

function renderLog(events){
  const el = document.getElementById('logList');
  el.innerHTML = events.map(e => `<div class="log-item"><span class="t">${escapeHtml(e.time)}</span>${escapeHtml(e.text)}</div>`).join('')
                 || '<div style="color:var(--text-dim); font-size:12px;">Noch keine Ereignisse.</div>';
}
async function pollLog(){
  try{
    const res = await fetch('/api/events');
    const data = await res.json();
    renderLog(data.events || []);
    if (data.markers){ MARKER_VALUES = data.markers; updateMarkerValues(); }
    if (data.weather){ WEATHER = data.weather; updateWeatherLive(); }
  } catch(e){}
}

// ---------------- Verschieben & Mehrfachauswahl ----------------
function setSelection(ids){
  selectedIds = new Set(ids);
  document.querySelectorAll('.flow-node').forEach(el => {
    el.classList.toggle('selected', selectedIds.has(el.dataset.id));
  });
}

function startDrag(e, id){
  if (e.target.closest('.node-del')) return; // Klick auf das x: kein Drag
  e.preventDefault();
  selectedConnId = null;
  if (e.shiftKey || e.ctrlKey || e.metaKey){      // Auswahl umschalten, kein Drag
    const ids = new Set(selectedIds);
    if (ids.has(id)) ids.delete(id); else ids.add(id);
    setSelection(ids); renderConnections();
    return;
  }
  if (!selectedIds.has(id)) setSelection([id]);   // sonst ganze Gruppe behalten
  const wrap = document.getElementById('canvasWrap');
  dragGroup = {
    sx: e.clientX + wrap.scrollLeft, sy: e.clientY + wrap.scrollTop, moved: false,
    items: [...selectedIds].map(i => nodeById(i)).filter(Boolean).map(n => ({ id: n.id, x: n.x, y: n.y }))
  };
  renderConnections();
}

function startMarquee(e){
  const canvas = document.getElementById('canvas');
  const rect = canvas.getBoundingClientRect();
  const additive = e.shiftKey || e.ctrlKey || e.metaKey;
  const el = document.createElement('div');
  el.className = 'marquee';
  canvas.appendChild(el);
  marquee = { el, x0: e.clientX - rect.left, y0: e.clientY - rect.top,
              base: additive ? new Set(selectedIds) : new Set() };
  if (!additive) setSelection([]);
  selectedConnId = null; renderConnections();
  updateMarquee(e);
}
function updateMarquee(e){
  const rect = document.getElementById('canvas').getBoundingClientRect();
  const x1 = e.clientX - rect.left, y1 = e.clientY - rect.top;
  const x = Math.min(marquee.x0, x1), y = Math.min(marquee.y0, y1);
  const w = Math.abs(x1 - marquee.x0), h = Math.abs(y1 - marquee.y0);
  Object.assign(marquee.el.style, { left: x + 'px', top: y + 'px', width: w + 'px', height: h + 'px' });
  const ids = new Set(marquee.base);
  document.querySelectorAll('.flow-node').forEach(el => {
    const l = el.offsetLeft, t = el.offsetTop, r = l + el.offsetWidth, b = t + el.offsetHeight;
    if (l < x + w && r > x && t < y + h && b > y) ids.add(el.dataset.id);
  });
  setSelection(ids);
}
function endMarquee(){ marquee.el.remove(); marquee = null; }

document.addEventListener('mousemove', (e) => {
  const wrap = document.getElementById('canvasWrap');
  if (dragGroup || marquee) autoScroll(e, wrap);
  if (dragGroup){
    let dx = e.clientX + wrap.scrollLeft - dragGroup.sx;
    let dy = e.clientY + wrap.scrollTop - dragGroup.sy;
    dx = Math.max(dx, -Math.min(...dragGroup.items.map(i => i.x)));   // nicht ueber den Rand schieben
    dy = Math.max(dy, -Math.min(...dragGroup.items.map(i => i.y)));
    dragGroup.items.forEach(it => {
      const n = nodeById(it.id);
      if (!n) return;
      n.x = it.x + dx; n.y = it.y + dy;
      const el = document.querySelector(`.flow-node[data-id="${it.id}"]`);
      if (el){ el.style.left = n.x + 'px'; el.style.top = n.y + 'px'; }
    });
    if (dx !== 0 || dy !== 0) dragGroup.moved = true;
    updateCanvasSize();
    renderConnections();
  }
  if (marquee) updateMarquee(e);
  if (drawingConn){
    const rect = document.getElementById('canvas').getBoundingClientRect();
    drawingConn.x2 = e.clientX - rect.left; drawingConn.y2 = e.clientY - rect.top;
    drawTempLine();
  }
});
document.addEventListener('mouseup', () => {
  if (dragGroup){ const moved = dragGroup.moved; dragGroup = null; if (moved) scheduleSave(); }
  if (marquee) endMarquee();
});

document.addEventListener('keydown', (e) => {
  const tag = document.activeElement.tagName;
  if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
  if (document.querySelector('.modal-backdrop.show')) return;
  if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'a'){
    e.preventDefault(); setSelection(STATE.nodes.filter(n => n.page === currentPage).map(n => n.id));
  } else if (e.key === 'Escape'){
    setSelection([]); selectedConnId = null; renderConnections();
  }
});

function deleteSelected(){
  const ids = new Set(selectedIds);
  if (!ids.size) return;
  warnMarkerUsed([...ids]);
  STATE.nodes = STATE.nodes.filter(n => !ids.has(n.id));
  STATE.connections = STATE.connections.filter(c => !ids.has(c.from) && !ids.has(c.to));
  selectedIds.clear();
  renderAll(); scheduleSave();
}

// ---------------- Verbinden ----------------
function startConn(nodeId, port, e){
  e.stopPropagation(); e.preventDefault();
  const pos = nodePortPos(nodeId, port);
  drawingConn = { fromId: nodeId, fromPort: port, x1: pos.x, y1: pos.y, x2: pos.x, y2: pos.y };
}
function drawTempLine(){
  let el = document.getElementById('tempConnLine');
  const svg = document.getElementById('connLayer');
  if (!el){
    el = document.createElementNS('http://www.w3.org/2000/svg','path');
    el.id = 'tempConnLine'; el.setAttribute('fill','none');
    el.setAttribute('stroke', '#ff9142'); el.setAttribute('stroke-width','2'); el.setAttribute('stroke-dasharray','5,4');
    svg.appendChild(el);
  }
  const a = {x:drawingConn.x1,y:drawingConn.y1}, b = {x:drawingConn.x2,y:drawingConn.y2};
  el.setAttribute('d', drawingConn.fromPort === 'result' ? pathVertical(a, b) : pathBetween(a, b));
}
function finishConn(targetId, e, toPort){
  e.stopPropagation();
  if (!drawingConn) return;
  toPort = toPort || 'in';
  const isDataLine = drawingConn.fromPort === 'result';
  const tmp = document.getElementById('tempConnLine');
  const abort = (msg) => { drawingConn = null; if (tmp) tmp.remove(); renderConnections(); if (msg) toast(msg, false); };
  if (drawingConn.fromId === targetId) return abort();
  if (isDataLine !== (toPort === 'data')) return abort(isDataLine
    ? 'Ergebnis-Ausgänge (unten) lassen sich nur mit den Eingängen (oben) eines UND/ODER-Elements verbinden.'
    : 'Ablauf-Ausgänge (rechts) lassen sich nur mit Ablauf-Eingängen (links) verbinden.');
  if (isDataLine && dataReaches(targetId, drawingConn.fromId)) return abort('Diese Verbindung würde einen Kreis bilden.');
  const exists = STATE.connections.some(c => c.from === drawingConn.fromId && c.from_port === drawingConn.fromPort
    && c.to === targetId && (c.to_port || 'in') === toPort);
  if (!exists){
    STATE.connections.push({ id:'conn_' + Math.random().toString(16).slice(2,10),
      from: drawingConn.fromId, from_port: drawingConn.fromPort, to: targetId, to_port: toPort });
    scheduleSave();
  }
  drawingConn = null;
  if (tmp) tmp.remove();
  renderAll();
}
document.getElementById('canvasWrap').addEventListener('mouseup', () => {
  if (drawingConn){
    drawingConn = null;
    const tmp = document.getElementById('tempConnLine'); if (tmp) tmp.remove();
    renderConnections();
  }
});
document.getElementById('canvasWrap').addEventListener('mousedown', (e) => {
  if (e.button !== 0) return;
  if (e.target.id !== 'canvasWrap' && e.target.id !== 'canvas') return;
  const wrap = e.currentTarget, r = wrap.getBoundingClientRect();
  if (e.clientX > r.left + wrap.clientWidth || e.clientY > r.top + wrap.clientHeight) return; // Scrollbalken
  e.preventDefault();
  startMarquee(e);
});

document.addEventListener('keydown', (e) => {
  if ((e.key === 'Delete' || e.key === 'Backspace') && document.activeElement.tagName !== 'INPUT'
      && document.activeElement.tagName !== 'TEXTAREA'){
    if (document.querySelector('.modal-backdrop.show')) return;
    if (selectedIds.size){ deleteSelected(); }
    else if (selectedConnId){ deleteConnById(selectedConnId); }
  }
});

function deleteNode(id, e){
  if (e) e.stopPropagation();
  warnMarkerUsed([id]);
  STATE.nodes = STATE.nodes.filter(n => n.id !== id);
  STATE.connections = STATE.connections.filter(c => c.from !== id && c.to !== id);
  selectedIds.delete(id);
  renderAll(); scheduleSave();
}
function deleteConnById(id){
  STATE.connections = STATE.connections.filter(c => c.id !== id);
  selectedConnId = null;
  renderAll(); scheduleSave();
}

async function simulateNode(id, e){
  e.stopPropagation();
  const payload = (nodeById(id) || {}).type === 'weather_in' ? '' : prompt('Test-Payload fuer dieses Ereignis:', '');
  if (payload === null) return;
  await fetch(`/api/nodes/${id}/simulate`, { method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ payload }) });
  toast('Ablauf simuliert.');
  setTimeout(pollLog, 400);
}

// ---------------- Node-Editor ----------------
function fillBrokerSelect(sel, selectedId){
  sel.innerHTML = STATE.brokers.length
    ? STATE.brokers.map(b => `<option value="${b.id}" ${b.id===selectedId?'selected':''}>${escapeHtml(b.name)} (${escapeHtml(b.host)})</option>`).join('')
    : `<option value="">-- zuerst einen Broker anlegen --</option>`;
}

function openNodeModal(id){
  editingNodeId = id;
  const n = nodeById(id);
  document.getElementById('nodeError').style.display = 'none';
  document.getElementById('nodeModalTitle').textContent = NODE_META[n.type].title + ' bearbeiten';
  document.getElementById('n_label').value = n.label || '';
  ['fields_mqtt_in','fields_mqtt_out','fields_delay','fields_condition','fields_marker','fields_and','fields_or','fields_weather_in'].forEach(id2 => {
    document.getElementById(id2).style.display = (id2 === 'fields_' + n.type) ? 'block' : 'none';
  });
  if (n.type === 'mqtt_in'){
    fillBrokerSelect(document.getElementById('in_broker'), n.broker_id);
    document.getElementById('in_topic').value = n.topic || '';
    document.getElementById('in_payload').value = n.payload || '';
  } else if (n.type === 'mqtt_out'){
    fillBrokerSelect(document.getElementById('out_broker'), n.broker_id);
    document.getElementById('out_topic').value = n.topic || '';
    document.getElementById('out_payload').value = n.payload || '';
    document.getElementById('out_qos').value = n.qos || 0;
    document.getElementById('out_retain').checked = !!n.retain;
  } else if (n.type === 'delay'){
    loadDelayFields(n);
  } else if (n.type === 'condition'){
    loadCondFields(n);
  } else if (n.type === 'marker'){
    loadMarkerFields(n);
  } else if (n.type === 'weather_in'){
    loadWeatherTrigger(n);
  }
  document.getElementById('nodeModal').classList.add('show');
}
function closeNodeModal(){
  document.getElementById('nodeModal').classList.remove('show');
  editingNodeId = null;
}
function submitNode(){
  const n = nodeById(editingNodeId);
  const errEl = document.getElementById('nodeError');
  const prevLabel = n.label;
  n.label = document.getElementById('n_label').value.trim();
  if (n.type === 'mqtt_in'){
    const bid = document.getElementById('in_broker').value;
    const topic = document.getElementById('in_topic').value.trim();
    if (!bid || !topic){ errEl.textContent='Broker und Topic sind erforderlich.'; errEl.style.display='block'; return; }
    n.broker_id = bid; n.topic = topic; n.payload = document.getElementById('in_payload').value.trim();
  } else if (n.type === 'mqtt_out'){
    const bid = document.getElementById('out_broker').value;
    const topic = document.getElementById('out_topic').value.trim();
    if (!bid || !topic){ errEl.textContent='Broker und Topic sind erforderlich.'; errEl.style.display='block'; return; }
    n.broker_id = bid; n.topic = topic; n.payload = document.getElementById('out_payload').value;
    n.qos = parseInt(document.getElementById('out_qos').value)||0;
    n.retain = document.getElementById('out_retain').checked;
  } else if (n.type === 'delay'){
    if (!readDelayFields(n, errEl)) return;
  } else if (n.type === 'condition'){
    if (!readCondFields(n, errEl)) return;
  } else if (n.type === 'marker'){
    if (!readMarkerFields(n, errEl, prevLabel)) return;
  } else if (n.type === 'weather_in'){
    if (!readWeatherTrigger(n, errEl)) return;
  }
  closeNodeModal();
  renderAll(); scheduleSave();
  toast('Block aktualisiert.');
}
async function fireTest(){
  const n = nodeById(editingNodeId);
  if (!n || n.type !== 'mqtt_out') return;
  const bid = document.getElementById('out_broker').value;
  if (!bid){ toast('Bitte zuerst einen Broker waehlen.', false); return; }
  const res = await fetch('/api/mqtt-test', { method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({
      broker_id: bid, topic: document.getElementById('out_topic').value,
      payload: document.getElementById('out_payload').value,
      qos: parseInt(document.getElementById('out_qos').value)||0,
      retain: document.getElementById('out_retain').checked
    })});
  const data = await res.json();
  if (res.ok) toast('Testnachricht gesendet.'); else toast(data.error || 'Fehler beim Senden.', false);
}

const WD = ['Mo','Di','Mi','Do','Fr','Sa','So'];
function condSummary(n){
  const t = n.cond_type || 'payload';
  if (t === 'time'){
    if (n.time_mode === 'after') return `Uhrzeit ab <b>${escapeHtml(n.time_from||'-')}</b>`;
    if (n.time_mode === 'before') return `Uhrzeit vor <b>${escapeHtml(n.time_from||'-')}</b>`;
    return `Uhrzeit <b>${escapeHtml(n.time_from||'-')} - ${escapeHtml(n.time_to||'-')}</b>`;
  }
  if (t === 'sun'){
    const off = parseInt(n.sun_offset_min) || 0;
    const offTxt = off ? ` (${off > 0 ? '+' : ''}${off} min)` : '';
    return `Sonne ist <b>${n.sun_state === 'down' ? 'untergegangen' : 'aufgegangen'}</b>${offTxt}`;
  }
  if (t === 'weekday'){
    const days = (n.weekdays || []).map(Number).sort((a,b) => a-b).map(d => WD[d]).join(', ');
    return `Wochentag: <b>${escapeHtml(days || '-')}</b>`;
  }
  if (t === 'date'){
    const y = n.date_yearly ? ' (jährlich)' : '';
    if (n.date_mode === 'between') return `Datum <b>${escapeHtml(n.date_from||'-')} bis ${escapeHtml(n.date_to||'-')}</b>${y}`;
    return `Datum ist <b>${escapeHtml(n.date_from||'-')}</b>${y}`;
  }
  if (t === 'weather') return `Wetter (Internet): <b>${escapeHtml(weatherSpecText(n))}</b>`;
  const OPS = {eq:'=', neq:'&ne;', contains:'enthält', gt:'&gt;', gte:'&ge;', lt:'&lt;', lte:'&le;'};
  if (t === 'marker'){
    const m = nodeById(n.marker_id);
    const nm = escapeHtml(m ? markerLabelFor(m, n) : '(gelöscht)');
    if (n.marker_op === 'true') return `Merker <b>${nm}</b> ist wahr`;
    if (n.marker_op === 'false') return `Merker <b>${nm}</b> ist falsch`;
    return `Merker <b>${nm}</b> ${OPS[n.marker_op] || '='} <b>${escapeHtml(n.marker_cmp || '-')}</b>`;
  }
  const lbl = OPS[n.operator] || escapeHtml(n.operator || '');
  return `Wenn Wert ${lbl} <b>${escapeHtml(n.compare_value||'-')}</b>`;
}

function onCondTypeChange(){
  const t = document.getElementById('cond_type').value;
  ['payload','time','sun','weekday','date','marker','weather'].forEach(k => {
    document.getElementById('cond_' + k).style.display = (k === t) ? 'block' : 'none';
  });
  const tm = document.getElementById('time_mode').value;
  document.getElementById('time_to_wrap').style.display = (tm === 'between') ? 'block' : 'none';
  document.getElementById('time_from_lbl').textContent = (tm === 'between') ? 'Von' : 'Uhrzeit';
  const dm = document.getElementById('date_mode').value;
  document.getElementById('date_to_wrap').style.display = (dm === 'between') ? 'block' : 'none';
  document.getElementById('date_from_lbl').textContent = (dm === 'between') ? 'Von' : 'Datum';
  const mop = document.getElementById('cond_m_op').value;
  document.getElementById('cond_m_value_wrap').style.display = (mop === 'true' || mop === 'false') ? 'none' : 'block';
}

function loadCondFields(n){
  document.getElementById('cond_type').value = n.cond_type || 'payload';
  document.getElementById('cond_op').value = n.operator || 'eq';
  document.getElementById('cond_value').value = n.compare_value || '';
  document.getElementById('time_mode').value = n.time_mode || 'between';
  document.getElementById('time_from').value = n.time_from || '08:00';
  document.getElementById('time_to').value = n.time_to || '18:00';
  document.getElementById('sun_state').value = n.sun_state || 'up';
  document.getElementById('sun_offset').value = n.sun_offset_min || 0;
  const days = (n.weekdays || [0,1,2,3,4]).map(Number);
  document.querySelectorAll('#cond_weekday input[type=checkbox]').forEach(cb => {
    cb.checked = days.includes(parseInt(cb.value));
  });
  document.getElementById('date_mode').value = n.date_mode || 'equal';
  document.getElementById('date_from').value = n.date_from || '';
  document.getElementById('date_to').value = n.date_to || '';
  document.getElementById('date_yearly').checked = !!n.date_yearly;
  fillMarkerSelect(document.getElementById('cond_marker_sel'), n.marker_id);
  document.getElementById('cond_m_op').value = n.marker_op || 'eq';
  document.getElementById('cond_m_value').value = n.marker_cmp || '';
  loadWeatherSpec('cw', n);
  onCondTypeChange();
}

function readCondFields(n, errEl){
  const t = document.getElementById('cond_type').value;
  const fail = (msg) => { errEl.textContent = msg; errEl.style.display = 'block'; return false; };
  if (t === 'time'){
    const mode = document.getElementById('time_mode').value;
    const from = document.getElementById('time_from').value;
    const to = document.getElementById('time_to').value;
    if (!from) return fail('Bitte eine Uhrzeit angeben.');
    if (mode === 'between' && !to) return fail('Bitte Start- und Endzeit angeben.');
    n.time_mode = mode; n.time_from = from; if (mode === 'between') n.time_to = to;
  } else if (t === 'sun'){
    n.sun_state = document.getElementById('sun_state').value;
    n.sun_offset_min = parseInt(document.getElementById('sun_offset').value) || 0;
  } else if (t === 'weekday'){
    const days = [];
    document.querySelectorAll('#cond_weekday input[type=checkbox]').forEach(cb => {
      if (cb.checked) days.push(parseInt(cb.value));
    });
    if (!days.length) return fail('Bitte mindestens einen Wochentag auswählen.');
    n.weekdays = days;
  } else if (t === 'date'){
    const mode = document.getElementById('date_mode').value;
    const from = document.getElementById('date_from').value;
    const to = document.getElementById('date_to').value;
    if (!from) return fail('Bitte ein Datum angeben.');
    if (mode === 'between' && !to) return fail('Bitte Start- und Enddatum angeben.');
    n.date_mode = mode; n.date_from = from; if (mode === 'between') n.date_to = to;
    n.date_yearly = document.getElementById('date_yearly').checked;
  } else if (t === 'weather'){
    if (!readWeatherSpec('cw', n, fail)) return false;
  } else if (t === 'marker'){
    const mid = document.getElementById('cond_marker_sel').value;
    if (!mid) return fail('Bitte einen Merker auswählen (zuerst einen Merker-Block platzieren).');
    const op = document.getElementById('cond_m_op').value;
    const val = document.getElementById('cond_m_value').value;
    if (['gt','gte','lt','lte'].includes(op) && isNaN(parseFloat(val.replace(',', '.'))))
      return fail('Für Größer-/Kleiner-Vergleiche bitte eine Zahl angeben.');
    n.marker_id = mid; n.marker_op = op; n.marker_cmp = val;
  } else {
    n.operator = document.getElementById('cond_op').value;
    n.compare_value = document.getElementById('cond_value').value;
  }
  n.cond_type = t;
  return true;
}

// ---------------- Standort-Modal ----------------
let locFound = [];
function openLocationModal(){
  document.getElementById('locError').style.display = 'none';
  document.getElementById('loc_lat').value = STATE.location.lat;
  document.getElementById('loc_lon').value = STATE.location.lon;
  document.getElementById('loc_city').value = STATE.location.name || '';
  document.getElementById('loc_results').style.display = 'none';
  locFound = [];
  document.getElementById('loc_interval').value = (STATE.weather || {}).interval_min || 10;
  document.getElementById('locationModal').classList.add('show');
}
function closeLocationModal(){ document.getElementById('locationModal').classList.remove('show'); }
function coordsEdited(){ document.getElementById('loc_city').value = ''; }   // Koordinaten von Hand: Ortsname entfaellt
function applyCity(r){
  document.getElementById('loc_lat').value = r.lat;
  document.getElementById('loc_lon').value = r.lon;
  document.getElementById('loc_city').value = r.name;
  document.getElementById('loc_hint').textContent = 'Gefunden: ' + r.label + ' (' + r.lat + ' / ' + r.lon + ')';
}
async function findCity(){
  const q = document.getElementById('loc_city').value.trim();
  const errEl = document.getElementById('locError');
  errEl.style.display = 'none';
  const res = await fetch('/api/geocode?q=' + encodeURIComponent(q));
  const data = await res.json();
  if (!data.ok){ errEl.textContent = data.error; errEl.style.display = 'block'; return []; }
  if (!data.results.length){ errEl.textContent = 'Kein Ort "' + q + '" gefunden.'; errEl.style.display = 'block'; return []; }
  return data.results;
}
async function searchCity(){
  const sel = document.getElementById('loc_results');
  locFound = await findCity();
  if (!locFound.length){ sel.style.display = 'none'; return; }
  sel.innerHTML = '';
  locFound.forEach((r, i) => {
    const o = document.createElement('option');
    o.value = i; o.textContent = r.label + '  (' + r.lat + ' / ' + r.lon + ')';
    sel.appendChild(o);
  });
  sel.style.display = locFound.length > 1 ? 'block' : 'none';
  sel.size = Math.min(locFound.length, 6);
  sel.selectedIndex = 0;
  applyCity(locFound[0]);
}
function pickCity(){
  const r = locFound[parseInt(document.getElementById('loc_results').value)];
  if (r) applyCity(r);
}
async function submitLocation(){
  const errEl = document.getElementById('locError');
  // Stadt eingetragen, aber nicht gesucht/geaendert: beim Speichern automatisch das erste Ergebnis nehmen
  const city = document.getElementById('loc_city').value.trim();
  if (city && city !== (STATE.location.name || '') && !locFound.some(r => r.name === city)){
    const found = await findCity();
    if (!found.length) return;
    applyCity(found[0]);
  }
  const lat = parseFloat(document.getElementById('loc_lat').value);
  const lon = parseFloat(document.getElementById('loc_lon').value);
  if (isNaN(lat) || isNaN(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180){
    errEl.textContent = 'Bitte gültige Koordinaten eingeben (Breite -90..90, Länge -180..180).';
    errEl.style.display = 'block'; return;
  }
  const iv = parseInt(document.getElementById('loc_interval').value);
  STATE.weather = { interval_min: Math.min(120, Math.max(5, isNaN(iv) ? 10 : iv)) };
  STATE.location = { lat, lon };
  const nm = document.getElementById('loc_city').value.trim();
  if (nm) STATE.location.name = nm;
  closeLocationModal(); scheduleSave(); toast('Standort gespeichert.');
}

// ---------------- Merker ----------------
function nextMarkerName(){
  let i = 1;
  while (STATE.nodes.some(n => n.type === 'marker' && (n.label || '').toLowerCase() === ('Merker ' + i).toLowerCase())) i++;
  return 'Merker ' + i;
}
function fillMarkerSelect(sel, selectedId){
  const ms = STATE.nodes.filter(n => n.type === 'marker');
  sel.innerHTML = ms.length
    ? ms.map(m => `<option value="${m.id}" ${m.id === selectedId ? 'selected' : ''}>${escapeHtml(markerLabel(m))}</option>`).join('')
    : `<option value="">-- zuerst einen Merker-Block platzieren --</option>`;
}
function markerActionText(n){
  const v = escapeHtml(n.marker_value ?? '');
  return ({ set: `setzen auf ${v}`, toggle: 'umschalten', inc: `+ ${v || 1}`, dec: `- ${v || 1}`,
            reset: 'auf Startwert',
            store: v ? `speichern (Feld ${v})` : 'Nachricht speichern' })[n.marker_action || 'set'] || '';
}
function markerSummary(n){
  const cur = MARKER_VALUES[n.id];
  return `Wert: <b class="marker-val" data-id="${n.id}">${escapeHtml(cur ?? n.marker_initial ?? '')}</b><br>Aktion: <b>${markerActionText(n)}</b>`;
}
function delaySummary(n){
  if (n.delay_source === 'marker'){
    const m = nodeById(n.delay_marker_id);
    const unit = ({ ms:'ms', s:'s', min:'min', h:'h' })[n.delay_marker_unit] || 's';
    return `Dauer: Merker <b>${escapeHtml(m ? markerLabelFor(m, n) : '(gelöscht)')}</b> [${unit}]`;
  }
  return `Dauer: <b>${escapeHtml(n.delay_sec)} s</b>`;
}
function updateMarkerValues(){
  document.querySelectorAll('.marker-val').forEach(el => {
    const v = MARKER_VALUES[el.dataset.id];
    if (v !== undefined) el.textContent = v;
  });
  // Ist der Merker-Dialog offen, den Live-Wert nachfuehren (solange nicht von Hand geaendert)
  const mc = document.getElementById('m_current');
  if (mc && editingNodeId && !mCurrentDirty && document.activeElement !== mc
      && document.getElementById('nodeModal').classList.contains('show')){
    const n = nodeById(editingNodeId);
    if (n && n.type === 'marker' && MARKER_VALUES[n.id] !== undefined) mc.value = MARKER_VALUES[n.id];
  }
}
function warnMarkerUsed(ids){
  const used = ids.filter(id => {
    const n = nodeById(id);
    return n && n.type === 'marker' && STATE.nodes.some(o =>
      (o.type === 'condition' && o.cond_type === 'marker' && o.marker_id === id) ||
      (o.type === 'delay' && o.delay_source === 'marker' && o.delay_marker_id === id));
  });
  if (used.length) toast('Hinweis: Ein gelöschter Merker wurde noch von einer Bedingung oder Wartezeit verwendet.', false);
}
async function editMarkerValue(id, e){
  e.stopPropagation();
  const n = nodeById(id);
  const v = prompt(`Neuer Wert für Merker "${n.label || 'Merker'}":`, MARKER_VALUES[id] ?? '');
  if (v === null) return;
  const res = await fetch(`/api/markers/${id}`, { method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ value: v }) });
  if (res.ok){ MARKER_VALUES[id] = v; updateMarkerValues(); toast('Merker gesetzt.'); }
  else toast('Merker noch nicht gespeichert - bitte kurz warten und erneut versuchen.', false);
}
let mCurrentDirty = false;
function onMarkerActionChange(userChanged){
  const a = document.getElementById('m_action').value;
  const valueEl = document.getElementById('m_value');
  document.getElementById('m_value_wrap').style.display = (a === 'toggle' || a === 'reset') ? 'none' : 'block';
  document.getElementById('m_value_lbl').textContent = ({
    set: 'Wert ({payload} = empfangene Nachricht, {payload.feld} = JSON-Feld)',
    store: 'JSON-Feld (leer = ganze Nachricht, z. B. temperature oder sensor.temp)'
  })[a] || 'Schrittweite';
  valueEl.placeholder = a === 'store' ? 'z. B. temperature' : 'z. B. 1';
  if (userChanged === true){
    if (a === 'store' && valueEl.value === '1') valueEl.value = '';
    else if (a !== 'store' && valueEl.value === '') valueEl.value = '1';
  }
}
function loadMarkerFields(n){
  document.getElementById('m_initial').value = n.marker_initial ?? '0';
  document.getElementById('m_action').value = n.marker_action || 'set';
  document.getElementById('m_value').value = n.marker_value ?? '1';
  document.getElementById('m_retain').checked = !!n.marker_retain;
  mCurrentDirty = false;
  document.getElementById('m_current').value = MARKER_VALUES[n.id] ?? '';
  onMarkerActionChange();
  fetch('/api/markers').then(r => r.json()).then(d => {      // aktuellen Wert frisch vom Server holen
    MARKER_VALUES = d.markers || MARKER_VALUES;
    if (!mCurrentDirty && editingNodeId === n.id && MARKER_VALUES[n.id] !== undefined)
      document.getElementById('m_current').value = MARKER_VALUES[n.id];
    updateMarkerValues();
  }).catch(() => {});
}
async function setMarkerNow(){
  const n = nodeById(editingNodeId);
  if (!n) return;
  const v = document.getElementById('m_current').value;
  await saveConfig(false);   // der Merker muss dem Server bekannt sein
  const res = await fetch(`/api/markers/${n.id}`, { method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ value: v }) });
  if (res.ok){ MARKER_VALUES[n.id] = v; mCurrentDirty = false; updateMarkerValues(); toast('Merker gesetzt.'); }
  else toast('Merker konnte nicht gesetzt werden.', false);
}
function readMarkerFields(n, errEl, prevLabel){
  const fail = (msg) => { n.label = prevLabel; errEl.textContent = msg; errEl.style.display = 'block'; return false; };
  if (!n.label) return fail('Bitte einen Namen für den Merker angeben.');
  if (STATE.nodes.some(o => o.type === 'marker' && o.id !== n.id && (o.label || '').toLowerCase() === n.label.toLowerCase()))
    return fail('Ein Merker mit diesem Namen existiert bereits.');
  const action = document.getElementById('m_action').value;
  const value = document.getElementById('m_value').value;
  if ((action === 'inc' || action === 'dec') && value.trim() !== '' && isNaN(parseFloat(value.replace(',', '.'))))
    return fail('Die Schrittweite muss eine Zahl sein.');
  n.marker_initial = document.getElementById('m_initial').value;
  n.marker_action = action;
  n.marker_value = value;
  n.marker_retain = document.getElementById('m_retain').checked;
  return true;
}

// ---------------- Wartezeit ----------------
function onDelaySourceChange(){
  const src = document.getElementById('delay_source').value;
  document.getElementById('delay_marker_wrap').style.display = (src === 'marker') ? 'block' : 'none';
  document.getElementById('delay_sec_lbl').textContent =
    (src === 'marker') ? 'Ersatzwert in Sekunden (falls der Merker-Wert ungültig ist)' : 'Wartezeit (Sekunden)';
}
function loadDelayFields(n){
  document.getElementById('delay_source').value = n.delay_source || 'fixed';
  document.getElementById('delay_sec').value = n.delay_sec ?? 1.0;
  fillMarkerSelect(document.getElementById('delay_marker'), n.delay_marker_id);
  document.getElementById('delay_unit').value = n.delay_marker_unit || 's';
  onDelaySourceChange();
}
function readDelayFields(n, errEl){
  const src = document.getElementById('delay_source').value;
  const fixed = parseFloat(document.getElementById('delay_sec').value) || 0;
  if (src === 'marker'){
    const mid = document.getElementById('delay_marker').value;
    if (!mid){
      errEl.textContent = 'Bitte einen Merker auswählen (zuerst einen Merker-Block platzieren).';
      errEl.style.display = 'block'; return false;
    }
    n.delay_marker_id = mid; n.delay_marker_unit = document.getElementById('delay_unit').value;
  }
  n.delay_sec = fixed; n.delay_source = src;
  return true;
}

// ---------------- Seiten ----------------
function pageName(id){ const p = STATE.pages.find(x => x.id === id); return p ? p.name : '?'; }
function markerLabel(m){ return (m.label || 'Merker') + (STATE.pages.length > 1 ? ` [${pageName(m.page)}]` : ''); }
function markerLabelFor(m, ref){
  return (m.label || 'Merker') + ((STATE.pages.length > 1 && m.page !== ref.page) ? ` [${pageName(m.page)}]` : '');
}
function renderPageTabs(){
  const el = document.getElementById('pageTabs');
  el.innerHTML = STATE.pages.map(p =>
      `<div class="tab ${p.id === currentPage ? 'active' : ''}" data-page="${escapeHtml(p.id)}" title="Doppelklick: umbenennen">`
      + `<span>${escapeHtml(p.name)}</span>`
      + (STATE.pages.length > 1 ? `<span class="tab-x" title="Seite löschen">&times;</span>` : '') + `</div>`).join('')
    + `<div class="tab tab-add" title="Neue Seite">+</div><div class="tab-spacer"></div>`
    + `<div class="tab tab-move" title="Markierte Elemente auf eine andere Seite verschieben">Auswahl verschieben &rarr;</div>`;
}
document.getElementById('pageTabs').addEventListener('click', (e) => {
  const tab = e.target.closest('.tab');
  if (!tab) return;
  if (e.target.closest('.tab-x')){ deletePage(tab.dataset.page); return; }
  if (tab.classList.contains('tab-add')) addPage();
  else if (tab.classList.contains('tab-move')) openMoveModal();
  else switchPage(tab.dataset.page);
});
document.getElementById('pageTabs').addEventListener('dblclick', (e) => {
  const tab = e.target.closest('.tab');
  if (tab && tab.dataset.page) renamePage(tab.dataset.page);
});
function switchPage(id){
  if (id === currentPage || !STATE.pages.some(p => p.id === id)) return;
  const wrap = document.getElementById('canvasWrap');
  pageScroll[currentPage] = { x: wrap.scrollLeft, y: wrap.scrollTop };
  currentPage = id;
  try { localStorage.setItem('ablaufPage', id); } catch(e) {}
  selectedIds.clear(); selectedConnId = null;
  renderAll(); renderPageTabs();
  const sc = pageScroll[id] || { x: 0, y: 0 };
  wrap.scrollLeft = sc.x; wrap.scrollTop = sc.y;
}
function uniquePageName(base){
  let name = base, i = 2;
  while (STATE.pages.some(p => p.name.toLowerCase() === name.toLowerCase())) name = base + ' ' + (i++);
  return name;
}
function addPage(){
  const page = { id: 'page_' + Math.random().toString(16).slice(2,10), name: uniquePageName('Seite ' + (STATE.pages.length + 1)) };
  STATE.pages.push(page);
  scheduleSave();
  switchPage(page.id);
  renderPageTabs();
  return page;
}
function renamePage(id){
  const p = STATE.pages.find(x => x.id === id);
  if (!p) return;
  const name = prompt('Name der Seite:', p.name);
  if (name === null) return;
  const clean = name.trim();
  if (!clean) { toast('Der Name darf nicht leer sein.', false); return; }
  if (STATE.pages.some(x => x.id !== id && x.name.toLowerCase() === clean.toLowerCase())){
    toast('Eine Seite mit diesem Namen gibt es schon.', false); return;
  }
  p.name = clean; renderPageTabs(); renderAll(); scheduleSave();
}
function deletePage(id){
  if (STATE.pages.length < 2){ toast('Die letzte Seite kann nicht gelöscht werden.', false); return; }
  const p = STATE.pages.find(x => x.id === id);
  if (!p) return;
  const ids = STATE.nodes.filter(n => n.page === id).map(n => n.id);
  if (!confirm(`Seite "${p.name}" mit ${ids.length} Element(en) wirklich löschen?`)) return;
  warnMarkerUsed(ids);
  const gone = new Set(ids);
  STATE.nodes = STATE.nodes.filter(n => !gone.has(n.id));
  STATE.connections = STATE.connections.filter(c => !gone.has(c.from) && !gone.has(c.to));
  STATE.pages = STATE.pages.filter(x => x.id !== id);
  if (currentPage === id){
    currentPage = STATE.pages[0].id;
    try { localStorage.setItem('ablaufPage', currentPage); } catch(e) {}
  }
  selectedIds.clear(); selectedConnId = null;
  renderAll(); renderPageTabs(); scheduleSave();
}
function openMoveModal(){
  if (!selectedIds.size){ toast('Zuerst Elemente markieren (Rahmen aufziehen).', false); return; }
  document.getElementById('moveList').innerHTML =
    STATE.pages.filter(p => p.id !== currentPage)
      .map(p => `<div class="pal-item" data-page="${escapeHtml(p.id)}">${escapeHtml(p.name)}</div>`).join('')
    + `<div class="pal-item" data-page="__new__">+ Neue Seite</div>`;
  document.getElementById('moveModal').classList.add('show');
}
function closeMoveModal(){ document.getElementById('moveModal').classList.remove('show'); }
document.getElementById('moveList').addEventListener('click', (e) => {
  const item = e.target.closest('.pal-item');
  if (item) moveSelection(item.dataset.page);
});
function moveSelection(pageId){
  const ids = new Set(selectedIds);
  if (!ids.size) return;
  if (pageId === '__new__'){
    const page = { id: 'page_' + Math.random().toString(16).slice(2,10), name: uniquePageName('Seite ' + (STATE.pages.length + 1)) };
    STATE.pages.push(page); pageId = page.id;
  }
  STATE.nodes.forEach(n => { if (ids.has(n.id)) n.page = pageId; });
  const before = STATE.connections.length;
  STATE.connections = STATE.connections.filter(c => {
    const a = nodeById(c.from), b = nodeById(c.to);
    return a && b && a.page === b.page;
  });
  const removed = before - STATE.connections.length;
  selectedIds.clear(); closeMoveModal();
  renderAll(); renderPageTabs(); scheduleSave();
  toast(`${ids.size} Element(e) nach "${pageName(pageId)}" verschoben` + (removed ? `, ${removed} Linie(n) getrennt.` : '.'));
}

// ---------------- Arbeitsfläche ----------------
function updateCanvasSize(){
  const canvas = document.getElementById('canvas');
  let maxX = 0, maxY = 0;
  STATE.nodes.filter(n => n.page === currentPage).forEach(n => {
    maxX = Math.max(maxX, n.x + 260); maxY = Math.max(maxY, n.y + 200);
  });
  canvas.style.width = Math.max(6000, maxX + 1200) + 'px';     // wächst mit den Elementen
  canvas.style.height = Math.max(4000, maxY + 900) + 'px';
}
function autoScroll(e, wrap){
  const r = wrap.getBoundingClientRect(), edge = 40, step = 24;
  if (e.clientX > r.right - edge) wrap.scrollLeft += step; else if (e.clientX < r.left + edge) wrap.scrollLeft -= step;
  if (e.clientY > r.bottom - edge) wrap.scrollTop += step; else if (e.clientY < r.top + edge) wrap.scrollTop -= step;
}

// ---------------- Ablauf-Anzeige (wandernder Punkt) ----------------
let traceSince = -1;
const traceRuns = new Map();
const sleepMs = (ms) => new Promise(r => setTimeout(r, ms));
function nodeEl(id){ return document.querySelector(`.flow-node[data-id="${id}"]`); }
function nodeTitle(id){ const n = nodeById(id); return n ? (n.label || NODE_META[n.type].title) : ''; }
function touchTab(nodeId){
  const n = nodeById(nodeId);
  if (!n || n.page === currentPage) return;
  const t = document.querySelector(`.tab[data-page="${n.page}"]`);
  if (t){ t.classList.add('run'); setTimeout(() => t.classList.remove('run'), 1000); }
}
function travel(key, color, label, ms){
  return new Promise(resolve => {
    const svg = document.getElementById('connLayer');
    const path = svg.querySelector(`path[data-key="${key}"]`);
    if (!path || typeof path.getTotalLength !== 'function'){ resolve(); return; }
    const len = path.getTotalLength();
    const dot = svgEl('circle', { r:'6', fill: color, stroke:'#0a0c0f', 'stroke-width':'1.5', class:'trace-dot' });
    svg.appendChild(dot);
    let text = null;
    if (label){
      text = svgEl('text', { fill:'#e5e8ec', 'font-size':'10', 'font-family':'monospace', class:'trace-label' });
      text.textContent = label; svg.appendChild(text);
    }
    const t0 = performance.now();
    function step(now){
      const k = Math.min(1, (now - t0) / ms);
      const pt = path.getPointAtLength(len * k);
      dot.setAttribute('cx', pt.x); dot.setAttribute('cy', pt.y);
      if (text){ text.setAttribute('x', pt.x + 9); text.setAttribute('y', pt.y - 8); }
      if (k < 1) requestAnimationFrame(step);
      else { dot.remove(); if (text) text.remove(); resolve(); }
    }
    requestAnimationFrame(step);
  });
}
async function playTrace(ev){
  if (ev.kind === 'start'){
    touchTab(ev.node);
    const el = nodeEl(ev.node);
    if (el){ el.classList.add('pulse'); await sleepMs(450); el.classList.remove('pulse'); }
  } else if (ev.kind === 'edge'){
    touchTab(ev.to);
    await travel('c:' + ev.conn, '#ff9142', nodeTitle(ev.to), 650);
  } else if (ev.kind === 'data'){
    await travel('c:' + ev.conn, ev.result ? '#3ddc97' : '#ff5d5d', ev.result ? 'Ja' : 'Nein', 450);
  } else if (ev.kind === 'ref'){
    await travel('r:' + ev.marker + '|' + ev.target, '#4cc9f0', 'Wert', 450);
  } else if (ev.kind === 'enter'){
    const el = nodeEl(ev.node);
    if (el) el.classList.add('running');
    await sleepMs(250);
  } else if (ev.kind === 'leave'){
    const el = nodeEl(ev.node);
    if (el){
      el.classList.remove('running');
      const cls = 'res-' + (ev.result || 'out');
      el.classList.add(cls); setTimeout(() => el.classList.remove(cls), 900);
    }
    await sleepMs(250);
  }
}
function enqueueTrace(ev){
  const key = ev.run || 'x';
  traceRuns.set(key, (traceRuns.get(key) || Promise.resolve()).then(() => playTrace(ev)).catch(() => {}));
  if (traceRuns.size > 100) traceRuns.delete(traceRuns.keys().next().value);
}
async function pollTrace(){
  try{
    if (document.hidden){ traceSince = -1; return; }   // im Hintergrund nichts anstauen
    const res = await fetch('/api/trace?since=' + traceSince);
    const data = await res.json();
    if (traceSince < 0){ traceSince = data.last; return; }
    (data.events || []).forEach(ev => { traceSince = Math.max(traceSince, ev.seq); enqueueTrace(ev); });
    if (data.last < traceSince) traceSince = data.last;   // Server wurde neu gestartet
  } catch(e){}
}

// ---------------- Wetter (Internet) ----------------
let WEATHER = { ok: false, error: '', data: null, age: null };
const WEATHER_LABELS = {
  temperature: ['Temperatur', '°C'], apparent_temperature: ['Gefühlte Temperatur', '°C'],
  humidity: ['Luftfeuchtigkeit', '%'], wind: ['Wind', 'km/h'], gusts: ['Böen', 'km/h'],
  precipitation: ['Niederschlag', 'mm'], cloud: ['Bewölkung', '%']
};
const WEATHER_KEYS = { temperature:'temperature', apparent_temperature:'apparent', humidity:'humidity',
                       wind:'wind', gusts:'gusts', precipitation:'precipitation', cloud:'cloud' };
const WEATHER_STATES = { rain:'es regnet', snow:'es schneit', thunder:'Gewitter', fog:'Nebel',
                         clear:'klar / heiter', cloudy:'bewölkt' };

function weatherSpecText(n){
  const m = n.weather_metric || 'temperature';
  if (m === 'state') return (n.weather_op === 'not' ? 'nicht: ' : '') + (WEATHER_STATES[n.weather_state] || n.weather_state);
  const lbl = WEATHER_LABELS[m] || [m, ''];
  const ops = { gt:'>', gte:'≥', lt:'<', lte:'≤', eq:'=' };
  return `${lbl[0]} ${ops[n.weather_op] || '>'} ${n.weather_value} ${lbl[1]}`.trim();
}
function weatherNowText(metric){
  const d = WEATHER.data;
  if (!d) return '-';
  if (metric === 'state') return d.text || '-';
  const v = d[WEATHER_KEYS[metric]];
  return (v === null || v === undefined) ? '-' : `${v} ${(WEATHER_LABELS[metric] || ['', ''])[1]}`.trim();
}
function weatherTriggerSummary(n){
  const modes = { rise:'einmal beim Eintreten', any:'bei jedem Wechsel', while:'bei jedem Abruf, solange zutreffend' };
  const metric = n.weather_metric || 'temperature';
  return `Wenn <b>${escapeHtml(weatherSpecText(n))}</b><br>${modes[n.weather_mode || 'rise']}<br>`
       + `Jetzt: <b class="weather-now" data-metric="${metric}">${escapeHtml(weatherNowText(metric))}</b>`;
}
function updateWeatherLive(){
  document.querySelectorAll('.weather-now').forEach(el => { el.textContent = weatherNowText(el.dataset.metric); });
  if (document.getElementById('weatherModal').classList.contains('show')) renderWeatherModal();
}

function weatherSpecHtml(p){
  return `
    <label>Wetterwert</label>
    <select id="${p}_metric" onchange="onWeatherSpecChange('${p}')">
      <option value="temperature">Temperatur (°C)</option>
      <option value="apparent_temperature">Gefühlte Temperatur (°C)</option>
      <option value="humidity">Luftfeuchtigkeit (%)</option>
      <option value="wind">Windgeschwindigkeit (km/h)</option>
      <option value="gusts">Windböen (km/h)</option>
      <option value="precipitation">Niederschlag aktuell (mm)</option>
      <option value="cloud">Bewölkung (%)</option>
      <option value="state">Wetterlage (Regen, Schnee ...)</option>
    </select>
    <div id="${p}_num">
      <label>Bedingung</label>
      <select id="${p}_op">
        <option value="gt">größer als</option><option value="gte">größer oder gleich</option>
        <option value="lt">kleiner als</option><option value="lte">kleiner oder gleich</option>
        <option value="eq">gleich</option>
      </select>
      <label>Wert</label>
      <input id="${p}_value" placeholder="z. B. 25">
    </div>
    <div id="${p}_state">
      <label>Wetterlage</label>
      <select id="${p}_st">
        <option value="rain">es regnet (Regen, Nieselregen, Schauer, Gewitter)</option>
        <option value="snow">es schneit</option>
        <option value="thunder">Gewitter</option>
        <option value="fog">Nebel</option>
        <option value="clear">klar / überwiegend klar</option>
        <option value="cloudy">bewölkt / bedeckt</option>
      </select>
      <select id="${p}_is" style="margin-top:8px;">
        <option value="is">trifft zu</option><option value="not">trifft nicht zu</option>
      </select>
    </div>
    <div class="hint">Daten: Open-Meteo (Internet) für den Standort oben rechts („Standort“). Bei Ausfall der Verbindung gilt „Nein“.</div>`;
}
document.getElementById('cond_weather').innerHTML = weatherSpecHtml('cw');
document.getElementById('wt_spec').innerHTML = weatherSpecHtml('wt');

function onWeatherSpecChange(p){
  const state = document.getElementById(p + '_metric').value === 'state';
  document.getElementById(p + '_num').style.display = state ? 'none' : 'block';
  document.getElementById(p + '_state').style.display = state ? 'block' : 'none';
}
function loadWeatherSpec(p, n){
  const metric = n.weather_metric || 'temperature';
  document.getElementById(p + '_metric').value = metric;
  const opSel = document.getElementById(p + '_op');
  opSel.value = ['gt','gte','lt','lte','eq'].includes(n.weather_op) ? n.weather_op : 'gt';
  document.getElementById(p + '_value').value = n.weather_value ?? '25';
  document.getElementById(p + '_st').value = n.weather_state || 'rain';
  document.getElementById(p + '_is').value = n.weather_op === 'not' ? 'not' : 'is';
  onWeatherSpecChange(p);
}
function readWeatherSpec(p, n, fail){
  const metric = document.getElementById(p + '_metric').value;
  if (metric === 'state'){
    n.weather_state = document.getElementById(p + '_st').value;
    n.weather_op = document.getElementById(p + '_is').value;
  } else {
    const val = document.getElementById(p + '_value').value;
    if (isNaN(parseFloat(val.replace(',', '.')))) return fail('Bitte eine Zahl als Wert angeben.');
    n.weather_op = document.getElementById(p + '_op').value;
    n.weather_value = val;
  }
  n.weather_metric = metric;
  return true;
}
function loadWeatherTrigger(n){
  loadWeatherSpec('wt', n);
  document.getElementById('wt_mode').value = n.weather_mode || 'rise';
  document.getElementById('wt_start').checked = !!n.weather_fire_start;
}
function readWeatherTrigger(n, errEl){
  const fail = (msg) => { errEl.textContent = msg; errEl.style.display = 'block'; return false; };
  if (!readWeatherSpec('wt', n, fail)) return false;
  n.weather_mode = document.getElementById('wt_mode').value;
  n.weather_fire_start = document.getElementById('wt_start').checked;
  return true;
}

async function refreshWeather(force){
  try{
    const res = await fetch('/api/weather' + (force ? '?refresh=1' : ''));
    WEATHER = await res.json();
  } catch(e){ WEATHER = { ok:false, error:'Server nicht erreichbar', data:null, age:null }; }
  updateWeatherLive(); renderWeatherModal();
}
function openWeatherModal(){
  document.getElementById('weatherModal').classList.add('show');
  renderWeatherModal();
  refreshWeather(false);
}
function closeWeatherModal(){ document.getElementById('weatherModal').classList.remove('show'); }
function renderWeatherModal(){
  const body = document.getElementById('weatherBody'), err = document.getElementById('weatherError');
  err.style.display = WEATHER.error ? 'block' : 'none';
  err.textContent = WEATHER.error ? 'Abruf fehlgeschlagen: ' + WEATHER.error : '';
  const d = WEATHER.data;
  if (!d){ body.innerHTML = '<div class="hint">Noch keine Wetterdaten abgerufen.</div>'; return; }
  const f = (v, unit) => (v === null || v === undefined) ? '-' : `${v} ${unit}`;
  const rows = [['Wetterlage', d.text], ['Temperatur', f(d.temperature, '°C')], ['Gefühlt', f(d.apparent, '°C')],
    ['Luftfeuchtigkeit', f(d.humidity, '%')], ['Wind', f(d.wind, 'km/h')], ['Böen', f(d.gusts, 'km/h')],
    ['Niederschlag', f(d.precipitation, 'mm')], ['Bewölkung', f(d.cloud, '%')], ['Tag/Nacht', d.is_day ? 'Tag' : 'Nacht']];
  body.innerHTML = `<table class="broker-table">` + rows.map(r =>
      `<tr><td>${escapeHtml(r[0])}</td><td><b>${escapeHtml(String(r[1]))}</b></td></tr>`).join('') + `</table>`
    + `<div class="hint">Stand: vor ${WEATHER.age} s (Meldezeit ${escapeHtml(d.time || '-')}), Ort ${d.lat} / ${d.lon}</div>`;
}

// ---------------- Broker-Modal ----------------
function openBrokerModal(){
  document.getElementById('brokerError').style.display='none';
  renderBrokerTable();
  document.getElementById('brokerModal').classList.add('show');
}
function closeBrokerModal(){ document.getElementById('brokerModal').classList.remove('show'); }
function renderBrokerTable(){
  const tbody = document.querySelector('#brokerTable tbody');
  tbody.innerHTML = STATE.brokers.map(b => `
    <tr><td>${escapeHtml(b.name)}</td><td>${escapeHtml(b.host)}</td><td>${escapeHtml(b.port)}</td>
    <td><span class="btn-mini" onclick="deleteBroker('${b.id}')" style="color:var(--danger);">Loeschen</span></td></tr>
  `).join('') || `<tr><td colspan="4" style="color:var(--text-dim);">Noch keine Broker angelegt.</td></tr>`;
}
function submitBroker(){
  const errEl = document.getElementById('brokerError');
  const name = document.getElementById('b_name').value.trim();
  const host = document.getElementById('b_host').value.trim();
  if (!name || !host){ errEl.textContent='Name und Host/IP sind erforderlich.'; errEl.style.display='block'; return; }
  STATE.brokers.push({
    id: 'broker_' + Math.random().toString(16).slice(2,10), name, host,
    port: parseInt(document.getElementById('b_port').value)||1883,
    username: document.getElementById('b_user').value.trim(),
    password: document.getElementById('b_pass').value,
    tls: document.getElementById('b_tls').checked
  });
  document.getElementById('b_name').value=''; document.getElementById('b_host').value='';
  document.getElementById('b_port').value=1883; document.getElementById('b_user').value='';
  document.getElementById('b_pass').value=''; document.getElementById('b_tls').checked=false;
  renderBrokerTable();
  scheduleSave();
  toast('Broker hinzugefuegt.');
}
function deleteBroker(id){
  const inUse = STATE.nodes.some(n => n.broker_id === id);
  if (inUse){ toast('Dieser Broker wird noch von mindestens einem Block verwendet.', false); return; }
  if (!confirm('Diesen Broker wirklich loeschen?')) return;
  STATE.brokers = STATE.brokers.filter(b => b.id !== id);
  renderBrokerTable();
  scheduleSave();
  toast('Broker geloescht.');
}

loadVersion();
loadConfig();
setInterval(pollLog, 2000);
pollTrace();
setInterval(pollTrace, 350);
</script>
</body>
</html>
"""


# --------------------------------------------------------------------------
# Start
# --------------------------------------------------------------------------
def main():
    cfg = load_config()
    try:
        ENGINE.rebuild(cfg)
    except Exception as exc:  # noqa: BLE001
        print(f" [WARNUNG] MQTT-Engine konnte nicht gestartet werden: {exc}")
    ENGINE.start_weather_poller()
    host = cfg["server"].get("host", "0.0.0.0")
    port = int(cfg["server"].get("port", 8020))
    print("=" * 64)
    print(f" MQTT-ABLAUFPLANER  v{APP_VERSION}")
    print("=" * 64)
    print(f" Lokal:          http://127.0.0.1:{port}")
    print(f" Konfiguration:  {CONFIG_PATH}")
    if not MQTT_AVAILABLE:
        print(" [WARNUNG] paho-mqtt ist nicht installiert - MQTT-Funktionen sind deaktiviert.")
    print(" Zum Beenden dieses Fenster schliessen oder STRG+C druecken.")
    print("=" * 64)
    app.run(host=host, port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
