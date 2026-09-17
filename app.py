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
  - Bedingung: vergleicht die zuletzt empfangene Nachricht mit einem
    Wert und verzweigt den Ablauf nach "Ja" oder "Nein".

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
APP_VERSION = "1.0.0"

import os
import sys
import json
import ssl
import time
import copy
import uuid
import threading
from datetime import datetime

from flask import Flask, request, jsonify, Response

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
LOCK = threading.RLock()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


DEFAULT_CONFIG = {
    "_kommentar": "Diese Datei wird vom MQTT-Ablaufplaner gelesen und "
                  "geschrieben. Manuelle Aenderungen sind moeglich, aber "
                  "bitte gueltiges JSON beibehalten.",
    "server": {"host": "0.0.0.0", "port": 8020},
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

    return cfg


def save_config(cfg: dict) -> None:
    with LOCK:
        tmp_path = CONFIG_PATH + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, CONFIG_PATH)


# --------------------------------------------------------------------------
# MQTT: einmaliges Veroeffentlichen (kurzlebige Verbindung pro Aufruf, damit
# gleichzeitig laufende Ablaeufe sich nicht gegenseitig blockieren koennen)
# --------------------------------------------------------------------------
def mqtt_publish_once(broker: dict, topic: str, payload: str, qos: int = 0,
                       retain: bool = False, timeout: float = 5.0) -> None:
    if not MQTT_AVAILABLE:
        raise RuntimeError(
            "Das Python-Paket 'paho-mqtt' ist nicht installiert. "
            "Bitte 'pip install paho-mqtt' ausfuehren."
        )

    done = threading.Event()
    result = {"error": None}
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"flow-{uuid.uuid4().hex[:8]}", protocol=mqtt.MQTTv311
    )

    if broker.get("username"):
        client.username_pw_set(broker.get("username"), broker.get("password") or None)
    if broker.get("tls"):
        ctx = ssl._create_unverified_context()
        client.tls_set_context(ctx)
        client.tls_insecure_set(True)

    def on_connect(c, userdata, flags, reason_code, properties):
        if reason_code == 0:
            c.publish(topic, payload, qos=qos, retain=retain)
        else:
            result["error"] = f"Verbindung zum Broker fehlgeschlagen ({reason_code})"
            done.set()

    def on_publish(c, userdata, mid, reason_code, properties):
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
                    self._start_client(bid, broker)
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
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"engine-{bid}-{uuid.uuid4().hex[:6]}", protocol=mqtt.MQTTv311
        )
        if broker.get("username"):
            client.username_pw_set(broker.get("username"), broker.get("password") or None)
        if broker.get("tls"):
            ctx = ssl._create_unverified_context()
            client.tls_set_context(ctx)
            client.tls_insecure_set(True)

        def on_connect(c, userdata, flags, reason_code, properties):
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

    # ---- Ablauf-Ausfuehrung -------------------------------------------
    def run_flow_from(self, start_node_id: str, payload_ctx: str, start_port: str) -> None:
        cfg = load_config()
        nodes_by_id = {n["id"]: n for n in cfg["nodes"]}
        brokers_by_id = {b["id"]: b for b in cfg["brokers"]}
        visited = set()
        self._walk(start_node_id, start_port, payload_ctx, nodes_by_id, brokers_by_id,
                   cfg["connections"], visited)

    def _walk(self, node_id, from_port, payload_ctx, nodes_by_id, brokers_by_id, connections, visited):
        edges = [c for c in connections if c["from"] == node_id and c.get("from_port", "out") == from_port]
        for edge in edges:
            target = nodes_by_id.get(edge["to"])
            if not target:
                continue
            guard = (target["id"], payload_ctx)
            if guard in visited:
                continue  # einfache Zyklen-Bremse
            visited.add(guard)
            next_port = self._execute(target, payload_ctx, brokers_by_id)
            payload_ctx = self._last_payload if hasattr(self, "_last_payload") else payload_ctx
            if next_port:
                self._walk(target["id"], next_port, payload_ctx, nodes_by_id, brokers_by_id, connections, visited)

    def _execute(self, node: dict, payload_ctx: str, brokers_by_id: dict) -> str:
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
                text = (node.get("payload") or "").replace("{payload}", payload_ctx)
                mqtt_publish_once(broker, node.get("topic", ""), text,
                                   qos=int(node.get("qos", 0) or 0), retain=bool(node.get("retain")))
                self.log_event(f"Gesendet: {node.get('topic')} = {text!r}")
                return "out"

            if ntype == "delay":
                time.sleep(max(0.0, float(node.get("delay_sec", 1.0) or 0)))
                return "out"

            if ntype == "condition":
                op = node.get("operator", "eq")
                cmp_val = node.get("compare_value", "")
                ok = False
                try:
                    if op == "eq":
                        ok = payload_ctx == cmp_val
                    elif op == "neq":
                        ok = payload_ctx != cmp_val
                    elif op == "contains":
                        ok = cmp_val in payload_ctx
                    elif op == "gt":
                        ok = float(payload_ctx) > float(cmp_val)
                    elif op == "lt":
                        ok = float(payload_ctx) < float(cmp_val)
                except Exception:
                    ok = False
                self.log_event(f"Bedingung '{node.get('label') or node['id']}': {'Ja' if ok else 'Nein'}")
                return "true" if ok else "false"

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
    ENGINE.log_event(f"Simuliert: '{node.get('label') or node_id}'")
    threading.Thread(target=ENGINE.run_flow_from, args=(node_id, payload_ctx, "out"), daemon=True).start()
    return jsonify({"status": "ok"})


@app.route("/api/events")
def api_events():
    return jsonify({"events": ENGINE.get_events()})


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

  .canvas-wrap{ flex:1; position:relative; overflow:auto; background:
    radial-gradient(circle, #1c212a 1px, transparent 1px) 0 0/22px 22px, var(--bg); }
  .canvas{ position:relative; width:2400px; height:1500px; }
  svg.conn-layer{ position:absolute; inset:0; width:100%; height:100%; pointer-events:none; }
  svg.conn-layer path{ pointer-events:stroke; cursor:pointer; }

  .flow-node{ position:absolute; width:190px; background:var(--panel); border:1.5px solid var(--border);
              border-radius:9px; box-shadow:0 4px 14px rgba(0,0,0,.35); user-select:none; }
  .flow-node.selected{ border-color:var(--accent); box-shadow:0 0 0 2px #ff914255; }
  .flow-node.type-mqtt_in{ border-left:4px solid var(--accent-2); }
  .flow-node.type-mqtt_out{ border-left:4px solid var(--accent); }
  .flow-node.type-delay{ border-left:4px solid var(--text-dim); }
  .flow-node.type-condition{ border-left:4px solid var(--accent-3); }
  .node-head{ display:flex; align-items:center; justify-content:space-between; padding:8px 10px;
              background:var(--panel-2); border-bottom:1px solid var(--border); border-radius:8px 8px 0 0;
              cursor:grab; font-size:12px; font-weight:600; }
  .node-head:active{ cursor:grabbing; }
  .node-del{ color:var(--text-dim); cursor:pointer; font-size:14px; line-height:1; }
  .node-del:hover{ color:var(--danger); }
  .node-body{ padding:8px 10px 10px; font-family:var(--mono); font-size:10.5px; color:var(--text-dim);
              line-height:1.5; cursor:pointer; }
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
    <h3>Hinweise</h3>
    <div style="font-size:11.5px; color:var(--text-dim); line-height:1.6;">
      Block ziehen: am Kopf anfassen.<br>
      Verbinden: am rechten Punkt ziehen, auf linken Punkt eines anderen Blocks loslassen.<br>
      Bearbeiten: auf den Blockinhalt klicken.<br>
      Loeschen: Block/Linie anklicken, dann &times; bzw. Entf.
    </div>
  </div>

  <div class="canvas-wrap" id="canvasWrap">
    <div class="canvas" id="canvas">
      <svg class="conn-layer" id="connLayer"></svg>
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
      <label>Wartezeit (Sekunden)</label>
      <input id="delay_sec" type="number" min="0" step="0.5">
    </div>
    <div id="fields_condition">
      <label>Vergleichsoperator</label>
      <select id="cond_op">
        <option value="eq">ist gleich</option>
        <option value="neq">ist ungleich</option>
        <option value="contains">enthaelt</option>
        <option value="gt">ist groesser als (Zahl)</option>
        <option value="lt">ist kleiner als (Zahl)</option>
      </select>
      <label>Vergleichswert</label>
      <input id="cond_value" placeholder="z. B. 25">
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

<script>
let STATE = { brokers: [], nodes: [], connections: [] };
let selectedNodeId = null, selectedConnId = null, editingNodeId = null;
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
  condition:  { title: 'Bedingung',      hasIn:true,  outs:['true','false'] },
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
  renderAll();
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
                           nodes: STATE.nodes, connections: STATE.connections })
  });
  if (showToast){
    if (res.ok) toast('Gespeichert.'); else toast('Fehler beim Speichern.', false);
  }
}

// ---------------- Bloecke ----------------
function addNode(type){
  const count = STATE.nodes.length;
  const node = {
    id: 'node_' + Math.random().toString(16).slice(2,10),
    type, x: 60 + (count % 6) * 40, y: 60 + Math.floor(count/6) * 120,
    label:'', broker_id:'', topic:'', payload:'', qos:0, retain:false,
    delay_sec:1.0, operator:'eq', compare_value:''
  };
  STATE.nodes.push(node);
  renderAll(); scheduleSave();
}

function nodeById(id){ return STATE.nodes.find(n => n.id === id); }

function renderAll(){
  renderNodes();
  renderConnections();
}

function nodeSummary(n){
  const b = STATE.brokers.find(x => x.id === n.broker_id);
  if (n.type === 'mqtt_in') return `Broker: <b>${escapeHtml(b?b.name:'-')}</b><br>Topic: <b>${escapeHtml(n.topic||'-')}</b>`;
  if (n.type === 'mqtt_out') return `Broker: <b>${escapeHtml(b?b.name:'-')}</b><br>Topic: <b>${escapeHtml(n.topic||'-')}</b><br>Payload: <b>${escapeHtml(n.payload||'-')}</b>`;
  if (n.type === 'delay') return `Dauer: <b>${escapeHtml(n.delay_sec)} s</b>`;
  if (n.type === 'condition') return `Wenn Wert ${escapeHtml(opLabel(n.operator))} <b>${escapeHtml(n.compare_value||'-')}</b>`;
  return '';
}
function opLabel(op){
  return {eq:'=', neq:'&ne;', contains:'enthaelt', gt:'&gt;', lt:'&lt;'}[op] || op;
}

function renderNodes(){
  const canvas = document.getElementById('canvas');
  canvas.querySelectorAll('.flow-node').forEach(el => el.remove());
  STATE.nodes.forEach(n => {
    const meta = NODE_META[n.type];
    const div = document.createElement('div');
    div.className = 'flow-node type-' + n.type + (n.id === selectedNodeId ? ' selected' : '');
    div.style.left = n.x + 'px'; div.style.top = n.y + 'px';
    div.dataset.id = n.id;

    const playBtn = n.type === 'mqtt_in' ? `<span class="node-play" onclick="simulateNode('${n.id}', event)">&#9654;</span>` : '';
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
      p.onmouseup = (e)=>finishConn(n.id, e);
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
    canvas.appendChild(div);
  });
}

function nodePortPos(nodeId, port){
  const el = document.querySelector(`.flow-node[data-id="${nodeId}"]`);
  if (!el) return {x:0,y:0};
  const w = el.offsetWidth, h = el.offsetHeight;
  const x = el.offsetLeft, y = el.offsetTop;
  if (port === 'in') return {x: x, y: y + h/2};
  if (port === 'true') return {x: x + w, y: y + h*0.34};
  if (port === 'false') return {x: x + w, y: y + h*0.66};
  return {x: x + w, y: y + h/2}; // out
}

function pathBetween(p1, p2){
  const dx = Math.max(40, Math.abs(p2.x - p1.x) / 2);
  return `M ${p1.x} ${p1.y} C ${p1.x+dx} ${p1.y}, ${p2.x-dx} ${p2.y}, ${p2.x} ${p2.y}`;
}

function renderConnections(){
  const svg = document.getElementById('connLayer');
  svg.innerHTML = '';
  STATE.connections.forEach(c => {
    const p1 = nodePortPos(c.from, c.from_port || 'out');
    const p2 = nodePortPos(c.to, 'in');
    const path = document.createElementNS('http://www.w3.org/2000/svg','path');
    path.setAttribute('d', pathBetween(p1,p2));
    path.setAttribute('fill','none');
    path.setAttribute('stroke', c.id === selectedConnId ? '#ff9142' : '#3ddc9799');
    path.setAttribute('stroke-width', c.id === selectedConnId ? '2.5' : '2');
    path.style.pointerEvents = 'stroke';
    path.onclick = () => { selectedConnId = c.id; selectedNodeId=null; renderAll(); };
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
  } catch(e){}
}

// ---------------- Verschieben ----------------
function startDrag(e, id){
  e.preventDefault();
  selectedNodeId = id; selectedConnId = null;
  const n = nodeById(id);
  const wrap = document.getElementById('canvasWrap');
  dragNode = id;
  dragOffset = { x: e.clientX + wrap.scrollLeft - n.x, y: e.clientY + wrap.scrollTop - n.y };
  renderAll();
}
document.addEventListener('mousemove', (e) => {
  if (dragNode){
    const wrap = document.getElementById('canvasWrap');
    const n = nodeById(dragNode);
    if (!n) return;
    n.x = Math.max(0, e.clientX + wrap.scrollLeft - dragOffset.x);
    n.y = Math.max(0, e.clientY + wrap.scrollTop - dragOffset.y);
    const el = document.querySelector(`.flow-node[data-id="${dragNode}"]`);
    if (el){ el.style.left = n.x+'px'; el.style.top = n.y+'px'; }
    renderConnections();
  }
  if (drawingConn){
    const wrap = document.getElementById('canvasWrap');
    const rect = document.getElementById('canvas').getBoundingClientRect();
    drawingConn.x2 = e.clientX - rect.left; drawingConn.y2 = e.clientY - rect.top;
    drawTempLine();
  }
});
document.addEventListener('mouseup', () => {
  if (dragNode){ dragNode = null; scheduleSave(); }
});

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
  el.setAttribute('d', pathBetween({x:drawingConn.x1,y:drawingConn.y1}, {x:drawingConn.x2,y:drawingConn.y2}));
}
function finishConn(targetId, e){
  e.stopPropagation();
  if (!drawingConn) return;
  if (drawingConn.fromId === targetId){ drawingConn = null; renderConnections(); return; }
  const exists = STATE.connections.some(c => c.from===drawingConn.fromId && c.from_port===drawingConn.fromPort && c.to===targetId);
  if (!exists){
    STATE.connections.push({ id:'conn_'+Math.random().toString(16).slice(2,10),
      from: drawingConn.fromId, from_port: drawingConn.fromPort, to: targetId });
    scheduleSave();
  }
  drawingConn = null;
  const tmp = document.getElementById('tempConnLine'); if (tmp) tmp.remove();
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
  if (e.target.id === 'canvasWrap' || e.target.id === 'canvas'){ selectedNodeId=null; selectedConnId=null; renderAll(); }
});

document.addEventListener('keydown', (e) => {
  if ((e.key === 'Delete' || e.key === 'Backspace') && document.activeElement.tagName !== 'INPUT'
      && document.activeElement.tagName !== 'TEXTAREA'){
    if (selectedNodeId){ deleteNode(selectedNodeId); }
    else if (selectedConnId){ deleteConnById(selectedConnId); }
  }
});

function deleteNode(id, e){
  if (e) e.stopPropagation();
  STATE.nodes = STATE.nodes.filter(n => n.id !== id);
  STATE.connections = STATE.connections.filter(c => c.from !== id && c.to !== id);
  if (selectedNodeId === id) selectedNodeId = null;
  renderAll(); scheduleSave();
}
function deleteConnById(id){
  STATE.connections = STATE.connections.filter(c => c.id !== id);
  selectedConnId = null;
  renderAll(); scheduleSave();
}

async function simulateNode(id, e){
  e.stopPropagation();
  const payload = prompt('Test-Payload fuer dieses Ereignis:', '');
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
  ['fields_mqtt_in','fields_mqtt_out','fields_delay','fields_condition'].forEach(id2 => {
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
    document.getElementById('delay_sec').value = n.delay_sec ?? 1.0;
  } else if (n.type === 'condition'){
    document.getElementById('cond_op').value = n.operator || 'eq';
    document.getElementById('cond_value').value = n.compare_value || '';
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
    n.delay_sec = parseFloat(document.getElementById('delay_sec').value)||0;
  } else if (n.type === 'condition'){
    n.operator = document.getElementById('cond_op').value;
    n.compare_value = document.getElementById('cond_value').value;
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
</script>
</body>
</html>
"""


# --------------------------------------------------------------------------
# Start
# --------------------------------------------------------------------------
def main():
    cfg = load_config()
    ENGINE.rebuild(cfg)
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
