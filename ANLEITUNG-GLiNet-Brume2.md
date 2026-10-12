# Installation auf dem GL.iNet Brume 2 (GL-MT2500/GL-MT2500A)

Der Brume 2 ist kein PC, sondern ein OpenWrt-Router (MediaTek MT7981B,
ARM64/aarch64, musl-libc). Die vorgefertigte Windows-EXE und die
macOS-App aus den GitHub-Releases laufen darauf **nicht** - beide sind
fuer eine andere Prozessorarchitektur/Bibliothek gebaut. Der Brume 2
bringt aber bereits eine eigene Python-Umgebung mit (per `opkg`), daher
laeuft `app.py` dort direkt als Skript, ganz ohne PyInstaller.

## Voraussetzungen

- Der Brume 2 ist eingerichtet und im Netzwerk erreichbar (Standard-IP
  im Router-Modus: `192.168.8.1`).
- SSH-Zugriff ist bei GL.iNet-Geraeten von Haus aus aktiv - Login mit
  Benutzer `root` und dem Passwort, das beim Ersteinrichten der
  Web-Oberflaeche vergeben wurde.
- Internetzugang des Routers, um Pakete nachzuinstallieren.

## 1. Per SSH verbinden

```bash
ssh root@192.168.8.1
```

## 2. Speicherplatz pruefen (optional)

Der Brume 2 hat 8 GB eMMC-Speicher, das reicht fuer Python + Flask +
paho-mqtt bequem aus. Zur Kontrolle:

```bash
df -h /overlay
```

## 3. Python3, pip und die benoetigten Pakete installieren

```bash
opkg update
opkg install python3 python3-pip python3-light python3-openssl ca-certificates
python3 -m pip install --upgrade pip
python3 -m pip install flask paho-mqtt
```

Hinweis: Je nach Firmware-Version koennen sich Paketnamen leicht
unterscheiden. Falls ein `opkg install`-Befehl "not found" meldet, mit
`opkg list | grep python3-` nachsehen, welche Pakete tatsaechlich zur
Verfuegung stehen.

Ab v1.3.0 funktioniert das Programm sowohl mit paho-mqtt 2.x als auch
mit der aelteren 1.x-Version aus den opkg-Paketen vieler Router. Die
installierte Version laesst sich pruefen mit:

```bash
python3 -c "import paho.mqtt as p; print(p.__version__)"
```

(Aeltere Programmversionen bis v1.2.0 brauchten zwingend paho-mqtt 2.x
und stuerzten sonst beim Start mit
`AttributeError: ... has no attribute 'CallbackAPIVersion'` ab.)

## 4. Programmdatei auf den Router kopieren

Auf dem Router einen Ordner anlegen:

```bash
mkdir -p /root/mqtt-ablaufplaner
```

Von deinem PC aus (im entpackten Projektordner, in einem zweiten
Terminal-Fenster) die Datei `app.py` per `scp` uebertragen:

```bash
scp app.py root@192.168.8.1:/root/mqtt-ablaufplaner/
```

Alternativ, falls das Projekt bereits in einem GitHub-Repo liegt, kann
die Datei auch direkt auf dem Router heruntergeladen werden (praktisch
fuer spaetere Updates):

```bash
cd /root/mqtt-ablaufplaner
wget -O app.py https://raw.githubusercontent.com/<DEIN-BENUTZERNAME>/<DEIN-REPO>/main/app.py
```

## 5. Testlauf

```bash
cd /root/mqtt-ablaufplaner
python3 app.py
```

Es sollte die gewohnte Startmeldung mit der Versionsnummer erscheinen.
Von einem beliebigen Geraet im selben Netzwerk im Browser aufrufen:

```
http://192.168.8.1:8020
```

Mit STRG+C im SSH-Fenster wieder beenden.

## 5b. Zeitzone pruefen (fuer Uhrzeit-/Sonnen-/Wochentag-/Datums-Bedingungen)

Die Bedingungen nutzen die Uhrzeit des Routers. Pruefen mit:

```bash
date
```

Falls die Zeit nicht zur lokalen Zeit passt, Zeitzone setzen (Beispiel
Deutschland):

```bash
uci set system.@system[0].zonename='Europe/Berlin'
uci set system.@system[0].timezone='CET-1CEST,M3.5.0,M10.5.0/3'
uci commit system
/etc/init.d/system reload
```

Zusaetzlich im Webinterface unter "Standort" den Breiten-/Laengengrad
fuer die Sonnen-Bedingung eintragen.

## 6. Autostart einrichten (damit es jeden Neustart des Routers uebersteht)

Datei `/etc/init.d/mqtt-ablaufplaner` mit folgendem Inhalt anlegen:

```sh
#!/bin/sh /etc/rc.common
START=99
STOP=10
USE_PROCD=1

start_service() {
    procd_open_instance
    procd_set_param command /usr/bin/python3 /root/mqtt-ablaufplaner/app.py
    [ -r /etc/TZ ] && procd_set_param env TZ="$(cat /etc/TZ)"
    procd_set_param respawn 3600 5 0
    procd_set_param stdout 1
    procd_set_param stderr 1
    procd_close_instance
}
```

Danach ausfuehrbar machen, aktivieren und starten:

```bash
chmod +x /etc/init.d/mqtt-ablaufplaner
/etc/init.d/mqtt-ablaufplaner enable
/etc/init.d/mqtt-ablaufplaner start
```

Status/Logs pruefen:

```bash
logread | grep -i -E "python|app.py"
```

Beenden/Neustarten des Dienstes:

```bash
/etc/init.d/mqtt-ablaufplaner stop
/etc/init.d/mqtt-ablaufplaner restart
```

## 7. Firewall

Standardmaessig ist LAN-Zugriff auf jeden Dienst des Routers bereits
erlaubt - es ist also keine zusaetzliche Firewall-Regel noetig, um die
Oberflaeche von einem Geraet im selben Netz aus zu erreichen. Von der
WAN-Seite (Internet) ist der Router im Auslieferungszustand gesperrt -
das sollte auch so bleiben; fuer Fernzugriff eignet sich stattdessen
eine der VPN-Funktionen des Brume 2 (WireGuard/OpenVPN), nicht das
Oeffnen des Ports 8020 nach aussen.

## 8. Updates einspielen

Einfach die neue `app.py` erneut per `scp` oder `wget` an dieselbe
Stelle kopieren und den Dienst neu starten:

```bash
/etc/init.d/mqtt-ablaufplaner restart
```

Die Konfiguration (`config.json` im selben Ordner) und die gespeicherten
Merker-Werte (`markers.json`) bleiben dabei unveraendert erhalten.
