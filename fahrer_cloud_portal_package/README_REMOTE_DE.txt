FAHRER-CLOUD-PORTAL – VON ÜBERALL ERREICHBAR
===========================================

WAS DIESE VERSION KANN
----------------------
- Jeder Fahrer loggt sich mit Benutzername + Passwort ein.
- Jeder Fahrer sieht NUR seine eigenen PDFs.
- PDFs werden auf dem Server gespeichert.
- Zugriff ist weltweit über einen HTTPS-Link möglich.
- Dein Desktop-Programm muss dafür NICHT geändert werden.
- Ein separates Sync-Skript erzeugt pro Fahrer eigene PDFs und lädt sie hoch.

WICHTIGES PRINZIP
-----------------
Dein bestehendes Desktop-Programm bleibt so wie es ist.
Zusätzlich nutzt du dieses Paket:
1. Cloud-Portal online bereitstellen
2. lokal Sync-Skript starten
3. Script erzeugt pro Fahrer private Monats-PDFs
4. Script lädt diese PDFs in das Cloud-Portal hoch
5. Fahrer loggen sich am Handy ein und sehen nur ihre PDFs

DATEIEN IN DIESEM PAKET
-----------------------
- fahrer_cloud_portal.py        -> die Web-App für Fahrer
- sync_fahrer_cloud.py         -> lokales Sync-Skript für Fahrer + PDFs
- requirements.txt
- render.yaml                  -> Render-Deployment
- Dockerfile

LOKALE VORAUSSETZUNGEN
----------------------
Diese Dateien/Ordner müssen im selben Ordner liegen:
- stunden_daten/datenbank.json

DAS CLOUD-PORTAL ONLINE STELLEN (RENDER)
----------------------------------------
1. Erstelle ein GitHub-Repository.
2. Lade den Inhalt dieses Ordners hoch.
3. Erstelle bei Render einen neuen Blueprint oder Web Service.
4. Nutze die render.yaml oder wähle das Repository direkt aus.
5. WICHTIG: Verwende eine bezahlte Web-Service-Instanz mit Persistent Disk.
6. Nach dem Deploy bekommst du eine öffentliche URL, z. B.:
   https://fahrer-cloud-portal.onrender.com

WICHTIGE ENV-VARS
-----------------
Render setzt laut render.yaml bereits automatisch:
- PORTAL_SECRET_KEY
- ADMIN_API_TOKEN
- PORTAL_DATA_DIR=/var/data

Nach dem ersten Deploy kopierst du dir den Wert von ADMIN_API_TOKEN aus Render heraus.
Diesen Token brauchst du lokal für das Sync-Skript.

LOKALES SYNC-SKRIPT STARTEN
---------------------------
1. Terminal in diesem Ordner öffnen.
2. Pakete installieren:
   pip install -r requirements.txt

3. Umgebungsvariablen setzen.
   Windows PowerShell Beispiel:
   $env:PORTAL_BASE_URL="https://DEIN-PORTAL.onrender.com"
   $env:ADMIN_API_TOKEN="DEIN_ADMIN_TOKEN"

4. Danach starten:
   python sync_fahrer_cloud.py

WAS DAS SYNC-SKRIPT MACHT
-------------------------
- fragt für jeden Fahrer Benutzername + Passwort ab
- erzeugt für jeden Fahrer und Monat eine eigene PDF
- lädt diese PDFs hoch
- legt die Fahrer im Cloud-Portal an bzw. aktualisiert sie

WIE DU SPÄTER MONAT FÜR MONAT ARBEITEST
---------------------------------------
1. Im Desktop-Programm wie gewohnt Daten pflegen.
2. PDFs wie gewohnt erzeugen lassen oder einfach nur die Daten in datenbank.json aktualisieren.
3. Dann dieses Script erneut starten:
   python sync_fahrer_cloud.py
4. Neue Monats-PDFs werden im Portal überschrieben/aktualisiert.

WAS DIE FAHRER SEHEN
--------------------
- Login-Seite
- ihre Jahre
- pro Jahr ihre Monats-PDFs
- beim Öffnen nur ihre eigene PDF

WICHTIG FÜR DATENSCHUTZ
-----------------------
- PDFs werden im Portal fahrerbezogen getrennt gespeichert.
- Der Download-Link prüft immer den eingeloggten Fahrer.
- Ohne passendes Login kommt niemand an fremde PDFs.
- Gib den ADMIN_API_TOKEN NIEMALS an Fahrer weiter.

EMPFEHLUNG
----------
Lege für jeden Fahrer ein eigenes Passwort fest und ändere es nur bei Bedarf.

HINWEIS
-------
Wenn du willst, kann später zusätzlich noch eingebaut werden:
- Passwort ändern durch den Fahrer selbst
- Admin-Bereich im Browser
- automatische Synchronisierung per Knopfdruck oder Zeitplan
- eigene Domain wie portal.deinefirma.de
