
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import psycopg
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from flask import Flask, abort, jsonify, redirect, render_template_string, request, session, url_for, make_response
from werkzeug.security import check_password_hash, generate_password_hash

try:
    import psycopg
except Exception:
    psycopg = None

APP_NAME = "Fahrer-Cloud-Portal"
ADMIN_API_TOKEN = os.environ.get("ADMIN_API_TOKEN", "")
SECRET_KEY = os.environ.get("PORTAL_SECRET_KEY", "dev-secret-change-me")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

app = Flask(__name__)

app.secret_key = SECRET_KEY
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024

MONATE = {
    1: "Januar", 2: "Februar", 3: "März", 4: "April", 5: "Mai", 6: "Juni",
    7: "Juli", 8: "August", 9: "September", 10: "Oktober", 11: "November", 12: "Dezember",
}

BASE_CSS = """
:root{--bg:#ececec;--card:#fff;--line:#d4d4d4;--head:#dfddd8;--text:#111827;--muted:#6b7280;--blue:#123e7c;}
*{box-sizing:border-box} body{margin:0;font-family:Segoe UI,Arial,sans-serif;background:var(--bg);color:var(--text)}
.wrapper{max-width:1080px;margin:0 auto;padding:16px}.topbar{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:16px}
.title{font-size:2rem;font-weight:800;color:var(--blue)} .card{background:#fff;border:1px solid var(--line);border-radius:16px;padding:16px;box-shadow:0 3px 12px rgba(0,0,0,.04)}
.month-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px}.month-item{border:1px solid var(--line);border-radius:12px;padding:14px;background:#fafafa;text-decoration:none;color:inherit}
.month-item strong{display:block;color:var(--blue);margin-bottom:5px}.btn{display:inline-block;border:1px solid #bfbfbf;border-radius:10px;padding:10px 14px;text-decoration:none;color:var(--text);background:#f8f8f8}
.btn.primary{background:var(--blue);color:#fff;border-color:var(--blue)} table{width:100%;border-collapse:collapse;font-size:14px} th,td{border:1px solid var(--line);padding:8px;vertical-align:top}
th{background:var(--head)} .muted{color:var(--muted)} .badge{display:inline-block;padding:6px 10px;border-radius:999px;background:#eff6ff;color:#1d4ed8;font-weight:700}
input,button{width:100%;padding:12px;border-radius:10px;border:1px solid #bfbfbf;font-size:16px}.error{background:#fff1f2;color:#9f1239;border:1px solid #fecdd3;border-radius:10px;padding:12px;margin-bottom:12px}
"""

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def is_postgres() -> bool:
    return DATABASE_URL.startswith("postgres")

@contextmanager
def db_cursor():
    if is_postgres():
        if psycopg is None:
            raise RuntimeError("psycopg nicht installiert")
        conn = psycopg.connect(DATABASE_URL)
        try:
            cur = conn.cursor()
            yield cur
            conn.commit()
        finally:
            conn.close()
    else:
        data_root = Path(os.environ.get("PORTAL_DATA_DIR", "/opt/render/project/src/data")).resolve()
        data_root.mkdir(parents=True, exist_ok=True)
        db_file = data_root / "portal.sqlite3"
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            yield cur
            conn.commit()
        finally:
            conn.close()

def qmark(sql: str) -> str:
    if is_postgres():
        out = []
        idx = 1
        for ch in sql:
            if ch == "?":
                out.append(f"%s")
                idx += 1
            else:
                out.append(ch)
        return "".join(out)
    return sql

def fetchall_dict(cur) -> list[dict]:
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def fetchone_dict(cur) -> Optional[dict]:
    row = cur.fetchone()
    if row is None:
        return None
    cols = [c[0] for c in cur.description]
    return dict(zip(cols, row))

def init_db() -> None:
    with db_cursor() as cur:
        if is_postgres():
            cur.execute("""
            CREATE TABLE IF NOT EXISTS drivers(
                id SERIAL PRIMARY KEY,
                external_driver_id INTEGER UNIQUE NOT NULL,
                name TEXT NOT NULL,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                starting_balance DOUBLE PRECISION NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS month_data(
                id SERIAL PRIMARY KEY,
                driver_id INTEGER NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                stunden DOUBLE PRECISION NOT NULL DEFAULT 0,
                abrechnung DOUBLE PRECISION NOT NULL DEFAULT 0,
                v DOUBLE PRECISION NOT NULL DEFAULT 0,
                zuschuesse DOUBLE PRECISION NOT NULL DEFAULT 0,
                zuschuss_kommentar TEXT NOT NULL DEFAULT '',
                abzuege DOUBLE PRECISION NOT NULL DEFAULT 0,
                abzug_kommentar TEXT NOT NULL DEFAULT '',
                differenz DOUBLE PRECISION NOT NULL DEFAULT 0,
                aktueller_stand DOUBLE PRECISION NOT NULL DEFAULT 0,
                neuer_stand DOUBLE PRECISION NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                UNIQUE(driver_id, year, month)
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS documents(
                id SERIAL PRIMARY KEY,
                driver_id INTEGER NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                original_filename TEXT NOT NULL,
                pdf_bytes BYTEA NOT NULL,
                uploaded_at TEXT NOT NULL,
                UNIQUE(driver_id, year, month)
            );
            """)
        else:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS drivers(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_driver_id INTEGER UNIQUE NOT NULL,
                name TEXT NOT NULL,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                starting_balance DOUBLE PRECISION NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS month_data(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                driver_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                stunden REAL NOT NULL DEFAULT 0,
                abrechnung REAL NOT NULL DEFAULT 0,
                v REAL NOT NULL DEFAULT 0,
                zuschuesse REAL NOT NULL DEFAULT 0,
                zuschuss_kommentar TEXT NOT NULL DEFAULT '',
                abzuege REAL NOT NULL DEFAULT 0,
                abzug_kommentar TEXT NOT NULL DEFAULT '',
                differenz REAL NOT NULL DEFAULT 0,
                aktueller_stand REAL NOT NULL DEFAULT 0,
                neuer_stand REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                UNIQUE(driver_id, year, month)
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS documents(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                driver_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                original_filename TEXT NOT NULL,
                pdf_bytes BLOB NOT NULL,
                uploaded_at TEXT NOT NULL,
                UNIQUE(driver_id, year, month)
            );
            """)


def ensure_driver_columns() -> None:
    with db_cursor() as cur:
        if is_postgres():
            try:
                cur.execute("ALTER TABLE drivers ADD COLUMN IF NOT EXISTS starting_balance DOUBLE PRECISION NOT NULL DEFAULT 0")
            except Exception:
                pass
        else:
            cur.execute("PRAGMA table_info(drivers)")
            cols = [r[1] if isinstance(r, tuple) else r["name"] for r in cur.fetchall()]
            if "starting_balance" not in cols:
                cur.execute("ALTER TABLE drivers ADD COLUMN starting_balance REAL NOT NULL DEFAULT 0")

def slugify(text: str) -> str:
    repl = {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss", "Ä": "ae", "Ö": "oe", "Ü": "ue"}
    for a, b in repl.items():
        text = text.replace(a, b)
    s = re.sub(r"[^a-zA-Z0-9._-]+", ".", text.strip().lower())
    s = re.sub(r"\.+", ".", s).strip(".")
    return s or "fahrer"

def make_unique_username(cur, username: str, exclude_id: Optional[int] = None) -> str:
    base = slugify(username)
    cand = base
    n = 2
    while True:
        if exclude_id is None:
            cur.execute(qmark("SELECT id FROM drivers WHERE lower(username)=lower(?)"), (cand,))
        else:
            cur.execute(qmark("SELECT id FROM drivers WHERE lower(username)=lower(?) AND id<>?"), (cand, exclude_id))
        if cur.fetchone() is None:
            return cand
        cand = f"{base}.{n}"
        n += 1

def admin_required() -> None:
    token = request.headers.get("X-Admin-Token", "")
    if not ADMIN_API_TOKEN or token != ADMIN_API_TOKEN:
        abort(401)

def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("driver_db_id"):
            return redirect(url_for("login"))
        return view(*args, **kwargs)
    return wrapped

def get_current_driver(cur) -> Optional[dict]:
    driver_id = session.get("driver_db_id")
    if not driver_id:
        return None
    cur.execute(qmark("SELECT * FROM drivers WHERE id=? AND is_active=1"), (driver_id,))
    return fetchone_dict(cur)

@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME, "db": "postgres" if is_postgres() else "sqlite"}

@app.get("/")
def index():
    if session.get("driver_db_id"):
        return redirect(url_for("years"))
    return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    error = ""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        with db_cursor() as cur:
            cur.execute(qmark("SELECT * FROM drivers WHERE lower(username)=lower(?)"), (username,))
            row = fetchone_dict(cur)
            if not row or not row["is_active"] or not check_password_hash(row["password_hash"], password):
                error = "Login fehlgeschlagen. Bitte Benutzername und Passwort prüfen."
            else:
                session["driver_db_id"] = int(row["id"])
                session["driver_name"] = row["name"]
                return redirect(url_for("years"))
    return render_template_string("""
    <!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{ app_name }} – Login</title><style>{{ css }}</style></head><body><div class="wrapper"><div class="topbar"><div class="title">{{ app_name }}</div></div>
    <div class="card" style="max-width:520px;margin:40px auto;"><h2 style="margin-top:0;">Fahrer-Login</h2>{% if error %}<div class="error">{{ error }}</div>{% endif %}
    <form method="post"><label>Benutzername</label><input name="username" required><label style="margin-top:12px;">Passwort</label><input type="password" name="password" required>
    <button style="margin-top:14px;">Einloggen</button></form></div></div></body></html>
    """, app_name=APP_NAME, css=BASE_CSS, error=error)

@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.get("/jahre")
@login_required
def years():
    with db_cursor() as cur:
        driver = get_current_driver(cur)
        if not driver:
            session.clear()
            return redirect(url_for("login"))
        cur.execute(qmark("SELECT year, COUNT(*) AS cnt FROM month_data WHERE driver_id=? GROUP BY year ORDER BY year DESC"), (driver["id"],))
        year_rows = fetchall_dict(cur)
    return render_template_string("""
    <!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{{ app_name }}</title><style>{{ css }}</style></head><body>
    <div class="wrapper"><div class="topbar"><div><div class="title">{{ app_name }}</div><div class="muted">Angemeldet als <span class="badge">{{ driver_name }}</span></div></div><a class="btn" href="{{ url_for('logout') }}">Logout</a></div>
    <div class="card"><h2 style="margin-top:0;">Deine Jahre</h2><div class="month-list">{% for y in years %}<a class="month-item" href="{{ url_for('months_for_year', year=y['year']) }}"><strong>{{ y['year'] }}</strong>{{ y['cnt'] }} Monat(e)</a>{% endfor %}</div></div></div></body></html>
    """, app_name=APP_NAME, css=BASE_CSS, driver_name=session.get("driver_name", ""), years=year_rows)

@app.get("/jahr/<int:year>")
@login_required
def months_for_year(year: int):
    with db_cursor() as cur:
        driver = get_current_driver(cur)
        if not driver:
            session.clear()
            return redirect(url_for("login"))
        cur.execute(qmark("""SELECT m.*, d.id AS document_id, d.uploaded_at
                             FROM month_data m LEFT JOIN documents d
                             ON d.driver_id=m.driver_id AND d.year=m.year AND d.month=m.month
                             WHERE m.driver_id=? AND m.year=? ORDER BY m.month ASC"""), (driver["id"], year))
        docs = fetchall_dict(cur)
    return render_template_string("""
    <!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{{ app_name }}</title><style>{{ css }}</style></head><body>
    <div class="wrapper"><div class="topbar"><div><div class="title">{{ app_name }}</div><div class="muted">{{ driver_name }} · Jahr {{ year }}</div></div><div><a class="btn" href="{{ url_for('years') }}">Zurück</a> <a class="btn" href="{{ url_for('logout') }}">Logout</a></div></div>
    <div class="card"><h2 style="margin-top:0;">Deine Monate für {{ year }}</h2><div class="month-list">
    {% for d in docs %}
      <div class="month-item">
        <strong>{{ months[d['month']] }} {{ d['year'] }}</strong>
        <div class="muted">Differenz: {{ '%+.2f'|format(d['differenz']).replace('.', ',') }}</div>
        <div class="muted">Neuer Stand: {{ '%+.2f'|format(d['neuer_stand']).replace('.', ',') }}</div>
        <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap">
          <a class="btn primary" href="{{ url_for('month_details', year=d['year'], month=d['month']) }}">Details</a>
          {% if d['document_id'] %}<a class="btn" href="{{ url_for('download_pdf', document_id=d['document_id'], v=d['uploaded_at']) }}">PDF</a>{% endif %}
        </div>
      </div>
    {% endfor %}
    </div></div></div></body></html>
    """, app_name=APP_NAME, css=BASE_CSS, driver_name=session.get("driver_name",""), year=year, docs=docs, months=MONATE)

@app.get("/monat/<int:year>/<int:month>")
@login_required
def month_details(year: int, month: int):
    with db_cursor() as cur:
        driver = get_current_driver(cur)
        if not driver:
            session.clear(); return redirect(url_for("login"))
        cur.execute(qmark("SELECT * FROM month_data WHERE driver_id=? AND year=? AND month=?"), (driver["id"], year, month))
        row = fetchone_dict(cur)
        if not row:
            abort(404)
    return render_template_string("""
    <!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{{ app_name }}</title><style>{{ css }}</style></head><body>
    <div class="wrapper"><div class="topbar"><div><div class="title">{{ app_name }}</div><div class="muted">{{ driver_name }} · {{ months[month] }} {{ year }}</div></div><div><a class="btn" href="{{ url_for('months_for_year', year=year) }}">Zurück</a></div></div>
    <div class="card"><table>
      <tr><th>Fahrer</th><td>{{ driver_name }}</td></tr>
      <tr><th>Stunden</th><td>{{ fmt_hours(row['stunden']) }}</td></tr>
      <tr><th>Abrechnung</th><td>{{ fmt_hours(row['abrechnung']) }}</td></tr>
      <tr><th>V</th><td>{{ fmt_hours(row['v']) }}</td></tr>
      <tr><th>Zuschüsse</th><td>{{ fmt_hours(row['zuschuesse']) }}{% if row['zuschuss_kommentar'] %}<br><span class="muted">{{ row['zuschuss_kommentar'] }}</span>{% endif %}</td></tr>
      <tr><th>Abzüge</th><td>{{ fmt_hours(row['abzuege']) }}{% if row['abzug_kommentar'] %}<br><span class="muted">{{ row['abzug_kommentar'] }}</span>{% endif %}</td></tr>
      <tr><th>Differenz</th><td>{{ fmt_signed(row['differenz']) }}</td></tr>
      <tr><th>Aktueller Stand</th><td>{{ fmt_signed(row['aktueller_stand']) }}</td></tr>
      <tr><th>Neuer Stand</th><td>{{ fmt_signed(row['neuer_stand']) }}</td></tr>
    </table></div></div></body></html>
    """, app_name=APP_NAME, css=BASE_CSS, driver_name=session.get("driver_name",""), year=year, month=month, months=MONATE, row=row, fmt_hours=fmt_hours, fmt_signed=fmt_signed)

def fmt_hours(v: float) -> str:
    return f"{float(v):.2f}".replace(".", ",") + " Std."

def fmt_signed(v: float) -> str:
    return f"{float(v):+.2f}".replace(".", ",")

@app.get("/pdf/<int:document_id>")
@login_required
def download_pdf(document_id: int):
    with db_cursor() as cur:
        driver = get_current_driver(cur)
        if not driver:
            session.clear(); return redirect(url_for("login"))
        cur.execute(qmark("SELECT * FROM documents WHERE id=? AND driver_id=?"), (document_id, driver["id"]))
        doc = fetchone_dict(cur)
        if not doc:
            abort(404)
        pdf_bytes = doc["pdf_bytes"]
        if hasattr(pdf_bytes, "tobytes"):
            pdf_bytes = pdf_bytes.tobytes()
        resp = make_response(pdf_bytes)
        resp.headers["Content-Type"] = "application/pdf"
        resp.headers["Content-Disposition"] = f'inline; filename="{doc["original_filename"]}"'
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

@app.post("/api/admin/upsert-driver")
def api_upsert_driver():
    admin_required()
    payload = request.get_json(force=True)
    ext_id = int(payload["external_driver_id"])
    name = str(payload["name"]).strip()
    username = str(payload.get("username") or slugify(name)).strip()
    password = payload.get("password")
    starting_balance = round(float(payload.get("starting_balance", 0.0) or 0.0), 2)
    if not password:
        abort(400, "password fehlt")
    with db_cursor() as cur:
        cur.execute(qmark("SELECT * FROM drivers WHERE external_driver_id=?"), (ext_id,))
        existing = fetchone_dict(cur)
        ts = now_iso()
        if existing:
            final_username = make_unique_username(cur, username, exclude_id=int(existing["id"]))
            cur.execute(qmark("UPDATE drivers SET name=?, username=?, password_hash=?, starting_balance=?, is_active=1, updated_at=? WHERE id=?"),
                        (name, final_username, generate_password_hash(password), starting_balance, ts, int(existing["id"])))
            row_id = int(existing["id"])
        else:
            final_username = make_unique_username(cur, username)
            cur.execute(qmark("INSERT INTO drivers (external_driver_id, name, username, password_hash, starting_balance, is_active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 1, ?, ?) RETURNING id") if is_postgres()
                        else qmark("INSERT INTO drivers (external_driver_id, name, username, password_hash, starting_balance, is_active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 1, ?, ?)"),
                        (ext_id, name, final_username, generate_password_hash(password), starting_balance, ts, ts))
            if is_postgres():
                row_id = int(cur.fetchone()[0])
            else:
                row_id = int(cur.lastrowid)
    return jsonify({"ok": True, "driver_db_id": row_id, "username": final_username})

@app.get("/api/admin/drivers")
def api_drivers():
    admin_required()
    with db_cursor() as cur:
        cur.execute("SELECT id, external_driver_id, name, username, starting_balance, is_active FROM drivers ORDER BY name")
        return jsonify({"drivers": fetchall_dict(cur)})

@app.delete("/api/admin/drivers/<int:external_driver_id>")
def api_delete_driver(external_driver_id: int):
    admin_required()
    with db_cursor() as cur:
        cur.execute(qmark("DELETE FROM drivers WHERE external_driver_id=?"), (external_driver_id,))
    return jsonify({"ok": True})

@app.post("/api/admin/upsert-month-data")
def api_upsert_month():
    admin_required()
    payload = request.get_json(force=True)
    ext_id = int(payload["external_driver_id"])
    year = int(payload["year"]); month = int(payload["month"])
    ts = now_iso()
    with db_cursor() as cur:
        cur.execute(qmark("SELECT id FROM drivers WHERE external_driver_id=?"), (ext_id,))
        driver = fetchone_dict(cur)
        if not driver:
            abort(400, "Fahrer nicht vorhanden")
        vals = (
            int(driver["id"]), year, month,
            float(payload.get("stunden", payload.get("worked_hours", 0)) or 0), float(payload.get("abrechnung", payload.get("payroll_hours", 0)) or 0), float(payload.get("v", payload.get("v_hours", 0)) or 0),
            float(payload.get("zuschuesse", payload.get("bonus_hours", 0)) or 0), str(payload.get("zuschuss_kommentar", payload.get("bonus_comment", "")) or ""),
            float(payload.get("abzuege", payload.get("deduction_hours", 0)) or 0), str(payload.get("abzug_kommentar", payload.get("deduction_comment", "")) or ""),
            float(payload.get("differenz", payload.get("difference", 0)) or 0), float(payload.get("aktueller_stand", payload.get("previous_balance", 0)) or 0), float(payload.get("neuer_stand", payload.get("new_balance", 0)) or 0), ts
        )
        cur.execute(qmark("SELECT id FROM month_data WHERE driver_id=? AND year=? AND month=?"), (int(driver["id"]), year, month))
        existing = fetchone_dict(cur)
        if existing:
            cur.execute(qmark("""UPDATE month_data SET stunden=?, abrechnung=?, v=?, zuschuesse=?, zuschuss_kommentar=?, abzuege=?, abzug_kommentar=?, differenz=?, aktueller_stand=?, neuer_stand=?, updated_at=? WHERE id=?"""),
                        (vals[3], vals[4], vals[5], vals[6], vals[7], vals[8], vals[9], vals[10], vals[11], vals[12], vals[13], int(existing["id"])))
        else:
            cur.execute(qmark("""INSERT INTO month_data (driver_id, year, month, stunden, abrechnung, v, zuschuesse, zuschuss_kommentar, abzuege, abzug_kommentar, differenz, aktueller_stand, neuer_stand, updated_at)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""), vals)
    return jsonify({"ok": True})

@app.get("/api/admin/month-data")
def api_list_month_data():
    admin_required()
    with db_cursor() as cur:
        cur.execute("""SELECT d.external_driver_id, d.name, m.* FROM month_data m JOIN drivers d ON d.id=m.driver_id ORDER BY d.name, m.year, m.month""")
        return jsonify({"rows": fetchall_dict(cur)})

@app.delete("/api/admin/month-data/<int:external_driver_id>/<int:year>/<int:month>")
def api_delete_month_data(external_driver_id: int, year: int, month: int):
    admin_required()
    with db_cursor() as cur:
        cur.execute(qmark("SELECT id FROM drivers WHERE external_driver_id=?"), (external_driver_id,))
        driver = fetchone_dict(cur)
        if not driver:
            return jsonify({"ok": True})
        cur.execute(qmark("DELETE FROM month_data WHERE driver_id=? AND year=? AND month=?"), (int(driver["id"]), year, month))
        cur.execute(qmark("DELETE FROM documents WHERE driver_id=? AND year=? AND month=?"), (int(driver["id"]), year, month))
    return jsonify({"ok": True})

@app.post("/api/admin/upload-pdf")
def api_upload_pdf():
    admin_required()
    ext_id = int(request.form["external_driver_id"])
    year = int(request.form["year"]); month = int(request.form["month"])
    upload = request.files.get("file")
    if not upload or not upload.filename.lower().endswith(".pdf"):
        abort(400, "PDF-Datei fehlt")
    pdf_bytes = upload.read()
    ts = now_iso()
    with db_cursor() as cur:
        cur.execute(qmark("SELECT id FROM drivers WHERE external_driver_id=?"), (ext_id,))
        driver = fetchone_dict(cur)
        if not driver:
            abort(400, "Fahrer nicht vorhanden")
        cur.execute(qmark("SELECT id FROM documents WHERE driver_id=? AND year=? AND month=?"), (int(driver["id"]), year, month))
        existing = fetchone_dict(cur)
        if existing:
            cur.execute(qmark("UPDATE documents SET original_filename=?, pdf_bytes=?, uploaded_at=? WHERE id=?"),
                        (upload.filename, pdf_bytes, ts, int(existing["id"])))
        else:
            cur.execute(qmark("INSERT INTO documents (driver_id, year, month, original_filename, pdf_bytes, uploaded_at) VALUES (?, ?, ?, ?, ?, ?)"),
                        (int(driver["id"]), year, month, upload.filename, pdf_bytes, ts))
    return jsonify({"ok": True})
@app.route("/setup-db")
def setup_db():
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        return {"ok": False, "error": "DATABASE_URL fehlt"}, 500

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS drivers(
                    id SERIAL PRIMARY KEY,
                    external_driver_id INTEGER UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    starting_balance DOUBLE PRECISION NOT NULL DEFAULT 0,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS month_data(
                    id SERIAL PRIMARY KEY,
                    driver_id INTEGER NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    stunden DOUBLE PRECISION NOT NULL DEFAULT 0,
                    abrechnung DOUBLE PRECISION NOT NULL DEFAULT 0,
                    v DOUBLE PRECISION NOT NULL DEFAULT 0,
                    zuschuesse DOUBLE PRECISION NOT NULL DEFAULT 0,
                    zuschuss_kommentar TEXT NOT NULL DEFAULT '',
                    abzuege DOUBLE PRECISION NOT NULL DEFAULT 0,
                    abzug_kommentar TEXT NOT NULL DEFAULT '',
                    differenz DOUBLE PRECISION NOT NULL DEFAULT 0,
                    aktueller_stand DOUBLE PRECISION NOT NULL DEFAULT 0,
                    neuer_stand DOUBLE PRECISION NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    UNIQUE(driver_id, year, month)
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS documents(
                    id SERIAL PRIMARY KEY,
                    driver_id INTEGER NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    original_filename TEXT NOT NULL,
                    pdf_bytes BYTEA NOT NULL,
                    uploaded_at TEXT NOT NULL,
                    UNIQUE(driver_id, year, month)
                );
            """)

        conn.commit()

    return {"ok": True, "status": "database tables created"}
if __name__ == "__main__":
    init_db()
    ensure_driver_columns()
    port = int(os.environ.get("PORT", "5050"))
    app.run(host="0.0.0.0", port=port, debug=False)
