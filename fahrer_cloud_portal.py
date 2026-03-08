#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Optional

from flask import Flask, abort, jsonify, redirect, render_template_string, request, send_file, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

APP_NAME = "Fahrer-Cloud-Portal"
DATA_ROOT = Path(os.environ.get("PORTAL_DATA_DIR", "/opt/render/project/src/data")).resolve()
FILES_DIR = DATA_ROOT / "pdfs"
DB_FILE = DATA_ROOT / "portal.sqlite3"
ADMIN_API_TOKEN = os.environ.get("ADMIN_API_TOKEN", "")
SECRET_KEY = os.environ.get("PORTAL_SECRET_KEY", "dev-secret-change-me")
MAX_CONTENT_LENGTH = 25 * 1024 * 1024

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

BASE_CSS = """
:root {
  --bg: #ececec; --card: #fff; --line: #d4d4d4; --head: #dfddd8; --text: #111827;
  --muted: #6b7280; --blue: #123e7c;
}
* { box-sizing: border-box; }
body { margin:0; font-family: Segoe UI, Arial, sans-serif; background: var(--bg); color:var(--text); }
.wrapper { max-width: 1040px; margin: 0 auto; padding: 18px; }
.topbar { display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:18px; }
.title { font-size: 2rem; font-weight:800; color:var(--blue); }
.card { background:var(--card); border:1px solid var(--line); border-radius:16px; padding:16px; box-shadow:0 3px 12px rgba(0,0,0,.04); }
.grid { display:grid; gap:16px; }
.grid-2 { grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); }
label { display:block; font-weight:700; margin-bottom:6px; }
input, button, .btn { width:100%; padding:12px; border-radius:10px; border:1px solid #bfbfbf; font-size:16px; }
button, .btn { background:#f8f8f8; cursor:pointer; text-decoration:none; color:var(--text); display:inline-block; text-align:center; }
button.primary, .btn.primary { background:var(--blue); color:white; border-color:var(--blue); font-weight:700; }
.error, .success { border-radius:10px; padding:12px; margin-bottom:12px; }
.error { background:#fff1f2; color:#9f1239; border:1px solid #fecdd3; }
.success { background:#ecfdf5; color:#065f46; border:1px solid #a7f3d0; }
.note { color:var(--muted); }
.month-list { display:grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap:12px; }
.month-item { border:1px solid var(--line); border-radius:12px; padding:14px; background:#fafafa; text-decoration:none; color:inherit; }
.month-item strong { display:block; color:var(--blue); margin-bottom:5px; }
.actions { display:flex; gap:10px; flex-wrap:wrap; }
.actions .btn { width:auto; padding:10px 14px; }
.badge { display:inline-block; padding:6px 10px; border-radius:999px; background:#eff6ff; color:#1d4ed8; font-weight:700; }
@media (max-width: 800px) { .wrapper { padding:12px; } .title { font-size: 1.65rem; } }
"""

MONATE = {
    1: "Januar", 2: "Februar", 3: "März", 4: "April", 5: "Mai", 6: "Juni",
    7: "Juli", 8: "August", 9: "September", 10: "Oktober", 11: "November", 12: "Dezember",
}

def fmt_hours(value: float) -> str:
    return f"{float(value):.2f}".replace(".", ",") + " Std."


def fmt_signed(value: float) -> str:
    return f"{float(value):+.2f}".replace(".", ",")


def get_month_data(conn: sqlite3.Connection, driver_db_id: int, year: int, month: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM monthly_data WHERE driver_id=? AND year=? AND month=?",
        (driver_db_id, year, month),
    ).fetchone()



def ensure_paths() -> None:
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    FILES_DIR.mkdir(parents=True, exist_ok=True)


def db_conn() -> sqlite3.Connection:
    ensure_paths()
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS drivers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_driver_id INTEGER UNIQUE,
            name TEXT NOT NULL,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            driver_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            filename TEXT NOT NULL,
            original_filename TEXT NOT NULL,
            relative_path TEXT NOT NULL UNIQUE,
            uploaded_at TEXT NOT NULL,
            FOREIGN KEY (driver_id) REFERENCES drivers(id) ON DELETE CASCADE,
            UNIQUE (driver_id, year, month)
        );

        CREATE TABLE IF NOT EXISTS monthly_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            driver_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            worked_hours REAL NOT NULL DEFAULT 0,
            v_hours REAL NOT NULL DEFAULT 0,
            adjustment_hours REAL NOT NULL DEFAULT 0,
            comment TEXT NOT NULL DEFAULT '',
            payroll_hours REAL NOT NULL DEFAULT 0,
            difference_hours REAL NOT NULL DEFAULT 0,
            previous_balance REAL NOT NULL DEFAULT 0,
            new_balance REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (driver_id) REFERENCES drivers(id) ON DELETE CASCADE,
            UNIQUE (driver_id, year, month)
        );
        """
    )
    conn.commit()
    return conn


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify(text: str) -> str:
    repl = {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss", "Ä": "ae", "Ö": "oe", "Ü": "ue"}
    for a, b in repl.items():
        text = text.replace(a, b)
    s = re.sub(r"[^a-zA-Z0-9._-]+", ".", text.strip().lower())
    s = re.sub(r"\.+", ".", s).strip(".")
    return s or "fahrer"


def make_unique_username(conn: sqlite3.Connection, username: str, exclude_id: Optional[int] = None) -> str:
    base = slugify(username)
    candidate = base
    n = 2
    while True:
        if exclude_id is None:
            row = conn.execute("SELECT id FROM drivers WHERE lower(username)=lower(?)", (candidate,)).fetchone()
        else:
            row = conn.execute("SELECT id FROM drivers WHERE lower(username)=lower(?) AND id<>?", (candidate, exclude_id)).fetchone()
        if not row:
            return candidate
        candidate = f"{base}.{n}"
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


def get_current_driver(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    driver_id = session.get("driver_db_id")
    if not driver_id:
        return None
    return conn.execute("SELECT * FROM drivers WHERE id=? AND is_active=1", (driver_id,)).fetchone()


@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME}


@app.route("/", methods=["GET"])
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
        with db_conn() as conn:
            row = conn.execute("SELECT * FROM drivers WHERE lower(username)=lower(?)", (username,)).fetchone()
            if not row or not row["is_active"] or not check_password_hash(row["password_hash"], password):
                error = "Login fehlgeschlagen. Bitte Benutzername und Passwort prüfen."
            else:
                session["driver_db_id"] = int(row["id"])
                session["driver_name"] = row["name"]
                return redirect(url_for("years"))

    return render_template_string(
        """
        <!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{{ app_name }} – Login</title><style>{{ css }}</style></head><body>
        <div class="wrapper">
          <div class="topbar"><div class="title">{{ app_name }}</div></div>
          <div class="card" style="max-width:520px;margin:40px auto;">
            <h2 style="margin-top:0;">Fahrer-Login</h2>
            {% if error %}<div class="error">{{ error }}</div>{% endif %}
            <form method="post">
              <label>Benutzername</label><input name="username" autocomplete="username" required>
              <label style="margin-top:12px;">Passwort</label><input name="password" type="password" autocomplete="current-password" required>
              <button class="primary" type="submit" style="margin-top:14px;">Einloggen</button>
            </form>
            <p class="note">Nach dem Login siehst du nur deine eigenen PDFs.</p>
          </div>
        </div></body></html>
        """,
        app_name=APP_NAME,
        css=BASE_CSS,
        error=error,
    )


@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.get("/jahre")
@login_required
def years():
    with db_conn() as conn:
        driver = get_current_driver(conn)
        if not driver:
            session.clear()
            return redirect(url_for("login"))
        year_rows = conn.execute(
            "SELECT year, COUNT(*) AS cnt, MAX(uploaded_at) AS uploaded_at FROM documents WHERE driver_id=? GROUP BY year ORDER BY year DESC",
            (driver["id"],),
        ).fetchall()
    return render_template_string(
        """
        <!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{{ app_name }}</title><style>{{ css }}</style></head><body>
        <div class="wrapper">
          <div class="topbar">
            <div><div class="title">{{ app_name }}</div><div class="note">Angemeldet als <span class="badge">{{ driver_name }}</span></div></div>
            <a class="btn" style="width:auto;padding:10px 14px;" href="{{ url_for('logout') }}">Logout</a>
          </div>
          <div class="card">
            <h2 style="margin-top:0;">Deine Jahre</h2>
            {% if years %}
            <div class="month-list">
              {% for y in years %}
              <a class="month-item" href="{{ url_for('months_for_year', year=y['year']) }}"><strong>{{ y['year'] }}</strong>{{ y['cnt'] }} PDF(s)</a>
              {% endfor %}
            </div>
            {% else %}
            <p>Noch keine PDFs vorhanden.</p>
            {% endif %}
          </div>
        </div></body></html>
        """,
        app_name=APP_NAME,
        css=BASE_CSS,
        driver_name=session.get("driver_name", ""),
        years=year_rows,
    )


@app.get("/jahr/<int:year>")
@login_required
def months_for_year(year: int):
    with db_conn() as conn:
        driver = get_current_driver(conn)
        if not driver:
            session.clear()
            return redirect(url_for("login"))
        docs = conn.execute(
            "SELECT id, year, month, original_filename, uploaded_at FROM documents WHERE driver_id=? AND year=? ORDER BY month ASC",
            (driver["id"], year),
        ).fetchall()
    return render_template_string(
        """
        <!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{{ app_name }}</title><style>{{ css }}</style></head><body>
        <div class="wrapper">
          <div class="topbar">
            <div><div class="title">{{ app_name }}</div><div class="note">{{ driver_name }} · Jahr {{ year }}</div></div>
            <div class="actions">
              <a class="btn" href="{{ url_for('years') }}">Zurück</a>
              <a class="btn" href="{{ url_for('logout') }}">Logout</a>
            </div>
          </div>
          <div class="card">
            <h2 style="margin-top:0;">Deine PDFs für {{ year }}</h2>
            {% if docs %}
            <div class="month-list">
              {% for d in docs %}
              <div class="month-item">
                <strong>{{ months[d['month']] }} {{ d['year'] }}</strong>
                <div class="note" style="margin-bottom:10px;">Datei: {{ d['original_filename'] }}</div>
                <div class="actions">
                  <a class="btn" href="{{ url_for('month_detail', year=d['year'], month=d['month']) }}">Details ansehen</a>
                  <a class="btn primary" href="{{ url_for('download_pdf', document_id=d['id']) }}">PDF öffnen</a>
                </div>
              </div>
              {% endfor %}
            </div>
            {% else %}
            <p>Für dieses Jahr sind noch keine PDFs vorhanden.</p>
            {% endif %}
          </div>
        </div></body></html>
        """,
        app_name=APP_NAME,
        css=BASE_CSS,
        driver_name=session.get("driver_name", ""),
        year=year,
        docs=docs,
        months=MONATE,
    )


@app.get("/jahr/<int:year>/<int:month>")
@login_required
def month_detail(year: int, month: int):
    with db_conn() as conn:
        driver = get_current_driver(conn)
        if not driver:
            session.clear()
            return redirect(url_for("login"))
        doc = conn.execute(
            "SELECT * FROM documents WHERE driver_id=? AND year=? AND month=?",
            (driver["id"], year, month),
        ).fetchone()
        data_row = get_month_data(conn, int(driver["id"]), year, month)
    if not doc and not data_row:
        abort(404)
    return render_template_string(
        """
        <!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{{ app_name }}</title><style>{{ css }}</style></head><body>
        <div class="wrapper">
          <div class="topbar">
            <div><div class="title">{{ app_name }}</div><div class="note">{{ driver_name }} · {{ month_name }} {{ year }}</div></div>
            <div class="actions">
              <a class="btn" href="{{ url_for('months_for_year', year=year) }}">Zurück</a>
              <a class="btn" href="{{ url_for('logout') }}">Logout</a>
            </div>
          </div>

          <div class="card" style="margin-bottom:16px;">
            <h2 style="margin-top:0;">Monatsdetails</h2>
            {% if data_row %}
            <div class="grid grid-2">
              <div><label>Tatsächliche Arbeitsstunden</label><div class="note">{{ fmt_hours(data_row['worked_hours']) }}</div></div>
              <div><label>Abrechnung</label><div class="note">{{ fmt_hours(data_row['payroll_hours']) }}</div></div>
              <div><label>V</label><div class="note">{{ fmt_hours(data_row['v_hours']) }}</div></div>
              <div><label>Sonstige Abzüge/Zuschüsse</label><div class="note">{{ fmt_signed(data_row['adjustment_hours']) }}</div></div>
              <div><label>Differenz</label><div class="note">{{ fmt_signed(data_row['difference_hours']) }}</div></div>
              <div><label>Aktuelles Saldo</label><div class="note">{{ fmt_signed(data_row['previous_balance']) }}</div></div>
              <div><label>Neues Saldo</label><div class="note">{{ fmt_signed(data_row['new_balance']) }}</div></div>
              <div><label>Kommentar</label><div class="note">{{ data_row['comment'] or '-' }}</div></div>
            </div>
            {% else %}
            <p>Noch keine Detaildaten vorhanden.</p>
            {% endif %}
          </div>

          {% if doc %}
          <div class="card">
            <h2 style="margin-top:0;">PDF</h2>
            <a class="btn primary" style="width:auto;padding:10px 14px;" href="{{ url_for('download_pdf', document_id=doc['id']) }}">PDF öffnen</a>
          </div>
          {% endif %}
        </div></body></html>
        """,
        app_name=APP_NAME,
        css=BASE_CSS,
        driver_name=session.get("driver_name", ""),
        year=year,
        month=month,
        month_name=MONATE.get(month, str(month)),
        data_row=data_row,
        doc=doc,
        fmt_hours=fmt_hours,
        fmt_signed=fmt_signed,
    )


@app.get("/pdf/<int:document_id>")
@login_required
def download_pdf(document_id: int):
    with db_conn() as conn:
        driver = get_current_driver(conn)
        if not driver:
            session.clear()
            return redirect(url_for("login"))
        doc = conn.execute("SELECT * FROM documents WHERE id=? AND driver_id=?", (document_id, driver["id"])).fetchone()
        if not doc:
            abort(404)
        path = DATA_ROOT / doc["relative_path"]
        if not path.exists():
            abort(404)
        return send_file(path, mimetype="application/pdf", as_attachment=False, download_name=doc["original_filename"])


@app.post("/api/admin/upsert-driver")
def api_upsert_driver():
    admin_required()
    payload = request.get_json(force=True, silent=False)
    ext_id = int(payload["external_driver_id"])
    name = str(payload["name"]).strip()
    username = str(payload.get("username") or slugify(name)).strip()
    password = payload.get("password")
    if not password:
        abort(400, "password fehlt")

    with db_conn() as conn:
        existing = conn.execute("SELECT * FROM drivers WHERE external_driver_id=?", (ext_id,)).fetchone()
        ts = now_iso()
        if existing:
            final_username = make_unique_username(conn, username, exclude_id=int(existing["id"]))
            conn.execute(
                "UPDATE drivers SET name=?, username=?, password_hash=?, is_active=1, updated_at=? WHERE id=?",
                (name, final_username, generate_password_hash(password), ts, int(existing["id"])),
            )
            conn.commit()
            row_id = int(existing["id"])
        else:
            final_username = make_unique_username(conn, username)
            cur = conn.execute(
                "INSERT INTO drivers (external_driver_id, name, username, password_hash, is_active, created_at, updated_at) VALUES (?, ?, ?, ?, 1, ?, ?)",
                (ext_id, name, final_username, generate_password_hash(password), ts, ts),
            )
            conn.commit()
            row_id = int(cur.lastrowid)
    return jsonify({"ok": True, "driver_db_id": row_id, "username": final_username})


@app.post("/api/admin/upload-pdf")
def api_upload_pdf():
    admin_required()
    ext_id = int(request.form["external_driver_id"])
    year = int(request.form["year"])
    month = int(request.form["month"])
    upload = request.files.get("file")
    if not upload or not upload.filename.lower().endswith(".pdf"):
        abort(400, "PDF-Datei fehlt")

    with db_conn() as conn:
        driver = conn.execute("SELECT * FROM drivers WHERE external_driver_id=? AND is_active=1", (ext_id,)).fetchone()
        if not driver:
            abort(400, "Fahrer nicht vorhanden")
        driver_slug = slugify(driver["name"]) + f"-{driver['id']}"
        target_dir = FILES_DIR / driver_slug / str(year)
        target_dir.mkdir(parents=True, exist_ok=True)
        safe_name = secure_filename(f"{month:02d}_{MONATE.get(month, str(month))}_{year}.pdf")
        relative_path = Path("pdfs") / driver_slug / str(year) / safe_name
        abs_path = DATA_ROOT / relative_path
        upload.save(abs_path)
        ts = now_iso()
        existing = conn.execute(
            "SELECT id FROM documents WHERE driver_id=? AND year=? AND month=?",
            (int(driver["id"]), year, month),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE documents SET filename=?, original_filename=?, relative_path=?, uploaded_at=? WHERE id=?",
                (safe_name, upload.filename, str(relative_path), ts, int(existing["id"])),
            )
        else:
            conn.execute(
                "INSERT INTO documents (driver_id, year, month, filename, original_filename, relative_path, uploaded_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (int(driver["id"]), year, month, safe_name, upload.filename, str(relative_path), ts),
            )
        conn.commit()
    return jsonify({"ok": True, "stored_as": str(relative_path)})


@app.post("/api/admin/upsert-month-data")
def api_upsert_month_data():
    admin_required()
    payload = request.get_json(force=True, silent=False)
    ext_id = int(payload["external_driver_id"])
    year = int(payload["year"])
    month = int(payload["month"])

    with db_conn() as conn:
        driver = conn.execute(
            "SELECT * FROM drivers WHERE external_driver_id=? AND is_active=1",
            (ext_id,),
        ).fetchone()
        if not driver:
            abort(400, "Fahrer nicht vorhanden")
        ts = now_iso()
        values = (
            float(payload.get("worked_hours", 0) or 0),
            abs(float(payload.get("v_hours", 0) or 0)),
            float(payload.get("adjustment_hours", 0) or 0),
            str(payload.get("comment") or "").strip(),
            float(payload.get("payroll_hours", 0) or 0),
            float(payload.get("difference", 0) or 0),
            float(payload.get("previous_balance", 0) or 0),
            float(payload.get("new_balance", 0) or 0),
            ts,
            int(driver["id"]),
            year,
            month,
        )
        existing = conn.execute(
            "SELECT id FROM monthly_data WHERE driver_id=? AND year=? AND month=?",
            (int(driver["id"]), year, month),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE monthly_data SET worked_hours=?, v_hours=?, adjustment_hours=?, comment=?, payroll_hours=?, difference_hours=?, previous_balance=?, new_balance=?, updated_at=? WHERE driver_id=? AND year=? AND month=?",
                values,
            )
        else:
            conn.execute(
                "INSERT INTO monthly_data (worked_hours, v_hours, adjustment_hours, comment, payroll_hours, difference_hours, previous_balance, new_balance, updated_at, driver_id, year, month) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                values,
            )
        conn.commit()
    return jsonify({"ok": True})


@app.get("/api/admin/list-drivers")
def api_list_drivers():
    admin_required()
    with db_conn() as conn:
        rows = conn.execute("SELECT id, external_driver_id, name, username, is_active FROM drivers ORDER BY name COLLATE NOCASE ASC").fetchall()
        return jsonify({"drivers": [dict(r) for r in rows]})


if __name__ == "__main__":
    ensure_paths()
    port = int(os.environ.get("PORT", "5050"))
    app.run(host="0.0.0.0", port=port, debug=False)

