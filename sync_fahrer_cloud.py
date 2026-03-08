#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List

import requests
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
os.environ["PORTAL_BASE_URL"] = "http://127.0.0.1:5050"
os.environ["ADMIN_API_TOKEN"] = "mein_geheimer_token_123"

BASE_DIR = Path(__file__).resolve().parent
LOCAL_DATA_DIR = BASE_DIR / "stunden_daten"
JSON_DB_FILE = LOCAL_DATA_DIR / "datenbank.json"
OUT_DIR = BASE_DIR / "fahrer_pdfs_cloud"
MONATE = {
    1: "Januar", 2: "Februar", 3: "März", 4: "April", 5: "Mai", 6: "Juni",
    7: "Juli", 8: "August", 9: "September", 10: "Oktober", 11: "November", 12: "Dezember",
}


def slugify(text: str) -> str:
    repl = {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss", "Ä": "ae", "Ö": "oe", "Ü": "ue"}
    for a, b in repl.items():
        text = text.replace(a, b)
    text = re.sub(r"[^a-zA-Z0-9._-]+", ".", text.strip().lower())
    text = re.sub(r"\.+", ".", text).strip(".")
    return text or "fahrer"


def fmt_signed(v: float) -> str:
    return f"{v:+.2f}".replace(".", ",")


def fmt_hours(v: float) -> str:
    return f"{v:.2f}".replace(".", ",") + " Std."


def load_main_db() -> Dict[str, Any]:
    if not JSON_DB_FILE.exists():
        raise SystemExit(f"datenbank.json nicht gefunden: {JSON_DB_FILE}")
    with JSON_DB_FILE.open("r", encoding="utf-8") as f:
        db = json.load(f)
    db.setdefault("drivers", [])
    db.setdefault("records", {})
    return db


def pdf_table(data: List[List[str]], widths: List[float]) -> Table:
    table = Table(data, colWidths=widths, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dfddd8")),
        ("TEXTCOLOR", (0, 0), (-1, -1), colors.black),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 10),
        ("FONTSIZE", (0, 1), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#b8b4ad")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    return table


def create_pdf_report(pdf_path: Path, title: str, subtitle: str, rows: List[List[str]]) -> None:
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "t", parent=styles["Heading1"], fontName="Helvetica-Bold", fontSize=18,
        leading=22, textColor=colors.HexColor("#123e7c"), spaceAfter=6,
    )
    sub_style = ParagraphStyle(
        "s", parent=styles["BodyText"], fontName="Helvetica", fontSize=10,
        leading=13, textColor=colors.HexColor("#444444"), spaceAfter=10,
    )
    headers = [
        "Fahrer", "Tatsächliche Arbeitsstunden", "V", "Sonstige Abzüge/Zuschüsse",
        "Kommentar", "Abrechnung", "Differenz", "Aktuelles Saldo", "Neues Saldo",
    ]
    width_mm = [36, 34, 16, 34, 44, 24, 20, 22, 22]
    doc = SimpleDocTemplate(str(pdf_path), pagesize=A4, leftMargin=10*mm, rightMargin=10*mm, topMargin=10*mm, bottomMargin=10*mm)
    story = [Paragraph(title, title_style), Paragraph(subtitle, sub_style), Spacer(1, 3*mm), pdf_table([headers] + rows, [w*mm for w in width_mm])]
    doc.build(story)


def generate_driver_monthly_pdfs(db: Dict[str, Any]) -> List[Dict[str, Any]]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    drivers = {int(d["id"]): d for d in db.get("drivers", [])}
    generated: List[Dict[str, Any]] = []
    for year_str, months in db.get("records", {}).items():
        year = int(year_str)
        for month_str, rows in months.items():
            month = int(month_str)
            for row in rows:
                driver_id = int(row["driver_id"])
                driver = drivers.get(driver_id)
                if not driver:
                    continue
                driver_name = row.get("driver_name") or driver.get("name", f"Fahrer {driver_id}")
                pdf_dir = OUT_DIR / slugify(driver_name) / str(year)
                pdf_path = pdf_dir / f"{month:02d}_{MONATE[month]}_{year}.pdf"
                pdf_rows = [[
                    driver_name,
                    fmt_hours(float(row.get("worked_hours", 0.0))),
                    fmt_hours(abs(float(row.get("v_hours", 0.0)))),
                    fmt_signed(float(row.get("adjustment_hours", 0.0))),
                    row.get("comment", "") or "-",
                    fmt_hours(float(row.get("payroll_hours", 0.0))),
                    fmt_signed(float(row.get("difference", 0.0))),
                    fmt_signed(float(row.get("previous_balance", 0.0))),
                    fmt_signed(float(row.get("new_balance", 0.0))),
                ]]
                create_pdf_report(
                    pdf_path,
                    f"Monatsübersicht {driver_name} – {MONATE[month]} {year}",
                    "Nur die Daten dieses Fahrers",
                    pdf_rows,
                )
                generated.append({
                    "external_driver_id": driver_id,
                    "driver_name": driver_name,
                    "year": year,
                    "month": month,
                    "path": pdf_path,
                })
    return generated


def ask_credentials_for_drivers(db: Dict[str, Any]) -> List[Dict[str, Any]]:
    drivers = sorted(db.get("drivers", []), key=lambda d: d.get("name", "").lower())
    result = []
    print("\nLogins für das Cloud-Portal festlegen\n")
    for driver in drivers:
        ext_id = int(driver["id"])
        name = driver["name"]
        default_username = slugify(name)
        print(f"Fahrer: {name}")
        username = input(f"Benutzername [{default_username}]: ").strip() or default_username
        password = ""
        while not password:
            password = input("Passwort: ").strip()
        result.append({"external_driver_id": ext_id, "name": name, "username": username, "password": password})
        print()
    return result


def main() -> None:
    base_url = os.environ.get("PORTAL_BASE_URL", "").strip().rstrip("/")
    admin_token = os.environ.get("ADMIN_API_TOKEN", "").strip()
    if not base_url or not admin_token:
        raise SystemExit("Bitte PORTAL_BASE_URL und ADMIN_API_TOKEN als Umgebungsvariablen setzen.")

    db = load_main_db()
    users = ask_credentials_for_drivers(db)
    print("Erzeuge Fahrer-PDFs ...")
    files = generate_driver_monthly_pdfs(db)
    session = requests.Session()
    session.headers.update({"X-Admin-Token": admin_token})

    print("Synchronisiere Fahrer-Logins ...")
    for u in users:
        r = session.post(base_url + "/api/admin/upsert-driver", json=u, timeout=60)
        r.raise_for_status()
        data = r.json()
        print(f"  OK Login: {u['name']} -> {data['username']}")

    print("Lade PDFs hoch ...")
    for item in files:
        with item["path"].open("rb") as f:
            r = session.post(
                base_url + "/api/admin/upload-pdf",
                data={
                    "external_driver_id": str(item["external_driver_id"]),
                    "year": str(item["year"]),
                    "month": str(item["month"]),
                },
                files={"file": (item["path"].name, f, "application/pdf")},
                timeout=120,
            )
        r.raise_for_status()
        print(f"  OK PDF: {item['driver_name']} {MONATE[item['month']]} {item['year']}")

    print("\nFertig. Das Cloud-Portal ist jetzt mit Fahrern und PDFs befüllt.")


if __name__ == "__main__":
    main()
