#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

import requests
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

APP_NAME = "Plus/Minus-Stunden-Rechner"
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "stunden_daten"
EXPORT_DIR = BASE_DIR / "monatsuebersicht"
DB_FILE = DATA_DIR / "datenbank.json"
CLOUD_DIR = BASE_DIR / "cloud_portal_sync"
CLOUD_SETTINGS_FILE = CLOUD_DIR / "portal_settings.json"
CLOUD_USERS_FILE = CLOUD_DIR / "fahrer_logins.json"
CLOUD_PDF_DIR = CLOUD_DIR / "fahrer_pdfs"

DEFAULT_PORTAL_BASE_URL = os.environ.get("PORTAL_BASE_URL", "https://fahrer-portal.onrender.com").strip()
DEFAULT_ADMIN_API_TOKEN = os.environ.get("ADMIN_API_TOKEN", "").strip()

MONATE = {
    1: "Januar",
    2: "Februar",
    3: "März",
    4: "April",
    5: "Mai",
    6: "Juni",
    7: "Juli",
    8: "August",
    9: "September",
    10: "Oktober",
    11: "November",
    12: "Dezember",
}


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def fmt_signed(v: float) -> str:
    return f"{v:+.2f}".replace(".", ",")


def fmt_hours(v: float) -> str:
    return f"{v:.2f}".replace(".", ",") + " Std."


def parse_hours(raw: str) -> float:
    text = (raw or "").strip().lower().replace(" ", "")
    if not text:
        raise ValueError("Bitte einen Wert eingeben.")

    # 145:30 oder 145h30
    if re.fullmatch(r"[+-]?\d+[:h]\d{1,2}", text):
        sign = -1 if text.startswith("-") else 1
        clean = text[1:] if text[:1] in "+-" else text
        h, m = re.split(r"[:h]", clean)
        h, m = int(h), int(m)
        if m >= 60:
            raise ValueError("Minuten müssen kleiner als 60 sein.")
        return sign * (h + m / 60)

    # direkt 145,5 oder -1
    direct = text.replace(",", ".")
    if re.fullmatch(r"[+-]?\d+(\.\d+)?", direct):
        return float(direct)

    # 130+15V oder 80+18+8V
    nums = re.findall(r"[+-]?\d+(?:[\.,]\d+)?", text)
    if nums and ("+" in text or any(ch.isalpha() for ch in text)):
        return round(sum(float(n.replace(",", ".")) for n in nums), 2)

    raise ValueError("Ungültiges Format. Beispiele: 145,5 | 145:30 | 130+15 | +0,5")




def slugify(text: str) -> str:
    repl = {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss", "Ä": "ae", "Ö": "oe", "Ü": "ue"}
    for a, b in repl.items():
        text = text.replace(a, b)
    s = re.sub(r"[^a-zA-Z0-9._-]+", ".", (text or "").strip().lower())
    s = re.sub(r"\.+", ".", s).strip(".")
    return s or "fahrer"


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json_file(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_cloud_settings() -> Dict[str, str]:
    data = load_json_file(CLOUD_SETTINGS_FILE, {})
    return {
        "base_url": str(data.get("base_url") or DEFAULT_PORTAL_BASE_URL).strip().rstrip("/"),
        "admin_token": str(data.get("admin_token") or DEFAULT_ADMIN_API_TOKEN).strip(),
    }


def save_cloud_settings(base_url: str, admin_token: str) -> None:
    save_json_file(CLOUD_SETTINGS_FILE, {
        "base_url": (base_url or "").strip().rstrip("/"),
        "admin_token": (admin_token or "").strip(),
    })


def load_cloud_users() -> Dict[str, Dict[str, str]]:
    return load_json_file(CLOUD_USERS_FILE, {})


def save_cloud_users(data: Dict[str, Dict[str, str]]) -> None:
    save_json_file(CLOUD_USERS_FILE, data)

def load_db() -> Dict[str, Any]:
    ensure_dirs()
    if not DB_FILE.exists():
        db = {"drivers": [], "records": {}, "next_driver_id": 1}
        save_db(db)
        return db
    with DB_FILE.open("r", encoding="utf-8") as f:
        db = json.load(f)
    db.setdefault("drivers", [])
    db.setdefault("records", {})
    db.setdefault("next_driver_id", 1)
    changed = False
    for year_data in db.get("records", {}).values():
        for month_data in year_data.values():
            for row in month_data:
                if "v_hours" not in row:
                    row["v_hours"] = 0.0
                    changed = True
                if "adjustment_hours" not in row:
                    row["adjustment_hours"] = 0.0
                    changed = True
                if "comment" not in row:
                    row["comment"] = ""
                    changed = True
                new_diff = round((float(row.get("worked_hours", 0.0)) + abs(float(row.get("v_hours", 0.0))) + float(row.get("adjustment_hours", 0.0))) - float(row.get("payroll_hours", 0.0)), 2)
                if row.get("difference") != new_diff:
                    row["difference"] = new_diff
                    changed = True
    if changed:
        for driver in db.get("drivers", []):
            recalc_driver(db, driver["id"])
        save_db(db)
    return db


def save_db(db: Dict[str, Any]) -> None:
    ensure_dirs()
    with DB_FILE.open("w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)


def get_driver(db: Dict[str, Any], driver_id: int) -> Optional[Dict[str, Any]]:
    for d in db["drivers"]:
        if d["id"] == driver_id:
            return d
    return None


def get_driver_records(db: Dict[str, Any], driver_id: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for year_data in db.get("records", {}).values():
        for month_data in year_data.values():
            for row in month_data:
                if row["driver_id"] == driver_id:
                    rows.append(row)
    rows.sort(key=lambda r: (r["year"], r["month"], r.get("created_index", 0)))
    return rows


def latest_balance(db: Dict[str, Any], driver_id: int) -> float:
    driver = get_driver(db, driver_id)
    if not driver:
        return 0.0
    rows = get_driver_records(db, driver_id)
    if not rows:
        return float(driver.get("starting_balance", 0.0) or 0.0)
    return float(rows[-1]["new_balance"])


def recalc_driver(db: Dict[str, Any], driver_id: int) -> None:
    driver = get_driver(db, driver_id)
    if not driver:
        return
    balance = float(driver.get("starting_balance", 0.0) or 0.0)
    rows = get_driver_records(db, driver_id)
    for row in rows:
        row["previous_balance"] = round(balance, 2)
        row["new_balance"] = round(balance + float(row["difference"]), 2)
        balance = row["new_balance"]


def upsert_month_record(
    db: Dict[str, Any],
    driver_id: int,
    year: int,
    month: int,
    worked_hours: float,
    payroll_hours: float,
    v_hours: float = 0.0,
    adjustment_hours: float = 0.0,
    comment: str = "",
) -> None:
    driver = get_driver(db, driver_id)
    if not driver:
        raise ValueError("Fahrer nicht gefunden.")

    year_key = str(year)
    month_key = str(month)
    db.setdefault("records", {}).setdefault(year_key, {}).setdefault(month_key, [])
    rows = db["records"][year_key][month_key]

    existing = next((r for r in rows if r["driver_id"] == driver_id), None)
    if existing:
        existing["worked_hours"] = round(worked_hours, 2)
        existing["payroll_hours"] = round(payroll_hours, 2)
        existing["v_hours"] = round(abs(v_hours), 2)
        existing["adjustment_hours"] = round(adjustment_hours, 2)
        existing["comment"] = (comment or "").strip()
        existing["difference"] = round((worked_hours + abs(v_hours) + adjustment_hours) - payroll_hours, 2)
    else:
        rows.append(
            {
                "driver_id": driver_id,
                "driver_name": driver["name"],
                "year": year,
                "month": month,
                "worked_hours": round(worked_hours, 2),
                "payroll_hours": round(payroll_hours, 2),
                "v_hours": round(abs(v_hours), 2),
                "adjustment_hours": round(adjustment_hours, 2),
                "comment": (comment or "").strip(),
                "difference": round((worked_hours + abs(v_hours) + adjustment_hours) - payroll_hours, 2),
                "previous_balance": 0.0,
                "new_balance": 0.0,
                "created_index": len(rows) + 1,
            }
        )

    recalc_driver(db, driver_id)
    save_db(db)


def delete_month_record(db: Dict[str, Any], driver_id: int, year: int, month: int) -> None:
    rows = db.get("records", {}).get(str(year), {}).get(str(month), [])
    before = len(rows)
    rows[:] = [r for r in rows if r["driver_id"] != driver_id]
    if len(rows) != before:
        recalc_driver(db, driver_id)
        save_db(db)


def delete_driver(db: Dict[str, Any], driver_id: int) -> None:
    db["drivers"] = [d for d in db["drivers"] if d["id"] != driver_id]
    for year_data in db.get("records", {}).values():
        for month_rows in year_data.values():
            month_rows[:] = [r for r in month_rows if r["driver_id"] != driver_id]
    save_db(db)


def find_driver_ids(db: Dict[str, Any], query: str) -> List[int]:
    q = normalize(query)
    drivers = sorted(db["drivers"], key=lambda d: normalize(d["name"]))
    if not q:
        return [d["id"] for d in drivers]
    exact = [d["id"] for d in drivers if normalize(d["name"]) == q]
    if exact:
        return exact
    contains = [d["id"] for d in drivers if q in normalize(d["name"])]
    return contains


def month_rows(db: Dict[str, Any], year: int, month: int) -> List[Dict[str, Any]]:
    rows = db.get("records", {}).get(str(year), {}).get(str(month), [])
    for row in rows:
        row.setdefault("v_hours", 0.0)
        row.setdefault("adjustment_hours", 0.0)
        row.setdefault("comment", "")
        row["difference"] = round((float(row.get("worked_hours", 0.0)) + abs(float(row.get("v_hours", 0.0))) + float(row.get("adjustment_hours", 0.0))) - float(row.get("payroll_hours", 0.0)), 2)
    return sorted(rows, key=lambda r: normalize(r["driver_name"]))


def all_year_rows(db: Dict[str, Any], year: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for m in range(1, 13):
        out.extend(month_rows(db, year, m))
    out.sort(key=lambda r: (r["month"], normalize(r["driver_name"])))
    return out


def safe_replace_file(path: Path) -> None:
    if path.exists():
        path.unlink()


# ---------------- PDF ----------------

def _pdf_table(data: List[List[str]], widths: List[float]) -> Table:
    styles = getSampleStyleSheet()
    cell = ParagraphStyle(
        "Cell",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=9,
        leading=11,
        textColor=colors.HexColor("#111827"),
    )
    header = ParagraphStyle(
        "HeaderCell",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=10,
        leading=12,
        alignment=1,
        textColor=colors.black,
    )
    wrapped = []
    for r, row in enumerate(data):
        style = header if r == 0 else cell
        wrapped.append([Paragraph(str(c).replace("\n", "<br/>"), style) for c in row])

    table = Table(wrapped, colWidths=widths, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dfddd8")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("BACKGROUND", (0, 1), (-1, -1), colors.white),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f9fafb")]),
                ("BOX", (0, 0), (-1, -1), 0.9, colors.HexColor("#8f8b84")),
                ("INNERGRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#b8b4ad")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ALIGN", (0, 0), (0, -1), "LEFT"),
                ("ALIGN", (1, 0), (-1, -1), "CENTER"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 9),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
            ]
        )
    )
    return table


def create_pdf_report(pdf_path: Path, title: str, subtitle: str, headers: List[str], rows: List[List[str]]) -> None:
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    safe_replace_file(pdf_path)

    page_width, _ = landscape(A4)
    left_margin = 12 * mm
    right_margin = 12 * mm
    usable_width = page_width - left_margin - right_margin

    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=landscape(A4),
        leftMargin=left_margin,
        rightMargin=right_margin,
        topMargin=12 * mm,
        bottomMargin=12 * mm,
        title=title,
        author=APP_NAME,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "TitleCustom",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        textColor=colors.HexColor("#123e7c"),
        spaceAfter=3,
    )
    subtitle_style = ParagraphStyle(
        "SubtitleCustom",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10,
        leading=13,
        textColor=colors.HexColor("#374151"),
        spaceAfter=7,
    )
    info_style = ParagraphStyle(
        "Info",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=8.5,
        leading=10,
        textColor=colors.HexColor("#6b7280"),
        spaceAfter=8,
    )

    if len(headers) == 10:
        col_widths = [
            0.10 * usable_width,
            0.13 * usable_width,
            0.10 * usable_width,
            0.06 * usable_width,
            0.12 * usable_width,
            0.18 * usable_width,
            0.09 * usable_width,
            0.08 * usable_width,
            0.07 * usable_width,
            0.07 * usable_width,
        ]
    elif len(headers) == 4:
        col_widths = [0.40 * usable_width, 0.20 * usable_width, 0.20 * usable_width, 0.20 * usable_width]
    else:
        col_widths = [usable_width / max(len(headers), 1)] * len(headers)

    story = [
        Paragraph(title, title_style),
        Paragraph(subtitle, subtitle_style),
        Paragraph("Erstellt mit dem Plus/Minus-Stunden-Rechner", info_style),
        Spacer(1, 5 * mm),
        _pdf_table([headers] + rows, col_widths),
    ]
    doc.build(story)




def create_driver_portal_pdf(pdf_path: Path, driver_name: str, year: int, month: int, row: Dict[str, Any]) -> None:
    create_pdf_report(
        pdf_path,
        f"Monatsübersicht {driver_name} – {MONATE[month]} {year}",
        "Nur die Daten dieses Fahrers",
        [
            "Fahrer",
            "Tatsächliche Arbeitsstunden",
            "V",
            "Sonstige Abzüge/Zuschüsse",
            "Kommentar",
            "Abrechnung",
            "Differenz",
            "Aktuelles Saldo",
            "Neues Saldo",
        ],
        [[
            driver_name,
            fmt_hours(float(row.get("worked_hours", 0.0))),
            fmt_hours(abs(float(row.get("v_hours", 0.0)))),
            fmt_signed(float(row.get("adjustment_hours", 0.0))),
            row.get("comment", "") or "-",
            fmt_hours(float(row.get("payroll_hours", 0.0))),
            fmt_signed(float(row.get("difference", 0.0))),
            fmt_signed(float(row.get("previous_balance", 0.0))),
            fmt_signed(float(row.get("new_balance", 0.0))),
        ]],
    )


def generate_driver_portal_pdfs(db: Dict[str, Any]) -> List[Dict[str, Any]]:
    CLOUD_PDF_DIR.mkdir(parents=True, exist_ok=True)
    generated: List[Dict[str, Any]] = []
    drivers = {int(d["id"]): d for d in db.get("drivers", [])}
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
                pdf_dir = CLOUD_PDF_DIR / slugify(driver_name) / str(year)
                pdf_path = pdf_dir / f"{month:02d}_{MONATE[month]}_{year}.pdf"
                pdf_dir.mkdir(parents=True, exist_ok=True)
                create_driver_portal_pdf(pdf_path, driver_name, year, month, row)
                generated.append({
                    "external_driver_id": driver_id,
                    "driver_name": driver_name,
                    "year": year,
                    "month": month,
                    "path": pdf_path,
                })
    return generated

def export_pdfs(db: Dict[str, Any]) -> None:
    ensure_dirs()
    headers = [
        "Monat",
        "Fahrer",
        "Tatsächliche Arbeitsstunden",
        "V",
        "Sonstige Abzüge/Zuschüsse",
        "Kommentar",
        "Abrechnung",
        "Differenz",
        "Aktuelles Saldo",
        "Neues Saldo",
    ]

    years = set(int(y) for y in db.get("records", {}).keys())
    if not years:
        # trotzdem Gesamtübersicht erzeugen
        create_pdf_report(
            EXPORT_DIR / "Gesamtstand_alle_Fahrer.pdf",
            "Gesamtstand aller Fahrer",
            "Aktuelle Salden und Anzahl der vorhandenen Einträge",
            ["Fahrer", "Anfangssaldo", "Aktueller Saldo", "Anzahl Einträge"],
            [["Keine Fahrer vorhanden", "-", "-", "-"]],
        )
        return

    for year in sorted(years):
        year_dir = EXPORT_DIR / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        yearly_pdf_rows: List[List[str]] = []

        for month in range(1, 13):
            rows = month_rows(db, year, month)
            month_name = MONATE[month]
            pdf_rows: List[List[str]] = []
            for row in rows:
                pdf_rows.append(
                    [
                        f"{month_name} {year}",
                        row["driver_name"],
                        fmt_hours(float(row["worked_hours"])),
                        fmt_hours(abs(float(row.get("v_hours", 0.0)))),
                        fmt_signed(float(row.get("adjustment_hours", 0.0))),
                        row.get("comment", "") or "-",
                        fmt_hours(float(row["payroll_hours"])),
                        fmt_signed(float(row["difference"])),
                        fmt_signed(float(row["previous_balance"])),
                        fmt_signed(float(row["new_balance"])),
                    ]
                )
            if not pdf_rows:
                pdf_rows = [[f"{month_name} {year}", "Keine Einträge", "-", "-", "-", "-", "-", "-", "-", "-"]]
            yearly_pdf_rows.extend([r for r in pdf_rows if r[1] != "Keine Einträge"])
            create_pdf_report(
                year_dir / f"{month:02d}_{month_name}_{year}.pdf",
                f"Monatsübersicht {month_name} {year}",
                "Plus/Minus-Stunden pro Fahrer",
                headers,
                pdf_rows,
            )

        if not yearly_pdf_rows:
            yearly_pdf_rows = [[str(year), "Keine Einträge", "-", "-", "-", "-", "-", "-", "-", "-"]]
        create_pdf_report(
            year_dir / f"Jahresübersicht_{year}.pdf",
            f"Jahresübersicht {year}",
            "Alle Monatsdaten des ausgewählten Jahres",
            headers,
            yearly_pdf_rows,
        )

    summary_rows: List[List[str]] = []
    for driver in sorted(db["drivers"], key=lambda d: normalize(d["name"])):
        summary_rows.append(
            [
                driver["name"],
                fmt_signed(float(driver.get("starting_balance", 0.0) or 0.0)),
                fmt_signed(latest_balance(db, driver["id"])),
                str(len(get_driver_records(db, driver["id"]))),
            ]
        )
    if not summary_rows:
        summary_rows = [["Keine Fahrer vorhanden", "-", "-", "-"]]
    create_pdf_report(
        EXPORT_DIR / "Gesamtstand_alle_Fahrer.pdf",
        "Gesamtstand aller Fahrer",
        "Aktuelle Salden und Anzahl der vorhandenen Einträge",
        ["Fahrer", "Anfangssaldo", "Aktueller Saldo", "Anzahl Einträge"],
        summary_rows,
    )


# ---------------- GUI ----------------
class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1380x840")
        self.minsize(1180, 720)
        self.after(50, self._start_maximized)
        self.configure(bg="#ececec")

        self.db = load_db()
        self.selected_driver_id: Optional[int] = None

        self.style = ttk.Style(self)
        try:
            self.style.theme_use("clam")
        except Exception:
            pass
        self._configure_style()
        self._build_ui()
        self.refresh_everything()

    def _start_maximized(self) -> None:
        try:
            self.state("zoomed")
        except Exception:
            try:
                self.attributes("-zoomed", True)
            except Exception:
                pass

    def _configure_style(self) -> None:
        base_bg = "#ececec"
        card_bg = "#f6f6f6"
        header_bg = "#dfddd8"
        border = "#a6a39c"

        self.style.configure("TFrame", background=base_bg)
        self.style.configure("Card.TFrame", background=card_bg, relief="flat")
        self.style.configure("TLabel", background=base_bg, foreground="#0f172a", font=("Segoe UI", 10))
        self.style.configure("Title.TLabel", background=base_bg, font=("Segoe UI", 17, "bold"), foreground="#123e7c")
        self.style.configure("Sub.TLabel", background=base_bg, font=("Segoe UI", 10), foreground="#475569")
        self.style.configure("Header.TLabel", background=card_bg, foreground="#0f172a", font=("Segoe UI", 12, "bold"))
        self.style.configure("TButton", font=("Segoe UI", 10), padding=8)
        self.style.configure("Primary.TButton", font=("Segoe UI", 10, "bold"), padding=9)
        self.style.configure(
            "Treeview",
            background="#ffffff",
            fieldbackground="#ffffff",
            foreground="#111111",
            bordercolor=border,
            lightcolor=border,
            darkcolor=border,
            rowheight=32,
            font=("Segoe UI", 10),
            relief="solid",
            borderwidth=1,
        )
        self.style.configure(
            "Treeview.Heading",
            background=header_bg,
            foreground="#000000",
            bordercolor=border,
            lightcolor=border,
            darkcolor=border,
            relief="raised",
            font=("Segoe UI", 11, "bold"),
            padding=(8, 10),
        )
        self.style.map(
            "Treeview",
            background=[("selected", "#c9d7e8")],
            foreground=[("selected", "#000000")],
        )
        self.style.map(
            "Treeview.Heading",
            background=[("active", header_bg)],
            relief=[("active", "raised")],
        )

    def _build_ui(self) -> None:
        outer = ttk.Frame(self, padding=16)
        outer.pack(fill="both", expand=True)

        top = ttk.Frame(outer)
        top.pack(fill="x", pady=(0, 10))
        ttk.Label(top, text=APP_NAME, style="Title.TLabel").pack(anchor="w")

        content = ttk.Frame(outer)
        content.pack(fill="both", expand=True)
        content.columnconfigure(0, weight=0)
        content.columnconfigure(1, weight=1)
        content.rowconfigure(0, weight=1)

        self.left = ttk.Frame(content, style="Card.TFrame", padding=10, width=320)
        self.left.grid(row=0, column=0, sticky="nsw", padx=(0, 10))
        self.left.grid_propagate(False)

        self.right = ttk.Frame(content, style="Card.TFrame", padding=10)
        self.right.grid(row=0, column=1, sticky="nsew")
        self.right.columnconfigure(0, weight=1)
        self.right.rowconfigure(2, weight=1)

        self._build_left_panel()
        self._build_right_panel()

    def _build_left_panel(self) -> None:
        ttk.Label(self.left, text="Monat auswählen", style="Header.TLabel").pack(anchor="w")
        select_box = ttk.Frame(self.left, style="Card.TFrame")
        select_box.pack(fill="x", pady=(8, 14))

        ttk.Label(select_box, text="Jahr").grid(row=0, column=0, sticky="w")
        self.year_var = tk.StringVar()
        self.year_combo = ttk.Combobox(select_box, textvariable=self.year_var, width=10)
        self.year_combo.grid(row=1, column=0, sticky="ew", padx=(0, 8), pady=(4, 0))
        self.year_combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_month_table())
        self.year_combo.bind("<Return>", lambda e: self.add_or_select_year())
        self.year_combo.bind("<FocusOut>", lambda e: self.add_or_select_year())

        ttk.Label(select_box, text="Monat").grid(row=0, column=1, sticky="w")
        self.month_var = tk.StringVar()
        self.month_combo = ttk.Combobox(select_box, textvariable=self.month_var, width=14, state="readonly")
        self.month_combo["values"] = [f"{i:02d} - {MONATE[i]}" for i in range(1, 13)]
        self.month_combo.grid(row=1, column=1, sticky="ew", pady=(4, 0))
        self.month_combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_month_table())

        ttk.Button(select_box, text="Neues Jahr hinzufügen", command=self.add_year_dialog).grid(row=2, column=0, columnspan=2, sticky="ew", pady=(8, 0))

        select_box.columnconfigure(0, weight=1)
        select_box.columnconfigure(1, weight=1)

        ttk.Label(self.left, text="Fahrer suchen", style="Header.TLabel").pack(anchor="w")
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(self.left, textvariable=self.search_var)
        search_entry.pack(fill="x", pady=(8, 8))
        search_entry.bind("<KeyRelease>", lambda e: self.refresh_driver_list())

        driver_list_wrap = ttk.Frame(self.left, style="Card.TFrame")
        driver_list_wrap.pack(fill="both", expand=True)
        xscroll_driver = ttk.Scrollbar(driver_list_wrap, orient="horizontal")
        self.driver_list = tk.Listbox(driver_list_wrap, height=18, font=("Segoe UI", 10), activestyle="none", exportselection=False, xscrollcommand=xscroll_driver.set)
        self.driver_list.pack(fill="both", expand=True)
        xscroll_driver.configure(command=self.driver_list.xview)
        xscroll_driver.pack(fill="x")
        self.driver_list.bind("<<ListboxSelect>>", lambda e: self.on_driver_selected())
        self.driver_list.bind("<Double-Button-1>", lambda e: self.load_selected_driver_into_form())

        btns = ttk.Frame(self.left, style="Card.TFrame")
        btns.pack(fill="x", pady=(10, 0))
        ttk.Button(btns, text="Fahrer hinzufügen", command=self.add_driver_dialog).pack(fill="x", pady=3)
        ttk.Button(btns, text="Fahrer umbenennen", command=self.rename_driver_dialog).pack(fill="x", pady=3)
        ttk.Button(btns, text="Fahrer löschen", command=self.delete_driver_dialog).pack(fill="x", pady=3)
        ttk.Button(btns, text="Cloud-Portal einrichten", command=self.configure_cloud_portal).pack(fill="x", pady=(10, 3))
        ttk.Button(btns, text="PDFs jetzt neu erzeugen", style="Primary.TButton", command=self.generate_pdfs).pack(fill="x", pady=3)
        ttk.Button(btns, text="Cloud jetzt synchronisieren", command=self.sync_cloud_now).pack(fill="x", pady=3)

    def _build_right_panel(self) -> None:
        ttk.Label(self.right, text="Monatseintrag", style="Header.TLabel").grid(row=0, column=0, sticky="w")

        form = ttk.Frame(self.right, style="Card.TFrame")
        form.grid(row=1, column=0, sticky="ew", pady=(10, 12))
        for c in range(8):
            form.columnconfigure(c, weight=1)

        ttk.Label(form, text="Fahrer").grid(row=0, column=0, sticky="w")
        self.form_driver_var = tk.StringVar()
        self.driver_combo = ttk.Combobox(form, textvariable=self.form_driver_var)
        self.driver_combo.grid(row=1, column=0, columnspan=2, sticky="ew", padx=(0, 8), pady=(4, 10))
        self.driver_combo.bind("<KeyRelease>", lambda e: self.update_driver_combo_values())

        ttk.Label(form, text="Tatsächliche Stunden").grid(row=0, column=2, sticky="w")
        self.worked_var = tk.StringVar()
        ttk.Entry(form, textvariable=self.worked_var).grid(row=1, column=2, sticky="ew", padx=(0, 8), pady=(4, 10))

        ttk.Label(form, text="V").grid(row=0, column=3, sticky="w")
        self.v_var = tk.StringVar()
        ttk.Entry(form, textvariable=self.v_var).grid(row=1, column=3, sticky="ew", padx=(0, 8), pady=(4, 10))

        ttk.Label(form, text="Sonstige Abzüge/Zuschüsse").grid(row=0, column=4, sticky="w")
        self.adjustment_var = tk.StringVar()
        ttk.Entry(form, textvariable=self.adjustment_var).grid(row=1, column=4, sticky="ew", padx=(0, 8), pady=(4, 10))

        ttk.Label(form, text="Kommentar").grid(row=2, column=4, sticky="w")
        self.comment_var = tk.StringVar()
        ttk.Entry(form, textvariable=self.comment_var).grid(row=3, column=4, columnspan=2, sticky="ew", padx=(0, 8), pady=(4, 10))

        ttk.Label(form, text="Abrechnung").grid(row=0, column=5, sticky="w")
        self.payroll_var = tk.StringVar()
        ttk.Entry(form, textvariable=self.payroll_var).grid(row=1, column=5, sticky="ew", padx=(0, 8), pady=(4, 10))

        ttk.Label(form, text="Differenz").grid(row=0, column=6, sticky="w")
        self.diff_var = tk.StringVar(value="-")
        ttk.Label(form, textvariable=self.diff_var, style="Header.TLabel").grid(row=1, column=6, sticky="w", pady=(4, 10))

        ttk.Label(form, text="Neuer Stand").grid(row=0, column=7, sticky="w")
        self.new_balance_var = tk.StringVar(value="-")
        ttk.Label(form, textvariable=self.new_balance_var, style="Header.TLabel").grid(row=1, column=7, sticky="w", pady=(4, 10))

        for var in (self.worked_var, self.v_var, self.adjustment_var, self.payroll_var):
            var.trace_add("write", lambda *args: self.preview_calculation())

        actions = ttk.Frame(form, style="Card.TFrame")
        actions.grid(row=4, column=0, columnspan=8, sticky="ew")
        ttk.Button(actions, text="Eintrag speichern / aktualisieren", style="Primary.TButton", command=self.save_entry).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="Ausgewählten Eintrag laden", command=self.load_selected_row_into_form).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="Ausgewählten Eintrag löschen", command=self.delete_selected_row).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="Formular leeren", command=self.clear_form).pack(side="left")

        table_card = ttk.Frame(self.right, style="Card.TFrame", padding=4)
        table_card.grid(row=2, column=0, sticky="nsew")
        table_card.columnconfigure(0, weight=1)
        table_card.rowconfigure(1, weight=1)

        self.month_title_var = tk.StringVar(value="Einträge für den ausgewählten Monat")
        ttk.Label(table_card, textvariable=self.month_title_var, style="Title.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 10))

        columns = ("fahrer", "stunden", "v", "sonstige", "kommentar", "abrechnung", "differenz", "alt", "neu")
        self.tree = ttk.Treeview(table_card, columns=columns, show="headings", selectmode="browse")
        headings = {
            "fahrer": "Fahrer",
            "stunden": "Stunden",
            "v": "V",
            "sonstige": "Sonstige Abzüge/Zuschüsse",
            "kommentar": "Kommentar",
            "abrechnung": "Abrechnung",
            "differenz": "Differenz",
            "alt": "Akt. Stand",
            "neu": "Neuer Stand",
        }
        widths = {"fahrer": 220, "stunden": 125, "v": 80, "sonstige": 180, "kommentar": 220, "abrechnung": 125, "differenz": 120, "alt": 120, "neu": 120}
        for col in columns:
            self.tree.heading(col, text=headings[col])
            self.tree.column(col, width=widths[col], anchor="center")
        self.tree.column("fahrer", anchor="w")
        self.tree.grid(row=1, column=0, sticky="nsew")
        self.tree.bind("<<TreeviewSelect>>", lambda e: self.on_tree_selected())
        self.tree.bind("<Double-Button-1>", lambda e: self.load_selected_row_into_form())

        yscroll = ttk.Scrollbar(table_card, orient="vertical", command=self.tree.yview)
        yscroll.grid(row=1, column=1, sticky="ns")
        xscroll = ttk.Scrollbar(table_card, orient="horizontal", command=self.tree.xview)
        xscroll.grid(row=2, column=0, sticky="ew")
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        self.status_var = tk.StringVar(value="Bereit")
        ttk.Label(self.right, textvariable=self.status_var, style="Sub.TLabel").grid(row=3, column=0, sticky="w", pady=(10, 0))

    # ---------- Helpers ----------
    def current_year(self) -> int:
        raw = self.year_var.get().strip()
        if not re.fullmatch(r"\d{4}", raw):
            raise ValueError("Bitte ein gültiges Jahr mit 4 Ziffern eingeben.")
        return int(raw)

    def current_month(self) -> int:
        return int(self.month_var.get().split(" - ")[0])

    def selected_driver_from_list(self) -> Optional[int]:
        sel = self.driver_list.curselection()
        if not sel:
            return None
        return self.driver_list_ids[sel[0]]

    def current_form_driver_id(self) -> Optional[int]:
        raw = self.form_driver_var.get().strip()
        if not raw:
            return None
        matches = find_driver_ids(self.db, raw)
        if len(matches) == 1:
            return matches[0]
        exact = [d["id"] for d in self.db["drivers"] if normalize(d["name"]) == normalize(raw)]
        if len(exact) == 1:
            return exact[0]
        return None

    def update_driver_combo_values(self) -> None:
        q = self.form_driver_var.get().strip()
        ids = find_driver_ids(self.db, q)
        self.driver_combo["values"] = [get_driver(self.db, i)["name"] for i in ids if get_driver(self.db, i)]

    def add_or_select_year(self) -> None:
        raw = self.year_var.get().strip()
        if not raw:
            return
        if not re.fullmatch(r"\d{4}", raw):
            return
        values = list(self.year_combo.cget("values"))
        if raw not in values:
            values.append(raw)
            values = sorted(values, key=int)
            self.year_combo["values"] = values
        self.year_var.set(raw)
        self.refresh_month_table()

    def add_year_dialog(self) -> None:
        raw = simpledialog.askstring(APP_NAME, "Neues Jahr eingeben, z. B. 2028:", parent=self)
        if raw is None:
            return
        raw = raw.strip()
        if not re.fullmatch(r"\d{4}", raw):
            messagebox.showerror(APP_NAME, "Bitte ein gültiges Jahr mit 4 Ziffern eingeben.", parent=self)
            return
        values = list(self.year_combo.cget("values"))
        if raw not in values:
            values.append(raw)
            values = sorted(values, key=int)
            self.year_combo["values"] = values
        self.year_var.set(raw)
        self.refresh_month_table()
        self.status_var.set(f"Jahr {raw} wurde hinzugefügt.")

    # ---------- Refresh ----------
    def refresh_everything(self) -> None:
        import datetime as _dt
        current = _dt.datetime.now().year
        years = sorted({int(y) for y in self.db.get("records", {}).keys()} | {current})
        if not years:
            years = [current]
        self.year_combo["values"] = [str(y) for y in years]
        if not self.year_var.get():
            self.year_var.set(str(years[0]))
        if not self.month_var.get():
            self.month_var.set(self.month_combo["values"][0])
        self.refresh_driver_list()
        self.update_driver_combo_values()
        self.refresh_month_table()
        self.preview_calculation()

    def refresh_driver_list(self) -> None:
        q = self.search_var.get().strip()
        ids = find_driver_ids(self.db, q)
        self.driver_list_ids = ids
        self.driver_list.delete(0, tk.END)
        for driver_id in ids:
            d = get_driver(self.db, driver_id)
            if not d:
                continue
            self.driver_list.insert(tk.END, f"{d['name']}    |    Aktuell: {fmt_signed(latest_balance(self.db, driver_id))}")

    def refresh_month_table(self) -> None:
        if not self.year_var.get() or not self.month_var.get():
            return
        year = self.current_year()
        month = self.current_month()
        self.month_title_var.set(f"Einträge für {MONATE[month]} {year}")

        for item in self.tree.get_children():
            self.tree.delete(item)

        rows = month_rows(self.db, year, month)
        for row in rows:
            self.tree.insert(
                "",
                tk.END,
                iid=str(row["driver_id"]),
                values=(
                    row["driver_name"],
                    fmt_hours(float(row["worked_hours"])),
                    fmt_hours(abs(float(row.get("v_hours", 0.0)))),
                    fmt_signed(float(row.get("adjustment_hours", 0.0))),
                    row.get("comment", "") or "-",
                    fmt_hours(float(row["payroll_hours"])),
                    fmt_signed(float(row["difference"])),
                    fmt_signed(float(row["previous_balance"])),
                    fmt_signed(float(row["new_balance"])),
                ),
            )
        self.status_var.set(f"{len(rows)} Eintrag/Einträge geladen für {MONATE[month]} {year}.")

    # ---------- Driver management ----------
    def add_driver_dialog(self) -> None:
        name = simpledialog.askstring(APP_NAME, "Name des neuen Fahrers:", parent=self)
        if not name:
            return
        name = name.strip()
        if not name:
            return

        if any(normalize(d["name"]) == normalize(name) for d in self.db["drivers"]):
            if not messagebox.askyesno(APP_NAME, "Diesen Namen gibt es schon. Trotzdem hinzufügen?", parent=self):
                return

        start_raw = simpledialog.askstring(APP_NAME, "Anfangssaldo eingeben, z. B. +0,5 oder -1:", parent=self)
        if start_raw is None:
            return
        try:
            start = parse_hours(start_raw)
        except ValueError as e:
            messagebox.showerror(APP_NAME, str(e), parent=self)
            return

        self.db["drivers"].append({"id": self.db["next_driver_id"], "name": name, "starting_balance": round(start, 2)})
        self.db["next_driver_id"] += 1
        save_db(self.db)
        export_pdfs(self.db)
        self.refresh_everything()
        self.status_var.set(f"Fahrer '{name}' wurde hinzugefügt.")

    def rename_driver_dialog(self) -> None:
        driver_id = self.selected_driver_from_list()
        if not driver_id:
            messagebox.showinfo(APP_NAME, "Bitte links zuerst einen Fahrer auswählen.", parent=self)
            return
        driver = get_driver(self.db, driver_id)
        if not driver:
            return
        new_name = simpledialog.askstring(APP_NAME, "Neuer Name:", initialvalue=driver["name"], parent=self)
        if not new_name:
            return
        driver["name"] = new_name.strip()
        for year_data in self.db.get("records", {}).values():
            for month_rows_ in year_data.values():
                for row in month_rows_:
                    if row["driver_id"] == driver_id:
                        row["driver_name"] = driver["name"]
        save_db(self.db)
        export_pdfs(self.db)
        self.refresh_everything()
        self.status_var.set("Fahrer wurde umbenannt.")

    def delete_driver_dialog(self) -> None:
        driver_id = self.selected_driver_from_list()
        if not driver_id:
            messagebox.showinfo(APP_NAME, "Bitte links zuerst einen Fahrer auswählen.", parent=self)
            return
        driver = get_driver(self.db, driver_id)
        if not driver:
            return
        if not messagebox.askyesno(APP_NAME, f"Fahrer '{driver['name']}' wirklich löschen? Alle Monatsdaten dieses Fahrers werden entfernt.", parent=self):
            return
        delete_driver(self.db, driver_id)
        export_pdfs(self.db)
        self.selected_driver_id = None
        self.clear_form()
        self.refresh_everything()
        self.status_var.set(f"Fahrer '{driver['name']}' wurde gelöscht.")

    # ---------- Selection ----------
    def on_driver_selected(self) -> None:
        self.selected_driver_id = self.selected_driver_from_list()

    def load_selected_driver_into_form(self) -> None:
        driver_id = self.selected_driver_from_list()
        if not driver_id:
            return
        driver = get_driver(self.db, driver_id)
        if driver:
            self.form_driver_var.set(driver["name"])
            self.preview_calculation()

    def on_tree_selected(self) -> None:
        # reine Auswahl im Table; nichts weiter nötig
        pass

    def load_selected_row_into_form(self) -> None:
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo(APP_NAME, "Bitte unten zuerst einen Eintrag auswählen.", parent=self)
            return
        driver_id = int(sel[0])
        row = next((r for r in month_rows(self.db, self.current_year(), self.current_month()) if r["driver_id"] == driver_id), None)
        if not row:
            return
        self.form_driver_var.set(row["driver_name"])
        self.worked_var.set(str(row["worked_hours"]).replace(".", ","))
        self.v_var.set(str(abs(float(row.get("v_hours", 0.0)))).replace(".", ","))
        self.adjustment_var.set(str(row.get("adjustment_hours", 0.0)).replace(".", ","))
        self.comment_var.set(row.get("comment", ""))
        self.payroll_var.set(str(row["payroll_hours"]).replace(".", ","))
        self.preview_calculation()

    # ---------- Form ----------
    def clear_form(self) -> None:
        self.form_driver_var.set("")
        self.worked_var.set("")
        self.v_var.set("")
        self.adjustment_var.set("")
        self.payroll_var.set("")
        self.comment_var.set("")
        self.diff_var.set("-")
        self.new_balance_var.set("-")

    def preview_calculation(self) -> None:
        try:
            worked = parse_hours(self.worked_var.get()) if self.worked_var.get().strip() else None
            v_hours = abs(parse_hours(self.v_var.get())) if self.v_var.get().strip() else 0.0
            adjustment = parse_hours(self.adjustment_var.get()) if self.adjustment_var.get().strip() else 0.0
            payroll = parse_hours(self.payroll_var.get()) if self.payroll_var.get().strip() else None
        except ValueError:
            self.diff_var.set("-")
            self.new_balance_var.set("-")
            return
        if worked is None or payroll is None:
            self.diff_var.set("-")
            self.new_balance_var.set("-")
            return
        diff = round((worked + v_hours + adjustment) - payroll, 2)
        self.diff_var.set(fmt_signed(diff))

        driver_id = self.current_form_driver_id()
        if driver_id:
            # falls aktueller Monat schon existiert, den alten diff für Vorschau ausklammern
            driver = get_driver(self.db, driver_id)
            base = float(driver.get("starting_balance", 0.0) or 0.0) if driver else 0.0
            for row in get_driver_records(self.db, driver_id):
                if (row["year"], row["month"]) < (self.current_year(), self.current_month()):
                    base = float(row["new_balance"])
            self.new_balance_var.set(fmt_signed(base + diff))
        else:
            self.new_balance_var.set("-")

    def save_entry(self) -> None:
        raw_name = self.form_driver_var.get().strip()
        if not raw_name:
            messagebox.showinfo(APP_NAME, "Bitte einen Fahrer auswählen oder eintippen.", parent=self)
            return

        matches = find_driver_ids(self.db, raw_name)
        if not matches:
            messagebox.showerror(APP_NAME, "Fahrer nicht gefunden.", parent=self)
            return
        if len(matches) > 1 and normalize(raw_name) != normalize(get_driver(self.db, matches[0])["name"]):
            names = "\n".join(f"- {get_driver(self.db, i)['name']}" for i in matches)
            messagebox.showinfo(APP_NAME, f"Mehrere Fahrer passen auf die Eingabe:\n\n{names}\n\nBitte Namen genauer eingeben oder links auswählen.", parent=self)
            return
        driver_id = matches[0]

        try:
            worked = parse_hours(self.worked_var.get())
            v_hours = abs(parse_hours(self.v_var.get())) if self.v_var.get().strip() else 0.0
            adjustment = parse_hours(self.adjustment_var.get()) if self.adjustment_var.get().strip() else 0.0
            payroll = parse_hours(self.payroll_var.get())
        except ValueError as e:
            messagebox.showerror(APP_NAME, str(e), parent=self)
            return

        upsert_month_record(self.db, driver_id, self.current_year(), self.current_month(), worked, payroll, v_hours, adjustment, self.comment_var.get())
        export_pdfs(self.db)
        self.sync_cloud_portal(interactive=True)
        self.refresh_everything()
        self.form_driver_var.set(get_driver(self.db, driver_id)["name"])
        self.worked_var.set(str(round(worked, 2)).replace(".", ","))
        self.v_var.set(str(round(abs(v_hours), 2)).replace(".", ","))
        self.adjustment_var.set(str(round(adjustment, 2)).replace(".", ","))
        self.payroll_var.set(str(round(payroll, 2)).replace(".", ","))
        self.comment_var.set(self.comment_var.get().strip())
        self.preview_calculation()
        self.status_var.set("Eintrag gespeichert und PDFs aktualisiert.")

    def delete_selected_row(self) -> None:
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo(APP_NAME, "Bitte unten zuerst einen Eintrag auswählen.", parent=self)
            return
        driver_id = int(sel[0])
        driver = get_driver(self.db, driver_id)
        if not driver:
            return
        if not messagebox.askyesno(APP_NAME, f"Eintrag von '{driver['name']}' für {MONATE[self.current_month()]} {self.current_year()} löschen?", parent=self):
            return
        delete_month_record(self.db, driver_id, self.current_year(), self.current_month())
        export_pdfs(self.db)
        self.sync_cloud_portal(interactive=True)
        self.refresh_everything()
        self.status_var.set("Eintrag gelöscht, PDFs aktualisiert und Cloud synchronisiert.")


    def configure_cloud_portal(self) -> None:
        settings = load_cloud_settings()
        base_url = simpledialog.askstring(APP_NAME, "Cloud-Portal URL eingeben:", initialvalue=settings.get("base_url", ""), parent=self)
        if base_url is None:
            return
        admin_token = simpledialog.askstring(APP_NAME, "Admin-Token eingeben:", initialvalue=settings.get("admin_token", ""), parent=self)
        if admin_token is None:
            return
        save_cloud_settings(base_url, admin_token)
        self.status_var.set("Cloud-Portal-Einstellungen gespeichert.")
        messagebox.showinfo(APP_NAME, "Cloud-Portal-Einstellungen wurden gespeichert.", parent=self)

    def ensure_cloud_driver_credentials(self, driver: Dict[str, Any], users: Dict[str, Dict[str, str]]) -> Optional[Dict[str, str]]:
        key = str(driver["id"])
        existing = users.get(key, {})
        username = str(existing.get("username") or slugify(driver["name"]))
        password = str(existing.get("password") or "")
        if username and password:
            return {"external_driver_id": int(driver["id"]), "name": driver["name"], "username": username, "password": password}

        username_in = simpledialog.askstring(APP_NAME, f"Benutzername für Fahrer '{driver['name']}' festlegen:", initialvalue=username, parent=self)
        if username_in is None:
            return None
        password_in = simpledialog.askstring(APP_NAME, f"Passwort für Fahrer '{driver['name']}' festlegen:", initialvalue=password, parent=self)
        if password_in is None or not password_in.strip():
            return None
        users[key] = {"username": username_in.strip(), "password": password_in.strip(), "name": driver["name"]}
        save_cloud_users(users)
        return {"external_driver_id": int(driver["id"]), "name": driver["name"], "username": username_in.strip(), "password": password_in.strip()}

    def cloud_month_payloads(self) -> List[Dict[str, Any]]:
        payloads: List[Dict[str, Any]] = []
        for year_str, months in self.db.get("records", {}).items():
            year = int(year_str)
            for month_str, rows in months.items():
                month = int(month_str)
                for row in rows:
                    payloads.append({
                        "external_driver_id": int(row["driver_id"]),
                        "year": year,
                        "month": month,
                        "worked_hours": round(float(row.get("worked_hours", 0.0)), 2),
                        "v_hours": round(abs(float(row.get("v_hours", 0.0))), 2),
                        "adjustment_hours": round(float(row.get("adjustment_hours", 0.0)), 2),
                        "comment": row.get("comment", "") or "",
                        "payroll_hours": round(float(row.get("payroll_hours", 0.0)), 2),
                        "difference": round(float(row.get("difference", 0.0)), 2),
                        "previous_balance": round(float(row.get("previous_balance", 0.0)), 2),
                        "new_balance": round(float(row.get("new_balance", 0.0)), 2),
                    })
        payloads.sort(key=lambda x: (x["year"], x["month"], x["external_driver_id"]))
        return payloads

    def sync_cloud_portal(self, interactive: bool = True) -> bool:
        settings = load_cloud_settings()
        base_url = settings.get("base_url", "").strip().rstrip("/")
        admin_token = settings.get("admin_token", "").strip()
        if not base_url or not admin_token:
            if interactive:
                messagebox.showerror(APP_NAME, "Bitte zuerst über 'Cloud-Portal einrichten' die Portal-URL und den Admin-Token speichern.", parent=self)
            return False

        users = load_cloud_users()
        payload_users: List[Dict[str, str]] = []
        for driver in sorted(self.db.get("drivers", []), key=lambda d: normalize(d.get("name", ""))):
            key = str(driver["id"])
            existing = users.get(key, {})
            if interactive:
                creds = self.ensure_cloud_driver_credentials(driver, users)
            else:
                username = str(existing.get("username") or "").strip()
                password = str(existing.get("password") or "").strip()
                if username and password:
                    creds = {
                        "external_driver_id": int(driver["id"]),
                        "name": driver["name"],
                        "username": username,
                        "password": password,
                    }
                else:
                    creds = None
            if not creds:
                if not interactive:
                    return False
                messagebox.showwarning(APP_NAME, f"Cloud-Synchronisierung abgebrochen. Für '{driver['name']}' fehlt ein Benutzername oder Passwort.", parent=self)
                return False
            payload_users.append(creds)

        month_payloads = self.cloud_month_payloads()
        files = generate_driver_portal_pdfs(self.db)
        session = requests.Session()
        session.headers.update({"X-Admin-Token": admin_token})

        try:
            for user in payload_users:
                r = session.post(base_url + "/api/admin/upsert-driver", json=user, timeout=60)
                r.raise_for_status()

            for payload in month_payloads:
                r = session.post(base_url + "/api/admin/upsert-month-data", json=payload, timeout=60)
                r.raise_for_status()

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
        except requests.RequestException as e:
            if interactive:
                messagebox.showerror(APP_NAME, f"Cloud-Synchronisierung fehlgeschlagen:\n\n{e}", parent=self)
            self.status_var.set("Cloud-Synchronisierung fehlgeschlagen.")
            return False

        self.status_var.set("Cloud-Portal wurde automatisch synchronisiert.")
        if interactive:
            messagebox.showinfo(APP_NAME, "Cloud-Portal wurde erfolgreich synchronisiert.", parent=self)
        return True

    def sync_cloud_now(self) -> None:
        self.sync_cloud_portal(interactive=True)

    def generate_pdfs(self) -> None:
        export_pdfs(self.db)
        ok = self.sync_cloud_portal(interactive=True)
        if ok:
            self.status_var.set("Alle PDFs wurden neu erstellt und automatisch ins Cloud-Portal hochgeladen.")
            messagebox.showinfo(APP_NAME, "Alle PDFs wurden neu erstellt und automatisch ins Cloud-Portal hochgeladen.", parent=self)
        else:
            self.status_var.set("Alle PDFs wurden neu erstellt. Cloud-Upload bitte prüfen.")
            messagebox.showwarning(APP_NAME, "Die PDFs wurden lokal neu erstellt, aber der Cloud-Upload konnte nicht abgeschlossen werden.", parent=self)


def main() -> None:
    ensure_dirs()
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
