"""
pdf_generator.py — PDF-Erzeugung fuer LeadFinder Pro v3

Angebots-PDFs, DIN-5008-Geschaeftsbriefe und Wochen-Reports
mit reportlab. Fallback falls reportlab nicht installiert ist.
"""

import os
import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, List, Any, Tuple

import database as db

log = logging.getLogger("leadfinder.pdf")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  REPORTLAB-IMPORT MIT FALLBACK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HAS_PDF = False
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm, cm
    from reportlab.lib.colors import HexColor, black, white, gray, lightgrey
    from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        Image, PageBreak, HRFlowable, KeepTogether,
    )
    from reportlab.graphics.shapes import Drawing, Rect, String
    from reportlab.graphics.charts.barcharts import VerticalBarChart
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    HAS_PDF = True
    log.info("reportlab erfolgreich geladen — PDF-Erzeugung verfuegbar")
except ImportError:
    log.warning("reportlab nicht installiert — PDF-Erzeugung deaktiviert")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  KONFIGURATION & VERZEICHNISSE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PDF_DIR = os.path.join(BASE_DIR, "data", "pdfs")
os.makedirs(PDF_DIR, exist_ok=True)

# Markenfarben
COLOR_PRIMARY = HexColor("#7C6FFF") if HAS_PDF else None      # Lila
COLOR_PRIMARY_DARK = HexColor("#5A4FCC") if HAS_PDF else None
COLOR_TEXT = HexColor("#2D2D2D") if HAS_PDF else None
COLOR_TEXT_LIGHT = HexColor("#6B6B6B") if HAS_PDF else None
COLOR_BG_LIGHT = HexColor("#F5F3FF") if HAS_PDF else None
COLOR_GREEN = HexColor("#22C55E") if HAS_PDF else None
COLOR_YELLOW = HexColor("#EAB308") if HAS_PDF else None
COLOR_RED = HexColor("#EF4444") if HAS_PDF else None
COLOR_STAR = HexColor("#FBBF24") if HAS_PDF else None
COLOR_TABLE_HEADER = HexColor("#7C6FFF") if HAS_PDF else None
COLOR_TABLE_STRIPE = HexColor("#F8F7FF") if HAS_PDF else None

PAGE_WIDTH, PAGE_HEIGHT = A4 if HAS_PDF else (595.27, 841.89)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HILFSFUNKTIONEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _unique_filename(prefix: str) -> str:
    """Erzeugt einen eindeutigen Dateinamen mit UUID."""
    uid = uuid.uuid4().hex[:12]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(PDF_DIR, f"{prefix}_{ts}_{uid}.pdf")


def _safe_str(value: Any, fallback: str = "—") -> str:
    """Gibt den Wert als String zurueck oder einen Fallback."""
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return fallback
    return str(value)


def _score_color(score_value: int):
    """Ampelfarbe basierend auf Lead-Score-Wert (0-100)."""
    if score_value >= 70:
        return COLOR_GREEN
    elif score_value >= 40:
        return COLOR_YELLOW
    return COLOR_RED


def _score_label(score: str) -> str:
    """Uebersetzt den Score-Schluessel ins Deutsche."""
    labels = {
        "hot": "Heiss",
        "warm": "Warm",
        "cold": "Kalt",
    }
    return labels.get(score, score.capitalize() if score else "Unbekannt")


def _format_date(dt_str: Optional[str], fmt: str = "%d.%m.%Y") -> str:
    """Formatiert einen ISO-Datums-String im deutschen Format."""
    if not dt_str:
        return datetime.now().strftime(fmt)
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime(fmt)
    except (ValueError, TypeError):
        return datetime.now().strftime(fmt)


def _build_styles() -> Dict[str, Any]:
    """Erstellt die Paragraph-Styles fuer das PDF."""
    base = getSampleStyleSheet()
    styles = {}

    styles["title"] = ParagraphStyle(
        "LFTitle",
        parent=base["Heading1"],
        fontSize=22,
        leading=26,
        textColor=COLOR_PRIMARY,
        spaceAfter=6 * mm,
        alignment=TA_LEFT,
    )
    styles["heading2"] = ParagraphStyle(
        "LFHeading2",
        parent=base["Heading2"],
        fontSize=14,
        leading=18,
        textColor=COLOR_PRIMARY_DARK,
        spaceBefore=8 * mm,
        spaceAfter=3 * mm,
    )
    styles["heading3"] = ParagraphStyle(
        "LFHeading3",
        parent=base["Heading3"],
        fontSize=11,
        leading=14,
        textColor=COLOR_TEXT,
        spaceBefore=4 * mm,
        spaceAfter=2 * mm,
    )
    styles["body"] = ParagraphStyle(
        "LFBody",
        parent=base["Normal"],
        fontSize=10,
        leading=14,
        textColor=COLOR_TEXT,
        alignment=TA_JUSTIFY,
        spaceAfter=2 * mm,
    )
    styles["body_light"] = ParagraphStyle(
        "LFBodyLight",
        parent=base["Normal"],
        fontSize=9,
        leading=12,
        textColor=COLOR_TEXT_LIGHT,
    )
    styles["footer"] = ParagraphStyle(
        "LFFooter",
        parent=base["Normal"],
        fontSize=7,
        leading=9,
        textColor=COLOR_TEXT_LIGHT,
        alignment=TA_CENTER,
    )
    styles["table_header"] = ParagraphStyle(
        "LFTableHeader",
        parent=base["Normal"],
        fontSize=9,
        leading=12,
        textColor=white,
        alignment=TA_LEFT,
    )
    styles["table_cell"] = ParagraphStyle(
        "LFTableCell",
        parent=base["Normal"],
        fontSize=9,
        leading=12,
        textColor=COLOR_TEXT,
    )
    styles["letter_body"] = ParagraphStyle(
        "LFLetterBody",
        parent=base["Normal"],
        fontSize=10,
        leading=15,
        textColor=COLOR_TEXT,
        alignment=TA_JUSTIFY,
        spaceAfter=3 * mm,
    )
    styles["letter_subject"] = ParagraphStyle(
        "LFLetterSubject",
        parent=base["Normal"],
        fontSize=11,
        leading=14,
        textColor=COLOR_TEXT,
        spaceAfter=6 * mm,
        fontName="Helvetica-Bold",
    )
    return styles


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HEADER / FOOTER FUER PROPOSALS UND REPORTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _draw_proposal_header(canvas, doc, company_name: str):
    """Zeichnet den Header auf jeder Proposal-Seite."""
    canvas.saveState()

    # Lila Header-Balken
    canvas.setFillColor(COLOR_PRIMARY)
    canvas.rect(0, PAGE_HEIGHT - 28 * mm, PAGE_WIDTH, 28 * mm, fill=1, stroke=0)

    # Firmenname
    canvas.setFillColor(white)
    canvas.setFont("Helvetica-Bold", 16)
    canvas.drawString(20 * mm, PAGE_HEIGHT - 18 * mm, company_name)

    # Claim
    canvas.setFont("Helvetica", 8)
    canvas.drawString(20 * mm, PAGE_HEIGHT - 24 * mm, "Ihre individuelle Angebotsanalyse")

    # Datum rechts
    canvas.setFont("Helvetica", 9)
    date_str = datetime.now().strftime("%d.%m.%Y")
    canvas.drawRightString(PAGE_WIDTH - 20 * mm, PAGE_HEIGHT - 18 * mm, date_str)

    canvas.restoreState()


def _draw_proposal_footer(canvas, doc, company_name: str):
    """Zeichnet den Footer auf jeder Proposal-Seite."""
    canvas.saveState()

    # Trennlinie
    canvas.setStrokeColor(COLOR_PRIMARY)
    canvas.setLineWidth(0.5)
    canvas.line(20 * mm, 18 * mm, PAGE_WIDTH - 20 * mm, 18 * mm)

    # Footer-Text
    canvas.setFillColor(COLOR_TEXT_LIGHT)
    canvas.setFont("Helvetica", 7)
    footer_text = (
        f"{company_name}  |  Erstellt mit LeadFinder Pro  |  "
        f"Seite {doc.page}"
    )
    canvas.drawCentredString(PAGE_WIDTH / 2, 13 * mm, footer_text)
    canvas.drawCentredString(
        PAGE_WIDTH / 2, 9 * mm,
        "Dieses Dokument wurde automatisch generiert und ist vertraulich."
    )

    canvas.restoreState()


def _draw_report_header(canvas, doc, company_name: str):
    """Zeichnet den Header fuer Wochen-Reports."""
    canvas.saveState()

    canvas.setFillColor(COLOR_PRIMARY)
    canvas.rect(0, PAGE_HEIGHT - 22 * mm, PAGE_WIDTH, 22 * mm, fill=1, stroke=0)

    canvas.setFillColor(white)
    canvas.setFont("Helvetica-Bold", 14)
    canvas.drawString(20 * mm, PAGE_HEIGHT - 15 * mm, f"{company_name} — Wochenbericht")

    canvas.setFont("Helvetica", 8)
    date_str = datetime.now().strftime("%d.%m.%Y")
    canvas.drawRightString(PAGE_WIDTH - 20 * mm, PAGE_HEIGHT - 15 * mm, date_str)

    canvas.restoreState()


def _draw_report_footer(canvas, doc):
    """Zeichnet den Footer fuer Wochen-Reports."""
    canvas.saveState()
    canvas.setStrokeColor(lightgrey)
    canvas.setLineWidth(0.3)
    canvas.line(20 * mm, 15 * mm, PAGE_WIDTH - 20 * mm, 15 * mm)

    canvas.setFillColor(COLOR_TEXT_LIGHT)
    canvas.setFont("Helvetica", 7)
    canvas.drawCentredString(
        PAGE_WIDTH / 2, 10 * mm,
        f"LeadFinder Pro — Automatischer Wochenbericht  |  Seite {doc.page}"
    )
    canvas.restoreState()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  STAR-RATING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _build_star_drawing(rating: float, max_stars: int = 5) -> Drawing:
    """Erzeugt eine Zeichnung mit Sternen fuer das Google-Rating."""
    star_size = 12
    gap = 2
    width = max_stars * (star_size + gap)
    d = Drawing(width, star_size + 4)

    full_stars = int(rating)
    has_half = (rating - full_stars) >= 0.3

    for i in range(max_stars):
        x = i * (star_size + gap)
        if i < full_stars:
            fill_color = COLOR_STAR
        elif i == full_stars and has_half:
            fill_color = HexColor("#FDE68A")  # Hellgelb fuer halben Stern
        else:
            fill_color = HexColor("#E5E7EB")  # Grau

        # Einfaches Quadrat als Stern-Platzhalter (reportlab hat kein Star-Shape)
        r = Rect(x, 0, star_size, star_size, fillColor=fill_color,
                 strokeColor=HexColor("#D4A017"), strokeWidth=0.5)
        d.add(r)

        # Unicode-Stern-Text in der Mitte
        s = String(x + star_size / 2, 2, "\u2605",
                   fontSize=10, fillColor=white, textAnchor="middle")
        d.add(s)

    return d


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  1) ANGEBOTS-PDF
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_proposal_pdf(
    lead: Dict[str, Any],
    user: Dict[str, Any],
    profession: str,
    city: str,
) -> Optional[str]:
    """
    Erzeugt ein Angebots-PDF (A4) fuer einen Lead.

    Aufbau:
    - Header mit Firmenname in Lila (#7C6FFF)
    - Lead-Kontaktdaten
    - Score-Analyse mit Ampelfarbe (gruen/gelb/rot)
    - Google-Rating-Sterne
    - Vorgeschlagenes Angebot
    - Footer

    Returns:
        Dateipfad zum erzeugten PDF oder None bei Fehler.
    """
    if not HAS_PDF:
        log.error("PDF-Erzeugung nicht moeglich — reportlab fehlt")
        return None

    filepath = _unique_filename("angebot")
    styles = _build_styles()

    company = _safe_str(user.get("company"), "LeadFinder Pro")
    lead_name = _safe_str(lead.get("name"), "Unbekanntes Unternehmen")

    try:
        doc = SimpleDocTemplate(
            filepath,
            pagesize=A4,
            topMargin=35 * mm,
            bottomMargin=25 * mm,
            leftMargin=20 * mm,
            rightMargin=20 * mm,
            title=f"Angebot fuer {lead_name}",
            author=company,
        )

        elements = []

        # --- Titel ---
        elements.append(Paragraph(f"Angebot fuer {lead_name}", styles["title"]))
        elements.append(Spacer(1, 2 * mm))

        intro_text = (
            f"Sehr geehrte Damen und Herren von <b>{lead_name}</b>,<br/><br/>"
            f"basierend auf unserer Analyse Ihres Unternehmens im Bereich "
            f"<b>{_safe_str(profession)}</b> in <b>{_safe_str(city)}</b> "
            f"haben wir folgendes individuelles Angebot fuer Sie zusammengestellt."
        )
        elements.append(Paragraph(intro_text, styles["body"]))
        elements.append(Spacer(1, 4 * mm))

        # --- Kontaktdaten-Tabelle ---
        elements.append(Paragraph("Ihre Unternehmensdaten", styles["heading2"]))

        contact_data = [
            ["Eigenschaft", "Wert"],
            ["Unternehmen", _safe_str(lead.get("name"))],
            ["Inhaber", _safe_str(lead.get("owner_name"))],
            ["Adresse", _safe_str(lead.get("address"))],
            ["Telefon", _safe_str(lead.get("phone"))],
            ["E-Mail", _safe_str(lead.get("email"))],
            ["Website", _safe_str(lead.get("website"))],
        ]

        contact_table = Table(contact_data, colWidths=[55 * mm, 105 * mm])
        contact_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), COLOR_TABLE_HEADER),
            ("TEXTCOLOR", (0, 0), (-1, 0), white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("LEADING", (0, 0), (-1, -1), 13),
            ("BACKGROUND", (0, 2), (-1, 2), COLOR_TABLE_STRIPE),
            ("BACKGROUND", (0, 4), (-1, 4), COLOR_TABLE_STRIPE),
            ("BACKGROUND", (0, 6), (-1, 6), COLOR_TABLE_STRIPE),
            ("GRID", (0, 0), (-1, -1), 0.4, lightgrey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        elements.append(contact_table)
        elements.append(Spacer(1, 6 * mm))

        # --- Score-Analyse ---
        elements.append(Paragraph("Lead-Score-Analyse", styles["heading2"]))

        score_value = lead.get("lead_score_value", 50)
        score_label = _score_label(lead.get("lead_score", "warm"))
        score_color = _score_color(score_value)

        score_text = (
            f"Ihr Lead-Score betraegt <b>{score_value} von 100 Punkten</b> "
            f"(Kategorie: <b>{score_label}</b>)."
        )
        elements.append(Paragraph(score_text, styles["body"]))
        elements.append(Spacer(1, 2 * mm))

        # Ampel-Visualisierung als Tabelle
        ampel_data = [
            ["Bewertung", "Punkte", "Status"],
        ]

        # Website-Score
        site_score = lead.get("site_score", 0) or 0
        site_color = _score_color(site_score)
        ampel_data.append(["Website-Qualitaet", f"{site_score}/100", ""])

        # SSL
        has_ssl = lead.get("site_has_ssl", 0)
        ampel_data.append([
            "SSL-Verschluesselung",
            "Ja" if has_ssl else "Nein",
            "",
        ])

        # Mobil-Optimierung
        is_mobile = lead.get("site_is_mobile", 0)
        ampel_data.append([
            "Mobil-Optimierung",
            "Ja" if is_mobile else "Nein",
            "",
        ])

        # Ladezeit
        load_time = lead.get("site_load_time")
        if load_time:
            ampel_data.append([
                "Ladezeit",
                f"{load_time:.1f}s",
                "",
            ])

        # Cookie-Banner
        has_cookie = lead.get("site_has_cookie_banner", 0)
        ampel_data.append([
            "Cookie-Banner (DSGVO)",
            "Ja" if has_cookie else "Nein",
            "",
        ])

        ampel_table = Table(ampel_data, colWidths=[70 * mm, 45 * mm, 45 * mm])

        # Farbige Zellen basierend auf Score
        ampel_style_cmds = [
            ("BACKGROUND", (0, 0), (-1, 0), COLOR_TABLE_HEADER),
            ("TEXTCOLOR", (0, 0), (-1, 0), white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("LEADING", (0, 0), (-1, -1), 13),
            ("GRID", (0, 0), (-1, -1), 0.4, lightgrey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]

        # Ampelfarbe fuer Website-Score-Zeile
        row_color = _score_color(site_score)
        ampel_style_cmds.append(("BACKGROUND", (2, 1), (2, 1), row_color))

        # SSL-Zeile
        ssl_color = COLOR_GREEN if has_ssl else COLOR_RED
        ampel_style_cmds.append(("BACKGROUND", (2, 2), (2, 2), ssl_color))

        # Mobil-Zeile
        mobile_color = COLOR_GREEN if is_mobile else COLOR_RED
        ampel_style_cmds.append(("BACKGROUND", (2, 3), (2, 3), mobile_color))

        # Ladezeit-Zeile (falls vorhanden)
        row_idx = 4
        if load_time:
            lt_color = COLOR_GREEN if load_time < 2.0 else (
                COLOR_YELLOW if load_time < 4.0 else COLOR_RED
            )
            ampel_style_cmds.append(("BACKGROUND", (2, row_idx), (2, row_idx), lt_color))
            row_idx += 1

        # Cookie-Banner-Zeile
        cookie_color = COLOR_GREEN if has_cookie else COLOR_YELLOW
        ampel_style_cmds.append(("BACKGROUND", (2, row_idx), (2, row_idx), cookie_color))

        # Zebrastreifen fuer ungerade Zeilen
        for i in range(1, len(ampel_data)):
            if i % 2 == 0:
                ampel_style_cmds.append(
                    ("BACKGROUND", (0, i), (1, i), COLOR_TABLE_STRIPE)
                )

        ampel_table.setStyle(TableStyle(ampel_style_cmds))
        elements.append(ampel_table)
        elements.append(Spacer(1, 6 * mm))

        # --- Google-Rating ---
        google_rating = lead.get("google_rating")
        google_reviews = lead.get("google_reviews", 0)

        elements.append(Paragraph("Google-Bewertung", styles["heading2"]))

        if google_rating and google_rating > 0:
            star_drawing = _build_star_drawing(google_rating)
            elements.append(star_drawing)
            elements.append(Spacer(1, 2 * mm))
            rating_text = (
                f"<b>{google_rating:.1f}</b> von 5.0 Sternen "
                f"({google_reviews or 0} Bewertungen)"
            )
            elements.append(Paragraph(rating_text, styles["body"]))

            if google_rating < 4.0:
                elements.append(Spacer(1, 1 * mm))
                hint = (
                    "<i>Hinweis: Eine Google-Bewertung unter 4.0 kann potenzielle "
                    "Kunden abschrecken. Wir helfen Ihnen, Ihre Online-Reputation "
                    "zu verbessern.</i>"
                )
                elements.append(Paragraph(hint, styles["body_light"]))
        else:
            elements.append(Paragraph(
                "Keine Google-Bewertung vorhanden. Dies stellt eine "
                "Verbesserungsmoeglichkeit dar.",
                styles["body"],
            ))

        elements.append(Spacer(1, 6 * mm))

        # --- Vorgeschlagenes Angebot ---
        elements.append(Paragraph("Unser Angebot fuer Sie", styles["heading2"]))

        offer_items = []

        if site_score < 60:
            offer_items.append(
                "Website-Optimierung: Verbesserung der Ladezeit, "
                "mobilen Darstellung und SEO-Grundlagen"
            )

        if not has_ssl:
            offer_items.append(
                "SSL-Zertifikat: Installation und Einrichtung einer "
                "sicheren HTTPS-Verbindung"
            )

        if not has_cookie:
            offer_items.append(
                "DSGVO-Konformitaet: Einrichtung eines rechtssicheren "
                "Cookie-Banners und Datenschutzerklaerung"
            )

        if not google_rating or google_rating < 4.0:
            offer_items.append(
                "Reputation-Management: Strategie zur Verbesserung "
                "Ihrer Google-Bewertungen"
            )

        if not is_mobile:
            offer_items.append(
                "Responsive Redesign: Optimierung Ihrer Website fuer "
                "Smartphones und Tablets"
            )

        # Falls keine Maengel gefunden, allgemeines Angebot
        if not offer_items:
            offer_items.append(
                "Premium-Betreuung: Laufende Optimierung und "
                "Monitoring Ihrer Online-Praesenz"
            )
            offer_items.append(
                "Content-Marketing: Erstellung hochwertiger Inhalte "
                "fuer Ihre Zielgruppe"
            )

        for item in offer_items:
            elements.append(Paragraph(f"\u2022  {item}", styles["body"]))

        elements.append(Spacer(1, 6 * mm))

        # --- Naechste Schritte ---
        elements.append(Paragraph("Naechste Schritte", styles["heading2"]))

        steps_text = (
            "1. Vereinbaren Sie ein kostenloses Erstgespraech mit uns.<br/>"
            "2. Wir analysieren Ihre aktuelle Situation im Detail.<br/>"
            "3. Sie erhalten ein massgeschneidertes Angebot.<br/>"
            "4. Nach Ihrer Freigabe starten wir sofort mit der Umsetzung."
        )
        elements.append(Paragraph(steps_text, styles["body"]))
        elements.append(Spacer(1, 4 * mm))

        # Kontakt des Absenders
        contact_text = (
            f"<b>Kontakt:</b><br/>"
            f"{_safe_str(user.get('name'), 'Ihr Ansprechpartner')}<br/>"
            f"{company}<br/>"
            f"E-Mail: {_safe_str(user.get('email'))}<br/>"
            f"Telefon: {_safe_str(user.get('phone'))}"
        )
        elements.append(Paragraph(contact_text, styles["body"]))

        # --- Erstellen ---
        def on_first_page(canvas_obj, doc_obj):
            _draw_proposal_header(canvas_obj, doc_obj, company)
            _draw_proposal_footer(canvas_obj, doc_obj, company)

        def on_later_pages(canvas_obj, doc_obj):
            _draw_proposal_header(canvas_obj, doc_obj, company)
            _draw_proposal_footer(canvas_obj, doc_obj, company)

        doc.build(elements, onFirstPage=on_first_page, onLaterPages=on_later_pages)

        log.info("Angebots-PDF erstellt: %s", filepath)
        return filepath

    except Exception as exc:
        log.error("Fehler bei Angebots-PDF-Erzeugung: %s", exc, exc_info=True)
        # Aufraumen bei Fehler
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except OSError:
                pass
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  2) GESCHAEFTSBRIEF-PDF (DIN 5008)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_letter_pdf(
    lead: Dict[str, Any],
    user: Dict[str, Any],
    profession: str,
    city: str,
    subject: str,
    body: str,
) -> Optional[str]:
    """
    Erzeugt einen Geschaeftsbrief im DIN-5008-Format.

    - Fenster-Briefumschlag-kompatibel (Adresse bei 20mm/45mm bis 85mm/100mm)
    - Absender-Ruecksendezeile klein ueber der Empfaengeradresse
    - Datum rechtsbuendig
    - Betreff, Anrede, Textteil, Grussformel, Unterschrift

    Returns:
        Dateipfad zum erzeugten PDF oder None bei Fehler.
    """
    if not HAS_PDF:
        log.error("PDF-Erzeugung nicht moeglich — reportlab fehlt")
        return None

    filepath = _unique_filename("brief")

    company = _safe_str(user.get("company"), "LeadFinder Pro")
    sender_name = _safe_str(user.get("name"), "")
    sender_email = _safe_str(user.get("email"), "")
    sender_phone = _safe_str(user.get("phone"), "")

    lead_name = _safe_str(lead.get("name"), "")
    lead_owner = _safe_str(lead.get("owner_name"), "")
    lead_address = _safe_str(lead.get("address"), "")

    today_str = datetime.now().strftime("%d. %B %Y")
    # Deutsche Monatsnamen
    month_map = {
        "January": "Januar", "February": "Februar", "March": "Maerz",
        "April": "April", "May": "Mai", "June": "Juni",
        "July": "Juli", "August": "August", "September": "September",
        "October": "Oktober", "November": "November", "December": "Dezember",
    }
    for en, de in month_map.items():
        today_str = today_str.replace(en, de)

    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen.canvas import Canvas

        c = Canvas(filepath, pagesize=A4)
        c.setTitle(f"Brief an {lead_name}")
        c.setAuthor(company)

        page_w, page_h = A4

        # ────── Absender-Zeile (Ruecksende-Angabe, DIN 5008) ──────
        # Positioniert im Sichtfenster des Briefumschlags
        # Fenster: links 20mm, unten ab 15mm, Breite 90mm, Hoehe 45mm
        # Ruecksendezeile: bei ca. 50mm vom oberen Rand
        ruecksende_y = page_h - 50 * mm
        c.setFont("Helvetica", 6)
        c.setFillColor(COLOR_TEXT_LIGHT)
        ruecksende_text = f"{company} - {sender_name} - {sender_email}"
        c.drawString(20 * mm, ruecksende_y, ruecksende_text)

        # Trennlinie unter Ruecksendezeile
        c.setStrokeColor(lightgrey)
        c.setLineWidth(0.3)
        c.line(20 * mm, ruecksende_y - 1.5 * mm, 105 * mm, ruecksende_y - 1.5 * mm)

        # ────── Empfaengeradresse (Fensterposition DIN 5008) ──────
        # Ab ca. 51mm von oben, links bei 20mm
        addr_y = page_h - 55 * mm
        c.setFont("Helvetica", 10)
        c.setFillColor(COLOR_TEXT)

        addr_lines = []
        if lead_owner and lead_owner != "\u2014":
            addr_lines.append(lead_owner)
        if lead_name and lead_name != "\u2014":
            addr_lines.append(lead_name)

        # Adresse aufteilen (kann Komma-getrennt sein)
        if lead_address and lead_address != "\u2014":
            parts = [p.strip() for p in lead_address.replace("\n", ",").split(",")]
            addr_lines.extend(parts)

        for i, line in enumerate(addr_lines[:6]):  # Maximal 6 Zeilen
            c.drawString(20 * mm, addr_y - (i * 4.5 * mm), line)

        # ────── Absender-Block rechts (DIN 5008: rechte Spalte) ──────
        info_x = 125 * mm
        info_y = page_h - 50 * mm
        c.setFont("Helvetica-Bold", 9)
        c.setFillColor(COLOR_PRIMARY)
        c.drawString(info_x, info_y, company)

        c.setFont("Helvetica", 8)
        c.setFillColor(COLOR_TEXT)
        info_lines = []
        if sender_name and sender_name != "\u2014":
            info_lines.append(sender_name)
        if sender_email and sender_email != "\u2014":
            info_lines.append(sender_email)
        if sender_phone and sender_phone != "\u2014":
            info_lines.append(f"Tel: {sender_phone}")

        for i, line in enumerate(info_lines):
            c.drawString(info_x, info_y - ((i + 1) * 4 * mm), line)

        # ────── Datum (rechtsbuendig, DIN 5008: ca. 98.5mm von oben) ──────
        datum_y = page_h - 98.5 * mm
        c.setFont("Helvetica", 10)
        c.setFillColor(COLOR_TEXT)
        c.drawRightString(page_w - 20 * mm, datum_y, f"{city}, {today_str}")

        # ────── Betreff (fett, DIN 5008: 2 Leerzeilen nach Datum) ──────
        betreff_y = datum_y - 12 * mm
        c.setFont("Helvetica-Bold", 11)
        c.setFillColor(COLOR_TEXT)
        c.drawString(20 * mm, betreff_y, subject)

        # ────── Anrede ──────
        anrede_y = betreff_y - 10 * mm
        c.setFont("Helvetica", 10)

        if lead_owner and lead_owner != "\u2014":
            anrede = f"Sehr geehrte(r) {lead_owner},"
        else:
            anrede = "Sehr geehrte Damen und Herren,"
        c.drawString(20 * mm, anrede_y, anrede)

        # ────── Brieftext ──────
        text_y = anrede_y - 8 * mm
        text_x = 20 * mm
        max_width = page_w - 40 * mm

        # Textumbruch mit reportlab textobject
        text_obj = c.beginText(text_x, text_y)
        text_obj.setFont("Helvetica", 10)
        text_obj.setFillColor(COLOR_TEXT)
        text_obj.setLeading(15)

        # Body-Text in Zeilen umbrechen
        paragraphs = body.split("\n")
        for para in paragraphs:
            if para.strip() == "":
                text_obj.textLine("")
                continue

            # Einfacher Zeilenumbruch: Woerter umbrechen bei ~80 Zeichen
            words = para.split()
            current_line = ""
            for word in words:
                test_line = f"{current_line} {word}".strip() if current_line else word
                # Ungefaehre Breitenberechnung: 5.5pt pro Zeichen bei 10pt
                if len(test_line) * 5.5 > max_width / (72 / 25.4):
                    text_obj.textLine(current_line)
                    current_line = word
                else:
                    current_line = test_line
            if current_line:
                text_obj.textLine(current_line)

        # Pruefen, ob wir noch auf der Seite sind
        remaining_y = text_obj.getY()

        # ────── Grussformel ──────
        text_obj.textLine("")
        text_obj.textLine("Mit freundlichen Gruessen")
        text_obj.textLine("")
        text_obj.textLine("")

        # Unterschrift
        if sender_name and sender_name != "\u2014":
            text_obj.setFont("Helvetica-Bold", 10)
            text_obj.textLine(sender_name)
        text_obj.setFont("Helvetica", 9)
        text_obj.setFillColor(COLOR_TEXT_LIGHT)
        text_obj.textLine(company)

        c.drawText(text_obj)

        # ────── Footer-Linie ──────
        c.setStrokeColor(COLOR_PRIMARY)
        c.setLineWidth(0.5)
        c.line(20 * mm, 20 * mm, page_w - 20 * mm, 20 * mm)

        c.setFont("Helvetica", 7)
        c.setFillColor(COLOR_TEXT_LIGHT)
        c.drawCentredString(
            page_w / 2, 15 * mm,
            f"{company}  |  {sender_email}  |  Erstellt mit LeadFinder Pro"
        )

        c.save()

        log.info("Brief-PDF erstellt: %s", filepath)
        return filepath

    except Exception as exc:
        log.error("Fehler bei Brief-PDF-Erzeugung: %s", exc, exc_info=True)
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except OSError:
                pass
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  3) WOCHEN-REPORT-PDF
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _build_bar_chart(
    data: List[Tuple[str, float]],
    title: str,
    bar_color: Any = None,
    chart_width: float = 450,
    chart_height: float = 180,
) -> Drawing:
    """Erzeugt ein einfaches Balkendiagramm als Drawing."""
    if bar_color is None:
        bar_color = COLOR_PRIMARY

    d = Drawing(chart_width, chart_height + 40)

    # Titel
    d.add(String(chart_width / 2, chart_height + 25, title,
                 fontSize=11, fillColor=COLOR_TEXT, textAnchor="middle",
                 fontName="Helvetica-Bold"))

    chart = VerticalBarChart()
    chart.x = 50
    chart.y = 30
    chart.width = chart_width - 80
    chart.height = chart_height - 30

    categories = [item[0] for item in data]
    values = [item[1] for item in data]

    chart.data = [values]
    chart.categoryAxis.categoryNames = categories
    chart.categoryAxis.labels.fontSize = 7
    chart.categoryAxis.labels.angle = 30
    chart.categoryAxis.labels.boxAnchor = "ne"
    chart.categoryAxis.labels.dx = -2
    chart.categoryAxis.labels.dy = -2

    chart.valueAxis.valueMin = 0
    chart.valueAxis.labels.fontSize = 8
    chart.valueAxis.labelTextFormat = "%d"

    # Max-Wert etwas hoehersetzten
    if values:
        max_val = max(values) if max(values) > 0 else 10
        chart.valueAxis.valueMax = max_val * 1.2
        chart.valueAxis.valueStep = max(1, int(max_val / 5))

    chart.bars[0].fillColor = bar_color
    chart.bars[0].strokeColor = None
    chart.bars[0].strokeWidth = 0

    d.add(chart)
    return d


def generate_report_pdf(
    report_data: Dict[str, Any],
    user: Dict[str, Any],
) -> Optional[str]:
    """
    Erzeugt einen Wochen-Report als PDF.

    report_data erwartet:
        - period_start: str (ISO-Datum)
        - period_end: str (ISO-Datum)
        - leads_found: int
        - emails_sent: int
        - emails_opened: int
        - emails_replied: int
        - emails_bounced: int
        - open_rate: float (%)
        - reply_rate: float (%)
        - top_leads: List[Dict] (Name, Score, Stadt)
        - daily_stats: List[Dict] (date, leads, emails, opens, replies)
        - conversion_count: int
        - revenue: float

    Returns:
        Dateipfad zum erzeugten PDF oder None bei Fehler.
    """
    if not HAS_PDF:
        log.error("PDF-Erzeugung nicht moeglich — reportlab fehlt")
        return None

    filepath = _unique_filename("report")
    styles = _build_styles()

    company = _safe_str(user.get("company"), "LeadFinder Pro")
    period_start = _format_date(report_data.get("period_start"))
    period_end = _format_date(report_data.get("period_end"))

    try:
        doc = SimpleDocTemplate(
            filepath,
            pagesize=A4,
            topMargin=30 * mm,
            bottomMargin=22 * mm,
            leftMargin=20 * mm,
            rightMargin=20 * mm,
            title=f"Wochenbericht {period_start} - {period_end}",
            author=company,
        )

        elements = []

        # --- Titel ---
        elements.append(Spacer(1, 2 * mm))
        elements.append(Paragraph(
            f"Wochenbericht: {period_start} — {period_end}",
            styles["title"],
        ))
        elements.append(Paragraph(
            f"Erstellt fuer {company} am {datetime.now().strftime('%d.%m.%Y um %H:%M Uhr')}",
            styles["body_light"],
        ))
        elements.append(Spacer(1, 6 * mm))

        # --- Kennzahlen-Uebersicht ---
        elements.append(Paragraph("Kennzahlen-Uebersicht", styles["heading2"]))

        leads_found = report_data.get("leads_found", 0)
        emails_sent = report_data.get("emails_sent", 0)
        emails_opened = report_data.get("emails_opened", 0)
        emails_replied = report_data.get("emails_replied", 0)
        emails_bounced = report_data.get("emails_bounced", 0)
        open_rate = report_data.get("open_rate", 0.0)
        reply_rate = report_data.get("reply_rate", 0.0)
        conversion_count = report_data.get("conversion_count", 0)
        revenue = report_data.get("revenue", 0.0)

        kpi_data = [
            ["Kennzahl", "Wert", "Kennzahl", "Wert"],
            ["Leads gefunden", str(leads_found),
             "E-Mails versendet", str(emails_sent)],
            ["E-Mails geoeffnet", str(emails_opened),
             "Antworten erhalten", str(emails_replied)],
            ["Oeffnungsrate", f"{open_rate:.1f}%",
             "Antwortrate", f"{reply_rate:.1f}%"],
            ["Bounces", str(emails_bounced),
             "Konversionen", str(conversion_count)],
            ["Umsatz", f"{revenue:,.2f} EUR".replace(",", "."),
             "", ""],
        ]

        kpi_table = Table(
            kpi_data,
            colWidths=[45 * mm, 35 * mm, 45 * mm, 35 * mm],
        )
        kpi_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), COLOR_TABLE_HEADER),
            ("TEXTCOLOR", (0, 0), (-1, 0), white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("LEADING", (0, 0), (-1, -1), 13),
            ("GRID", (0, 0), (-1, -1), 0.4, lightgrey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("FONTNAME", (1, 1), (1, -1), "Helvetica-Bold"),
            ("FONTNAME", (3, 1), (3, -1), "Helvetica-Bold"),
            ("BACKGROUND", (0, 2), (-1, 2), COLOR_TABLE_STRIPE),
            ("BACKGROUND", (0, 4), (-1, 4), COLOR_TABLE_STRIPE),
        ]))
        elements.append(kpi_table)
        elements.append(Spacer(1, 8 * mm))

        # --- Taegliche Statistiken ---
        daily_stats = report_data.get("daily_stats", [])

        if daily_stats:
            elements.append(Paragraph("Tagesverlauf", styles["heading2"]))

            daily_header = ["Datum", "Leads", "E-Mails", "Oeffnungen", "Antworten"]
            daily_rows = [daily_header]

            for day in daily_stats:
                daily_rows.append([
                    _format_date(day.get("date")),
                    str(day.get("leads", 0)),
                    str(day.get("emails", 0)),
                    str(day.get("opens", 0)),
                    str(day.get("replies", 0)),
                ])

            # Summenzeile
            total_leads = sum(d.get("leads", 0) for d in daily_stats)
            total_emails = sum(d.get("emails", 0) for d in daily_stats)
            total_opens = sum(d.get("opens", 0) for d in daily_stats)
            total_replies = sum(d.get("replies", 0) for d in daily_stats)
            daily_rows.append([
                "Gesamt",
                str(total_leads),
                str(total_emails),
                str(total_opens),
                str(total_replies),
            ])

            daily_table = Table(
                daily_rows,
                colWidths=[35 * mm, 27 * mm, 27 * mm, 27 * mm, 27 * mm],
            )

            daily_style_cmds = [
                ("BACKGROUND", (0, 0), (-1, 0), COLOR_TABLE_HEADER),
                ("TEXTCOLOR", (0, 0), (-1, 0), white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("LEADING", (0, 0), (-1, -1), 12),
                ("GRID", (0, 0), (-1, -1), 0.4, lightgrey),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ALIGN", (1, 0), (-1, -1), "CENTER"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                # Summenzeile
                ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ("BACKGROUND", (0, -1), (-1, -1), COLOR_BG_LIGHT),
                ("LINEABOVE", (0, -1), (-1, -1), 1, COLOR_PRIMARY),
            ]

            # Zebrastreifen
            for i in range(1, len(daily_rows) - 1):
                if i % 2 == 0:
                    daily_style_cmds.append(
                        ("BACKGROUND", (0, i), (-1, i), COLOR_TABLE_STRIPE)
                    )

            daily_table.setStyle(TableStyle(daily_style_cmds))
            elements.append(daily_table)
            elements.append(Spacer(1, 8 * mm))

            # --- Balkendiagramm: E-Mails versendet pro Tag ---
            email_chart_data = [
                (_format_date(d.get("date"), "%d.%m."), d.get("emails", 0))
                for d in daily_stats
            ]
            if any(v > 0 for _, v in email_chart_data):
                chart_emails = _build_bar_chart(
                    email_chart_data,
                    "E-Mails versendet pro Tag",
                    bar_color=COLOR_PRIMARY,
                )
                elements.append(KeepTogether([chart_emails]))
                elements.append(Spacer(1, 6 * mm))

            # --- Balkendiagramm: Leads gefunden pro Tag ---
            leads_chart_data = [
                (_format_date(d.get("date"), "%d.%m."), d.get("leads", 0))
                for d in daily_stats
            ]
            if any(v > 0 for _, v in leads_chart_data):
                chart_leads = _build_bar_chart(
                    leads_chart_data,
                    "Leads gefunden pro Tag",
                    bar_color=COLOR_GREEN,
                )
                elements.append(KeepTogether([chart_leads]))
                elements.append(Spacer(1, 6 * mm))

            # --- Balkendiagramm: Oeffnungen & Antworten ---
            interact_chart_data = [
                (_format_date(d.get("date"), "%d.%m."), d.get("opens", 0))
                for d in daily_stats
            ]
            if any(v > 0 for _, v in interact_chart_data):
                chart_opens = _build_bar_chart(
                    interact_chart_data,
                    "E-Mail-Oeffnungen pro Tag",
                    bar_color=COLOR_STAR,
                )
                elements.append(KeepTogether([chart_opens]))
                elements.append(Spacer(1, 6 * mm))

        # --- Top-Leads ---
        top_leads = report_data.get("top_leads", [])

        if top_leads:
            elements.append(Paragraph("Top-Leads dieser Woche", styles["heading2"]))

            top_header = ["#", "Unternehmen", "Score", "Stadt", "Status"]
            top_rows = [top_header]

            for idx, tl in enumerate(top_leads[:10], start=1):
                score_val = tl.get("lead_score_value", 50)
                score_txt = f"{score_val}/100"
                status = _score_label(tl.get("lead_score", "warm"))
                top_rows.append([
                    str(idx),
                    _safe_str(tl.get("name")),
                    score_txt,
                    _safe_str(tl.get("search_city", tl.get("city", ""))),
                    status,
                ])

            top_table = Table(
                top_rows,
                colWidths=[10 * mm, 60 * mm, 25 * mm, 35 * mm, 25 * mm],
            )

            top_style_cmds = [
                ("BACKGROUND", (0, 0), (-1, 0), COLOR_TABLE_HEADER),
                ("TEXTCOLOR", (0, 0), (-1, 0), white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("LEADING", (0, 0), (-1, -1), 12),
                ("GRID", (0, 0), (-1, -1), 0.4, lightgrey),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ALIGN", (0, 0), (0, -1), "CENTER"),
                ("ALIGN", (2, 0), (2, -1), "CENTER"),
                ("ALIGN", (4, 0), (4, -1), "CENTER"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]

            # Ampelfarben in der Score-Spalte
            for i in range(1, len(top_rows)):
                tl_data = top_leads[i - 1] if (i - 1) < len(top_leads) else {}
                sv = tl_data.get("lead_score_value", 50)
                sc = _score_color(sv)
                top_style_cmds.append(("TEXTCOLOR", (2, i), (2, i), sc))
                top_style_cmds.append(("FONTNAME", (2, i), (2, i), "Helvetica-Bold"))

                # Zebrastreifen
                if i % 2 == 0:
                    top_style_cmds.append(
                        ("BACKGROUND", (0, i), (-1, i), COLOR_TABLE_STRIPE)
                    )

            top_table.setStyle(TableStyle(top_style_cmds))
            elements.append(top_table)
            elements.append(Spacer(1, 6 * mm))

        # --- Zusammenfassung ---
        elements.append(Paragraph("Zusammenfassung", styles["heading2"]))

        if emails_sent > 0 and open_rate > 0:
            if open_rate >= 30:
                perf_text = (
                    "Ihre Oeffnungsrate liegt ueber dem Branchendurchschnitt. "
                    "Hervorragende Betreffzeilen und gutes Timing."
                )
            elif open_rate >= 15:
                perf_text = (
                    "Ihre Oeffnungsrate ist solide. Testen Sie verschiedene "
                    "Betreffzeilen mit A/B-Tests fuer weitere Verbesserungen."
                )
            else:
                perf_text = (
                    "Ihre Oeffnungsrate hat Verbesserungspotenzial. "
                    "Ueberpruefen Sie Betreffzeilen, Versandzeitpunkt "
                    "und die Qualitaet Ihrer Lead-Daten."
                )
        else:
            perf_text = (
                "In dieser Woche wurden keine E-Mails versendet oder "
                "es liegen noch keine Tracking-Daten vor."
            )

        elements.append(Paragraph(perf_text, styles["body"]))

        if leads_found > 0:
            lead_text = (
                f"Es wurden <b>{leads_found} neue Leads</b> gefunden. "
            )
            if conversion_count > 0:
                conv_rate = (conversion_count / leads_found * 100)
                lead_text += (
                    f"Davon wurden <b>{conversion_count}</b> konvertiert "
                    f"(Konversionsrate: {conv_rate:.1f}%)."
                )
            else:
                lead_text += (
                    "Bisher gab es keine Konversionen — bleiben Sie dran!"
                )
            elements.append(Paragraph(lead_text, styles["body"]))

        if revenue > 0:
            rev_text = (
                f"Der generierte Umsatz in diesem Zeitraum betraegt "
                f"<b>{revenue:,.2f} EUR</b>.".replace(",", ".")
            )
            elements.append(Paragraph(rev_text, styles["body"]))

        elements.append(Spacer(1, 6 * mm))

        # --- Empfehlungen ---
        elements.append(Paragraph("Empfehlungen", styles["heading2"]))

        recommendations = []

        if open_rate < 20 and emails_sent > 5:
            recommendations.append(
                "Optimieren Sie Ihre Betreffzeilen — kurz, persoenlich und "
                "mit klarem Mehrwert."
            )

        if reply_rate < 5 and emails_sent > 10:
            recommendations.append(
                "Ueberarbeiten Sie Ihre E-Mail-Vorlagen. Stellen Sie den "
                "Nutzen fuer den Empfaenger in den Vordergrund."
            )

        if leads_found == 0:
            recommendations.append(
                "Erweitern Sie Ihre Suchkriterien oder pruefen Sie "
                "weitere Staedte und Branchen."
            )

        if emails_bounced > emails_sent * 0.1 and emails_sent > 5:
            recommendations.append(
                "Ihre Bounce-Rate ist hoch. Ueberpruefen Sie die "
                "E-Mail-Adressen Ihrer Leads auf Gueltigkeit."
            )

        if not recommendations:
            recommendations.append(
                "Weiter so! Ihre Kennzahlen sind auf einem guten Weg. "
                "Nutzen Sie die Follow-Up-Funktion, um den Kontakt "
                "zu Ihren Leads aufrechtzuerhalten."
            )

        for rec in recommendations:
            elements.append(Paragraph(f"\u2022  {rec}", styles["body"]))

        # --- Erstellen ---
        def on_first_page(canvas_obj, doc_obj):
            _draw_report_header(canvas_obj, doc_obj, company)
            _draw_report_footer(canvas_obj, doc_obj)

        def on_later_pages(canvas_obj, doc_obj):
            _draw_report_header(canvas_obj, doc_obj, company)
            _draw_report_footer(canvas_obj, doc_obj)

        doc.build(elements, onFirstPage=on_first_page, onLaterPages=on_later_pages)

        log.info("Report-PDF erstellt: %s", filepath)
        return filepath

    except Exception as exc:
        log.error("Fehler bei Report-PDF-Erzeugung: %s", exc, exc_info=True)
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except OSError:
                pass
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HILFSFUNKTIONEN FUER EXTERNE AUFRUFE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_proposal_for_lead(lead_id: int, user_id: int) -> Optional[str]:
    """
    Laedt Lead und User aus der Datenbank und erzeugt ein Angebots-PDF.

    Returns:
        Dateipfad oder None.
    """
    try:
        lead = db.dict_from_row(db.get_lead(lead_id))
        user = db.dict_from_row(db.get_user_by_id(user_id))

        if not lead or not user:
            log.error(
                "Lead (%s) oder User (%s) nicht gefunden", lead_id, user_id
            )
            return None

        profession = lead.get("search_query", "Dienstleistung")
        city = lead.get("search_city", "")

        return generate_proposal_pdf(lead, user, profession, city)

    except Exception as exc:
        log.error("Fehler bei generate_proposal_for_lead: %s", exc, exc_info=True)
        return None


def generate_letter_for_lead(
    lead_id: int,
    user_id: int,
    subject: str,
    body: str,
) -> Optional[str]:
    """
    Laedt Lead und User aus der Datenbank und erzeugt einen Brief.

    Returns:
        Dateipfad oder None.
    """
    try:
        lead = db.dict_from_row(db.get_lead(lead_id))
        user = db.dict_from_row(db.get_user_by_id(user_id))

        if not lead or not user:
            log.error(
                "Lead (%s) oder User (%s) nicht gefunden", lead_id, user_id
            )
            return None

        profession = lead.get("search_query", "Dienstleistung")
        city = lead.get("search_city", "")

        return generate_letter_pdf(lead, user, profession, city, subject, body)

    except Exception as exc:
        log.error("Fehler bei generate_letter_for_lead: %s", exc, exc_info=True)
        return None


def generate_weekly_report(user_id: int) -> Optional[str]:
    """
    Sammelt Daten der letzten 7 Tage und erzeugt einen Wochen-Report.

    Returns:
        Dateipfad oder None.
    """
    try:
        from datetime import timedelta

        user = db.dict_from_row(db.get_user_by_id(user_id))
        if not user:
            log.error("User %s nicht gefunden", user_id)
            return None

        now = datetime.now()
        week_ago = now - timedelta(days=7)
        period_start = week_ago.strftime("%Y-%m-%d")
        period_end = now.strftime("%Y-%m-%d")

        conn = db.get_db()

        # Leads dieser Woche
        leads_row = conn.execute(
            "SELECT COUNT(*) as cnt FROM leads "
            "WHERE user_id = ? AND created_at >= ?",
            (user_id, period_start),
        ).fetchone()
        leads_found = leads_row["cnt"] if leads_row else 0

        # E-Mail-Statistiken
        email_row = conn.execute(
            "SELECT "
            "  COUNT(*) as sent, "
            "  SUM(CASE WHEN opened > 0 THEN 1 ELSE 0 END) as opened, "
            "  SUM(CASE WHEN replied > 0 THEN 1 ELSE 0 END) as replied, "
            "  SUM(CASE WHEN bounced > 0 THEN 1 ELSE 0 END) as bounced "
            "FROM emails "
            "WHERE user_id = ? AND sent_at >= ? AND status = 'sent'",
            (user_id, period_start),
        ).fetchone()

        emails_sent = email_row["sent"] if email_row else 0
        emails_opened = email_row["opened"] if email_row else 0
        emails_replied = email_row["replied"] if email_row else 0
        emails_bounced = email_row["bounced"] if email_row else 0

        open_rate = (emails_opened / emails_sent * 100) if emails_sent > 0 else 0
        reply_rate = (emails_replied / emails_sent * 100) if emails_sent > 0 else 0

        # Konversionen
        conv_row = conn.execute(
            "SELECT COUNT(*) as cnt, COALESCE(SUM(revenue), 0) as rev "
            "FROM leads "
            "WHERE user_id = ? AND converted = 1 AND converted_date >= ?",
            (user_id, period_start),
        ).fetchone()
        conversion_count = conv_row["cnt"] if conv_row else 0
        revenue = conv_row["rev"] if conv_row else 0.0

        # Taeglich aufgeschluesselt
        daily_stats = []
        for day_offset in range(7):
            day = week_ago + timedelta(days=day_offset)
            day_str = day.strftime("%Y-%m-%d")
            next_day_str = (day + timedelta(days=1)).strftime("%Y-%m-%d")

            d_leads = conn.execute(
                "SELECT COUNT(*) as cnt FROM leads "
                "WHERE user_id = ? AND created_at >= ? AND created_at < ?",
                (user_id, day_str, next_day_str),
            ).fetchone()

            d_emails = conn.execute(
                "SELECT "
                "  COUNT(*) as sent, "
                "  SUM(CASE WHEN opened > 0 THEN 1 ELSE 0 END) as opened, "
                "  SUM(CASE WHEN replied > 0 THEN 1 ELSE 0 END) as replied "
                "FROM emails "
                "WHERE user_id = ? AND sent_at >= ? AND sent_at < ? "
                "AND status = 'sent'",
                (user_id, day_str, next_day_str),
            ).fetchone()

            daily_stats.append({
                "date": day_str,
                "leads": d_leads["cnt"] if d_leads else 0,
                "emails": d_emails["sent"] if d_emails else 0,
                "opens": d_emails["opened"] if d_emails else 0,
                "replies": d_emails["replied"] if d_emails else 0,
            })

        # Top-Leads (hoechster Score)
        top_leads_rows = conn.execute(
            "SELECT name, lead_score, lead_score_value, search_city "
            "FROM leads "
            "WHERE user_id = ? AND created_at >= ? "
            "ORDER BY lead_score_value DESC LIMIT 10",
            (user_id, period_start),
        ).fetchall()
        top_leads = db.rows_to_dicts(top_leads_rows)

        report_data = {
            "period_start": period_start,
            "period_end": period_end,
            "leads_found": leads_found,
            "emails_sent": emails_sent,
            "emails_opened": emails_opened,
            "emails_replied": emails_replied,
            "emails_bounced": emails_bounced,
            "open_rate": open_rate,
            "reply_rate": reply_rate,
            "conversion_count": conversion_count,
            "revenue": revenue,
            "daily_stats": daily_stats,
            "top_leads": top_leads,
        }

        return generate_report_pdf(report_data, user)

    except Exception as exc:
        log.error("Fehler bei generate_weekly_report: %s", exc, exc_info=True)
        return None


def cleanup_old_pdfs(max_age_days: int = 30) -> int:
    """
    Loescht PDFs, die aelter als max_age_days sind.

    Returns:
        Anzahl der geloeschten Dateien.
    """
    deleted = 0
    now = datetime.now().timestamp()
    max_age_seconds = max_age_days * 86400

    try:
        for filename in os.listdir(PDF_DIR):
            if not filename.endswith(".pdf"):
                continue
            filepath = os.path.join(PDF_DIR, filename)
            file_age = now - os.path.getmtime(filepath)
            if file_age > max_age_seconds:
                try:
                    os.remove(filepath)
                    deleted += 1
                    log.debug("Alte PDF geloescht: %s", filename)
                except OSError as exc:
                    log.warning("Konnte %s nicht loeschen: %s", filename, exc)
    except OSError as exc:
        log.error("Fehler beim Auflisten von %s: %s", PDF_DIR, exc)

    if deleted > 0:
        log.info("%d alte PDFs geloescht (aelter als %d Tage)", deleted, max_age_days)

    return deleted
