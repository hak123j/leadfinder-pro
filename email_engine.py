"""
email_engine.py — E-Mail-Versand, IMAP-Monitoring, Tracking, Warmup

SMTP-Versand mit Tracking-Pixel, Vorlagen, IMAP-Reply/Bounce-Erkennung,
Warmup-System, Versandzeit-Optimierung, Follow-Up-Scheduling.
"""

import re
import uuid
import smtplib
import imaplib
import email as email_lib
import logging
import html as html_module
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import Optional, Dict, List, Any, Tuple

import database as db
from auth import decrypt_smtp_password

log = logging.getLogger("leadfinder.email")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  E-MAIL-VORLAGEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TEMPLATES: Dict[str, Dict[str, str]] = {
    "webdesign": {
        "name": "Webdesign-Analyse",
        "subject": "Ihre Webseite {name} — kostenlose Analyse",
        "body": """Sehr geehrte Damen und Herren,

ich habe mir Ihre Webseite {website} angesehen und einige Optimierungsmoeglichkeiten entdeckt.
{score_details}
Als {profession}-Spezialist in {city} moechte ich Ihnen anbieten, Ihre Online-Praesenz zu verbessern — ganz unverbindlich und kostenlos.

Darf ich Ihnen einen kurzen Analyse-Report zusenden?

Mit freundlichen Gruessen
{sender_name}
{sender_email}""",
    },
    "bewertungen": {
        "name": "Google-Bewertungen",
        "subject": "Mehr Google-Bewertungen fuer {name}?",
        "body": """Sehr geehrte Damen und Herren,
{rating_text}
gute Google-Bewertungen sind heute entscheidend fuer neue Kunden. Ich habe ein System entwickelt, mit dem Sie automatisch mehr positive Bewertungen erhalten.

Darf ich Ihnen zeigen, wie das funktioniert?

Mit freundlichen Gruessen
{sender_name}
{sender_email}""",
    },
    "allgemein": {
        "name": "Allgemein",
        "subject": "Zusammenarbeit mit {name}?",
        "body": """Sehr geehrte Damen und Herren,

ich bin auf Ihr Unternehmen {name} in {city} aufmerksam geworden und wuerde gerne mit Ihnen ueber eine moegliche Zusammenarbeit sprechen.
{tech_text}
Haetten Sie diese Woche Zeit fuer ein kurzes Telefonat?

Mit freundlichen Gruessen
{sender_name}
{sender_email}""",
    },
}

FOLLOWUP_TEMPLATES: Dict[int, Dict[str, str]] = {
    1: {
        "name": "Follow-Up 1 (3 Tage)",
        "subject": "Re: {original_subject}",
        "body": """Guten Tag,

ich wollte kurz nachfragen, ob Sie meine E-Mail von letzter Woche erhalten haben.

Ich wuerde mich freuen, von Ihnen zu hoeren.

Mit freundlichen Gruessen
{sender_name}""",
    },
    2: {
        "name": "Follow-Up 2 (7 Tage)",
        "subject": "Kurze Rueckmeldung — {name}",
        "body": """Guten Tag,

ich moechte Sie nicht bedraengen, aber mein Angebot steht natuerlich weiterhin.

Falls Sie kein Interesse haben, ist das voellig in Ordnung — sagen Sie mir einfach kurz Bescheid, dann streiche ich Sie von meiner Liste.

Beste Gruesse
{sender_name}""",
    },
    3: {
        "name": "Follow-Up 3 (14 Tage)",
        "subject": "Letzte Nachricht — {name}",
        "body": """Guten Tag,

dies ist meine letzte Nachricht zu diesem Thema.

Falls Sie in Zukunft doch Interesse haben, koennen Sie mich jederzeit erreichen unter {sender_email}.

Alles Gute!
{sender_name}""",
    },
}

# Platzhalter-Dokumentation
PLACEHOLDERS = [
    ("{name}", "Firmenname"),
    ("{email}", "E-Mail-Adresse"),
    ("{phone}", "Telefonnummer"),
    ("{address}", "Adresse"),
    ("{website}", "Webseite"),
    ("{city}", "Stadt"),
    ("{profession}", "Branche"),
    ("{sender_name}", "Dein Name"),
    ("{sender_email}", "Deine E-Mail"),
    ("{owner_name}", "Inhaber-Name"),
    ("{score_details}", "Website-Score-Details"),
    ("{rating_text}", "Google-Bewertungstext"),
    ("{tech_text}", "Technologie-Hinweis"),
    ("{unsubscribe_link}", "Abmelde-Link"),
    ("{original_subject}", "Original-Betreff (Follow-Up)"),
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  E-MAIL GENERIERUNG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_email_content(
    lead: Dict[str, Any],
    template_key: str,
    user: Dict[str, Any],
    profession: str = "",
    city: str = "",
    custom_subject: str = "",
    custom_body: str = "",
    followup_number: int = 0,
    original_subject: str = "",
    unsubscribe_link: str = "",
) -> Dict[str, str]:
    """Generiert E-Mail-Inhalt aus Template + Lead-Daten."""
    if followup_number > 0 and followup_number in FOLLOWUP_TEMPLATES:
        tpl = FOLLOWUP_TEMPLATES[followup_number]
        subject_tpl, body_tpl = tpl["subject"], tpl["body"]
    elif template_key == "custom":
        subject_tpl, body_tpl = custom_subject, custom_body
    else:
        tpl = TEMPLATES.get(template_key, TEMPLATES["allgemein"])
        subject_tpl, body_tpl = tpl["subject"], tpl["body"]

    # Score-Details
    score_text = ""
    if lead.get("score") and isinstance(lead["score"], dict) and lead["score"].get("issues"):
        issues = lead["score"]["issues"][:3]
        score_text = (
            "\n\nBei meiner Analyse Ihrer Webseite sind mir folgende Punkte aufgefallen:\n"
            + "".join(f"- {i}\n" for i in issues)
        )

    # Rating-Text
    rating_text = ""
    if lead.get("rating") and isinstance(lead["rating"], dict):
        r = lead["rating"]
        if r.get("rating"):
            rating_text = f"\nich sehe, dass {lead.get('name', '')} derzeit {r['rating']} Sterne bei {r.get('reviews', 0)} Bewertungen hat.\n"

    # Tech-Text
    tech_text = ""
    tech_stack = lead.get("tech_stack", [])
    if isinstance(tech_stack, list) and tech_stack:
        tech_text = f"\nIch sehe, dass Sie {tech_stack[0]} nutzen. "

    # Owner-Name
    owner_name = lead.get("owner_name", "")
    greeting = f"Sehr geehrte(r) {owner_name}" if owner_name else "Sehr geehrte Damen und Herren"

    replacements = {
        "{name}": lead.get("name", ""),
        "{email}": lead.get("email", ""),
        "{phone}": lead.get("phone", ""),
        "{address}": lead.get("address", ""),
        "{website}": lead.get("website", ""),
        "{city}": city or lead.get("city", ""),
        "{profession}": profession or lead.get("profession", ""),
        "{sender_name}": user.get("smtp_from_name") or user.get("name") or "Ihr Name",
        "{sender_email}": user.get("smtp_from_email") or user.get("email") or "",
        "{owner_name}": owner_name,
        "{score_details}": score_text,
        "{rating_text}": rating_text,
        "{tech_text}": tech_text,
        "{unsubscribe_link}": unsubscribe_link,
        "{original_subject}": original_subject,
    }

    subject = subject_tpl
    body = body_tpl
    for key, value in replacements.items():
        subject = subject.replace(key, value or "")
        body = body.replace(key, value or "")

    return {
        "subject": subject.strip(),
        "body": body.strip(),
        "to": lead.get("email", ""),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SMTP-VERSAND
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _create_html_body(text_body: str, tracking_pixel_url: str = "",
                      unsubscribe_url: str = "") -> str:
    """Erstellt den HTML-Body mit Tracking-Pixel und Unsubscribe-Link."""
    escaped = html_module.escape(text_body).replace("\n", "<br>\n")
    unsub_html = ""
    if unsubscribe_url:
        unsub_html = f"""
<hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
<p style="font-size: 11px; color: #999;">
  <a href="{html_module.escape(unsubscribe_url)}" style="color: #999;">Abmelden</a>
</p>"""

    pixel_html = ""
    if tracking_pixel_url:
        pixel_html = f'<img src="{html_module.escape(tracking_pixel_url)}" width="1" height="1" style="display:none;" alt="">'

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333;">
{escaped}
{unsub_html}
{pixel_html}
</body></html>"""


def send_email(
    user_id: int,
    to_email: str,
    subject: str,
    body: str,
    lead_id: int = None,
    template_key: str = "allgemein",
    followup_number: int = 0,
    ab_test_id: int = None,
    ab_variant: str = None,
    pdf_path: str = None,
    tracking_host: str = "http://localhost:5000",
) -> Tuple[bool, str, Optional[int]]:
    """Sendet eine E-Mail via SMTP. Gibt (success, error_msg, email_id) zurueck."""
    user = db.get_user_with_plan(user_id)
    if not user:
        return False, "User nicht gefunden", None

    smtp_host = user.get("smtp_host", "")
    smtp_port = user.get("smtp_port", 587)
    smtp_user = user.get("smtp_user", "")
    smtp_pass = decrypt_smtp_password(user.get("smtp_pass_encrypted", ""))
    from_name = user.get("smtp_from_name") or user.get("name", "")
    from_email = user.get("smtp_from_email") or smtp_user

    if not smtp_host or not smtp_user or not smtp_pass:
        return False, "SMTP-Einstellungen nicht konfiguriert", None

    # Tracking-ID generieren
    tracking_id = uuid.uuid4().hex[:16]
    tracking_pixel_url = f"{tracking_host}/t/{tracking_id}/open.png"

    # Unsubscribe
    unsubscribe_url = f"{tracking_host}/unsubscribe/{tracking_id}"

    # HTML-Body
    body_html = _create_html_body(body, tracking_pixel_url, unsubscribe_url)

    # E-Mail zusammenbauen
    msg = MIMEMultipart("alternative")
    msg["From"] = f"{from_name} <{from_email}>" if from_name else from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg["List-Unsubscribe"] = f"<{unsubscribe_url}>"
    msg["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"
    msg["X-Mailer"] = "LeadFinder Pro v3"

    msg.attach(MIMEText(body, "plain", "utf-8"))
    msg.attach(MIMEText(body_html, "html", "utf-8"))

    # PDF-Anhang
    if pdf_path:
        try:
            import os
            if os.path.exists(pdf_path):
                with open(pdf_path, "rb") as f:
                    pdf_data = f.read()
                part = MIMEBase("application", "pdf")
                part.set_payload(pdf_data)
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", "attachment",
                                filename=os.path.basename(pdf_path))
                msg.attach(part)
        except (OSError, IOError) as exc:
            log.warning("PDF-Anhang konnte nicht gelesen werden: %s", exc)

    # E-Mail-Record erstellen
    email_id = db.create_email_record(user_id, {
        "lead_id": lead_id,
        "to_email": to_email,
        "from_email": from_email,
        "subject": subject,
        "body": body,
        "body_html": body_html,
        "template_key": template_key,
        "tracking_id": tracking_id,
        "followup_number": followup_number,
        "ab_test_id": ab_test_id,
        "ab_variant": ab_variant,
        "pdf_path": pdf_path,
        "status": "pending",
    })

    # SMTP-Versand
    try:
        if smtp_port == 465:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=15)
        else:
            server = smtplib.SMTP(smtp_host, smtp_port, timeout=15)
            server.ehlo()
            server.starttls()
            server.ehlo()

        server.login(smtp_user, smtp_pass)
        server.sendmail(from_email, [to_email], msg.as_string())
        server.quit()

        # Status aktualisieren
        now = datetime.utcnow().isoformat()
        if email_id:
            db.update_email(email_id, user_id, status="sent", sent_at=now)
        db.increment_user_emails(user_id)

        log.info("E-Mail gesendet an %s (Tracking: %s)", to_email, tracking_id)
        return True, "", email_id

    except smtplib.SMTPAuthenticationError:
        error = "SMTP-Authentifizierung fehlgeschlagen — Zugangsdaten pruefen"
        if email_id:
            db.update_email(email_id, user_id, status="failed", error_message=error)
        return False, error, email_id

    except smtplib.SMTPRecipientsRefused:
        error = f"Empfaenger abgelehnt: {to_email}"
        if email_id:
            db.update_email(email_id, user_id, status="bounced",
                            error_message=error, bounced=1, bounce_reason="recipient_refused")
        return False, error, email_id

    except (smtplib.SMTPException, OSError) as exc:
        error = f"SMTP-Fehler: {str(exc)}"
        if email_id:
            db.update_email(email_id, user_id, status="failed", error_message=error)
        log.error("E-Mail-Versand fehlgeschlagen: %s", exc)
        return False, error, email_id


def test_smtp_connection(smtp_host: str, smtp_port: int,
                         smtp_user: str, smtp_pass: str) -> Tuple[bool, str]:
    """Testet die SMTP-Verbindung."""
    try:
        if smtp_port == 465:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=10)
        else:
            server = smtplib.SMTP(smtp_host, smtp_port, timeout=10)
            server.ehlo()
            server.starttls()
            server.ehlo()
        server.login(smtp_user, smtp_pass)
        server.quit()
        return True, "Verbindung erfolgreich"
    except smtplib.SMTPAuthenticationError:
        return False, "Authentifizierung fehlgeschlagen"
    except (smtplib.SMTPException, OSError) as exc:
        return False, f"Verbindungsfehler: {str(exc)}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  IMAP ANTWORT-ERKENNUNG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def check_replies(user_id: int) -> List[Dict[str, Any]]:
    """Prueft per IMAP auf Antworten. Gibt gefundene Replies zurueck."""
    user = db.get_user_with_plan(user_id)
    if not user or not user.get("imap_host"):
        return []

    imap_host = user["imap_host"]
    imap_port = user.get("imap_port", 993)
    imap_user = user["imap_user"]
    imap_pass = decrypt_smtp_password(user.get("imap_pass_encrypted", ""))

    if not imap_pass:
        return []

    replies: List[Dict[str, Any]] = []

    try:
        mail = imaplib.IMAP4_SSL(imap_host, imap_port)
        mail.login(imap_user, imap_pass)
        mail.select("INBOX")

        # Suche nach Antworten der letzten 7 Tage
        since_date = (datetime.utcnow() - timedelta(days=7)).strftime("%d-%b-%Y")
        status, data = mail.search(None, f'(SINCE {since_date})')
        if status != "OK":
            mail.logout()
            return replies

        for num in data[0].split():
            status, msg_data = mail.fetch(num, "(RFC822)")
            if status != "OK":
                continue
            raw_email = msg_data[0][1]
            msg = email_lib.message_from_bytes(raw_email)

            subject = _decode_header(msg.get("Subject", ""))
            from_addr = _extract_email_addr(msg.get("From", ""))
            date_str = msg.get("Date", "")

            # Pruefen ob es eine Antwort ist (Re: / AW:)
            if not re.match(r'^(Re:|AW:|Aw:|RE:)', subject, re.IGNORECASE):
                continue

            # Pruefen ob der Absender ein bekannter Lead ist
            sent_emails = db.get_db().execute(
                "SELECT * FROM emails WHERE user_id = ? AND to_email = ? AND status = 'sent'",
                (user_id, from_addr),
            ).fetchall()

            if sent_emails:
                sent_email = dict(sent_emails[0])
                now = datetime.utcnow().isoformat()

                # E-Mail als beantwortet markieren
                db.update_email(sent_email["id"], user_id,
                                replied=1, replied_at=now)

                # Lead als responded markieren + Kanban verschieben
                if sent_email.get("lead_id"):
                    db.update_lead(sent_email["lead_id"], user_id,
                                   responded=1, responded_date=now,
                                   kanban_stage="kontaktiert")
                    # Follow-Ups abbrechen
                    db.cancel_followups_for_lead(user_id, sent_email["lead_id"])

                replies.append({
                    "from": from_addr,
                    "subject": subject,
                    "date": date_str,
                    "email_id": sent_email["id"],
                    "lead_id": sent_email.get("lead_id"),
                })

        mail.logout()

    except (imaplib.IMAP4.error, OSError) as exc:
        log.error("IMAP-Fehler: %s", exc)

    return replies


def check_bounces(user_id: int) -> List[Dict[str, Any]]:
    """Prueft per IMAP auf Bounces. Gibt gefundene Bounces zurueck."""
    user = db.get_user_with_plan(user_id)
    if not user or not user.get("imap_host"):
        return []

    imap_host = user["imap_host"]
    imap_port = user.get("imap_port", 993)
    imap_user = user["imap_user"]
    imap_pass = decrypt_smtp_password(user.get("imap_pass_encrypted", ""))

    if not imap_pass:
        return []

    bounces: List[Dict[str, Any]] = []

    try:
        mail = imaplib.IMAP4_SSL(imap_host, imap_port)
        mail.login(imap_user, imap_pass)
        mail.select("INBOX")

        since_date = (datetime.utcnow() - timedelta(days=7)).strftime("%d-%b-%Y")
        status, data = mail.search(None, f'(SINCE {since_date} FROM "mailer-daemon")')
        if status != "OK":
            mail.logout()
            return bounces

        for num in data[0].split():
            status, msg_data = mail.fetch(num, "(RFC822)")
            if status != "OK":
                continue
            raw_email = msg_data[0][1]
            msg = email_lib.message_from_bytes(raw_email)

            body_text = _get_email_body(msg)
            bounce_email = _extract_bounced_email(body_text)
            reason = _extract_bounce_reason(body_text)

            if bounce_email:
                # Zugehoerige gesendete E-Mail finden
                sent_email = db.get_db().execute(
                    "SELECT * FROM emails WHERE user_id = ? AND to_email = ? AND status = 'sent' ORDER BY sent_at DESC LIMIT 1",
                    (user_id, bounce_email),
                ).fetchone()

                if sent_email:
                    sent = dict(sent_email)
                    db.update_email(sent["id"], user_id,
                                    bounced=1, bounce_reason=reason, status="bounced")

                # Bei "user unknown" auf Blacklist
                if "user unknown" in reason.lower() or "does not exist" in reason.lower():
                    db.add_to_blacklist(user_id, bounce_email, f"Bounce: {reason}")

                bounces.append({
                    "email": bounce_email,
                    "reason": reason,
                    "email_id": dict(sent_email)["id"] if sent_email else None,
                })

        mail.logout()

    except (imaplib.IMAP4.error, OSError) as exc:
        log.error("IMAP-Bounce-Check-Fehler: %s", exc)

    return bounces


def test_imap_connection(imap_host: str, imap_port: int,
                         imap_user: str, imap_pass: str) -> Tuple[bool, str]:
    """Testet die IMAP-Verbindung."""
    try:
        mail = imaplib.IMAP4_SSL(imap_host, imap_port)
        mail.login(imap_user, imap_pass)
        mail.select("INBOX")
        status, data = mail.search(None, "ALL")
        count = len(data[0].split()) if data[0] else 0
        mail.logout()
        return True, f"Verbindung erfolgreich ({count} E-Mails im Posteingang)"
    except imaplib.IMAP4.error as exc:
        return False, f"IMAP-Authentifizierung fehlgeschlagen: {exc}"
    except (OSError, TimeoutError) as exc:
        return False, f"Verbindungsfehler: {exc}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  WARMUP-SYSTEM
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_warmup_limit(user_id: int) -> int:
    """Berechnet das aktuelle Warmup-Tageslimit.
    Tag 1-3: 5/Tag, 4-7: 10, 8-14: 20, 15-21: 40, ab 22: voll (Plan-Limit).
    """
    user = db.get_user_with_plan(user_id)
    if not user or not user.get("smtp_user"):
        return 0

    warmup = db.get_warmup_status(user_id, user["smtp_user"])
    if not warmup:
        # Warmup initialisieren
        db.upsert_warmup_status(user_id, user["smtp_user"], day_number=1, daily_limit=5)
        return 5

    day = warmup.get("day_number", 1)
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # Tag hochzaehlen wenn neuer Tag
    if warmup.get("last_send_date") and warmup["last_send_date"] != today:
        day += 1
        db.upsert_warmup_status(user_id, user["smtp_user"],
                                day_number=day, emails_sent_today=0,
                                last_send_date=today)

    plan_limit = user.get("emails_per_day", 10)

    if day <= 3:
        return min(5, plan_limit)
    elif day <= 7:
        return min(10, plan_limit)
    elif day <= 14:
        return min(20, plan_limit)
    elif day <= 21:
        return min(40, plan_limit)
    else:
        return plan_limit


def get_warmup_info(user_id: int) -> Dict[str, Any]:
    """Gibt Warmup-Status-Info zurueck."""
    user = db.get_user_with_plan(user_id)
    if not user or not user.get("smtp_user"):
        return {"day": 0, "limit": 0, "sent_today": 0, "reputation": 50}

    warmup = db.get_warmup_status(user_id, user["smtp_user"])
    if not warmup:
        return {"day": 1, "limit": 5, "sent_today": 0, "reputation": 50}

    return {
        "day": warmup.get("day_number", 1),
        "limit": get_warmup_limit(user_id),
        "sent_today": warmup.get("emails_sent_today", 0),
        "reputation": warmup.get("reputation_score", 50),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  VERSANDZEIT-OPTIMIERUNG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_optimal_send_time(index: int = 0) -> datetime:
    """Berechnet die optimale Sendezeit (Di-Do 9-11 Uhr, gestaffelt).
    index: Position in der Sende-Warteschlange (fuer Staffelung).
    """
    now = datetime.utcnow()
    # Naechsten Di/Mi/Do finden
    target = now
    while target.weekday() not in (1, 2, 3):  # Di=1, Mi=2, Do=3
        target += timedelta(days=1)
    # Basis: 9:00 Uhr + Staffelung
    target = target.replace(hour=9, minute=0, second=0, microsecond=0)
    # Staffelung: alle 3 Minuten ein Versand
    target += timedelta(minutes=index * 3)
    # Nicht nach 11 Uhr
    if target.hour >= 11:
        target += timedelta(days=1)
        while target.weekday() not in (1, 2, 3):
            target += timedelta(days=1)
        target = target.replace(hour=9, minute=0)
    # Wenn in der Vergangenheit, naechste Woche
    if target <= now:
        target += timedelta(weeks=1)
        while target.weekday() not in (1, 2, 3):
            target += timedelta(days=1)
        target = target.replace(hour=9, minute=0)
    return target


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FOLLOW-UP SCHEDULING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FOLLOWUP_DELAYS = {1: 3, 2: 7, 3: 14}  # Tage nach Erst-E-Mail


def schedule_followups(user_id: int, lead_id: int, original_email_id: int) -> List[int]:
    """Plant Follow-Ups fuer einen Lead (3/7/14 Tage). Gibt Follow-Up-IDs zurueck."""
    ids = []
    now = datetime.utcnow()
    for number, days in FOLLOWUP_DELAYS.items():
        scheduled = (now + timedelta(days=days)).isoformat()
        fid = db.create_followup(user_id, lead_id, original_email_id, number, scheduled)
        if fid:
            ids.append(fid)
    return ids


def schedule_email(user_id: int, to_email: str, subject: str, body: str,
                   lead_id: int = None, send_at: str = None,
                   template_key: str = "allgemein") -> Optional[int]:
    """Plant eine E-Mail fuer spaeteren Versand."""
    if not send_at:
        send_at = get_optimal_send_time().isoformat()

    tracking_id = uuid.uuid4().hex[:16]
    email_id = db.create_email_record(user_id, {
        "lead_id": lead_id,
        "to_email": to_email,
        "subject": subject,
        "body": body,
        "template_key": template_key,
        "tracking_id": tracking_id,
        "status": "pending",
        "scheduled_at": send_at,
    })
    return email_id


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PROVIDER-LIMITS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PROVIDER_LIMITS: Dict[str, int] = {
    "gmail.com": 500,
    "googlemail.com": 500,
    "gmx.de": 100,
    "gmx.net": 100,
    "web.de": 100,
    "t-online.de": 100,
    "outlook.com": 300,
    "hotmail.com": 300,
    "yahoo.com": 500,
}


def get_provider_limit(smtp_user: str) -> int:
    """Gibt das Provider-Tageslimit zurueck."""
    domain = smtp_user.split("@")[1].lower() if "@" in smtp_user else ""
    return PROVIDER_LIMITS.get(domain, 500)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HILFSFUNKTIONEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _decode_header(header_value: str) -> str:
    """Dekodiert einen E-Mail-Header."""
    if not header_value:
        return ""
    decoded_parts = email_lib.header.decode_header(header_value)
    parts = []
    for content, charset in decoded_parts:
        if isinstance(content, bytes):
            parts.append(content.decode(charset or "utf-8", errors="replace"))
        else:
            parts.append(content)
    return " ".join(parts)


def _extract_email_addr(from_header: str) -> str:
    """Extrahiert die E-Mail-Adresse aus einem From-Header."""
    match = re.search(r'<([^>]+)>', from_header)
    if match:
        return match.group(1).lower()
    match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', from_header)
    if match:
        return match.group(0).lower()
    return from_header.lower().strip()


def _get_email_body(msg) -> str:
    """Extrahiert den Body einer E-Mail."""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace")
    return ""


def _extract_bounced_email(text: str) -> Optional[str]:
    """Extrahiert die gebouncte E-Mail-Adresse aus einem Bounce-Text."""
    patterns = [
        re.compile(r'Original-Recipient:.*?<([^>]+)>', re.IGNORECASE),
        re.compile(r'Final-Recipient:.*?;\s*(\S+@\S+)', re.IGNORECASE),
        re.compile(r'<(\S+@\S+)>\s*was\s+not\s+delivered', re.IGNORECASE),
        re.compile(r'delivery\s+to\s+(\S+@\S+)\s+failed', re.IGNORECASE),
    ]
    for pat in patterns:
        match = pat.search(text)
        if match:
            return match.group(1).lower().strip()
    return None


def _extract_bounce_reason(text: str) -> str:
    """Extrahiert den Bounce-Grund aus einem Bounce-Text."""
    reasons = [
        ("user unknown", "user unknown"),
        ("does not exist", "mailbox does not exist"),
        ("mailbox full", "mailbox full"),
        ("spam", "rejected as spam"),
        ("blocked", "blocked"),
        ("quota", "over quota"),
        ("550", "permanent failure (550)"),
        ("553", "address rejected (553)"),
    ]
    text_lower = text.lower()
    for indicator, reason in reasons:
        if indicator in text_lower:
            return reason
    return "unknown bounce reason"
