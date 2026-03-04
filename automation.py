"""
automation.py — Hintergrund-Scheduler, Webhooks, Telegram-Benachrichtigungen

Automatisierung fuer LeadFinder Pro v3:
- Daemon-Thread Scheduler: Follow-Ups, geplante E-Mails, IMAP-Checks, Zaehler-Resets
- Webhook-System mit HMAC-Signatur, Retry-Logik und Auto-Deaktivierung
- Telegram-Benachrichtigungen fuer Opens, Replies, Follow-Ups
"""

import json
import hmac
import hashlib
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

import requests

import database as db
import email_engine

log = logging.getLogger("leadfinder.automation")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  KONSTANTEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SCHEDULER_INTERVAL = 60          # Sekunden zwischen Scheduler-Durchlaeufen
WEBHOOK_TIMEOUT = 10             # Sekunden Timeout fuer Webhook-Requests
WEBHOOK_MAX_RETRIES = 3          # Wiederholungsversuche pro Ausloesung
WEBHOOK_RETRY_DELAY = 2          # Basis-Sekunden zwischen Retries (exponentiell)
WEBHOOK_FAIL_THRESHOLD = 10      # Konsekutive Fehler bis Auto-Deaktivierung
TELEGRAM_API_BASE = "https://api.telegram.org"
TELEGRAM_TIMEOUT = 10            # Sekunden Timeout fuer Telegram-Requests

VALID_WEBHOOK_EVENTS = {
    "email.opened",
    "email.replied",
    "lead.created",
    "lead.converted",
    "followup.due",
}

# Globaler Flag um den Scheduler sauber zu stoppen
_scheduler_stop_event = threading.Event()
_scheduler_thread: Optional[threading.Thread] = None
_scheduler_last_daily_reset: Optional[str] = None
_scheduler_last_monthly_reset: Optional[str] = None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SCHEDULER — HAUPTSCHLEIFE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_scheduler() -> None:
    """Startet den Scheduler als Daemon-Thread.

    Der Thread laeuft im Hintergrund und fuehrt alle 60 Sekunden folgende
    Aufgaben aus:
      1. Faellige Follow-Ups generieren und versenden
      2. Geplante E-Mails versenden
      3. IMAP-Reply-Checks fuer berechtigte User
      4. IMAP-Bounce-Checks fuer berechtigte User
      5. Taegliche E-Mail-Zaehler zuruecksetzen (Mitternacht)
      6. Monatliche Such-Zaehler zuruecksetzen (1. des Monats)
    """
    global _scheduler_thread

    if _scheduler_thread is not None and _scheduler_thread.is_alive():
        log.warning("Scheduler laeuft bereits — kein neuer Start.")
        return

    _scheduler_stop_event.clear()

    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        name="LeadFinderScheduler",
        daemon=True,
    )
    _scheduler_thread.start()
    log.info("Scheduler-Thread gestartet (Intervall: %ds).", SCHEDULER_INTERVAL)


def stop_scheduler() -> None:
    """Signalisiert dem Scheduler-Thread, sich zu beenden."""
    global _scheduler_thread
    _scheduler_stop_event.set()
    if _scheduler_thread is not None and _scheduler_thread.is_alive():
        _scheduler_thread.join(timeout=SCHEDULER_INTERVAL + 5)
        log.info("Scheduler-Thread beendet.")
    _scheduler_thread = None


def _scheduler_loop() -> None:
    """Interne Schleife — wird im Daemon-Thread ausgefuehrt."""
    log.info("Scheduler-Schleife gestartet.")

    while not _scheduler_stop_event.is_set():
        cycle_start = time.monotonic()

        try:
            _run_scheduler_cycle()
        except Exception as exc:
            log.exception("Unbehandelter Fehler im Scheduler-Zyklus: %s", exc)

        # Warte bis zum naechsten Zyklus (abzueglich Ausfuehrungsdauer)
        elapsed = time.monotonic() - cycle_start
        remaining = max(0.0, SCHEDULER_INTERVAL - elapsed)
        if remaining > 0:
            _scheduler_stop_event.wait(timeout=remaining)

    # Thread-lokale DB-Verbindung schliessen
    try:
        db.close_db()
    except Exception:
        pass

    log.info("Scheduler-Schleife beendet.")


def _run_scheduler_cycle() -> None:
    """Ein einzelner Durchlauf aller Scheduler-Aufgaben."""
    log.debug("Scheduler-Zyklus gestartet.")

    # 1. Faellige Follow-Ups verarbeiten
    process_followups()

    # 2. Geplante E-Mails versenden
    process_scheduled_emails()

    # 3. IMAP-Reply-Checks
    process_reply_checks()

    # 4. IMAP-Bounce-Checks
    process_bounce_checks()

    # 5. Taegliche Zaehler-Resets
    reset_daily_counters()

    # 6. Monatliche Zaehler-Resets
    reset_monthly_counters()

    log.debug("Scheduler-Zyklus abgeschlossen.")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SCHEDULER-AUFGABEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_followups() -> int:
    """Findet faellige Follow-Ups, generiert den Inhalt und versendet sie.

    Fuer jeden faelligen Follow-Up wird:
      - Der zugehoerige Lead und User geladen
      - Der Follow-Up-Text aus dem Template generiert
      - Die E-Mail via email_engine.send_email() gesendet
      - Der Follow-Up-Status aktualisiert
      - Ein Webhook (followup.due) und ggf. eine Telegram-Benachrichtigung ausgeloest

    Returns:
        Anzahl erfolgreich versendeter Follow-Ups.
    """
    pending = db.get_pending_followups()
    if not pending:
        return 0

    sent_count = 0
    log.info("Verarbeite %d faellige Follow-Ups.", len(pending))

    for followup in pending:
        followup_id = followup["id"]
        user_id = followup["user_id"]
        lead_id = followup["lead_id"]
        original_email_id = followup.get("original_email_id")
        followup_number = followup.get("followup_number", 1)
        lead_email = followup.get("lead_email", "")
        lead_name = followup.get("lead_name", "")

        if not lead_email:
            log.warning("Follow-Up %d: Lead %d hat keine E-Mail — uebersprungen.",
                        followup_id, lead_id)
            db.update_followup(followup_id, status="skipped")
            continue

        # User und Lead laden
        user = db.get_user_with_plan(user_id)
        if not user:
            log.warning("Follow-Up %d: User %d nicht gefunden — uebersprungen.",
                        followup_id, user_id)
            db.update_followup(followup_id, status="skipped")
            continue

        # Pruefen ob Follow-Ups im Plan enthalten sind
        if not user.get("has_followups"):
            log.debug("Follow-Up %d: User %d hat keine Follow-Up-Berechtigung.",
                      followup_id, user_id)
            db.update_followup(followup_id, status="skipped")
            continue

        lead = db.get_lead(lead_id, user_id)
        if not lead:
            log.warning("Follow-Up %d: Lead %d nicht gefunden — uebersprungen.",
                        followup_id, lead_id)
            db.update_followup(followup_id, status="skipped")
            continue

        # Pruefen ob Lead bereits geantwortet hat
        if lead.get("responded"):
            log.info("Follow-Up %d: Lead %d hat bereits geantwortet — abbrechen.",
                     followup_id, lead_id)
            db.update_followup(followup_id, status="cancelled")
            continue

        # Original-Betreff holen fuer Re:-Subject
        original_subject = ""
        if original_email_id:
            original_email = db.get_email_by_id(original_email_id, user_id)
            if original_email:
                original_subject = original_email.get("subject", "")

        # E-Mail-Inhalt generieren
        content = email_engine.generate_email_content(
            lead=lead,
            template_key="allgemein",
            user=user,
            followup_number=followup_number,
            original_subject=original_subject,
        )

        # Tageslimit pruefen
        daily_limit = user.get("emails_per_day", 10)
        emails_today = user.get("emails_today", 0)
        emails_today_date = user.get("emails_today_date", "")
        today_str = datetime.utcnow().strftime("%Y-%m-%d")

        if emails_today_date == today_str and emails_today >= daily_limit:
            log.info("Follow-Up %d: User %d hat Tageslimit erreicht (%d/%d).",
                     followup_id, user_id, emails_today, daily_limit)
            continue  # Wird beim naechsten Zyklus erneut versucht

        # Warmup-Limit pruefen
        warmup_limit = email_engine.get_warmup_limit(user_id)
        if emails_today_date == today_str and emails_today >= warmup_limit:
            log.info("Follow-Up %d: User %d hat Warmup-Limit erreicht (%d/%d).",
                     followup_id, user_id, emails_today, warmup_limit)
            continue

        # E-Mail senden
        success, error_msg, email_id = email_engine.send_email(
            user_id=user_id,
            to_email=lead_email,
            subject=content["subject"],
            body=content["body"],
            lead_id=lead_id,
            template_key="allgemein",
            followup_number=followup_number,
        )

        if success:
            now = datetime.utcnow().isoformat()
            db.update_followup(
                followup_id,
                status="sent",
                sent_email_id=email_id,
                sent_at=now,
            )
            sent_count += 1
            log.info("Follow-Up %d (#%d) fuer Lead '%s' (%s) gesendet.",
                     followup_id, followup_number, lead_name, lead_email)

            # Webhook ausloesen
            trigger_webhook(user_id, "followup.due", {
                "followup_id": followup_id,
                "followup_number": followup_number,
                "lead_id": lead_id,
                "lead_name": lead_name,
                "lead_email": lead_email,
                "email_id": email_id,
                "sent_at": now,
            })

            # Telegram-Benachrichtigung
            notify_user_telegram(
                user_id,
                "Follow-Up #{num} gesendet an {name} ({email})".format(
                    num=followup_number,
                    name=lead_name,
                    email=lead_email,
                ),
            )
        else:
            db.update_followup(followup_id, status="failed")
            log.error("Follow-Up %d fehlgeschlagen: %s", followup_id, error_msg)

    if sent_count > 0:
        log.info("Follow-Up-Verarbeitung abgeschlossen: %d/%d gesendet.",
                 sent_count, len(pending))

    return sent_count


def process_scheduled_emails() -> int:
    """Versendet faellige geplante E-Mails.

    Sucht E-Mails mit status='pending' und scheduled_at <= jetzt,
    versendet sie ueber email_engine und aktualisiert den Status.

    Returns:
        Anzahl erfolgreich versendeter E-Mails.
    """
    pending = db.get_pending_scheduled_emails()
    if not pending:
        return 0

    sent_count = 0
    log.info("Verarbeite %d geplante E-Mails.", len(pending))

    for email_record in pending:
        email_id = email_record["id"]
        user_id = email_record["user_id"]
        to_email = email_record.get("to_email", "")
        subject = email_record.get("subject", "")
        body = email_record.get("body", "")
        lead_id = email_record.get("lead_id")
        template_key = email_record.get("template_key", "allgemein")
        followup_number = email_record.get("followup_number", 0)

        if not to_email:
            log.warning("Geplante E-Mail %d: Kein Empfaenger — uebersprungen.", email_id)
            db.update_email(email_id, user_id, status="skipped",
                            error_message="Kein Empfaenger")
            continue

        # User laden und Berechtigung pruefen
        user = db.get_user_with_plan(user_id)
        if not user:
            log.warning("Geplante E-Mail %d: User %d nicht gefunden.", email_id, user_id)
            db.update_email(email_id, user_id, status="failed",
                            error_message="User nicht gefunden")
            continue

        # Tageslimit pruefen
        daily_limit = user.get("emails_per_day", 10)
        emails_today = user.get("emails_today", 0)
        emails_today_date = user.get("emails_today_date", "")
        today_str = datetime.utcnow().strftime("%Y-%m-%d")

        if emails_today_date == today_str and emails_today >= daily_limit:
            log.info("Geplante E-Mail %d: User %d hat Tageslimit erreicht (%d/%d) — "
                     "wird spaeter erneut versucht.", email_id, user_id,
                     emails_today, daily_limit)
            continue

        # Warmup-Limit pruefen
        warmup_limit = email_engine.get_warmup_limit(user_id)
        if emails_today_date == today_str and emails_today >= warmup_limit:
            log.info("Geplante E-Mail %d: User %d hat Warmup-Limit erreicht.",
                     email_id, user_id)
            continue

        # Diese E-Mail ist bereits als Record in der DB — wir muessen sie direkt
        # ueber SMTP senden, ohne einen neuen Record zu erstellen.
        # Dazu nutzen wir die send_email-Funktion, die den bestehenden Record aktualisiert.
        # Aber send_email erstellt einen neuen Record. Daher aktualisieren wir den
        # bestehenden Record manuell und senden direkt.
        success, error_msg = _send_scheduled_email_direct(user, email_record)

        if success:
            now = datetime.utcnow().isoformat()
            db.update_email(email_id, user_id, status="sent", sent_at=now)
            db.increment_user_emails(user_id)
            sent_count += 1
            log.info("Geplante E-Mail %d an %s gesendet.", email_id, to_email)
        else:
            db.update_email(email_id, user_id, status="failed",
                            error_message=error_msg)
            log.error("Geplante E-Mail %d fehlgeschlagen: %s", email_id, error_msg)

    if sent_count > 0:
        log.info("Geplante E-Mails abgeschlossen: %d/%d gesendet.",
                 sent_count, len(pending))

    return sent_count


def _send_scheduled_email_direct(
    user: Dict[str, Any],
    email_record: Dict[str, Any],
) -> tuple:
    """Sendet eine bereits in der DB existierende geplante E-Mail direkt via SMTP.

    Im Gegensatz zu email_engine.send_email() wird hier kein neuer DB-Record erstellt,
    da dieser bereits existiert.

    Returns:
        (success: bool, error_message: str)
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    from auth import decrypt_smtp_password

    smtp_host = user.get("smtp_host", "")
    smtp_port = user.get("smtp_port", 587)
    smtp_user = user.get("smtp_user", "")
    smtp_pass = decrypt_smtp_password(user.get("smtp_pass_encrypted", ""))
    from_name = user.get("smtp_from_name") or user.get("name", "")
    from_email = user.get("smtp_from_email") or smtp_user

    if not smtp_host or not smtp_user or not smtp_pass:
        return False, "SMTP-Einstellungen nicht konfiguriert"

    to_email = email_record.get("to_email", "")
    subject = email_record.get("subject", "")
    body = email_record.get("body", "")
    body_html = email_record.get("body_html", "")

    # Falls kein HTML-Body vorhanden, einfachen Text verwenden
    if not body_html and body:
        tracking_id = email_record.get("tracking_id", "")
        tracking_pixel_url = ""
        if tracking_id:
            tracking_pixel_url = f"http://localhost:5000/t/{tracking_id}/open.png"
        body_html = email_engine._create_html_body(body, tracking_pixel_url)

    # E-Mail zusammenbauen
    msg = MIMEMultipart("alternative")
    msg["From"] = f"{from_name} <{from_email}>" if from_name else from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg["X-Mailer"] = "LeadFinder Pro v3"

    msg.attach(MIMEText(body, "plain", "utf-8"))
    if body_html:
        msg.attach(MIMEText(body_html, "html", "utf-8"))

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

        log.debug("SMTP-Versand erfolgreich: %s -> %s", from_email, to_email)
        return True, ""

    except smtplib.SMTPAuthenticationError:
        return False, "SMTP-Authentifizierung fehlgeschlagen"
    except smtplib.SMTPRecipientsRefused:
        return False, f"Empfaenger abgelehnt: {to_email}"
    except (smtplib.SMTPException, OSError) as exc:
        return False, f"SMTP-Fehler: {str(exc)}"


def process_reply_checks() -> int:
    """Fuehrt IMAP-Reply-Checks fuer alle berechtigten User durch.

    Nutzer muessen:
      - IMAP-Zugangsdaten konfiguriert haben (imap_host gesetzt)
      - Einen Plan mit has_imap=1 besitzen

    Fuer jede gefundene Antwort werden Webhook und Telegram ausgeloest.

    Returns:
        Gesamtzahl gefundener Antworten.
    """
    users_with_imap = _get_users_with_imap()
    if not users_with_imap:
        return 0

    total_replies = 0

    for user_row in users_with_imap:
        user_id = user_row["id"]

        try:
            replies = email_engine.check_replies(user_id)
        except Exception as exc:
            log.error("Reply-Check fuer User %d fehlgeschlagen: %s", user_id, exc)
            continue

        for reply in replies:
            total_replies += 1
            from_addr = reply.get("from", "")
            subject = reply.get("subject", "")
            email_id = reply.get("email_id")
            lead_id = reply.get("lead_id")

            log.info("Antwort von %s fuer User %d erkannt (Subject: %s).",
                     from_addr, user_id, subject)

            # Webhook ausloesen
            trigger_webhook(user_id, "email.replied", {
                "from_email": from_addr,
                "subject": subject,
                "email_id": email_id,
                "lead_id": lead_id,
                "detected_at": datetime.utcnow().isoformat(),
            })

            # Telegram-Benachrichtigung
            notify_user_telegram(
                user_id,
                "Antwort erhalten von {addr}\nBetreff: {subj}".format(
                    addr=from_addr,
                    subj=subject,
                ),
            )

    if total_replies > 0:
        log.info("Reply-Checks abgeschlossen: %d Antworten bei %d Usern gefunden.",
                 total_replies, len(users_with_imap))

    return total_replies


def process_bounce_checks() -> int:
    """Fuehrt IMAP-Bounce-Checks fuer alle berechtigten User durch.

    Gleiche Berechtigungspruefung wie bei Reply-Checks.

    Returns:
        Gesamtzahl gefundener Bounces.
    """
    users_with_imap = _get_users_with_imap()
    if not users_with_imap:
        return 0

    total_bounces = 0

    for user_row in users_with_imap:
        user_id = user_row["id"]

        try:
            bounces = email_engine.check_bounces(user_id)
        except Exception as exc:
            log.error("Bounce-Check fuer User %d fehlgeschlagen: %s", user_id, exc)
            continue

        for bounce in bounces:
            total_bounces += 1
            bounce_email = bounce.get("email", "")
            reason = bounce.get("reason", "")
            email_id = bounce.get("email_id")

            log.info("Bounce erkannt: %s (Grund: %s) fuer User %d.",
                     bounce_email, reason, user_id)

    if total_bounces > 0:
        log.info("Bounce-Checks abgeschlossen: %d Bounces bei %d Usern gefunden.",
                 total_bounces, len(users_with_imap))

    return total_bounces


def reset_daily_counters() -> bool:
    """Setzt die taeglichen E-Mail-Zaehler um Mitternacht (UTC) zurueck.

    Prueft ob Mitternacht ueberschritten wurde seit dem letzten Reset.
    Aktualisiert emails_today auf 0 fuer alle User, deren emails_today_date
    nicht dem heutigen Datum entspricht.

    Returns:
        True wenn ein Reset durchgefuehrt wurde, False sonst.
    """
    global _scheduler_last_daily_reset

    now = datetime.utcnow()
    today_str = now.strftime("%Y-%m-%d")

    # Nur einmal pro Tag zuruecksetzen
    if _scheduler_last_daily_reset == today_str:
        return False

    # Pruefen ob wir in der Mitternachtsstunde sind (00:00 - 00:01)
    # oder ob ein Reset noch nie stattgefunden hat
    is_midnight_window = (now.hour == 0 and now.minute < 2)
    is_first_run = (_scheduler_last_daily_reset is None)

    if not is_midnight_window and not is_first_run:
        return False

    try:
        conn = db.get_db()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET emails_today = 0 WHERE emails_today_date != ? OR emails_today_date = ''",
            (today_str,),
        )
        affected = cursor.rowcount
        conn.commit()

        _scheduler_last_daily_reset = today_str

        if affected > 0:
            log.info("Taegliche E-Mail-Zaehler zurueckgesetzt (%d User).", affected)
        return True

    except Exception as exc:
        log.error("Fehler beim Reset der Tageszaehler: %s", exc)
        return False


def reset_monthly_counters() -> bool:
    """Setzt die monatlichen Such-Zaehler am 1. des Monats zurueck.

    Nutzt db.reset_monthly_searches() fuer den eigentlichen Reset.

    Returns:
        True wenn ein Reset durchgefuehrt wurde, False sonst.
    """
    global _scheduler_last_monthly_reset

    now = datetime.utcnow()
    month_str = now.strftime("%Y-%m")

    # Nur einmal pro Monat zuruecksetzen
    if _scheduler_last_monthly_reset == month_str:
        return False

    # Nur am 1. des Monats (in der ersten Stunde)
    is_first_of_month = (now.day == 1 and now.hour == 0 and now.minute < 2)
    is_first_run = (_scheduler_last_monthly_reset is None)

    if not is_first_of_month and not is_first_run:
        return False

    try:
        db.reset_monthly_searches()
        _scheduler_last_monthly_reset = month_str
        log.info("Monatliche Such-Zaehler zurueckgesetzt (Monat: %s).", month_str)
        return True

    except Exception as exc:
        log.error("Fehler beim Reset der Monatszaehler: %s", exc)
        return False


def _get_users_with_imap() -> List[Dict[str, Any]]:
    """Gibt alle User zurueck, die IMAP konfiguriert und berechtigt haben.

    Kriterien:
      - imap_host ist nicht leer
      - Plan hat has_imap = 1

    Returns:
        Liste von User-Dicts mit mindestens id, imap_host.
    """
    try:
        rows = db.get_db().execute(
            """SELECT u.id, u.imap_host, u.imap_port, u.imap_user,
                      u.imap_pass_encrypted, u.telegram_chat_id,
                      u.telegram_bot_token, u.telegram_enabled
               FROM users u
               JOIN plans p ON u.plan_id = p.id
               WHERE u.imap_host != '' AND u.imap_user != ''
                     AND p.has_imap = 1""",
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        log.error("Fehler beim Laden der IMAP-User: %s", exc)
        return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  WEBHOOKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def trigger_webhook(user_id: int, event_type: str, payload: Dict[str, Any]) -> int:
    """Loest alle aktiven Webhooks eines Users fuer einen Event-Typ aus.

    Fuer jeden registrierten Webhook wird ein POST-Request mit JSON-Payload
    gesendet. Die Payload wird mit HMAC-SHA256 signiert (wenn ein Secret
    konfiguriert ist) und die Signatur im X-Webhook-Signature Header mitgegeben.

    Bei Fehlern wird bis zu WEBHOOK_MAX_RETRIES Mal wiederholt (mit exponentiell
    steigender Wartezeit). Nach WEBHOOK_FAIL_THRESHOLD konsekutiven Fehlern
    wird der Webhook automatisch deaktiviert.

    Args:
        user_id: ID des Nutzers
        event_type: Art des Events (z.B. 'email.opened', 'email.replied')
        payload: Daten die als JSON gesendet werden

    Returns:
        Anzahl erfolgreich zugestellter Webhooks.
    """
    if event_type not in VALID_WEBHOOK_EVENTS:
        log.warning("Ungueltiger Webhook-Event-Typ: %s", event_type)
        return 0

    # Aktive Webhooks fuer diesen Event-Typ laden
    webhooks = db.get_active_webhooks(user_id, event_type)
    if not webhooks:
        return 0

    # Envelope mit Metadaten erstellen
    envelope = {
        "event": event_type,
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "data": payload,
    }

    success_count = 0

    for webhook in webhooks:
        webhook_id = webhook["id"]
        url = webhook.get("url", "")
        secret = webhook.get("secret", "")
        current_fail_count = webhook.get("fail_count", 0)

        if not url:
            continue

        delivered = _deliver_webhook(url, secret, envelope)

        now = datetime.utcnow().isoformat()

        if delivered:
            # Erfolg: Zaehler zuruecksetzen
            db.update_webhook(webhook_id, user_id,
                              last_triggered=now,
                              fail_count=0)
            success_count += 1
            log.info("Webhook %d (%s) fuer User %d erfolgreich zugestellt.",
                     webhook_id, event_type, user_id)
        else:
            # Fehlschlag: Zaehler erhoehen
            new_fail_count = current_fail_count + 1
            update_kwargs: Dict[str, Any] = {
                "fail_count": new_fail_count,
                "last_triggered": now,
            }

            if new_fail_count >= WEBHOOK_FAIL_THRESHOLD:
                update_kwargs["active"] = 0
                log.warning(
                    "Webhook %d fuer User %d nach %d konsekutiven Fehlern deaktiviert.",
                    webhook_id, user_id, new_fail_count,
                )
                # User per Telegram informieren
                notify_user_telegram(
                    user_id,
                    "Webhook deaktiviert: {url}\n"
                    "Grund: {count} aufeinanderfolgende Fehler\n"
                    "Event: {event}".format(
                        url=url,
                        count=new_fail_count,
                        event=event_type,
                    ),
                )
            else:
                log.warning("Webhook %d Zustellung fehlgeschlagen (%d/%d Fehler).",
                            webhook_id, new_fail_count, WEBHOOK_FAIL_THRESHOLD)

            db.update_webhook(webhook_id, user_id, **update_kwargs)

    return success_count


def _deliver_webhook(url: str, secret: str, envelope: Dict[str, Any]) -> bool:
    """Sendet einen einzelnen Webhook-Request mit Retry-Logik.

    Args:
        url: Ziel-URL
        secret: HMAC-Shared-Secret (kann leer sein)
        envelope: JSON-Payload

    Returns:
        True wenn die Zustellung erfolgreich war (HTTP 2xx), False sonst.
    """
    body_bytes = json.dumps(envelope, ensure_ascii=False, default=str).encode("utf-8")

    # HMAC-SHA256 Signatur berechnen
    signature = _compute_hmac_signature(body_bytes, secret)

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "LeadFinderPro-Webhook/3.0",
        "X-Webhook-Event": envelope.get("event", ""),
        "X-Webhook-Timestamp": envelope.get("timestamp", ""),
        "X-Webhook-Signature": signature,
    }

    for attempt in range(1, WEBHOOK_MAX_RETRIES + 1):
        try:
            response = requests.post(
                url,
                data=body_bytes,
                headers=headers,
                timeout=WEBHOOK_TIMEOUT,
                allow_redirects=False,
            )

            if 200 <= response.status_code < 300:
                log.debug("Webhook an %s zugestellt (HTTP %d, Versuch %d).",
                          url, response.status_code, attempt)
                return True

            log.warning("Webhook an %s: HTTP %d (Versuch %d/%d).",
                        url, response.status_code, attempt, WEBHOOK_MAX_RETRIES)

        except requests.exceptions.Timeout:
            log.warning("Webhook an %s: Timeout (Versuch %d/%d).",
                        url, attempt, WEBHOOK_MAX_RETRIES)

        except requests.exceptions.ConnectionError:
            log.warning("Webhook an %s: Verbindungsfehler (Versuch %d/%d).",
                        url, attempt, WEBHOOK_MAX_RETRIES)

        except requests.exceptions.RequestException as exc:
            log.warning("Webhook an %s: Fehler %s (Versuch %d/%d).",
                        url, exc, attempt, WEBHOOK_MAX_RETRIES)

        # Exponentielles Backoff vor dem naechsten Versuch
        if attempt < WEBHOOK_MAX_RETRIES:
            delay = WEBHOOK_RETRY_DELAY * (2 ** (attempt - 1))
            time.sleep(delay)

    log.error("Webhook an %s fehlgeschlagen nach %d Versuchen.",
              url, WEBHOOK_MAX_RETRIES)
    return False


def _compute_hmac_signature(body_bytes: bytes, secret: str) -> str:
    """Berechnet die HMAC-SHA256 Signatur fuer den Webhook-Body.

    Args:
        body_bytes: JSON-Payload als Bytes
        secret: Shared Secret (wenn leer, wird ein leerer String verwendet)

    Returns:
        Hex-codierte HMAC-SHA256 Signatur mit 'sha256=' Praefix.
    """
    key = secret.encode("utf-8") if secret else b""
    mac = hmac.new(key, body_bytes, hashlib.sha256)
    return "sha256=" + mac.hexdigest()


def verify_webhook_signature(body_bytes: bytes, secret: str, signature: str) -> bool:
    """Verifiziert eine eingehende Webhook-Signatur.

    Kann von externen Systemen verwendet werden, die Webhooks von LeadFinder
    empfangen, um die Authentizitaet zu pruefen.

    Args:
        body_bytes: Empfangener Request-Body
        secret: Shared Secret
        signature: Empfangene Signatur aus dem X-Webhook-Signature Header

    Returns:
        True wenn die Signatur gueltig ist, False sonst.
    """
    expected = _compute_hmac_signature(body_bytes, secret)
    return hmac.compare_digest(expected, signature)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TELEGRAM-BENACHRICHTIGUNGEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def send_telegram(bot_token: str, chat_id: str, message: str) -> bool:
    """Sendet eine Nachricht ueber die Telegram Bot API.

    Args:
        bot_token: Telegram Bot Token (z.B. '123456:ABC-DEF...')
        chat_id: Telegram Chat-ID des Empfaengers
        message: Nachrichtentext (Markdown wird unterstuetzt)

    Returns:
        True wenn die Nachricht erfolgreich gesendet wurde, False sonst.
    """
    if not bot_token or not chat_id or not message:
        log.debug("Telegram-Versand uebersprungen: Fehlende Parameter "
                  "(token=%s, chat_id=%s, message_len=%d).",
                  bool(bot_token), bool(chat_id), len(message) if message else 0)
        return False

    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=TELEGRAM_TIMEOUT)

        if response.status_code == 200:
            result = response.json()
            if result.get("ok"):
                log.debug("Telegram-Nachricht an Chat %s gesendet.", chat_id)
                return True
            else:
                log.warning("Telegram API-Fehler: %s",
                            result.get("description", "Unbekannt"))
                return False
        else:
            log.warning("Telegram HTTP-Fehler %d fuer Chat %s.",
                        response.status_code, chat_id)

            # Bei Markdown-Fehler ohne Formatierung erneut versuchen
            if response.status_code == 400:
                return _send_telegram_plain(url, chat_id, message)

            return False

    except requests.exceptions.Timeout:
        log.warning("Telegram-Timeout fuer Chat %s.", chat_id)
        return False

    except requests.exceptions.ConnectionError:
        log.warning("Telegram-Verbindungsfehler fuer Chat %s.", chat_id)
        return False

    except requests.exceptions.RequestException as exc:
        log.error("Telegram-Fehler fuer Chat %s: %s", chat_id, exc)
        return False


def _send_telegram_plain(url: str, chat_id: str, message: str) -> bool:
    """Fallback: Sendet Telegram-Nachricht ohne Markdown-Formatierung.

    Wird aufgerufen wenn die Markdown-Formatierung einen Fehler verursacht hat.

    Args:
        url: Telegram API-URL (bereits zusammengebaut)
        chat_id: Telegram Chat-ID
        message: Nachrichtentext

    Returns:
        True wenn erfolgreich, False sonst.
    """
    payload = {
        "chat_id": chat_id,
        "text": message,
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=TELEGRAM_TIMEOUT)
        if response.status_code == 200 and response.json().get("ok"):
            log.debug("Telegram-Nachricht (plain) an Chat %s gesendet.", chat_id)
            return True
        return False
    except requests.exceptions.RequestException:
        return False


def notify_user_telegram(user_id: int, message: str) -> bool:
    """Convenience-Wrapper: Sendet eine Telegram-Benachrichtigung an einen User.

    Laedt die Telegram-Einstellungen des Users aus der Datenbank und sendet
    die Nachricht, falls Telegram aktiviert ist.

    Args:
        user_id: ID des Nutzers
        message: Nachrichtentext

    Returns:
        True wenn gesendet, False sonst (auch wenn Telegram nicht aktiviert).
    """
    if not message:
        return False

    user = db.get_user_with_plan(user_id)
    if not user:
        log.debug("Telegram-Nachricht fuer User %d: User nicht gefunden.", user_id)
        return False

    # Pruefen ob Telegram aktiviert ist
    if not user.get("telegram_enabled"):
        log.debug("Telegram fuer User %d nicht aktiviert.", user_id)
        return False

    bot_token = user.get("telegram_bot_token", "")
    chat_id = user.get("telegram_chat_id", "")

    if not bot_token or not chat_id:
        log.debug("Telegram fuer User %d: Token oder Chat-ID fehlen.", user_id)
        return False

    # Prefix mit App-Name hinzufuegen
    formatted_message = "*LeadFinder Pro*\n{msg}".format(msg=message)

    return send_telegram(bot_token, chat_id, formatted_message)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EVENT-HANDLER — Von anderen Modulen aufrufbar
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def on_email_opened(user_id: int, email_id: int, lead_id: Optional[int] = None,
                    lead_name: str = "", to_email: str = "",
                    tracking_id: str = "", ip_address: str = "") -> None:
    """Handler fuer E-Mail-Oeffnungen — loest Webhook und Telegram aus.

    Wird typischerweise vom Tracking-Endpoint aufgerufen wenn ein
    Tracking-Pixel geladen wird.

    Args:
        user_id: ID des Nutzers
        email_id: ID der geoeffneten E-Mail
        lead_id: ID des zugehoerigen Leads (optional)
        lead_name: Name des Leads (fuer Benachrichtigungen)
        to_email: E-Mail-Adresse des Empfaengers
        tracking_id: Tracking-ID der E-Mail
        ip_address: IP-Adresse die das Pixel geladen hat
    """
    # Webhook
    trigger_webhook(user_id, "email.opened", {
        "email_id": email_id,
        "lead_id": lead_id,
        "lead_name": lead_name,
        "to_email": to_email,
        "tracking_id": tracking_id,
        "ip_address": ip_address,
        "opened_at": datetime.utcnow().isoformat(),
    })

    # Telegram
    if lead_name and to_email:
        notify_user_telegram(
            user_id,
            "E-Mail geoeffnet von *{name}* ({email})".format(
                name=_escape_markdown(lead_name),
                email=to_email,
            ),
        )
    elif to_email:
        notify_user_telegram(
            user_id,
            "E-Mail geoeffnet von {email}".format(email=to_email),
        )


def on_email_replied(user_id: int, email_id: int, from_email: str = "",
                     subject: str = "", lead_id: Optional[int] = None,
                     lead_name: str = "") -> None:
    """Handler fuer E-Mail-Antworten — loest Webhook und Telegram aus.

    Wird typischerweise von process_reply_checks() oder dem IMAP-Monitor
    aufgerufen.

    Args:
        user_id: ID des Nutzers
        email_id: ID der beantworteten E-Mail
        from_email: Absender der Antwort
        subject: Betreff der Antwort
        lead_id: ID des zugehoerigen Leads
        lead_name: Name des Leads
    """
    # Webhook
    trigger_webhook(user_id, "email.replied", {
        "email_id": email_id,
        "from_email": from_email,
        "subject": subject,
        "lead_id": lead_id,
        "lead_name": lead_name,
        "replied_at": datetime.utcnow().isoformat(),
    })

    # Telegram
    display_name = lead_name or from_email
    notify_user_telegram(
        user_id,
        "Antwort erhalten von *{name}*\nBetreff: {subject}".format(
            name=_escape_markdown(display_name),
            subject=subject,
        ),
    )


def on_lead_created(user_id: int, lead_id: int, lead_name: str = "",
                    lead_email: str = "", source: str = "search") -> None:
    """Handler fuer neue Leads — loest Webhook aus.

    Args:
        user_id: ID des Nutzers
        lead_id: ID des neuen Leads
        lead_name: Name des Leads
        lead_email: E-Mail des Leads
        source: Quelle des Leads (z.B. 'search', 'import', 'api')
    """
    trigger_webhook(user_id, "lead.created", {
        "lead_id": lead_id,
        "lead_name": lead_name,
        "lead_email": lead_email,
        "source": source,
        "created_at": datetime.utcnow().isoformat(),
    })


def on_lead_converted(user_id: int, lead_id: int, lead_name: str = "",
                      lead_email: str = "", revenue: float = 0.0) -> None:
    """Handler fuer Lead-Konvertierungen — loest Webhook und Telegram aus.

    Args:
        user_id: ID des Nutzers
        lead_id: ID des konvertierten Leads
        lead_name: Name des Leads
        lead_email: E-Mail des Leads
        revenue: Generierter Umsatz
    """
    trigger_webhook(user_id, "lead.converted", {
        "lead_id": lead_id,
        "lead_name": lead_name,
        "lead_email": lead_email,
        "revenue": revenue,
        "converted_at": datetime.utcnow().isoformat(),
    })

    # Telegram
    revenue_text = ""
    if revenue > 0:
        revenue_text = "\nUmsatz: {rev:.2f} EUR".format(rev=revenue)

    notify_user_telegram(
        user_id,
        "Lead konvertiert: *{name}* ({email}){revenue}".format(
            name=_escape_markdown(lead_name) if lead_name else "Unbenannt",
            email=lead_email or "keine E-Mail",
            revenue=revenue_text,
        ),
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HILFSFUNKTIONEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _escape_markdown(text: str) -> str:
    """Escaped Sonderzeichen fuer Telegram Markdown (v1).

    Telegram Markdown v1 interpretiert *, _, ` und [ als Formatierung.
    Diese werden hier escaped damit sie als normaler Text angezeigt werden.

    Args:
        text: Eingabetext

    Returns:
        Escaped Text.
    """
    if not text:
        return ""
    for char in ("_", "*", "`", "["):
        text = text.replace(char, "\\" + char)
    return text


def get_scheduler_status() -> Dict[str, Any]:
    """Gibt den aktuellen Status des Schedulers zurueck.

    Nuetzlich fuer Admin-Dashboards und Health-Checks.

    Returns:
        Dict mit Scheduler-Status-Informationen.
    """
    global _scheduler_thread, _scheduler_last_daily_reset, _scheduler_last_monthly_reset

    is_alive = (_scheduler_thread is not None and _scheduler_thread.is_alive())

    return {
        "running": is_alive,
        "thread_name": _scheduler_thread.name if _scheduler_thread else None,
        "interval_seconds": SCHEDULER_INTERVAL,
        "last_daily_reset": _scheduler_last_daily_reset,
        "last_monthly_reset": _scheduler_last_monthly_reset,
        "checked_at": datetime.utcnow().isoformat(),
    }


def trigger_webhook_async(user_id: int, event_type: str,
                          payload: Dict[str, Any]) -> threading.Thread:
    """Asynchrone Version von trigger_webhook — feuert in einem separaten Thread.

    Nuetzlich wenn der aufrufende Code nicht auf die Webhook-Zustellung
    warten soll (z.B. in Request-Handlern).

    Args:
        user_id: ID des Nutzers
        event_type: Art des Events
        payload: Daten die gesendet werden

    Returns:
        Der gestartete Thread (fuer optionales Joining).
    """
    thread = threading.Thread(
        target=trigger_webhook,
        args=(user_id, event_type, payload),
        name=f"webhook-{event_type}-{user_id}",
        daemon=True,
    )
    thread.start()
    return thread


def notify_user_telegram_async(user_id: int, message: str) -> threading.Thread:
    """Asynchrone Version von notify_user_telegram.

    Args:
        user_id: ID des Nutzers
        message: Nachrichtentext

    Returns:
        Der gestartete Thread.
    """
    thread = threading.Thread(
        target=notify_user_telegram,
        args=(user_id, message),
        name=f"telegram-{user_id}",
        daemon=True,
    )
    thread.start()
    return thread


def test_webhook(user_id: int, webhook_id: int) -> Dict[str, Any]:
    """Testet einen Webhook durch Senden eines Test-Events.

    Args:
        user_id: ID des Nutzers
        webhook_id: ID des zu testenden Webhooks

    Returns:
        Dict mit Testergebnis (success, status_code, error).
    """
    webhooks = db.get_webhooks(user_id)
    webhook = None
    for wh in webhooks:
        if wh["id"] == webhook_id:
            webhook = wh
            break

    if not webhook:
        return {"success": False, "error": "Webhook nicht gefunden"}

    url = webhook.get("url", "")
    secret = webhook.get("secret", "")
    event_type = webhook.get("event_type", "email.opened")

    if not url:
        return {"success": False, "error": "Keine URL konfiguriert"}

    envelope = {
        "event": event_type,
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "test": True,
        "data": {
            "message": "Dies ist ein Test-Webhook von LeadFinder Pro.",
            "webhook_id": webhook_id,
        },
    }

    body_bytes = json.dumps(envelope, ensure_ascii=False, default=str).encode("utf-8")
    signature = _compute_hmac_signature(body_bytes, secret)

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "LeadFinderPro-Webhook/3.0",
        "X-Webhook-Event": event_type,
        "X-Webhook-Timestamp": envelope["timestamp"],
        "X-Webhook-Signature": signature,
    }

    try:
        response = requests.post(
            url,
            data=body_bytes,
            headers=headers,
            timeout=WEBHOOK_TIMEOUT,
            allow_redirects=False,
        )

        success = 200 <= response.status_code < 300

        return {
            "success": success,
            "status_code": response.status_code,
            "response_body": response.text[:500],
            "error": "" if success else f"HTTP {response.status_code}",
        }

    except requests.exceptions.Timeout:
        return {"success": False, "error": "Timeout", "status_code": None}
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "Verbindungsfehler", "status_code": None}
    except requests.exceptions.RequestException as exc:
        return {"success": False, "error": str(exc), "status_code": None}


def test_telegram(bot_token: str, chat_id: str) -> Dict[str, Any]:
    """Testet die Telegram-Verbindung durch Senden einer Testnachricht.

    Args:
        bot_token: Telegram Bot Token
        chat_id: Telegram Chat-ID

    Returns:
        Dict mit Testergebnis (success, error).
    """
    if not bot_token:
        return {"success": False, "error": "Kein Bot-Token angegeben"}
    if not chat_id:
        return {"success": False, "error": "Keine Chat-ID angegeben"}

    test_message = (
        "*LeadFinder Pro — Testbenachrichtigung*\n\n"
        "Ihre Telegram-Benachrichtigungen sind korrekt konfiguriert.\n"
        "Sie werden ab sofort ueber E-Mail-Oeffnungen, Antworten und "
        "faellige Follow-Ups informiert."
    )

    success = send_telegram(bot_token, chat_id, test_message)

    if success:
        return {"success": True, "error": ""}
    else:
        return {
            "success": False,
            "error": "Telegram-Nachricht konnte nicht gesendet werden. "
                     "Pruefen Sie Bot-Token und Chat-ID.",
        }


def get_webhook_event_types() -> List[Dict[str, str]]:
    """Gibt die verfuegbaren Webhook-Event-Typen mit Beschreibung zurueck.

    Returns:
        Liste von Dicts mit 'event' und 'description' Keys.
    """
    return [
        {
            "event": "email.opened",
            "description": "Eine gesendete E-Mail wurde vom Empfaenger geoeffnet.",
        },
        {
            "event": "email.replied",
            "description": "Eine Antwort auf eine gesendete E-Mail wurde erkannt.",
        },
        {
            "event": "lead.created",
            "description": "Ein neuer Lead wurde erstellt (Suche, Import oder API).",
        },
        {
            "event": "lead.converted",
            "description": "Ein Lead wurde als konvertiert markiert.",
        },
        {
            "event": "followup.due",
            "description": "Ein geplantes Follow-Up wurde faellig und versendet.",
        },
    ]
