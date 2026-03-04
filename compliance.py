"""
compliance.py — DSGVO-Compliance, Unsubscribe-System, Rate-Limiting

Zentrale Compliance-Schicht fuer LeadFinder Pro v3.
Verwaltet DSGVO-Protokollierung, Abmelde-Tokens, Versandlimits
und stellt Verschluesselungs-Hilfsfunktionen bereit.
"""

import os
import csv
import io
import hmac
import hashlib
import secrets
import logging
from datetime import datetime
from typing import Optional, Dict, List, Any

import database as db
import auth
import email_engine

log = logging.getLogger("leadfinder.compliance")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  KONSTANTEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
_UNSUBSCRIBE_KEY_FILE = os.path.join(DATA_DIR, ".unsubscribe_key")

# Gueltige Kanaele und Rechtsgrundlagen
VALID_CHANNELS = {"email", "phone", "whatsapp", "linkedin", "sms", "other"}
VALID_LEGAL_BASES = {
    "berechtigtes_interesse",
    "einwilligung",
    "vertragserfuellung",
    "gesetzliche_pflicht",
    "opt_out",
}

# CSV-Spalten fuer den DSGVO-Export
DSGVO_CSV_COLUMNS = [
    "id",
    "user_id",
    "action",
    "target_email",
    "target_name",
    "channel",
    "content_summary",
    "legal_basis",
    "created_at",
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  UNSUBSCRIBE-SCHLUESSEL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_unsubscribe_key: Optional[bytes] = None


def _get_unsubscribe_key() -> bytes:
    """Laedt oder erzeugt den HMAC-Schluessel fuer Unsubscribe-Tokens.

    Der Schluessel wird in data/.unsubscribe_key gespeichert.
    Falls die Datei fehlt, wird ein neuer 32-Byte-Schluessel generiert
    und mit restriktiven Dateiberechtigungen (0o600) geschrieben.
    """
    global _unsubscribe_key
    if _unsubscribe_key is not None:
        return _unsubscribe_key

    os.makedirs(DATA_DIR, exist_ok=True)

    if os.path.exists(_UNSUBSCRIBE_KEY_FILE):
        with open(_UNSUBSCRIBE_KEY_FILE, "rb") as f:
            _unsubscribe_key = f.read()
        if len(_unsubscribe_key) < 16:
            log.warning(
                "Unsubscribe-Schluessel zu kurz (%d Bytes) — generiere neu",
                len(_unsubscribe_key),
            )
            _unsubscribe_key = None
    if _unsubscribe_key is None:
        _unsubscribe_key = secrets.token_bytes(32)
        with open(_UNSUBSCRIBE_KEY_FILE, "wb") as f:
            f.write(_unsubscribe_key)
        try:
            os.chmod(_UNSUBSCRIBE_KEY_FILE, 0o600)
        except OSError:
            pass  # Windows unterstuetzt chmod nicht vollstaendig
        log.info("Neuer Unsubscribe-Schluessel generiert: %s", _UNSUBSCRIBE_KEY_FILE)

    return _unsubscribe_key


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DSGVO-LOGGING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def log_contact(
    user_id: int,
    action: str,
    target_email: str = "",
    target_name: str = "",
    channel: str = "email",
    content_summary: str = "",
    legal_basis: str = "berechtigtes_interesse",
) -> Optional[int]:
    """Protokolliert eine DSGVO-relevante Kontaktaktion.

    Jeder ausgehende Kontakt (E-Mail, Anruf, WhatsApp etc.) wird
    in der dsgvo_log-Tabelle persistiert, um Auskunfts- und
    Loeschanfragen nach Art. 15/17 DSGVO bedienen zu koennen.

    Args:
        user_id:         ID des handelnden Nutzers.
        action:          Beschreibung der Aktion (z.B. 'email_sent',
                         'followup_sent', 'unsubscribe', 'data_export').
        target_email:    E-Mail-Adresse der kontaktierten Person.
        target_name:     Name der kontaktierten Person / Firma.
        channel:         Kommunikationskanal (email, phone, whatsapp ...).
        content_summary: Kurzzusammenfassung des Inhalts (Betreff o.ae.).
        legal_basis:     DSGVO-Rechtsgrundlage (berechtigtes_interesse,
                         einwilligung, vertragserfuellung ...).

    Returns:
        Die ID des neuen dsgvo_log-Eintrags oder None bei Fehler.
    """
    # Kanal und Rechtsgrundlage validieren
    if channel not in VALID_CHANNELS:
        log.warning("Unbekannter Kanal '%s' — verwende 'other'", channel)
        channel = "other"
    if legal_basis not in VALID_LEGAL_BASES:
        log.warning(
            "Unbekannte Rechtsgrundlage '%s' — verwende 'berechtigtes_interesse'",
            legal_basis,
        )
        legal_basis = "berechtigtes_interesse"

    entry_id = db.log_dsgvo(
        user_id=user_id,
        action=action,
        target_email=target_email.lower().strip() if target_email else "",
        target_name=target_name.strip() if target_name else "",
        channel=channel,
        content_summary=content_summary[:500] if content_summary else "",
        legal_basis=legal_basis,
    )

    if entry_id:
        log.debug(
            "DSGVO-Log #%d: user=%d action=%s target=%s",
            entry_id,
            user_id,
            action,
            target_email,
        )
    else:
        log.error(
            "DSGVO-Logging fehlgeschlagen: user=%d action=%s target=%s",
            user_id,
            action,
            target_email,
        )

    return entry_id


def get_contact_log(user_id: int, limit: int = 200) -> List[Dict[str, Any]]:
    """Ruft das DSGVO-Kontaktprotokoll fuer einen Nutzer ab.

    Args:
        user_id: ID des Nutzers, dessen Protokoll abgerufen wird.
        limit:   Maximale Anzahl zurueckgegebener Eintraege (Standard 200).

    Returns:
        Liste von dsgvo_log-Eintraegen als Dicts, absteigend nach Datum.
    """
    if limit < 1:
        limit = 1
    if limit > 10000:
        limit = 10000

    entries = db.get_dsgvo_log(user_id, limit=limit)
    log.debug(
        "DSGVO-Log abgerufen: user=%d entries=%d (limit=%d)",
        user_id,
        len(entries),
        limit,
    )
    return entries


def export_dsgvo_report(user_id: int) -> str:
    """Exportiert alle DSGVO-Eintraege eines Nutzers als CSV-String.

    Erzeugt eine vollstaendige CSV-Datei gemaess Art. 15 DSGVO
    (Recht auf Auskunft). Das Ergebnis kann direkt als Download
    an den Nutzer weitergegeben werden.

    Args:
        user_id: ID des Nutzers, fuer den der Report erstellt wird.

    Returns:
        CSV-String (UTF-8) mit allen dsgvo_log-Eintraegen.
        Bei leerem Log wird nur die Kopfzeile zurueckgegeben.
    """
    entries = db.get_dsgvo_log(user_id, limit=100000)

    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=DSGVO_CSV_COLUMNS,
        extrasaction="ignore",
        quoting=csv.QUOTE_ALL,
    )
    writer.writeheader()

    for entry in entries:
        # Nur definierte Spalten schreiben, fehlende Felder als Leerstring
        row = {col: entry.get(col, "") for col in DSGVO_CSV_COLUMNS}
        writer.writerow(row)

    csv_content = output.getvalue()
    output.close()

    log.info(
        "DSGVO-Report exportiert: user=%d entries=%d size=%d bytes",
        user_id,
        len(entries),
        len(csv_content),
    )

    # Export selbst protokollieren
    log_contact(
        user_id=user_id,
        action="dsgvo_report_export",
        content_summary=f"DSGVO-Report mit {len(entries)} Eintraegen exportiert",
        legal_basis="gesetzliche_pflicht",
    )

    return csv_content


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  UNSUBSCRIBE-TOKEN-SYSTEM
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_unsubscribe_token(tracking_id: str) -> str:
    """Erzeugt einen HMAC-basierten Unsubscribe-Token.

    Der Token wird aus der Tracking-ID und dem geheimen Schluessel
    (data/.unsubscribe_key) abgeleitet. So kann jeder Abmeldelink
    verifiziert werden, ohne Token in der Datenbank zu speichern.

    Args:
        tracking_id: Die eindeutige Tracking-ID der zugehoerigen E-Mail.

    Returns:
        Hex-String des HMAC-SHA256-Tokens (64 Zeichen).

    Raises:
        ValueError: Falls tracking_id leer ist.
    """
    if not tracking_id or not tracking_id.strip():
        raise ValueError("tracking_id darf nicht leer sein")

    key = _get_unsubscribe_key()
    mac = hmac.new(key, tracking_id.encode("utf-8"), hashlib.sha256)
    token = mac.hexdigest()

    log.debug("Unsubscribe-Token generiert fuer Tracking-ID: %s", tracking_id)
    return token


def verify_unsubscribe_token(token: str, tracking_id: str) -> bool:
    """Verifiziert einen Unsubscribe-Token gegen die Tracking-ID.

    Nutzt hmac.compare_digest fuer timing-sichere Vergleiche, um
    Timing-Seitenkanalangriffe zu verhindern.

    Args:
        token:       Der vom Nutzer uebermittelte Token (Hex-String).
        tracking_id: Die zugehoerige Tracking-ID der E-Mail.

    Returns:
        True wenn der Token gueltig ist, sonst False.
    """
    if not token or not tracking_id:
        return False

    try:
        expected = generate_unsubscribe_token(tracking_id)
        is_valid = hmac.compare_digest(token, expected)
    except (ValueError, TypeError) as exc:
        log.warning("Token-Verifizierung fehlgeschlagen: %s", exc)
        return False

    if not is_valid:
        log.warning(
            "Ungueltiger Unsubscribe-Token fuer Tracking-ID: %s", tracking_id
        )

    return is_valid


def process_unsubscribe(
    tracking_id: str,
    token: str,
) -> Dict[str, Any]:
    """Verarbeitet eine vollstaendige Abmeldeanfrage.

    Fuehrt folgende Schritte durch:
      1. Token verifizieren
      2. E-Mail anhand der Tracking-ID suchen
      3. E-Mail als abgemeldet markieren
      4. Empfaenger-Adresse auf die Blacklist setzen
      5. Ausstehende Follow-Ups abbrechen
      6. Vorgang im DSGVO-Log protokollieren

    Args:
        tracking_id: Tracking-ID der E-Mail, ueber die abgemeldet wird.
        token:       HMAC-Token zur Verifizierung des Abmeldelinks.

    Returns:
        Dict mit den Schluesseln:
            success (bool):  True bei erfolgreicher Abmeldung.
            message (str):   Menschenlesbare Status-Nachricht.
            email   (str):   Die abgemeldete E-Mail-Adresse (falls verfuegbar).
    """
    # 1. Token verifizieren
    if not verify_unsubscribe_token(token, tracking_id):
        log.warning("Abmeldung abgelehnt — ungueltiger Token: %s", tracking_id)
        return {
            "success": False,
            "message": "Ungueltiger oder abgelaufener Abmeldelink.",
            "email": "",
        }

    # 2. E-Mail anhand der Tracking-ID laden
    email_record = db.get_email_by_tracking_id(tracking_id)
    if not email_record:
        log.warning(
            "Abmeldung fehlgeschlagen — E-Mail nicht gefunden: %s", tracking_id
        )
        return {
            "success": False,
            "message": "E-Mail nicht gefunden. Moeglicherweise bereits abgemeldet.",
            "email": "",
        }

    to_email = email_record.get("to_email", "")
    user_id = email_record.get("user_id")
    lead_id = email_record.get("lead_id")
    email_id = email_record.get("id")

    # Pruefen ob bereits abgemeldet
    if email_record.get("unsubscribed"):
        log.info("Bereits abgemeldet: %s (Tracking: %s)", to_email, tracking_id)
        return {
            "success": True,
            "message": "Sie wurden bereits abgemeldet.",
            "email": to_email,
        }

    # 3. E-Mail als abgemeldet markieren
    now = datetime.utcnow().isoformat()
    if email_id and user_id:
        db.update_email(email_id, user_id, unsubscribed=1, unsubscribed_at=now)

    # 4. Auf Blacklist setzen
    if user_id and to_email:
        db.add_to_blacklist(
            user_id, to_email, reason="Unsubscribe via Abmeldelink"
        )

    # 5. Follow-Ups abbrechen
    cancelled_count = 0
    if user_id and lead_id:
        cancelled_count = db.cancel_followups_for_lead(user_id, lead_id) or 0

    # 6. DSGVO-Log schreiben
    if user_id:
        log_contact(
            user_id=user_id,
            action="unsubscribe",
            target_email=to_email,
            target_name=email_record.get("subject", ""),
            channel="email",
            content_summary=(
                f"Abmeldung via Tracking-ID {tracking_id}. "
                f"{cancelled_count} Follow-Up(s) abgebrochen."
            ),
            legal_basis="opt_out",
        )

    log.info(
        "Abmeldung erfolgreich: %s (Tracking: %s, %d Follow-Ups abgebrochen)",
        to_email,
        tracking_id,
        cancelled_count,
    )

    return {
        "success": True,
        "message": "Sie wurden erfolgreich abgemeldet und erhalten keine weiteren Nachrichten.",
        "email": to_email,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  RATE-LIMITER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def check_rate_limit(user_id: int) -> Dict[str, Any]:
    """Prueft das kombinierte Versandlimit fuer einen Nutzer.

    Kombiniert drei unabhaengige Limitierungen:
      - Plan-Limit:     Maximale E-Mails pro Tag gemaess Abo-Plan.
      - Warmup-Limit:   Reduziertes Tageslimit waehrend der Aufwaermphase
                        (stufenweise Erhoehung ueber 3 Wochen).
      - Provider-Limit: Technisches Tageslimit des E-Mail-Providers
                        (z.B. Gmail 500/Tag, GMX 100/Tag).

    Das effektive Limit ist das Minimum aller drei Limits.

    Args:
        user_id: ID des Nutzers, dessen Limit geprueft wird.

    Returns:
        Dict mit folgenden Schluesseln:
            allowed   (bool): True wenn noch E-Mails gesendet werden duerfen.
            remaining (int):  Verbleibende E-Mails fuer heute.
            limit     (int):  Effektives Tageslimit (Minimum aller Limits).
            reason    (str):  Menschenlesbare Begruendung bei Limit-Ueberschreitung,
                              Leerstring wenn erlaubt.
            details   (dict): Aufschluesselung der einzelnen Limits:
                plan_limit     (int): Limit gemaess Abo-Plan.
                warmup_limit   (int): Aktuelles Warmup-Limit.
                provider_limit (int): Provider-spezifisches Limit.
                sent_today     (int): Heute bereits gesendete E-Mails.
    """
    # Nutzer mit Plan-Details laden
    user = db.get_user_with_plan(user_id)
    if not user:
        log.warning("Rate-Limit-Check: User %d nicht gefunden", user_id)
        return {
            "allowed": False,
            "remaining": 0,
            "limit": 0,
            "reason": "Nutzer nicht gefunden.",
            "details": {
                "plan_limit": 0,
                "warmup_limit": 0,
                "provider_limit": 0,
                "sent_today": 0,
            },
        }

    # --- Plan-Limit ---
    plan_limit = user.get("emails_per_day", 10)

    # --- Warmup-Limit ---
    warmup_limit = email_engine.get_warmup_limit(user_id)

    # --- Provider-Limit ---
    smtp_user = user.get("smtp_user", "")
    if smtp_user:
        provider_limit = email_engine.get_provider_limit(smtp_user)
    else:
        provider_limit = 0

    # --- Effektives Limit (Minimum aller drei) ---
    if smtp_user:
        effective_limit = min(plan_limit, warmup_limit, provider_limit)
    else:
        # Ohne SMTP-Konfiguration kein Versand moeglich
        effective_limit = 0

    # --- Heutiger Versand ---
    sent_today = db.count_emails_sent_today(user_id)

    # --- Ergebnis berechnen ---
    remaining = max(0, effective_limit - sent_today)
    allowed = remaining > 0

    # Begruendung bestimmen
    reason = ""
    if not allowed:
        if not smtp_user:
            reason = "SMTP-Einstellungen nicht konfiguriert."
        elif sent_today >= plan_limit:
            reason = (
                f"Plan-Limit erreicht: {sent_today}/{plan_limit} E-Mails heute "
                f"(Plan: {user.get('plan_name', 'unbekannt')})."
            )
        elif sent_today >= warmup_limit:
            warmup_info = email_engine.get_warmup_info(user_id)
            warmup_day = warmup_info.get("day", 0)
            reason = (
                f"Warmup-Limit erreicht: {sent_today}/{warmup_limit} E-Mails heute "
                f"(Warmup-Tag {warmup_day}). Das Limit steigt automatisch."
            )
        elif sent_today >= provider_limit:
            domain = smtp_user.split("@")[1] if "@" in smtp_user else smtp_user
            reason = (
                f"Provider-Limit erreicht: {sent_today}/{provider_limit} E-Mails "
                f"heute ({domain})."
            )
        else:
            reason = f"Tageslimit erreicht: {sent_today}/{effective_limit} E-Mails."

    log.debug(
        "Rate-Limit user=%d: allowed=%s remaining=%d limit=%d sent=%d",
        user_id,
        allowed,
        remaining,
        effective_limit,
        sent_today,
    )

    return {
        "allowed": allowed,
        "remaining": remaining,
        "limit": effective_limit,
        "reason": reason,
        "details": {
            "plan_limit": plan_limit,
            "warmup_limit": warmup_limit,
            "provider_limit": provider_limit,
            "sent_today": sent_today,
        },
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  VERSCHLUESSELUNGS-HILFSFUNKTIONEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def encrypt_value(plaintext: str) -> str:
    """Verschluesselt einen Wert (Re-Export von auth.encrypt_smtp_password).

    Nutzt die in auth.py implementierte symmetrische Verschluesselung
    (XOR + HMAC-Integritaetspruefung) mit dem globalen Verschluesselungs-
    schluessel aus data/.encryption_key.

    Args:
        plaintext: Der zu verschluesselnde Klartext.

    Returns:
        Base64-kodierter Ciphertext-String. Leerstring bei leerem Input.
    """
    if not plaintext:
        return ""
    return auth.encrypt_smtp_password(plaintext)


def decrypt_value(ciphertext: str) -> str:
    """Entschluesselt einen Wert (Re-Export von auth.decrypt_smtp_password).

    Verifiziert die HMAC-Integritaet und entschluesselt den Ciphertext.
    Bei ungueltigem MAC oder Entschluesselungsfehler wird ein Leerstring
    zurueckgegeben (fail-safe).

    Args:
        ciphertext: Der Base64-kodierte Ciphertext-String.

    Returns:
        Entschluesselter Klartext oder Leerstring bei Fehler.
    """
    if not ciphertext:
        return ""
    return auth.decrypt_smtp_password(ciphertext)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HILFSFUNKTIONEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def is_blacklisted(user_id: int, email_address: str) -> bool:
    """Prueft ob eine E-Mail-Adresse auf der Blacklist steht.

    Convenience-Funktion, die vor dem Versand aufgerufen werden kann,
    um Abmeldungen und Bounces zu respektieren.

    Args:
        user_id:       ID des Nutzers (Blacklist ist nutzer-spezifisch).
        email_address: Zu pruefende E-Mail-Adresse.

    Returns:
        True wenn die Adresse auf der Blacklist steht.
    """
    if not email_address:
        return False
    conn = db.get_db()
    row = conn.execute(
        "SELECT 1 FROM blacklist WHERE user_id = ? AND email = ?",
        (user_id, email_address.lower().strip()),
    ).fetchone()
    return row is not None


def get_unsubscribe_url(tracking_id: str, base_url: str = "") -> str:
    """Generiert die vollstaendige Abmelde-URL fuer eine E-Mail.

    Args:
        tracking_id: Tracking-ID der E-Mail.
        base_url:    Basis-URL der Anwendung (z.B. 'https://app.example.com').
                     Standard: 'http://localhost:5000'.

    Returns:
        Vollstaendige URL mit Token als Query-Parameter.
    """
    if not base_url:
        base_url = "http://localhost:5000"
    base_url = base_url.rstrip("/")

    token = generate_unsubscribe_token(tracking_id)
    return f"{base_url}/unsubscribe/{tracking_id}?token={token}"


def check_send_allowed(user_id: int, to_email: str) -> Dict[str, Any]:
    """Kombinierte Pruefung vor dem E-Mail-Versand.

    Prueft sowohl das Rate-Limit als auch die Blacklist in einem Aufruf.
    Sollte als Gate vor jedem Versand verwendet werden.

    Args:
        user_id:  ID des sendenden Nutzers.
        to_email: Empfaenger-E-Mail-Adresse.

    Returns:
        Dict mit den Schluesseln:
            allowed (bool): True wenn Versand erlaubt.
            reason  (str):  Begruendung bei Ablehnung, Leerstring wenn erlaubt.
    """
    # 1. Blacklist pruefen
    if is_blacklisted(user_id, to_email):
        return {
            "allowed": False,
            "reason": f"Empfaenger {to_email} ist auf der Blacklist (abgemeldet oder Bounce).",
        }

    # 2. Rate-Limit pruefen
    rate = check_rate_limit(user_id)
    if not rate["allowed"]:
        return {
            "allowed": False,
            "reason": rate["reason"],
        }

    return {
        "allowed": True,
        "reason": "",
    }


def get_compliance_summary(user_id: int) -> Dict[str, Any]:
    """Gibt eine Zusammenfassung des Compliance-Status zurueck.

    Nuetzlich fuer das Dashboard — zeigt auf einen Blick den
    aktuellen Stand der DSGVO-Compliance und Versandlimits.

    Args:
        user_id: ID des Nutzers.

    Returns:
        Dict mit Compliance-Uebersicht:
            rate_limit       (dict): Aktueller Rate-Limit-Status.
            dsgvo_log_count  (int):  Anzahl DSGVO-Log-Eintraege.
            recent_actions   (list): Letzte 5 DSGVO-Aktionen.
            blacklist_count  (int):  Anzahl Blacklist-Eintraege.
    """
    rate_limit = check_rate_limit(user_id)

    # DSGVO-Log Anzahl und letzte Aktionen
    recent = get_contact_log(user_id, limit=5)

    # Gesamt-Anzahl aus einem groesseren Abruf ableiten
    all_entries = db.get_dsgvo_log(user_id, limit=100000)
    dsgvo_count = len(all_entries)

    # Blacklist-Groesse
    conn = db.get_db()
    bl_row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM blacklist WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    blacklist_count = bl_row["cnt"] if bl_row else 0

    return {
        "rate_limit": rate_limit,
        "dsgvo_log_count": dsgvo_count,
        "recent_actions": recent,
        "blacklist_count": blacklist_count,
    }
