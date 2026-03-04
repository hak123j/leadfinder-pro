"""
digistore.py — Digistore24-Zahlungsintegration fuer LeadFinder Pro v3

IPN-Verarbeitung (Instant Payment Notification), Abo-Verwaltung,
Checkout-URLs und Event-Handler fuer Zahlungsereignisse.
"""

import hashlib
import json
import logging
import urllib.parse
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple

import requests

import database as db

log = logging.getLogger("leadfinder.digistore")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  KONFIGURATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DIGISTORE_CONFIG = {
    "ipn_passphrase": "DEIN_GEHEIMES_IPN_PASSWORT_HIER_MIN_30_ZEICHEN_LANG",
    "product_ids": {
        "free": "000000",
        "pro": "000001",
        "business": "000002",
    },
    "api_key": "",
    "thank_you_url": "http://localhost:5000/dashboard?upgraded=true",
    "cancel_url": "http://localhost:5000/dashboard?cancelled=true",
}

# Umkehrmapping: Produkt-ID -> Plan-ID
_PRODUCT_TO_PLAN: Dict[str, str] = {
    pid: plan for plan, pid in DIGISTORE_CONFIG["product_ids"].items()
}

# Digistore24-API-Basis-URL
_DS_API_BASE = "https://www.digistore24.com/api/v1"
_DS_CHECKOUT_BASE = "https://www.digistore24.com/product"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SIGNATUR-VERIFIZIERUNG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def verify_ipn_signature(params: Dict[str, str], passphrase: str) -> bool:
    """
    Verifiziert die SHA-256-Signatur eines Digistore24-IPN-Calls.

    Algorithmus:
      1. Alle POST-Parameter ausser 'sha_sign' nehmen
      2. Alphabetisch nach Schluessel sortieren
      3. String bauen: key1=value1:key2=value2:...:passphrase
      4. SHA-256-Hash berechnen
      5. Mit dem uebergebenen sha_sign vergleichen (case-insensitive)

    Args:
        params:     Alle POST-Parameter als Dict
        passphrase: Das IPN-Passwort aus der Digistore24-Konfiguration

    Returns:
        True wenn Signatur gueltig, sonst False
    """
    received_sign = params.get("sha_sign", "").strip()
    if not received_sign:
        log.warning("IPN-Signatur fehlt in den Parametern")
        return False

    # Alle Parameter ausser sha_sign sammeln
    filtered = {
        k: v for k, v in params.items()
        if k.lower() != "sha_sign"
    }

    # Alphabetisch sortieren und String zusammenbauen
    sorted_keys = sorted(filtered.keys())
    parts = [f"{k}={filtered[k]}" for k in sorted_keys]
    sign_string = ":".join(parts) + ":" + passphrase

    # SHA-256-Hash berechnen
    computed_hash = hashlib.sha256(sign_string.encode("utf-8")).hexdigest().upper()
    received_upper = received_sign.upper()

    is_valid = computed_hash == received_upper
    if not is_valid:
        log.warning(
            "IPN-Signatur ungueltig: erwartet=%s, erhalten=%s",
            computed_hash[:16] + "...", received_upper[:16] + "..."
        )
    return is_valid


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  IPN-HANDLER (HAUPTEINGANG)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def handle_ipn(form_data: Dict[str, str]) -> Tuple[bool, str]:
    """
    Haupteingang fuer Digistore24-IPN-Notifications.

    Ablauf:
      1. Signatur verifizieren
      2. IPN in DB loggen
      3. Anhand des 'event'-Parameters zum richtigen Handler routen
      4. Ergebnis zurueckgeben

    Args:
        form_data: Die POST-Parameter des IPN-Calls

    Returns:
        Tuple (success: bool, message: str)
    """
    event = form_data.get("event", "unknown")
    product_id = form_data.get("product_id", "")
    order_id = form_data.get("order_id", "")
    transaction_id = form_data.get("transaction_id", "")
    email = form_data.get("email", "")
    custom = form_data.get("custom", "")

    log.info(
        "IPN empfangen: event=%s, product=%s, order=%s, email=%s",
        event, product_id, order_id, email
    )

    # 1. Signatur pruefen
    passphrase = DIGISTORE_CONFIG["ipn_passphrase"]
    sig_valid = verify_ipn_signature(form_data, passphrase)

    # 2. Roh-Daten in DB loggen (auch bei ungueltiger Signatur)
    raw_params = json.dumps(form_data, ensure_ascii=False, default=str)
    db.log_digistore_ipn(
        event=event,
        product_id=product_id,
        order_id=order_id,
        transaction_id=transaction_id,
        email=email,
        custom=custom,
        raw_params=raw_params,
        signature_valid=sig_valid,
        processed=False,
    )

    if not sig_valid:
        log.error("IPN-Signatur ungueltig — Verarbeitung abgebrochen")
        return False, "Ungueltige IPN-Signatur"

    # 3. Event-Routing
    event_handlers = {
        "payment":          on_payment,
        "refund":           on_refund,
        "chargeback":       on_chargeback,
        "rebill_cancelled": on_rebill_cancelled,
        "payment_missed":   on_payment_missed,
    }

    handler = event_handlers.get(event)
    if handler is None:
        log.info("Unbekannter IPN-Event '%s' — wird ignoriert", event)
        return True, f"Event '{event}' ignoriert (kein Handler)"

    try:
        success, message = handler(form_data)
        # Verarbeitungsstatus aktualisieren (im Log nachtraeglich)
        if success:
            log.info("IPN-Event '%s' erfolgreich verarbeitet: %s", event, message)
        else:
            log.warning("IPN-Event '%s' fehlgeschlagen: %s", event, message)
        return success, message
    except Exception as exc:
        log.exception("Fehler bei IPN-Event '%s': %s", event, exc)
        return False, f"Interner Fehler: {exc}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HILFSFUNKTION: USER FINDEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _find_user(params: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """
    Findet den User anhand der IPN-Parameter.

    Versucht in dieser Reihenfolge:
      1. 'custom'-Feld (enthaelt User-ID wenn beim Checkout gesetzt)
      2. 'email'-Feld (Digistore-Kaeufer-E-Mail)

    Returns:
        User-Dict oder None
    """
    # Versuch 1: custom-Feld als User-ID
    custom = params.get("custom", "").strip()
    if custom:
        try:
            user_id = int(custom)
            user = db.get_user_by_id(user_id)
            if user:
                return user
        except (ValueError, TypeError):
            pass

    # Versuch 2: E-Mail-Adresse
    email = params.get("email", "").strip().lower()
    if email:
        user = db.get_user_by_email(email)
        if user:
            return user

    return None


def _product_to_plan(product_id: str) -> Optional[str]:
    """Mappt eine Digistore24-Produkt-ID auf eine Plan-ID."""
    return _PRODUCT_TO_PLAN.get(product_id)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EVENT-HANDLER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def on_payment(params: Dict[str, str]) -> Tuple[bool, str]:
    """
    Handler fuer erfolgreiche Zahlungen.

    Ablauf:
      1. User finden (via E-Mail oder custom-Feld)
      2. Produkt-ID auf Plan mappen
      3. User-Plan upgraden
      4. Subscription in DB anlegen
      5. Benachrichtigung erstellen
      6. DSGVO-konformes Logging
    """
    user = _find_user(params)
    if not user:
        log.error("on_payment: User nicht gefunden (email=%s, custom=%s)",
                  params.get("email"), params.get("custom"))
        return False, "User nicht gefunden"

    user_id = user["id"]
    product_id = params.get("product_id", "")
    order_id = params.get("order_id", "")
    transaction_id = params.get("transaction_id", "")
    amount = params.get("amount", "0")
    buyer_email = params.get("email", "")

    # Plan-Mapping
    plan_id = _product_to_plan(product_id)
    if not plan_id:
        log.error("on_payment: Unbekannte Produkt-ID '%s'", product_id)
        return False, f"Unbekannte Produkt-ID: {product_id}"

    if plan_id == "free":
        log.info("on_payment: Produkt '%s' ist Free-Plan — kein Upgrade noetig", product_id)
        return True, "Free-Plan — kein Upgrade noetig"

    # Plan upgraden
    old_plan = user.get("plan_id", "free")
    db.update_user(user_id, plan_id=plan_id)
    log.info("User %d: Plan-Upgrade %s -> %s (Order: %s)", user_id, old_plan, plan_id, order_id)

    # Subscription anlegen
    try:
        amount_cents = int(float(amount) * 100)
    except (ValueError, TypeError):
        amount_cents = 0

    now = datetime.utcnow().isoformat()
    period_end = (datetime.utcnow() + timedelta(days=30)).isoformat()

    db.create_subscription(
        user_id=user_id,
        plan_id=plan_id,
        order_id=order_id,
        amount=amount_cents,
        period_start=now,
        period_end=period_end,
    )

    # Benachrichtigung
    plan_names = {"pro": "Pro", "business": "Business"}
    plan_label = plan_names.get(plan_id, plan_id.capitalize())
    db.create_notification(
        user_id=user_id,
        title=f"Upgrade auf {plan_label} erfolgreich!",
        message=(
            f"Dein Plan wurde auf {plan_label} aktualisiert. "
            f"Bestellnr.: {order_id}. Alle Premium-Features sind jetzt freigeschaltet."
        ),
        ntype="success",
        link="/dashboard?upgraded=true",
    )

    # DSGVO-konformes Logging
    db.log_dsgvo(
        user_id=user_id,
        action="payment_received",
        target_email=buyer_email,
        channel="digistore24",
        content_summary=(
            f"Zahlung erhalten: Plan={plan_id}, Betrag={amount}, "
            f"Order={order_id}, Transaction={transaction_id}"
        ),
        legal_basis="vertragserfuellung",
    )

    return True, f"User {user_id} auf Plan '{plan_id}' aktualisiert (Order: {order_id})"


def on_refund(params: Dict[str, str]) -> Tuple[bool, str]:
    """
    Handler fuer Erstattungen (Refunds).

    Setzt den User auf den Free-Plan zurueck und aktualisiert
    den Subscription-Status auf 'refunded'.
    """
    user = _find_user(params)
    if not user:
        log.error("on_refund: User nicht gefunden (email=%s, custom=%s)",
                  params.get("email"), params.get("custom"))
        return False, "User nicht gefunden"

    user_id = user["id"]
    order_id = params.get("order_id", "")
    old_plan = user.get("plan_id", "free")

    # Plan zuruecksetzen
    db.update_user(user_id, plan_id="free")
    log.info("User %d: Refund — Plan %s -> free (Order: %s)", user_id, old_plan, order_id)

    # Subscription-Status aktualisieren
    _update_subscription_by_order(user_id, order_id, status="refunded")

    # Benachrichtigung
    db.create_notification(
        user_id=user_id,
        title="Erstattung verarbeitet",
        message=(
            f"Deine Zahlung fuer Bestellnr. {order_id} wurde erstattet. "
            f"Dein Plan wurde auf Free zurueckgesetzt."
        ),
        ntype="warning",
        link="/dashboard",
    )

    # DSGVO-Log
    db.log_dsgvo(
        user_id=user_id,
        action="payment_refunded",
        target_email=params.get("email", ""),
        channel="digistore24",
        content_summary=f"Erstattung: Order={order_id}, alter Plan={old_plan}",
        legal_basis="vertragserfuellung",
    )

    return True, f"User {user_id} Refund verarbeitet — Plan zurueck auf 'free'"


def on_chargeback(params: Dict[str, str]) -> Tuple[bool, str]:
    """
    Handler fuer Chargebacks (Rueckbuchungen).

    Wie Refund, aber mit Status 'chargeback' fuer die Subscription.
    Chargebacks sind kritischer als Refunds und werden entsprechend geloggt.
    """
    user = _find_user(params)
    if not user:
        log.error("on_chargeback: User nicht gefunden (email=%s, custom=%s)",
                  params.get("email"), params.get("custom"))
        return False, "User nicht gefunden"

    user_id = user["id"]
    order_id = params.get("order_id", "")
    old_plan = user.get("plan_id", "free")

    # Plan zuruecksetzen
    db.update_user(user_id, plan_id="free")
    log.warning("User %d: CHARGEBACK — Plan %s -> free (Order: %s)", user_id, old_plan, order_id)

    # Subscription-Status auf 'chargeback'
    _update_subscription_by_order(user_id, order_id, status="chargeback")

    # Benachrichtigung
    db.create_notification(
        user_id=user_id,
        title="Chargeback erhalten",
        message=(
            f"Fuer Bestellnr. {order_id} wurde ein Chargeback ausgeloest. "
            f"Dein Plan wurde auf Free zurueckgesetzt. Bitte kontaktiere den Support."
        ),
        ntype="error",
        link="/dashboard",
    )

    # DSGVO-Log
    db.log_dsgvo(
        user_id=user_id,
        action="payment_chargeback",
        target_email=params.get("email", ""),
        channel="digistore24",
        content_summary=f"Chargeback: Order={order_id}, alter Plan={old_plan}",
        legal_basis="vertragserfuellung",
    )

    return True, f"User {user_id} Chargeback verarbeitet — Plan zurueck auf 'free'"


def on_rebill_cancelled(params: Dict[str, str]) -> Tuple[bool, str]:
    """
    Handler fuer Abo-Kuendigungen (Rebilling gestoppt).

    Der Plan wird NICHT sofort geaendert — der User behaelt seinen Plan
    bis zum Ende der bezahlten Periode. Das Kuendigungsdatum wird gespeichert,
    damit ein Cronjob den Downgrade zum richtigen Zeitpunkt ausfuehrt.
    """
    user = _find_user(params)
    if not user:
        log.error("on_rebill_cancelled: User nicht gefunden (email=%s, custom=%s)",
                  params.get("email"), params.get("custom"))
        return False, "User nicht gefunden"

    user_id = user["id"]
    order_id = params.get("order_id", "")
    current_plan = user.get("plan_id", "free")

    # Subscription-Status aktualisieren — Plan bleibt aktiv
    subs = db.get_subscriptions(user_id)
    cancelled_sub = None
    for sub in subs:
        if sub.get("order_id") == order_id and sub.get("status") == "active":
            db.update_subscription(sub["id"], status="cancelled")
            cancelled_sub = sub
            break

    if not cancelled_sub:
        # Auch ohne passende Subscription loggen
        log.warning(
            "on_rebill_cancelled: Keine aktive Subscription fuer Order '%s' gefunden (User %d)",
            order_id, user_id
        )

    # Kuendigungsdatum bestimmen: Ende der aktuellen Periode
    cancel_date = ""
    if cancelled_sub and cancelled_sub.get("period_end"):
        cancel_date = cancelled_sub["period_end"]
    else:
        # Fallback: 30 Tage ab jetzt
        cancel_date = (datetime.utcnow() + timedelta(days=30)).isoformat()

    log.info(
        "User %d: Abo-Kuendigung — Plan '%s' bleibt bis %s aktiv (Order: %s)",
        user_id, current_plan, cancel_date, order_id
    )

    # Benachrichtigung
    try:
        cancel_display = datetime.fromisoformat(cancel_date).strftime("%d.%m.%Y")
    except (ValueError, TypeError):
        cancel_display = cancel_date

    db.create_notification(
        user_id=user_id,
        title="Abo-Kuendigung bestaetigt",
        message=(
            f"Dein Abo (Bestellnr. {order_id}) wurde gekuendigt. "
            f"Dein aktueller Plan bleibt bis zum {cancel_display} aktiv. "
            f"Danach wirst du automatisch auf den Free-Plan zurueckgestuft."
        ),
        ntype="warning",
        link="/dashboard",
    )

    # DSGVO-Log
    db.log_dsgvo(
        user_id=user_id,
        action="subscription_cancelled",
        target_email=params.get("email", ""),
        channel="digistore24",
        content_summary=(
            f"Abo-Kuendigung: Order={order_id}, Plan={current_plan}, "
            f"aktiv bis {cancel_date}"
        ),
        legal_basis="vertragserfuellung",
    )

    return True, (
        f"User {user_id} Abo-Kuendigung gespeichert — "
        f"Plan '{current_plan}' aktiv bis {cancel_date}"
    )


def on_payment_missed(params: Dict[str, str]) -> Tuple[bool, str]:
    """
    Handler fuer fehlgeschlagene Zahlungen.

    Erstellt eine Benachrichtigung. Nach 3 aufeinanderfolgenden
    fehlgeschlagenen Zahlungen wird der User auf den Free-Plan
    zurueckgestuft.
    """
    user = _find_user(params)
    if not user:
        log.error("on_payment_missed: User nicht gefunden (email=%s, custom=%s)",
                  params.get("email"), params.get("custom"))
        return False, "User nicht gefunden"

    user_id = user["id"]
    order_id = params.get("order_id", "")
    current_plan = user.get("plan_id", "free")

    # Fehlgeschlagene Zahlungen zaehlen (aus IPN-Log)
    conn = db.get_db()
    row = conn.execute(
        """SELECT COUNT(*) AS cnt FROM digistore_ipn_log
           WHERE order_id = ? AND event = 'payment_missed'
           AND signature_valid = 1""",
        (order_id,),
    ).fetchone()
    fail_count = row["cnt"] if row else 0

    log.warning(
        "User %d: Zahlung fehlgeschlagen (%d. Mal) — Order: %s",
        user_id, fail_count, order_id
    )

    if fail_count >= 3:
        # Nach 3 Fehlversuchen: Downgrade auf Free
        db.update_user(user_id, plan_id="free")
        _update_subscription_by_order(user_id, order_id, status="payment_failed")

        db.create_notification(
            user_id=user_id,
            title="Plan auf Free zurueckgesetzt",
            message=(
                f"Deine Zahlung fuer Bestellnr. {order_id} ist {fail_count}x fehlgeschlagen. "
                f"Dein Plan wurde auf Free zurueckgesetzt. "
                f"Bitte aktualisiere deine Zahlungsmethode, um wieder zu upgraden."
            ),
            ntype="error",
            link="/dashboard",
        )

        db.log_dsgvo(
            user_id=user_id,
            action="payment_failed_downgrade",
            target_email=params.get("email", ""),
            channel="digistore24",
            content_summary=(
                f"Downgrade nach {fail_count} fehlgeschlagenen Zahlungen: "
                f"Order={order_id}, alter Plan={current_plan}"
            ),
            legal_basis="vertragserfuellung",
        )

        return True, (
            f"User {user_id} nach {fail_count} Fehlversuchen "
            f"auf Free downgraded (Order: {order_id})"
        )
    else:
        # Warnung, aber Plan bleibt erstmal aktiv
        db.create_notification(
            user_id=user_id,
            title="Zahlung fehlgeschlagen",
            message=(
                f"Deine Zahlung fuer Bestellnr. {order_id} konnte nicht eingezogen werden "
                f"(Versuch {fail_count} von 3). Bitte aktualisiere deine Zahlungsmethode."
            ),
            ntype="warning",
            link="/dashboard",
        )

        return True, (
            f"User {user_id} Zahlungsausfall #{fail_count} notiert — "
            f"Plan bleibt vorerst '{current_plan}'"
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HILFSFUNKTION: SUBSCRIPTION AKTUALISIEREN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _update_subscription_by_order(user_id: int, order_id: str,
                                  status: str) -> bool:
    """
    Aktualisiert den Status einer Subscription anhand der Order-ID.

    Sucht die neueste aktive Subscription des Users mit der
    gegebenen Order-ID und setzt den Status.
    """
    if not order_id:
        return False

    subs = db.get_subscriptions(user_id)
    for sub in subs:
        if sub.get("order_id") == order_id:
            db.update_subscription(sub["id"], status=status)
            log.info(
                "Subscription %d (Order: %s) Status -> '%s'",
                sub["id"], order_id, status
            )
            return True

    log.warning(
        "Keine Subscription mit Order '%s' fuer User %d gefunden",
        order_id, user_id
    )
    return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CHECKOUT-URL ERSTELLEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def create_checkout_url(user_id: int, plan_id: str,
                        user_email: str, user_name: str = "") -> Optional[str]:
    """
    Erstellt eine Digistore24-Checkout-URL mit vorausgefuellten Parametern.

    Args:
        user_id:    Interne User-ID (wird als custom-Parameter uebergeben)
        plan_id:    Plan-ID ('pro' oder 'business')
        user_email: E-Mail des Users (fuer Digistore-Vorausfuellung)
        user_name:  Name des Users (optional)

    Returns:
        Vollstaendige Checkout-URL oder None bei unbekanntem Plan
    """
    product_id = DIGISTORE_CONFIG["product_ids"].get(plan_id)
    if not product_id or plan_id == "free":
        log.warning("create_checkout_url: Ungueltiger Plan '%s'", plan_id)
        return None

    # Checkout-URL zusammenbauen
    base_url = f"{_DS_CHECKOUT_BASE}/{product_id}"

    query_params = {
        "custom": str(user_id),
        "email": user_email,
        "thank_you_url": DIGISTORE_CONFIG["thank_you_url"],
        "cancel_url": DIGISTORE_CONFIG["cancel_url"],
    }

    # Name aufteilen in Vor- und Nachname
    if user_name:
        name_parts = user_name.strip().split(" ", 1)
        query_params["first_name"] = name_parts[0]
        if len(name_parts) > 1:
            query_params["last_name"] = name_parts[1]

    checkout_url = base_url + "?" + urllib.parse.urlencode(query_params)

    log.info(
        "Checkout-URL erstellt fuer User %d: Plan=%s, Produkt=%s",
        user_id, plan_id, product_id
    )

    return checkout_url


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ABO KUENDIGEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def cancel_subscription(user_id: int) -> Optional[str]:
    """
    Gibt die Digistore24-Kuendigungsportal-URL zurueck.

    Sucht die aktive Subscription des Users und baut die URL
    zum Digistore24-Kundenportal, wo der Kunde sein Abo selbst
    kuendigen kann.

    Args:
        user_id: Interne User-ID

    Returns:
        URL zum Digistore24-Kuendigungsportal oder None
    """
    subs = db.get_subscriptions(user_id)

    # Neueste aktive Subscription finden
    active_sub = None
    for sub in subs:
        if sub.get("status") == "active":
            active_sub = sub
            break

    if not active_sub:
        log.info("cancel_subscription: Keine aktive Subscription fuer User %d", user_id)
        return None

    order_id = active_sub.get("order_id", "")
    if not order_id:
        log.warning("cancel_subscription: Subscription ohne Order-ID (User %d)", user_id)
        return None

    # Digistore24-Kuendigungsportal-URL
    cancel_url = (
        f"https://www.digistore24.com/selfmanage/{order_id}"
    )

    log.info(
        "Kuendigungsportal-URL erstellt fuer User %d (Order: %s)",
        user_id, order_id
    )

    return cancel_url


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ABO-STATUS PRUEFEN (OPTIONAL — API-CALL)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def check_subscription_status(order_id: str) -> Optional[Dict[str, Any]]:
    """
    Prueft den Abo-Status ueber die Digistore24-API (optional).

    Erfordert einen konfigurierten API-Key. Gibt die API-Antwort
    als Dict zurueck oder None bei Fehler.

    Args:
        order_id: Digistore24-Bestellnummer

    Returns:
        Dict mit Abo-Details oder None
    """
    api_key = DIGISTORE_CONFIG.get("api_key", "")
    if not api_key:
        log.debug("check_subscription_status: Kein API-Key konfiguriert")
        return None

    url = f"{_DS_API_BASE}/order/{order_id}"
    headers = {
        "Accept": "application/json",
        "X-DS-API-KEY": api_key,
    }

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()

        data = resp.json()
        if data.get("result") == "success":
            order_data = data.get("data", {})
            log.info(
                "Abo-Status fuer Order '%s': %s",
                order_id, order_data.get("status", "unbekannt")
            )
            return order_data
        else:
            log.warning(
                "Digistore24-API Fehler fuer Order '%s': %s",
                order_id, data.get("message", "Unbekannter Fehler")
            )
            return None

    except requests.Timeout:
        log.error("Digistore24-API Timeout fuer Order '%s'", order_id)
        return None
    except requests.RequestException as exc:
        log.error("Digistore24-API Fehler fuer Order '%s': %s", order_id, exc)
        return None
    except (ValueError, KeyError) as exc:
        log.error("Digistore24-API Antwort nicht parsebar: %s", exc)
        return None
