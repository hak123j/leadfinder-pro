"""
auth.py — Authentifizierung fuer LeadFinder Pro v3

Bcrypt-Hashing (PBKDF2-Fallback), Sessions, Decorators, Verschluesselung.
"""

import os
import hmac
import hashlib
import secrets
import logging
import base64
import functools
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple

import database as db

log = logging.getLogger("leadfinder.auth")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BCRYPT / PBKDF2 FALLBACK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
try:
    import bcrypt
    HAS_BCRYPT = True
except ImportError:
    HAS_BCRYPT = False
    log.warning("bcrypt nicht verfuegbar — nutze PBKDF2-Fallback")

# Fernet fuer SMTP/IMAP-Passwort-Verschluesselung
_ENCRYPTION_KEY: Optional[bytes] = None
_KEY_FILE = os.path.join(db.DATA_DIR, ".encryption_key")


def _get_encryption_key() -> bytes:
    """Laedt oder erzeugt den Verschluesselungsschluessel."""
    global _ENCRYPTION_KEY
    if _ENCRYPTION_KEY is not None:
        return _ENCRYPTION_KEY
    if os.path.exists(_KEY_FILE):
        with open(_KEY_FILE, "rb") as f:
            _ENCRYPTION_KEY = f.read()
    else:
        _ENCRYPTION_KEY = secrets.token_bytes(32)
        with open(_KEY_FILE, "wb") as f:
            f.write(_ENCRYPTION_KEY)
        os.chmod(_KEY_FILE, 0o600)
    return _ENCRYPTION_KEY


def hash_password(password: str) -> str:
    """Hasht ein Passwort mit bcrypt (12 Rounds) oder PBKDF2-Fallback."""
    if HAS_BCRYPT:
        hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12))
        return hashed.decode("utf-8")
    else:
        salt = secrets.token_hex(16)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode(), 260000)
        return f"pbkdf2:{salt}:{dk.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    """Verifiziert ein Passwort gegen einen Hash."""
    try:
        if password_hash.startswith("pbkdf2:"):
            parts = password_hash.split(":")
            if len(parts) != 3:
                return False
            salt = parts[1]
            stored_dk = parts[2]
            dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode(), 260000)
            return hmac.compare_digest(dk.hex(), stored_dk)
        elif HAS_BCRYPT:
            return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
        else:
            return False
    except (ValueError, TypeError) as exc:
        log.error("Passwort-Verifizierung fehlgeschlagen: %s", exc)
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  USER-VERWALTUNG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def create_user(email: str, password: str, name: str = "",
                company: str = "", phone: str = "") -> Tuple[Optional[int], str]:
    """Erstellt einen neuen User. Gibt (user_id, error_msg) zurueck."""
    email = email.lower().strip()
    if not email or "@" not in email or "." not in email.split("@")[1]:
        return None, "Ungueltige E-Mail-Adresse"
    if len(password) < 8:
        return None, "Passwort muss mindestens 8 Zeichen lang sein"
    if db.get_user_by_email(email):
        return None, "E-Mail-Adresse bereits registriert"

    pw_hash = hash_password(password)
    user_id = db.create_user(email, pw_hash, name, company, phone)
    if user_id is None:
        return None, "Registrierung fehlgeschlagen"

    # Verify-Token erstellen
    verify_token = secrets.token_urlsafe(32)
    db.update_user(user_id, verify_token=verify_token)

    log.info("Neuer User registriert: %s (ID=%d)", email, user_id)
    return user_id, ""


def authenticate_user(email: str, password: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """Authentifiziert einen User. Gibt (user_dict, error_msg) zurueck."""
    email = email.lower().strip()
    user = db.get_user_by_email(email)
    if not user:
        return None, "E-Mail oder Passwort falsch"
    if not verify_password(password, user["password_hash"]):
        return None, "E-Mail oder Passwort falsch"

    # Session-Token generieren
    session_token = secrets.token_urlsafe(48)
    session_expires = (datetime.utcnow() + timedelta(days=30)).isoformat()
    db.update_user(
        user["id"],
        session_token=session_token,
        session_expires=session_expires,
        last_login=datetime.utcnow().isoformat(),
    )
    user["session_token"] = session_token
    user["session_expires"] = session_expires
    log.info("User eingeloggt: %s", email)
    return user, ""


def create_session_for_user(user: Dict[str, Any]) -> Dict[str, Any]:
    """Erstellt eine Session fuer einen User (shared von Login + OAuth)."""
    session_token = secrets.token_urlsafe(48)
    session_expires = (datetime.utcnow() + timedelta(days=30)).isoformat()
    db.update_user(
        user["id"],
        session_token=session_token,
        session_expires=session_expires,
        last_login=datetime.utcnow().isoformat(),
    )
    user["session_token"] = session_token
    user["session_expires"] = session_expires
    return user


def create_or_login_oauth_user(provider: str, oauth_id: str, email: str,
                                name: str = "") -> Tuple[Optional[Dict[str, Any]], str]:
    """Erstellt oder findet einen OAuth-User. Gibt (user_dict, error_msg) zurueck."""
    email = email.lower().strip()
    if not email:
        return None, "Keine E-Mail vom OAuth-Provider erhalten"

    # 1. Suche per OAuth-ID
    user = db.get_user_by_oauth(provider, oauth_id)
    if user:
        user = create_session_for_user(user)
        log.info("OAuth-Login: %s via %s", email, provider)
        return user, ""

    # 2. Suche per E-Mail (Account verknuepfen)
    user = db.get_user_by_email(email)
    if user:
        db.update_user(user["id"], oauth_provider=provider, oauth_id=oauth_id)
        user["oauth_provider"] = provider
        user["oauth_id"] = oauth_id
        user = create_session_for_user(user)
        log.info("OAuth-Verknuepfung: %s mit %s", email, provider)
        return user, ""

    # 3. Neuen User erstellen
    user_id = db.create_user(email, password_hash="", name=name or "")
    if not user_id:
        return None, "User-Erstellung fehlgeschlagen"
    db.update_user(user_id, oauth_provider=provider, oauth_id=oauth_id, email_verified=1)
    user = db.get_user_by_id(user_id)
    if not user:
        return None, "User nach Erstellung nicht gefunden"
    user = create_session_for_user(user)
    log.info("Neuer OAuth-User: %s via %s", email, provider)
    return user, ""


def get_current_user(request) -> Optional[Dict[str, Any]]:
    """Liest den aktuellen User aus Cookie oder Authorization-Header."""
    # 1. Cookie pruefen
    token = request.cookies.get("lf_session")
    # 2. Authorization-Header als Fallback
    if not token:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
    # 3. API-Key pruefen
    if not token:
        api_key = request.headers.get("X-API-Key", "")
        if api_key:
            key_hash = hashlib.sha256(api_key.encode()).hexdigest()
            key_row = db.verify_api_key(key_hash)
            if key_row:
                return db.get_user_with_plan(key_row["user_id"])
        return None

    if not token:
        return None
    return db.get_user_by_session(token)


def logout_user(user_id: int) -> bool:
    """Invalidiert die Session eines Users."""
    return db.update_user(user_id, session_token="", session_expires="")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PASSWORT-RESET
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def create_password_reset(email: str) -> Optional[str]:
    """Erstellt einen Passwort-Reset-Token. Gibt den Token zurueck."""
    user = db.get_user_by_email(email.lower().strip())
    if not user:
        return None
    token = secrets.token_urlsafe(32)
    expires = (datetime.utcnow() + timedelta(hours=1)).isoformat()
    db.update_user(user["id"], reset_token=token, reset_token_expires=expires)
    return token


def reset_password(token: str, new_password: str) -> Tuple[bool, str]:
    """Setzt das Passwort mit einem Reset-Token zurueck."""
    if len(new_password) < 8:
        return False, "Passwort muss mindestens 8 Zeichen lang sein"

    conn = db.get_db()
    row = conn.execute(
        "SELECT * FROM users WHERE reset_token = ? AND reset_token_expires > datetime('now')",
        (token,),
    ).fetchone()
    if not row:
        return False, "Token ungueltig oder abgelaufen"

    user = dict(row)
    pw_hash = hash_password(new_password)
    db.update_user(
        user["id"],
        password_hash=pw_hash,
        reset_token="",
        reset_token_expires="",
        session_token="",
        session_expires="",
    )
    log.info("Passwort zurueckgesetzt fuer User %s", user["email"])
    return True, ""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EMAIL-VERIFIZIERUNG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def verify_email(token: str) -> bool:
    """Verifiziert eine E-Mail-Adresse via Token."""
    conn = db.get_db()
    row = conn.execute(
        "SELECT * FROM users WHERE verify_token = ?", (token,)
    ).fetchone()
    if not row:
        return False
    db.update_user(row["id"], email_verified=1, verify_token="")
    log.info("E-Mail verifiziert fuer User %s", row["email"])
    return True


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DECORATORS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def login_required(f):
    """Decorator: Erfordert einen eingeloggten User."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        from flask import request as flask_request, jsonify, redirect
        user = get_current_user(flask_request)
        if not user:
            if flask_request.is_json or flask_request.path.startswith("/api/"):
                return jsonify({"error": "Nicht authentifiziert"}), 401
            return redirect("/login")
        # User mit Plan-Details laden
        user_full = db.get_user_with_plan(user["id"])
        if not user_full:
            return jsonify({"error": "User nicht gefunden"}), 401
        kwargs["current_user"] = user_full
        return f(*args, **kwargs)
    return decorated


def plan_required(feature: str):
    """Decorator: Erfordert ein Plan-Feature (z.B. 'has_pdf', 'has_kanban')."""
    def decorator(f):
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            user = kwargs.get("current_user")
            if not user:
                from flask import jsonify
                return jsonify({"error": "Nicht authentifiziert"}), 401
            if not user.get(feature):
                from flask import jsonify
                return jsonify({
                    "error": "Upgrade erforderlich",
                    "feature": feature,
                    "plan": user.get("plan_id", "free"),
                }), 403
            return f(*args, **kwargs)
        return decorated
    return decorator


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  VERSCHLUESSELUNG (SMTP/IMAP-PASSWOERTER)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def encrypt_smtp_password(plaintext: str) -> str:
    """Verschluesselt ein SMTP/IMAP-Passwort mit AES-GCM-like via HMAC."""
    if not plaintext:
        return ""
    key = _get_encryption_key()
    # Einfache symmetrische Verschluesselung mit XOR + HMAC
    nonce = secrets.token_bytes(16)
    # KDF: HKDF-aehnlich
    derived = hashlib.pbkdf2_hmac("sha256", key, nonce, 1000)
    plaintext_bytes = plaintext.encode("utf-8")
    # XOR-Verschluesselung
    encrypted = bytes(p ^ derived[i % 32] for i, p in enumerate(plaintext_bytes))
    # HMAC fuer Integritaet
    mac = hmac.new(key, nonce + encrypted, hashlib.sha256).digest()[:16]
    payload = nonce + encrypted + mac
    return base64.urlsafe_b64encode(payload).decode("ascii")


def decrypt_smtp_password(ciphertext: str) -> str:
    """Entschluesselt ein SMTP/IMAP-Passwort."""
    if not ciphertext:
        return ""
    try:
        key = _get_encryption_key()
        payload = base64.urlsafe_b64decode(ciphertext.encode("ascii"))
        if len(payload) < 33:  # 16 nonce + 1 data + 16 mac minimum
            return ""
        nonce = payload[:16]
        mac_stored = payload[-16:]
        encrypted = payload[16:-16]
        # MAC verifizieren
        mac_computed = hmac.new(key, nonce + encrypted, hashlib.sha256).digest()[:16]
        if not hmac.compare_digest(mac_stored, mac_computed):
            log.warning("SMTP-Passwort-Entschluesselung: MAC ungueltig")
            return ""
        derived = hashlib.pbkdf2_hmac("sha256", key, nonce, 1000)
        decrypted = bytes(c ^ derived[i % 32] for i, c in enumerate(encrypted))
        return decrypted.decode("utf-8")
    except (ValueError, UnicodeDecodeError) as exc:
        log.error("SMTP-Passwort-Entschluesselung fehlgeschlagen: %s", exc)
        return ""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  API-KEY-VERWALTUNG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_api_key(user_id: int, name: str = "") -> Optional[str]:
    """Generiert einen neuen API-Key. Gibt den Klartext-Key zurueck (nur einmal sichtbar)."""
    raw_key = f"lf_{secrets.token_urlsafe(32)}"
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    key_id = db.create_api_key(user_id, key_hash, name)
    if key_id:
        return raw_key
    return None


def revoke_api_key(key_id: int, user_id: int) -> bool:
    """Widerruft (loescht) einen API-Key."""
    return db.delete_api_key(key_id, user_id)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HILFSFUNKTIONEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def change_password(user_id: int, old_password: str, new_password: str) -> Tuple[bool, str]:
    """Aendert das Passwort (erfordert altes Passwort)."""
    if len(new_password) < 8:
        return False, "Neues Passwort muss mindestens 8 Zeichen lang sein"
    user = db.get_user_by_id(user_id)
    if not user:
        return False, "User nicht gefunden"
    if not verify_password(old_password, user["password_hash"]):
        return False, "Altes Passwort falsch"
    pw_hash = hash_password(new_password)
    db.update_user(user_id, password_hash=pw_hash)
    return True, ""


def get_session_cookie_params(session_token: str) -> Dict[str, Any]:
    """Gibt die Parameter fuer das Session-Cookie zurueck."""
    return {
        "key": "lf_session",
        "value": session_token,
        "max_age": 30 * 24 * 3600,  # 30 Tage
        "httponly": True,
        "samesite": "Lax",
        "secure": False,  # True in Produktion
        "path": "/",
    }
