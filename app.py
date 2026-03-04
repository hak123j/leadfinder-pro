"""
app.py — Haupt-Server fuer LeadFinder Pro v3

Flask-Anwendung mit allen API-Endpunkten, Middleware, Static-Serving.
Start: python app.py  ->  http://localhost:5000
"""

import os
import sys
import json
import time
import logging
import hashlib
import secrets
import base64
import uuid
import threading
import urllib.request
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LOGGING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("leadfinder.app")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  IMPORTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
try:
    from flask import (
        Flask, jsonify, request as flask_request, redirect, url_for,
        send_from_directory, send_file, Response, make_response,
        render_template,
    )
except ImportError:
    print("Flask nicht installiert: pip install flask")
    sys.exit(1)

import database as db
import auth
import scraper
import email_engine
import analytics
import compliance
import digistore

try:
    import pdf_generator
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

try:
    import automation
    HAS_AUTOMATION = True
except ImportError:
    HAS_AUTOMATION = False

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FLASK APP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
    static_folder=os.path.join(BASE_DIR, "static"),
)
app.config["SECRET_KEY"] = os.urandom(32).hex()
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10 MB

TRACKING_HOST = os.environ.get("TRACKING_HOST", "https://getleadfinder.de")

# OAuth-Konfiguration
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
APPLE_CLIENT_ID = os.environ.get("APPLE_CLIENT_ID", "")
APPLE_TEAM_ID = os.environ.get("APPLE_TEAM_ID", "")
APPLE_KEY_ID = os.environ.get("APPLE_KEY_ID", "")
APPLE_PRIVATE_KEY = os.environ.get("APPLE_PRIVATE_KEY", "")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ADMIN / VISITOR-TRACKING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ADMIN_TOKEN = uuid.uuid4().hex
VISITORS_FILE = os.path.join(BASE_DIR, "data", "visitors.json")
_visitors_lock = threading.Lock()
_geo_cache: Dict[str, Dict] = {}


def load_visitors() -> Dict:
    """Lade visitors.json (thread-safe)."""
    with _visitors_lock:
        if os.path.exists(VISITORS_FILE):
            try:
                with open(VISITORS_FILE, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {"sessions": {}}
        return {"sessions": {}}


def save_visitors(data: Dict):
    """Speichere visitors.json (thread-safe)."""
    with _visitors_lock:
        os.makedirs(os.path.dirname(VISITORS_FILE), exist_ok=True)
        with open(VISITORS_FILE, "w") as f:
            json.dump(data, f, indent=2)


def geoip_lookup(ip: str) -> Dict:
    """GeoIP via ip-api.com mit Cache. Private IPs → 'Local'."""
    if ip in _geo_cache:
        return _geo_cache[ip]

    # Private / Loopback erkennen
    if ip.startswith(("127.", "10.", "192.168.", "172.16.", "172.17.",
                      "172.18.", "172.19.", "172.2", "172.30.", "172.31.")) \
       or ip in ("::1", "localhost", "0.0.0.0"):
        result = {"city": "Local", "country": "Local", "lat": 0, "lon": 0}
        _geo_cache[ip] = result
        return result

    try:
        url = f"http://ip-api.com/json/{ip}?fields=city,country,lat,lon"
        req = urllib.request.Request(url, headers={"User-Agent": "LeadFinder/3"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            geo = json.loads(resp.read().decode())
            result = {
                "city": geo.get("city", "Unknown"),
                "country": geo.get("country", "Unknown"),
                "lat": geo.get("lat", 0),
                "lon": geo.get("lon", 0),
            }
            _geo_cache[ip] = result
            return result
    except Exception:
        result = {"city": "Unknown", "country": "Unknown", "lat": 0, "lon": 0}
        _geo_cache[ip] = result
        return result


def cleanup_old_sessions(data: Dict, max_age_days: int = 30) -> int:
    """Entferne Sessions aelter als max_age_days. Gibt Anzahl entfernter zurueck."""
    cutoff = time.time() - (max_age_days * 86400)
    old_keys = [k for k, v in data.get("sessions", {}).items()
                if v.get("last_seen", 0) < cutoff]
    for k in old_keys:
        del data["sessions"][k]
    return len(old_keys)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MIDDLEWARE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.after_request
def add_security_headers(response):
    """Sicherheitsheader hinzufuegen."""
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return response


@app.teardown_appcontext
def close_connection(exception):
    """DB-Verbindung schliessen."""
    db.close_db()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SEITEN-ROUTES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/")
def index():
    """Startseite — weiter zum Login oder Dashboard."""
    user = auth.get_current_user(flask_request)
    if user:
        return redirect("/dashboard")
    return redirect("/login")


@app.route("/login")
def login_page():
    """Login-Seite."""
    return render_template("login.html")


@app.route("/dashboard")
@auth.login_required
def dashboard_page(current_user=None):
    """Dashboard — Haupt-Anwendung."""
    tpl_path = os.path.join(BASE_DIR, "templates", "dashboard.html")
    with open(tpl_path, "r") as f:
        html = f.read()
    html = html.replace("{{ADMIN_TOKEN}}", ADMIN_TOKEN)
    return Response(html, mimetype="text/html")


@app.route("/unsubscribe/<tracking_id>")
def unsubscribe_page(tracking_id):
    """Abmelde-Seite."""
    token = flask_request.args.get("token", "")
    return render_template("unsubscribe.html",
                           tracking_id=tracking_id, token=token)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  AUTH-API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/auth/register", methods=["POST"])
def api_register():
    """Neuen User registrieren."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    email = data.get("email", "").strip()
    password = data.get("password", "")
    name = data.get("name", "").strip()
    company = data.get("company", "").strip()
    phone = data.get("phone", "").strip()

    user_id, error = auth.create_user(email, password, name, company, phone)
    if error:
        return jsonify({"error": error}), 400

    # Auto-Login
    user, _ = auth.authenticate_user(email, password)
    if user:
        resp = jsonify({"ok": True, "user_id": user_id})
        cookie = auth.get_session_cookie_params(user["session_token"])
        resp.set_cookie(**cookie)
        return resp
    return jsonify({"ok": True, "user_id": user_id})


@app.route("/api/auth/login", methods=["POST"])
def api_login():
    """User einloggen."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    email = data.get("email", "").strip()
    password = data.get("password", "")

    user, error = auth.authenticate_user(email, password)
    if error:
        return jsonify({"error": error}), 401

    resp = jsonify({"ok": True, "user_id": user["id"]})
    cookie = auth.get_session_cookie_params(user["session_token"])
    resp.set_cookie(**cookie)
    return resp


@app.route("/api/auth/logout", methods=["POST"])
@auth.login_required
def api_logout(current_user=None):
    """User ausloggen."""
    auth.logout_user(current_user["id"])
    resp = jsonify({"ok": True})
    resp.delete_cookie("lf_session")
    return resp


@app.route("/api/auth/me")
@auth.login_required
def api_me(current_user=None):
    """Aktueller User mit Plan-Details."""
    safe_user = {k: v for k, v in current_user.items()
                 if k not in ("password_hash", "session_token", "reset_token",
                              "verify_token", "smtp_pass_encrypted", "imap_pass_encrypted")}
    safe_user["unread_notifications"] = db.count_unread_notifications(current_user["id"])
    return jsonify(safe_user)


@app.route("/api/auth/password-reset", methods=["POST"])
def api_password_reset_request():
    """Passwort-Reset anfordern."""
    data = flask_request.json
    if not data or not data.get("email"):
        return jsonify({"error": "E-Mail fehlt"}), 400
    token = auth.create_password_reset(data["email"])
    # Immer OK antworten (kein Hinweis ob E-Mail existiert)
    return jsonify({"ok": True, "message": "Falls die E-Mail existiert, wurde ein Reset-Link gesendet."})


@app.route("/api/auth/password-reset/confirm", methods=["POST"])
def api_password_reset_confirm():
    """Passwort mit Token zuruecksetzen."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    token = data.get("token", "")
    password = data.get("password", "")
    ok, error = auth.reset_password(token, password)
    if not ok:
        return jsonify({"error": error}), 400
    return jsonify({"ok": True})


@app.route("/api/auth/change-password", methods=["POST"])
@auth.login_required
def api_change_password(current_user=None):
    """Passwort aendern."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    ok, error = auth.change_password(
        current_user["id"],
        data.get("old_password", ""),
        data.get("new_password", ""),
    )
    if not ok:
        return jsonify({"error": error}), 400
    return jsonify({"ok": True})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  OAUTH — GOOGLE + APPLE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
import urllib.parse
import json as _json

@app.route("/api/auth/google")
def oauth_google_redirect():
    """Redirect zu Google OAuth."""
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        return redirect("/login?error=oauth_not_configured")
    state = secrets.token_urlsafe(32)
    resp = redirect("https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode({
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": TRACKING_HOST + "/api/auth/google/callback",
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "offline",
        "prompt": "select_account",
    }))
    resp.set_cookie("oauth_state", state, max_age=600, httponly=True, samesite="Lax")
    return resp


@app.route("/api/auth/google/callback")
def oauth_google_callback():
    """Google OAuth Callback — tauscht Code gegen User-Info."""
    import urllib.request
    error = flask_request.args.get("error")
    if error:
        return redirect(f"/login?error={error}")
    code = flask_request.args.get("code", "")
    state = flask_request.args.get("state", "")
    stored_state = flask_request.cookies.get("oauth_state", "")
    if not code or not state or state != stored_state:
        return redirect("/login?error=invalid_state")

    # Code gegen Access-Token tauschen
    try:
        token_data = urllib.parse.urlencode({
            "code": code,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": TRACKING_HOST + "/api/auth/google/callback",
            "grant_type": "authorization_code",
        }).encode()
        token_req = urllib.request.Request(
            "https://oauth2.googleapis.com/token",
            data=token_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(token_req, timeout=10) as token_resp:
            tokens = _json.loads(token_resp.read())
        access_token = tokens.get("access_token", "")
        if not access_token:
            return redirect("/login?error=token_exchange_failed")
    except Exception as exc:
        log.error("Google Token-Austausch fehlgeschlagen: %s", exc)
        return redirect("/login?error=token_exchange_failed")

    # User-Info abrufen
    try:
        info_req = urllib.request.Request(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        with urllib.request.urlopen(info_req, timeout=10) as info_resp:
            userinfo = _json.loads(info_resp.read())
        google_id = userinfo.get("id", "")
        email = userinfo.get("email", "")
        name = userinfo.get("name", "")
        if not google_id or not email:
            return redirect("/login?error=missing_user_info")
    except Exception as exc:
        log.error("Google User-Info fehlgeschlagen: %s", exc)
        return redirect("/login?error=userinfo_failed")

    # User erstellen/finden + Session
    user, error_msg = auth.create_or_login_oauth_user("google", google_id, email, name)
    if error_msg:
        return redirect(f"/login?error={urllib.parse.quote(error_msg)}")
    resp = redirect("/dashboard")
    cookie = auth.get_session_cookie_params(user["session_token"])
    resp.set_cookie(**cookie)
    resp.delete_cookie("oauth_state")
    return resp


@app.route("/api/auth/apple")
def oauth_apple_redirect():
    """Redirect zu Apple OAuth."""
    if not APPLE_CLIENT_ID or not APPLE_TEAM_ID:
        return redirect("/login?error=oauth_not_configured")
    state = secrets.token_urlsafe(32)
    resp = redirect("https://appleid.apple.com/auth/authorize?" + urllib.parse.urlencode({
        "client_id": APPLE_CLIENT_ID,
        "redirect_uri": TRACKING_HOST + "/api/auth/apple/callback",
        "response_type": "code id_token",
        "scope": "name email",
        "response_mode": "form_post",
        "state": state,
    }))
    resp.set_cookie("oauth_state", state, max_age=600, httponly=True, samesite="None", secure=True)
    return resp


@app.route("/api/auth/apple/callback", methods=["POST"])
def oauth_apple_callback():
    """Apple OAuth Callback — empfaengt code + id_token als form-data."""
    error = flask_request.form.get("error")
    if error:
        return redirect(f"/login?error={error}")
    state = flask_request.form.get("state", "")
    stored_state = flask_request.cookies.get("oauth_state", "")
    if not state or state != stored_state:
        return redirect("/login?error=invalid_state")

    id_token = flask_request.form.get("id_token", "")
    if not id_token:
        return redirect("/login?error=missing_id_token")

    # JWT-Payload dekodieren (Base64 ohne Signatur-Verifikation)
    try:
        payload_b64 = id_token.split(".")[1]
        # Base64-Padding korrigieren
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        payload = _json.loads(base64.urlsafe_b64decode(payload_b64))
        apple_sub = payload.get("sub", "")
        email = payload.get("email", "")
        if not apple_sub or not email:
            return redirect("/login?error=missing_user_info")
    except Exception as exc:
        log.error("Apple ID-Token Dekodierung fehlgeschlagen: %s", exc)
        return redirect("/login?error=token_decode_failed")

    # Name aus user-JSON (Apple sendet ihn nur beim ersten Login)
    name = ""
    user_json = flask_request.form.get("user", "")
    if user_json:
        try:
            user_data = _json.loads(user_json)
            name_data = user_data.get("name", {})
            name = f"{name_data.get('firstName', '')} {name_data.get('lastName', '')}".strip()
        except (ValueError, TypeError):
            pass

    # User erstellen/finden + Session
    user, error_msg = auth.create_or_login_oauth_user("apple", apple_sub, email, name)
    if error_msg:
        return redirect(f"/login?error={urllib.parse.quote(error_msg)}")
    resp = redirect("/dashboard")
    cookie = auth.get_session_cookie_params(user["session_token"])
    resp.set_cookie(**cookie)
    resp.delete_cookie("oauth_state")
    return resp


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LEAD-SUCHE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/search")
@auth.login_required
def api_search(current_user=None):
    """Lead-Suche nach Branche und Stadt."""
    q = flask_request.args.get("q", "").strip()
    city = flask_request.args.get("city", "").strip()
    n = min(max(int(flask_request.args.get("n", "10")), 1), 20)
    if not q:
        return jsonify({"error": "Suchbegriff fehlt"}), 400

    # Plan-Limit pruefen
    user = current_user
    if user.get("searches_per_month", 0) < 999999:
        if user.get("searches_this_month", 0) >= user.get("searches_per_month", 5):
            return jsonify({"error": "Monatliches Suchlimit erreicht. Upgrade auf Pro!"}), 403

    try:
        start = time.time()
        blacklist_set = db.get_blacklist_set(user["id"])
        found = scraper.find_leads(q, city, n, blacklist_set=blacklist_set)
        duration = round(time.time() - start, 1)

        # In DB speichern
        db.increment_user_searches(user["id"])
        contacted = db.get_contacted_emails(user["id"])
        for lead in found:
            em = lead.get("email") or ""
            lead["already_contacted"] = bool(em and em in contacted)
            lead["is_duplicate"] = db.lead_exists(user["id"], email=em, website=lead.get("website"))

        comp = scraper.competitor_analysis(q, city)
        db.record_search(user["id"], q, city, len(found), comp.get("total_estimated", 0), duration)

        return jsonify({
            "query": q, "city": city, "count": len(found),
            "leads": found, "competition": comp, "duration": duration,
        })
    except Exception as exc:
        log.error("Suchfehler: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/search-multi", methods=["POST"])
@auth.login_required
@auth.plan_required("has_multi_city")
def api_search_multi(current_user=None):
    """Multi-Stadt-Suche."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    q = data.get("query", "").strip()
    cities = data.get("cities", [])
    n = min(max(int(data.get("count", 5)), 1), 10)
    if not q or not cities:
        return jsonify({"error": "Query und Staedte benoetigt"}), 400
    if not isinstance(cities, list):
        return jsonify({"error": "cities muss eine Liste sein"}), 400

    all_leads: List[Dict[str, Any]] = []
    blacklist_set = db.get_blacklist_set(current_user["id"])
    contacted = db.get_contacted_emails(current_user["id"])

    for city in cities[:10]:
        try:
            found = scraper.find_leads(q, city, count=n, blacklist_set=blacklist_set)
            for lead in found:
                em = lead.get("email") or ""
                lead["already_contacted"] = bool(em and em in contacted)
                lead["search_city"] = city
            all_leads.extend(found)
            db.record_search(current_user["id"], q, city, len(found))
        except Exception as exc:
            log.warning("Multi-Stadt-Fehler fuer %s: %s", city, exc)

    return jsonify({"query": q, "cities": cities, "total": len(all_leads), "leads": all_leads})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LEAD-CRUD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/leads")
@auth.login_required
def api_leads_list(current_user=None):
    """Alle Leads des Users."""
    stage = flask_request.args.get("stage")
    search = flask_request.args.get("search")
    limit = min(int(flask_request.args.get("limit", "200")), 500)
    offset = int(flask_request.args.get("offset", "0"))
    leads = db.get_leads_by_user(current_user["id"], limit, offset, stage, search)
    total = db.count_leads(current_user["id"])
    return jsonify({"leads": leads, "total": total})


@app.route("/api/leads", methods=["POST"])
@auth.login_required
def api_leads_create(current_user=None):
    """Lead speichern."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    # Max Leads pruefen
    max_leads = current_user.get("max_leads_stored", 100)
    if max_leads < 999999 and db.count_leads(current_user["id"]) >= max_leads:
        return jsonify({"error": f"Lead-Limit erreicht ({max_leads}). Upgrade erforderlich!"}), 403
    lead_id = db.create_lead(current_user["id"], data)
    if lead_id:
        return jsonify({"ok": True, "lead_id": lead_id})
    return jsonify({"error": "Lead konnte nicht gespeichert werden"}), 500


@app.route("/api/leads/save-all", methods=["POST"])
@auth.login_required
def api_leads_save_all(current_user=None):
    """Mehrere Leads auf einmal speichern."""
    data = flask_request.json
    if not data or not isinstance(data.get("leads"), list):
        return jsonify({"error": "leads-Array fehlt"}), 400
    saved = 0
    max_leads = current_user.get("max_leads_stored", 100)
    current_count = db.count_leads(current_user["id"])
    for lead_data in data["leads"]:
        if max_leads < 999999 and current_count + saved >= max_leads:
            break
        if db.lead_exists(current_user["id"], email=lead_data.get("email"),
                          website=lead_data.get("website")):
            continue
        if db.create_lead(current_user["id"], lead_data):
            saved += 1
    return jsonify({"ok": True, "saved": saved})


@app.route("/api/leads/<int:lead_id>")
@auth.login_required
def api_lead_get(lead_id, current_user=None):
    """Einzelnen Lead abrufen."""
    lead = db.get_lead(lead_id, current_user["id"])
    if not lead:
        return jsonify({"error": "Lead nicht gefunden"}), 404
    lead["tags"] = db.get_lead_tags(lead_id)
    return jsonify(lead)


@app.route("/api/leads/<int:lead_id>", methods=["PUT"])
@auth.login_required
def api_lead_update(lead_id, current_user=None):
    """Lead aktualisieren."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    ok = db.update_lead(lead_id, current_user["id"], **data)
    return jsonify({"ok": ok})


@app.route("/api/leads/<int:lead_id>", methods=["DELETE"])
@auth.login_required
def api_lead_delete(lead_id, current_user=None):
    """Lead loeschen."""
    ok = db.delete_lead(lead_id, current_user["id"])
    return jsonify({"ok": ok})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  KANBAN / PIPELINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/kanban")
@auth.login_required
def api_kanban(current_user=None):
    """Kanban-Board: Stages + Leads pro Stage."""
    stages = db.get_kanban_stages(current_user["id"])
    leads_by_stage = db.get_leads_by_stage(current_user["id"])
    # Tags pro Lead
    for stage_name, leads in leads_by_stage.items():
        for lead in leads:
            lead["tags"] = db.get_lead_tags(lead["id"])
    return jsonify({"stages": stages, "leads": leads_by_stage})


@app.route("/api/kanban/move", methods=["POST"])
@auth.login_required
def api_kanban_move(current_user=None):
    """Lead in Kanban verschieben."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    lead_id = data.get("lead_id")
    new_stage = data.get("stage", "")
    if not lead_id or not new_stage:
        return jsonify({"error": "lead_id und stage benoetigt"}), 400
    ok = db.update_lead(lead_id, current_user["id"], kanban_stage=new_stage)
    # Bei "gewonnen" als converted markieren
    if new_stage.lower() == "gewonnen":
        db.update_lead(lead_id, current_user["id"],
                       converted=1, converted_date=datetime.utcnow().isoformat())
    return jsonify({"ok": ok})


@app.route("/api/kanban/stages")
@auth.login_required
def api_kanban_stages(current_user=None):
    """Kanban-Spalten abrufen."""
    stages = db.get_kanban_stages(current_user["id"])
    return jsonify({"stages": stages})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TAGS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/tags")
@auth.login_required
def api_tags_list(current_user=None):
    """Alle Tags."""
    return jsonify({"tags": db.get_tags(current_user["id"])})


@app.route("/api/tags", methods=["POST"])
@auth.login_required
def api_tags_create(current_user=None):
    """Neuen Tag erstellen."""
    data = flask_request.json
    if not data or not data.get("name"):
        return jsonify({"error": "Name fehlt"}), 400
    tag_id = db.create_tag(current_user["id"], data["name"], data.get("color", "#7C6FFF"))
    if tag_id:
        return jsonify({"ok": True, "tag_id": tag_id})
    return jsonify({"error": "Tag existiert bereits"}), 409


@app.route("/api/tags/<int:tag_id>", methods=["DELETE"])
@auth.login_required
def api_tags_delete(tag_id, current_user=None):
    """Tag loeschen."""
    return jsonify({"ok": db.delete_tag(tag_id, current_user["id"])})


@app.route("/api/leads/<int:lead_id>/tags", methods=["POST"])
@auth.login_required
def api_lead_add_tag(lead_id, current_user=None):
    """Tag zu Lead hinzufuegen."""
    data = flask_request.json
    if not data or not data.get("tag_id"):
        return jsonify({"error": "tag_id fehlt"}), 400
    return jsonify({"ok": db.add_lead_tag(lead_id, data["tag_id"])})


@app.route("/api/leads/<int:lead_id>/tags/<int:tag_id>", methods=["DELETE"])
@auth.login_required
def api_lead_remove_tag(lead_id, tag_id, current_user=None):
    """Tag von Lead entfernen."""
    return jsonify({"ok": db.remove_lead_tag(lead_id, tag_id)})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  E-MAIL-VERSAND
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/emails/generate", methods=["POST"])
@auth.login_required
def api_email_generate(current_user=None):
    """E-Mail-Vorschau generieren."""
    data = flask_request.json
    if not data or not data.get("lead"):
        return jsonify({"error": "lead fehlt"}), 400
    content = email_engine.generate_email_content(
        lead=data["lead"],
        template_key=data.get("template", "allgemein"),
        user=current_user,
        profession=data.get("profession", ""),
        city=data.get("city", ""),
        custom_subject=data.get("custom_subject", ""),
        custom_body=data.get("custom_body", ""),
    )
    return jsonify(content)


@app.route("/api/emails/send", methods=["POST"])
@auth.login_required
def api_email_send(current_user=None):
    """E-Mail senden."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400

    # Rate-Limit pruefen
    rate = compliance.check_rate_limit(current_user["id"])
    if not rate["allowed"]:
        return jsonify({"error": rate["reason"], "rate_limit": rate}), 429

    to_email = data.get("to_email", "")
    subject = data.get("subject", "")
    body = data.get("body", "")
    lead_id = data.get("lead_id")
    template_key = data.get("template", "allgemein")

    if not to_email or not subject:
        return jsonify({"error": "to_email und subject benoetigt"}), 400

    # Blacklist pruefen
    if db.is_blacklisted(current_user["id"], to_email):
        return jsonify({"error": "E-Mail-Adresse ist auf der Blacklist"}), 400

    ok, error, email_id = email_engine.send_email(
        user_id=current_user["id"],
        to_email=to_email,
        subject=subject,
        body=body,
        lead_id=lead_id,
        template_key=template_key,
        pdf_path=data.get("pdf_path"),
        tracking_host=TRACKING_HOST,
    )

    # DSGVO-Log
    compliance.log_contact(
        current_user["id"], "email_sent", to_email,
        content_summary=f"Betreff: {subject[:100]}",
    )

    # Lead als kontaktiert markieren
    if ok and lead_id:
        db.update_lead(lead_id, current_user["id"],
                       contacted=1, contacted_date=datetime.utcnow().isoformat(),
                       kanban_stage="kontaktiert")

    if ok:
        return jsonify({"ok": True, "email_id": email_id, "tracking_id": ""})
    return jsonify({"error": error, "email_id": email_id}), 500


@app.route("/api/emails/send-all", methods=["POST"])
@auth.login_required
def api_email_send_all(current_user=None):
    """Mehrere E-Mails versenden."""
    data = flask_request.json
    if not data or not isinstance(data.get("emails"), list):
        return jsonify({"error": "emails-Array fehlt"}), 400

    results = []
    for item in data["emails"]:
        rate = compliance.check_rate_limit(current_user["id"])
        if not rate["allowed"]:
            results.append({"to": item.get("to_email"), "ok": False, "error": rate["reason"]})
            break

        ok, error, email_id = email_engine.send_email(
            user_id=current_user["id"],
            to_email=item.get("to_email", ""),
            subject=item.get("subject", ""),
            body=item.get("body", ""),
            lead_id=item.get("lead_id"),
            template_key=item.get("template", "allgemein"),
            tracking_host=TRACKING_HOST,
        )
        if ok:
            compliance.log_contact(
                current_user["id"], "email_sent", item.get("to_email", ""),
                content_summary=f"Betreff: {item.get('subject', '')[:100]}",
            )
        results.append({"to": item.get("to_email"), "ok": ok, "error": error})
        time.sleep(0.5)

    sent = sum(1 for r in results if r["ok"])
    return jsonify({"sent": sent, "total": len(results), "results": results})


@app.route("/api/emails/templates")
@auth.login_required
def api_email_templates(current_user=None):
    """E-Mail-Vorlagen abrufen."""
    templates = []
    for key, tpl in email_engine.TEMPLATES.items():
        templates.append({
            "key": key,
            "name": tpl["name"],
            "subject": tpl["subject"],
            "body": tpl["body"],
        })
    return jsonify({
        "templates": templates,
        "followups": {k: v for k, v in email_engine.FOLLOWUP_TEMPLATES.items()},
        "placeholders": email_engine.PLACEHOLDERS,
    })


@app.route("/api/emails")
@auth.login_required
def api_emails_list(current_user=None):
    """E-Mail-Verlauf."""
    status = flask_request.args.get("status")
    limit = min(int(flask_request.args.get("limit", "100")), 200)
    emails = db.get_emails_by_user(current_user["id"], status, limit)
    return jsonify({"emails": emails})


@app.route("/api/emails/schedule", methods=["POST"])
@auth.login_required
def api_email_schedule(current_user=None):
    """E-Mail planen."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    email_id = email_engine.schedule_email(
        current_user["id"],
        data.get("to_email", ""),
        data.get("subject", ""),
        data.get("body", ""),
        data.get("lead_id"),
        data.get("send_at"),
    )
    if email_id:
        return jsonify({"ok": True, "email_id": email_id})
    return jsonify({"error": "Planung fehlgeschlagen"}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FOLLOW-UPS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/followups")
@auth.login_required
def api_followups_list(current_user=None):
    """Follow-Ups auflisten."""
    followups = db.get_followups_by_user(current_user["id"])
    return jsonify({"followups": followups})


@app.route("/api/followups/schedule", methods=["POST"])
@auth.login_required
@auth.plan_required("has_followups")
def api_followups_schedule(current_user=None):
    """Follow-Ups fuer einen Lead planen."""
    data = flask_request.json
    if not data or not data.get("lead_id") or not data.get("email_id"):
        return jsonify({"error": "lead_id und email_id benoetigt"}), 400
    ids = email_engine.schedule_followups(
        current_user["id"], data["lead_id"], data["email_id"]
    )
    return jsonify({"ok": True, "followup_ids": ids})


@app.route("/api/followups/<int:followup_id>/cancel", methods=["POST"])
@auth.login_required
def api_followup_cancel(followup_id, current_user=None):
    """Einzelnen Follow-Up abbrechen."""
    ok = db.update_followup(followup_id, status="cancelled")
    return jsonify({"ok": ok})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TRACKING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/t/<tracking_id>/open.png")
def tracking_pixel(tracking_id):
    """Tracking-Pixel fuer E-Mail-Oeffnung."""
    email_rec = db.get_email_by_tracking_id(tracking_id)
    if email_rec:
        now = datetime.utcnow().isoformat()
        db.update_email(
            email_rec["id"], email_rec["user_id"],
            opened=1, opened_at=now,
            opened_count=email_rec.get("opened_count", 0) + 1,
        )
        db.record_tracking_event(
            tracking_id, email_rec["id"], email_rec["user_id"],
            "open",
            flask_request.remote_addr or "",
            flask_request.headers.get("User-Agent", ""),
        )
        # Webhook + Telegram
        if HAS_AUTOMATION:
            try:
                automation.trigger_webhook(
                    email_rec["user_id"], "email.opened",
                    {"email_id": email_rec["id"], "to": email_rec["to_email"]},
                )
                automation.notify_user_telegram(
                    email_rec["user_id"],
                    f"E-Mail geoeffnet: {email_rec['to_email']} hat Ihre E-Mail geoeffnet!",
                )
            except Exception:
                pass

    # 1x1 transparentes PNG
    pixel = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
        b"\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
        b"\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01"
        b"\r\n\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    return Response(pixel, mimetype="image/png",
                    headers={"Cache-Control": "no-cache,no-store"})


@app.route("/api/tracking")
@auth.login_required
@auth.plan_required("has_tracking")
def api_tracking(current_user=None):
    """Tracking-Events abrufen."""
    events = db.get_tracking_events(current_user["id"])
    email_stats = db.get_email_stats(current_user["id"])
    return jsonify({"events": events, "stats": email_stats})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BLACKLIST
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/blacklist")
@auth.login_required
def api_blacklist_list(current_user=None):
    """Blacklist anzeigen."""
    return jsonify({"blacklist": db.get_blacklist(current_user["id"])})


@app.route("/api/blacklist", methods=["POST"])
@auth.login_required
def api_blacklist_add(current_user=None):
    """Zur Blacklist hinzufuegen."""
    data = flask_request.json
    if not data or not data.get("email"):
        return jsonify({"error": "email fehlt"}), 400
    ok = db.add_to_blacklist(current_user["id"], data["email"], data.get("reason", ""))
    return jsonify({"ok": ok})


@app.route("/api/blacklist", methods=["DELETE"])
@auth.login_required
def api_blacklist_remove(current_user=None):
    """Von Blacklist entfernen."""
    data = flask_request.json
    if not data or not data.get("email"):
        return jsonify({"error": "email fehlt"}), 400
    ok = db.remove_from_blacklist(current_user["id"], data["email"])
    return jsonify({"ok": ok})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ANALYTICS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/analytics/stats")
@auth.login_required
def api_analytics_stats(current_user=None):
    """Dashboard-Statistiken."""
    stats = analytics.get_user_stats(current_user["id"])
    return jsonify(stats)


@app.route("/api/analytics/weekly")
@auth.login_required
def api_analytics_weekly(current_user=None):
    """Woechentliche Statistiken."""
    weeks = min(int(flask_request.args.get("weeks", "8")), 52)
    data = analytics.get_weekly_stats(current_user["id"], weeks)
    return jsonify({"weeks": data})


@app.route("/api/analytics/monthly")
@auth.login_required
def api_analytics_monthly(current_user=None):
    """Monatliche Statistiken."""
    months = min(int(flask_request.args.get("months", "6")), 24)
    data = analytics.get_monthly_stats(current_user["id"], months)
    return jsonify({"months": data})


@app.route("/api/analytics/funnel")
@auth.login_required
def api_analytics_funnel(current_user=None):
    """Conversion-Funnel."""
    return jsonify(analytics.get_funnel(current_user["id"]))


@app.route("/api/analytics/roi")
@auth.login_required
def api_analytics_roi(current_user=None):
    """ROI-Berechnung."""
    cv = float(flask_request.args.get("customer_value", "1500"))
    return jsonify(analytics.calculate_roi(current_user["id"], cv))


@app.route("/api/analytics/performance")
@auth.login_required
def api_analytics_performance(current_user=None):
    """Branchen- und Stadt-Performance."""
    return jsonify(analytics.get_branch_city_performance(current_user["id"]))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  A/B-TESTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/ab-tests")
@auth.login_required
@auth.plan_required("has_ab_testing")
def api_ab_tests_list(current_user=None):
    """A/B-Tests auflisten."""
    tests = db.get_ab_tests(current_user["id"])
    return jsonify({"tests": tests})


@app.route("/api/ab-tests", methods=["POST"])
@auth.login_required
@auth.plan_required("has_ab_testing")
def api_ab_tests_create(current_user=None):
    """Neuen A/B-Test erstellen."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    test_id = db.create_ab_test(
        current_user["id"],
        data.get("name", "Test"),
        data.get("variant_a_subject", ""),
        data.get("variant_a_body", ""),
        data.get("variant_b_subject", ""),
        data.get("variant_b_body", ""),
    )
    if test_id:
        return jsonify({"ok": True, "test_id": test_id})
    return jsonify({"error": "Erstellung fehlgeschlagen"}), 500


@app.route("/api/ab-tests/<int:test_id>/evaluate")
@auth.login_required
@auth.plan_required("has_ab_testing")
def api_ab_tests_evaluate(test_id, current_user=None):
    """A/B-Test auswerten."""
    result = analytics.evaluate_ab_test(test_id, current_user["id"])
    return jsonify(result)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DSGVO
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/dsgvo")
@auth.login_required
def api_dsgvo_log(current_user=None):
    """DSGVO-Log abrufen."""
    limit = min(int(flask_request.args.get("limit", "200")), 500)
    log_entries = db.get_dsgvo_log(current_user["id"], limit)
    return jsonify({"log": log_entries})


@app.route("/api/dsgvo/export")
@auth.login_required
def api_dsgvo_export(current_user=None):
    """DSGVO-Log als CSV exportieren."""
    csv_str = compliance.export_dsgvo_report(current_user["id"])
    return Response(
        csv_str,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=dsgvo_log.csv"},
    )


@app.route("/api/unsubscribe", methods=["POST"])
def api_unsubscribe():
    """Abmeldung verarbeiten."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    tracking_id = data.get("tracking_id", "")
    token = data.get("token", "")
    ok, msg = compliance.process_unsubscribe(tracking_id, token)
    if ok:
        return jsonify({"ok": True, "message": msg})
    return jsonify({"error": msg}), 400


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SMTP/IMAP SETTINGS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/settings/smtp", methods=["POST"])
@auth.login_required
def api_settings_smtp(current_user=None):
    """SMTP-Einstellungen speichern."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    updates = {}
    if "smtp_host" in data:
        updates["smtp_host"] = data["smtp_host"]
    if "smtp_port" in data:
        updates["smtp_port"] = int(data["smtp_port"])
    if "smtp_user" in data:
        updates["smtp_user"] = data["smtp_user"]
    if "smtp_pass" in data and data["smtp_pass"]:
        updates["smtp_pass_encrypted"] = auth.encrypt_smtp_password(data["smtp_pass"])
    if "smtp_from_name" in data:
        updates["smtp_from_name"] = data["smtp_from_name"]
    if "smtp_from_email" in data:
        updates["smtp_from_email"] = data["smtp_from_email"]
    ok = db.update_user(current_user["id"], **updates)
    return jsonify({"ok": ok})


@app.route("/api/settings/smtp/test", methods=["POST"])
@auth.login_required
def api_settings_smtp_test(current_user=None):
    """SMTP-Verbindung testen."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    smtp_pass = data.get("smtp_pass", "")
    if not smtp_pass:
        smtp_pass = auth.decrypt_smtp_password(current_user.get("smtp_pass_encrypted", ""))
    ok, msg = email_engine.test_smtp_connection(
        data.get("smtp_host", current_user.get("smtp_host", "")),
        int(data.get("smtp_port", current_user.get("smtp_port", 587))),
        data.get("smtp_user", current_user.get("smtp_user", "")),
        smtp_pass,
    )
    return jsonify({"ok": ok, "message": msg})


@app.route("/api/settings/imap", methods=["POST"])
@auth.login_required
@auth.plan_required("has_imap")
def api_settings_imap(current_user=None):
    """IMAP-Einstellungen speichern."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    updates = {}
    if "imap_host" in data:
        updates["imap_host"] = data["imap_host"]
    if "imap_port" in data:
        updates["imap_port"] = int(data["imap_port"])
    if "imap_user" in data:
        updates["imap_user"] = data["imap_user"]
    if "imap_pass" in data and data["imap_pass"]:
        updates["imap_pass_encrypted"] = auth.encrypt_smtp_password(data["imap_pass"])
    ok = db.update_user(current_user["id"], **updates)
    return jsonify({"ok": ok})


@app.route("/api/settings/imap/test", methods=["POST"])
@auth.login_required
@auth.plan_required("has_imap")
def api_settings_imap_test(current_user=None):
    """IMAP-Verbindung testen."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    imap_pass = data.get("imap_pass", "")
    if not imap_pass:
        imap_pass = auth.decrypt_smtp_password(current_user.get("imap_pass_encrypted", ""))
    ok, msg = email_engine.test_imap_connection(
        data.get("imap_host", current_user.get("imap_host", "")),
        int(data.get("imap_port", current_user.get("imap_port", 993))),
        data.get("imap_user", current_user.get("imap_user", "")),
        imap_pass,
    )
    return jsonify({"ok": ok, "message": msg})


@app.route("/api/settings/telegram", methods=["POST"])
@auth.login_required
def api_settings_telegram(current_user=None):
    """Telegram-Einstellungen speichern."""
    data = flask_request.json
    if not data:
        return jsonify({"error": "JSON-Body fehlt"}), 400
    updates = {
        "telegram_bot_token": data.get("bot_token", ""),
        "telegram_chat_id": data.get("chat_id", ""),
        "telegram_enabled": 1 if data.get("enabled") else 0,
    }
    ok = db.update_user(current_user["id"], **updates)
    return jsonify({"ok": ok})


@app.route("/api/settings/telegram/test", methods=["POST"])
@auth.login_required
def api_settings_telegram_test(current_user=None):
    """Telegram-Verbindung testen."""
    if HAS_AUTOMATION:
        ok = automation.send_telegram(
            current_user.get("telegram_bot_token", ""),
            current_user.get("telegram_chat_id", ""),
            "LeadFinder Pro: Testverbindung erfolgreich!",
        )
        if ok:
            return jsonify({"ok": True, "message": "Nachricht gesendet"})
    return jsonify({"ok": False, "message": "Verbindung fehlgeschlagen"}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  WEBHOOKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/webhooks")
@auth.login_required
@auth.plan_required("has_webhooks")
def api_webhooks_list(current_user=None):
    """Webhooks auflisten."""
    return jsonify({"webhooks": db.get_webhooks(current_user["id"])})


@app.route("/api/webhooks", methods=["POST"])
@auth.login_required
@auth.plan_required("has_webhooks")
def api_webhooks_create(current_user=None):
    """Webhook erstellen."""
    data = flask_request.json
    if not data or not data.get("url") or not data.get("event_type"):
        return jsonify({"error": "url und event_type benoetigt"}), 400
    wh_id = db.create_webhook(
        current_user["id"], data["url"], data["event_type"],
        data.get("secret", ""),
    )
    if wh_id:
        return jsonify({"ok": True, "webhook_id": wh_id})
    return jsonify({"error": "Erstellung fehlgeschlagen"}), 500


@app.route("/api/webhooks/<int:webhook_id>", methods=["DELETE"])
@auth.login_required
@auth.plan_required("has_webhooks")
def api_webhooks_delete(webhook_id, current_user=None):
    """Webhook loeschen."""
    return jsonify({"ok": db.delete_webhook(webhook_id, current_user["id"])})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  API-KEYS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/api-keys")
@auth.login_required
@auth.plan_required("has_api")
def api_keys_list(current_user=None):
    """API-Keys auflisten."""
    return jsonify({"keys": db.get_api_keys(current_user["id"])})


@app.route("/api/api-keys", methods=["POST"])
@auth.login_required
@auth.plan_required("has_api")
def api_keys_create(current_user=None):
    """API-Key erstellen."""
    data = flask_request.json or {}
    key = auth.generate_api_key(current_user["id"], data.get("name", ""))
    if key:
        return jsonify({"ok": True, "key": key, "message": "Key nur einmal sichtbar!"})
    return jsonify({"error": "Erstellung fehlgeschlagen"}), 500


@app.route("/api/api-keys/<int:key_id>", methods=["DELETE"])
@auth.login_required
@auth.plan_required("has_api")
def api_keys_delete(key_id, current_user=None):
    """API-Key loeschen."""
    return jsonify({"ok": db.delete_api_key(key_id, current_user["id"])})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  WARMUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/warmup")
@auth.login_required
def api_warmup_status(current_user=None):
    """Warmup-Status abrufen."""
    info = email_engine.get_warmup_info(current_user["id"])
    return jsonify(info)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PDF-GENERIERUNG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/pdf/proposal", methods=["POST"])
@auth.login_required
@auth.plan_required("has_pdf")
def api_pdf_proposal(current_user=None):
    """Angebots-PDF generieren."""
    if not HAS_PDF:
        return jsonify({"error": "reportlab nicht installiert"}), 500
    data = flask_request.json
    if not data or not data.get("lead"):
        return jsonify({"error": "lead fehlt"}), 400
    try:
        path = pdf_generator.generate_proposal_pdf(
            data["lead"], current_user,
            data.get("profession", ""), data.get("city", ""),
        )
        return jsonify({"ok": True, "path": path})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/pdf/letter", methods=["POST"])
@auth.login_required
@auth.plan_required("has_pdf")
def api_pdf_letter(current_user=None):
    """Geschaeftsbrief-PDF generieren."""
    if not HAS_PDF:
        return jsonify({"error": "reportlab nicht installiert"}), 500
    data = flask_request.json
    if not data or not data.get("lead"):
        return jsonify({"error": "lead fehlt"}), 400
    try:
        path = pdf_generator.generate_letter_pdf(
            data["lead"], current_user,
            data.get("profession", ""), data.get("city", ""),
            data.get("subject", ""), data.get("body", ""),
        )
        return jsonify({"ok": True, "path": path})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/pdf/download/<path:filename>")
@auth.login_required
def api_pdf_download(filename, current_user=None):
    """PDF herunterladen."""
    pdf_dir = os.path.join(BASE_DIR, "data", "pdfs")
    filepath = os.path.join(pdf_dir, os.path.basename(filename))
    if not os.path.exists(filepath):
        return jsonify({"error": "Datei nicht gefunden"}), 404
    return send_file(filepath, as_attachment=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  NOTIFICATIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/notifications")
@auth.login_required
def api_notifications_list(current_user=None):
    """Notifications abrufen."""
    unread = flask_request.args.get("unread") == "1"
    notifs = db.get_notifications(current_user["id"], unread_only=unread)
    count = db.count_unread_notifications(current_user["id"])
    return jsonify({"notifications": notifs, "unread_count": count})


@app.route("/api/notifications/read-all", methods=["POST"])
@auth.login_required
def api_notifications_read_all(current_user=None):
    """Alle Notifications als gelesen markieren."""
    count = db.mark_all_notifications_read(current_user["id"])
    return jsonify({"ok": True, "marked": count})


@app.route("/api/notifications/<int:notif_id>/read", methods=["POST"])
@auth.login_required
def api_notification_read(notif_id, current_user=None):
    """Einzelne Notification als gelesen markieren."""
    return jsonify({"ok": db.mark_notification_read(notif_id, current_user["id"])})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ABO / DIGISTORE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/plans")
def api_plans():
    """Alle Plaene abrufen."""
    return jsonify({"plans": db.get_all_plans()})


@app.route("/api/subscriptions")
@auth.login_required
def api_subscriptions(current_user=None):
    """Abos des Users."""
    return jsonify({"subscriptions": db.get_subscriptions(current_user["id"])})


@app.route("/api/checkout/<plan_id>", methods=["POST"])
@auth.login_required
def api_checkout(plan_id, current_user=None):
    """Checkout-URL fuer Digistore24 generieren."""
    url = digistore.create_checkout_url(
        current_user["id"], plan_id,
        current_user["email"], current_user.get("name", ""),
    )
    if url:
        return jsonify({"ok": True, "url": url})
    return jsonify({"error": "Checkout-URL konnte nicht generiert werden"}), 500


@app.route("/api/digistore/ipn", methods=["POST"])
def api_digistore_ipn():
    """Digistore24 IPN-Webhook."""
    ok, msg = digistore.handle_ipn(flask_request.form.to_dict())
    if ok:
        return "OK", 200
    return msg, 403


@app.route("/api/subscription/cancel", methods=["POST"])
@auth.login_required
def api_subscription_cancel(current_user=None):
    """Abo kuendigen."""
    url = digistore.cancel_subscription(current_user["id"])
    if url:
        return jsonify({"ok": True, "url": url})
    # Mock: Direkt downgraden
    db.update_user(current_user["id"], plan_id="free")
    db.create_notification(
        current_user["id"],
        "Abo gekuendigt",
        "Dein Abo wurde gekuendigt. Du nutzt jetzt den Free-Plan.",
        ntype="warning",
    )
    return jsonify({"ok": True, "message": "Plan auf Free zurueckgesetzt"})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EXPORT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/export/csv")
@auth.login_required
def api_export_csv(current_user=None):
    """Leads als CSV exportieren."""
    import csv
    from io import StringIO

    leads = db.get_leads_by_user(current_user["id"], limit=10000)
    output = StringIO()
    output.write('\ufeff')  # UTF-8 BOM
    writer = csv.writer(output, delimiter=';')
    writer.writerow(["Name", "E-Mail", "Telefon", "Adresse", "Website", "Score",
                      "Google Rating", "Inhaber", "Branche", "Stadt", "Stage", "Notizen"])
    for lead in leads:
        writer.writerow([
            lead.get("name", ""),
            lead.get("email", ""),
            lead.get("phone", ""),
            lead.get("address", ""),
            lead.get("website", ""),
            lead.get("site_score", ""),
            lead.get("google_rating", ""),
            lead.get("owner_name", ""),
            lead.get("search_query", ""),
            lead.get("search_city", ""),
            lead.get("kanban_stage", ""),
            lead.get("notes", ""),
        ])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads_export.csv"},
    )


@app.route("/api/export/vcard")
@auth.login_required
def api_export_vcard(current_user=None):
    """Leads als vCard exportieren."""
    leads = db.get_leads_by_user(current_user["id"], limit=10000)
    vcards = []
    for lead in leads:
        if not lead.get("email") and not lead.get("phone"):
            continue
        vcard = f"""BEGIN:VCARD
VERSION:3.0
FN:{lead.get('name', '')}
ORG:{lead.get('name', '')}
EMAIL:{lead.get('email', '')}
TEL:{lead.get('phone', '')}
ADR:;;{lead.get('address', '')}
URL:{lead.get('website', '')}
NOTE:{lead.get('notes', '')}
END:VCARD"""
        vcards.append(vcard)
    return Response(
        "\n".join(vcards),
        mimetype="text/vcard",
        headers={"Content-Disposition": "attachment; filename=leads.vcf"},
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SEARCH HISTORY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/search-history")
@auth.login_required
def api_search_history(current_user=None):
    """Suchhistorie abrufen."""
    history = db.get_search_history(current_user["id"])
    return jsonify({"history": history})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ADMIN — VISITOR-TRACKING ENDPOINTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/visitor/heartbeat", methods=["POST"])
def api_visitor_heartbeat():
    """Oeffentlicher Heartbeat — trackt Besucher-Sessions."""
    data = flask_request.json or {}
    session_id = data.get("session_id", "")
    event = data.get("event", "pulse")       # start | pulse | end
    section = data.get("section", "unknown")
    admin_token = data.get("admin_token", "")

    # Admin nicht tracken
    if admin_token == ADMIN_TOKEN:
        return jsonify({"ok": True, "skipped": True})

    if not session_id:
        return jsonify({"error": "session_id required"}), 400

    ip = flask_request.headers.get("X-Forwarded-For",
         flask_request.remote_addr or "127.0.0.1").split(",")[0].strip()
    geo = geoip_lookup(ip)
    now = time.time()

    visitors = load_visitors()
    sessions = visitors.setdefault("sessions", {})

    if event == "start" or session_id not in sessions:
        sessions[session_id] = {
            "ip": ip,
            "city": geo["city"],
            "country": geo["country"],
            "lat": geo["lat"],
            "lon": geo["lon"],
            "section": section,
            "first_seen": now,
            "last_seen": now,
            "page_views": 1,
            "user_agent": flask_request.headers.get("User-Agent", "")[:200],
        }
    elif event == "end":
        if session_id in sessions:
            sessions[session_id]["last_seen"] = now
            sessions[session_id]["active"] = False
    else:
        # pulse
        if session_id in sessions:
            sessions[session_id]["last_seen"] = now
            sessions[session_id]["section"] = section
            sessions[session_id]["page_views"] = sessions[session_id].get("page_views", 1) + 1

    save_visitors(visitors)
    return jsonify({"ok": True})


@app.route("/api/admin/visitors")
@auth.login_required
def api_admin_visitors(current_user=None):
    """Admin-Endpoint: KPIs + aktive Sessions + Charts."""
    visitors = load_visitors()
    sessions = visitors.get("sessions", {})
    now = time.time()

    active_threshold = 120  # 2 Minuten
    today_start = datetime.now().replace(hour=0, minute=0, second=0).timestamp()

    active_sessions = []
    today_total = 0
    durations = []
    section_counts: Dict[str, int] = {}
    hourly_timeline: Dict[str, int] = {}
    bounces = 0
    total_today = 0

    for sid, s in sessions.items():
        last_seen = s.get("last_seen", 0)
        first_seen = s.get("first_seen", 0)

        # Heute?
        if first_seen >= today_start:
            today_total += 1
            total_today += 1
            duration = last_seen - first_seen
            durations.append(duration)
            if s.get("page_views", 1) <= 1 and duration < 10:
                bounces += 1

            hour = datetime.fromtimestamp(first_seen).strftime("%H:00")
            hourly_timeline[hour] = hourly_timeline.get(hour, 0) + 1

        # Aktiv jetzt?
        is_active = (now - last_seen) < active_threshold and s.get("active", True) is not False
        if is_active:
            sec = s.get("section", "unknown")
            section_counts[sec] = section_counts.get(sec, 0) + 1
            active_sessions.append({
                "session_id": sid,
                "city": s.get("city", "Unknown"),
                "country": s.get("country", "Unknown"),
                "lat": s.get("lat", 0),
                "lon": s.get("lon", 0),
                "section": sec,
                "duration": int(now - first_seen),
                "page_views": s.get("page_views", 1),
            })

    avg_duration = int(sum(durations) / len(durations)) if durations else 0
    bounce_rate = round((bounces / total_today * 100), 1) if total_today > 0 else 0

    return jsonify({
        "kpis": {
            "active_now": len(active_sessions),
            "today_total": today_total,
            "avg_duration": avg_duration,
            "bounce_rate": bounce_rate,
        },
        "active_sessions": active_sessions,
        "section_counts": section_counts,
        "hourly_timeline": hourly_timeline,
    })


@app.route("/api/admin/cleanup", methods=["POST"])
@auth.login_required
def api_admin_cleanup(current_user=None):
    """Sessions aelter als 30 Tage loeschen."""
    visitors = load_visitors()
    removed = cleanup_old_sessions(visitors, max_age_days=30)
    save_visitors(visitors)
    return jsonify({"ok": True, "removed": removed})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  STARTUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def start_app():
    """Initialisiert die App und startet den Server."""
    db.init_db()
    log.info("Datenbank initialisiert")

    # Scheduler starten
    if HAS_AUTOMATION:
        automation.run_scheduler()
        log.info("Scheduler gestartet")

    log.info("=" * 60)
    log.info("  LeadFinder Pro v3")
    log.info("  http://localhost:5000")
    log.info("  Admin-Token: %s", ADMIN_TOKEN)
    log.info("=" * 60)

    app.run(host="0.0.0.0", port=5000, debug=False)


if __name__ == "__main__":
    start_app()
