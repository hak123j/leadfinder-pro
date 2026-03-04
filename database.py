"""
database.py — SQLite Schema + CRUD fuer LeadFinder Pro v3

Alle Tabellen, Indizes, Seed-Daten und CRUD-Operationen.
SQLite mit WAL-Modus fuer bessere Concurrent-Performance.
"""

import os
import json
import sqlite3
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from contextlib import contextmanager

log = logging.getLogger("leadfinder.db")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "leadfinder.db")

_local = threading.local()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  VERBINDUNG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_db() -> sqlite3.Connection:
    """Thread-lokale DB-Verbindung (eine pro Thread)."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH, timeout=15)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA foreign_keys=ON")
        _local.conn.execute("PRAGMA busy_timeout=10000")
    return _local.conn


@contextmanager
def get_cursor():
    """Context-Manager fuer DB-Cursor mit Auto-Commit/Rollback."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def close_db():
    """Schliesst die Thread-lokale Verbindung."""
    if hasattr(_local, "conn") and _local.conn is not None:
        _local.conn.close()
        _local.conn = None


def dict_from_row(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    """Konvertiert eine sqlite3.Row in ein Dict."""
    if row is None:
        return None
    return dict(row)


def rows_to_dicts(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    """Konvertiert eine Liste von Rows in Dicts."""
    return [dict(r) for r in rows]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SCHEMA ERSTELLEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCHEMA_SQL = """
-- Abo-Plaene
CREATE TABLE IF NOT EXISTS plans (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    price_monthly INTEGER NOT NULL DEFAULT 0,
    searches_per_month INTEGER NOT NULL DEFAULT 5,
    emails_per_day INTEGER NOT NULL DEFAULT 10,
    max_leads_stored INTEGER NOT NULL DEFAULT 100,
    has_followups INTEGER NOT NULL DEFAULT 0,
    has_tracking INTEGER NOT NULL DEFAULT 0,
    has_pdf INTEGER NOT NULL DEFAULT 0,
    has_multi_city INTEGER NOT NULL DEFAULT 0,
    has_api INTEGER NOT NULL DEFAULT 0,
    has_team INTEGER NOT NULL DEFAULT 0,
    has_ab_testing INTEGER NOT NULL DEFAULT 0,
    has_webhooks INTEGER NOT NULL DEFAULT 0,
    has_kanban INTEGER NOT NULL DEFAULT 0,
    has_imap INTEGER NOT NULL DEFAULT 0,
    has_scheduler INTEGER NOT NULL DEFAULT 0
);

-- Benutzer
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    company TEXT NOT NULL DEFAULT '',
    phone TEXT NOT NULL DEFAULT '',
    plan_id TEXT NOT NULL DEFAULT 'free' REFERENCES plans(id),
    email_verified INTEGER NOT NULL DEFAULT 0,
    verify_token TEXT,
    reset_token TEXT,
    reset_token_expires TEXT,
    session_token TEXT,
    session_expires TEXT,
    smtp_host TEXT NOT NULL DEFAULT '',
    smtp_port INTEGER NOT NULL DEFAULT 587,
    smtp_user TEXT NOT NULL DEFAULT '',
    smtp_pass_encrypted TEXT NOT NULL DEFAULT '',
    smtp_from_name TEXT NOT NULL DEFAULT '',
    smtp_from_email TEXT NOT NULL DEFAULT '',
    imap_host TEXT NOT NULL DEFAULT '',
    imap_port INTEGER NOT NULL DEFAULT 993,
    imap_user TEXT NOT NULL DEFAULT '',
    imap_pass_encrypted TEXT NOT NULL DEFAULT '',
    telegram_chat_id TEXT NOT NULL DEFAULT '',
    telegram_bot_token TEXT NOT NULL DEFAULT '',
    telegram_enabled INTEGER NOT NULL DEFAULT 0,
    oauth_provider TEXT NOT NULL DEFAULT '',
    oauth_id TEXT NOT NULL DEFAULT '',
    stripe_customer_id TEXT NOT NULL DEFAULT '',
    stripe_subscription_id TEXT NOT NULL DEFAULT '',
    searches_this_month INTEGER NOT NULL DEFAULT 0,
    emails_today INTEGER NOT NULL DEFAULT 0,
    emails_today_date TEXT NOT NULL DEFAULT '',
    last_login TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Leads
CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL DEFAULT '',
    email TEXT,
    phone TEXT,
    address TEXT,
    website TEXT,
    whatsapp_url TEXT,
    google_maps_url TEXT,
    owner_name TEXT,
    owner_title TEXT,
    company_type TEXT,
    tech_stack TEXT,
    tech_details TEXT,
    site_score INTEGER,
    site_score_details TEXT,
    site_last_updated TEXT,
    site_has_ssl INTEGER,
    site_is_mobile INTEGER,
    site_has_cookie_banner INTEGER,
    site_load_time REAL,
    google_rating REAL,
    google_reviews INTEGER,
    opening_hours TEXT,
    lead_score TEXT NOT NULL DEFAULT 'warm',
    lead_score_value INTEGER NOT NULL DEFAULT 50,
    kanban_stage TEXT NOT NULL DEFAULT 'neu',
    notes TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'search',
    search_query TEXT,
    search_city TEXT,
    contacted INTEGER NOT NULL DEFAULT 0,
    contacted_date TEXT,
    responded INTEGER NOT NULL DEFAULT 0,
    responded_date TEXT,
    converted INTEGER NOT NULL DEFAULT 0,
    converted_date TEXT,
    revenue REAL NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- E-Mails
CREATE TABLE IF NOT EXISTS emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    lead_id INTEGER REFERENCES leads(id) ON DELETE SET NULL,
    to_email TEXT NOT NULL,
    from_email TEXT NOT NULL DEFAULT '',
    subject TEXT NOT NULL DEFAULT '',
    body TEXT NOT NULL DEFAULT '',
    body_html TEXT NOT NULL DEFAULT '',
    template_key TEXT NOT NULL DEFAULT 'allgemein',
    status TEXT NOT NULL DEFAULT 'pending',
    followup_number INTEGER NOT NULL DEFAULT 0,
    ab_test_id INTEGER,
    ab_variant TEXT,
    tracking_id TEXT UNIQUE,
    opened INTEGER NOT NULL DEFAULT 0,
    opened_at TEXT,
    opened_count INTEGER NOT NULL DEFAULT 0,
    clicked INTEGER NOT NULL DEFAULT 0,
    clicked_at TEXT,
    bounced INTEGER NOT NULL DEFAULT 0,
    bounce_reason TEXT,
    replied INTEGER NOT NULL DEFAULT 0,
    replied_at TEXT,
    unsubscribed INTEGER NOT NULL DEFAULT 0,
    unsubscribed_at TEXT,
    scheduled_at TEXT,
    sent_at TEXT,
    error_message TEXT,
    pdf_path TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Tracking-Events
CREATE TABLE IF NOT EXISTS tracking_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tracking_id TEXT NOT NULL,
    email_id INTEGER REFERENCES emails(id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    ip_address TEXT,
    user_agent TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Blacklist
CREATE TABLE IF NOT EXISTS blacklist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(user_id, email)
);

-- Suchhistorie
CREATE TABLE IF NOT EXISTS searches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    query TEXT NOT NULL,
    city TEXT NOT NULL DEFAULT '',
    cities_json TEXT,
    result_count INTEGER NOT NULL DEFAULT 0,
    competition_count INTEGER NOT NULL DEFAULT 0,
    duration_seconds REAL NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Follow-Ups
CREATE TABLE IF NOT EXISTS followups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    original_email_id INTEGER REFERENCES emails(id) ON DELETE SET NULL,
    followup_number INTEGER NOT NULL DEFAULT 1,
    scheduled_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    sent_email_id INTEGER REFERENCES emails(id) ON DELETE SET NULL,
    sent_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Tags
CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    color TEXT NOT NULL DEFAULT '#7C6FFF',
    UNIQUE(user_id, name)
);

-- Lead-Tags (M:N)
CREATE TABLE IF NOT EXISTS lead_tags (
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (lead_id, tag_id)
);

-- Kanban-Spalten
CREATE TABLE IF NOT EXISTS kanban_stages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    color TEXT NOT NULL DEFAULT '#7C6FFF',
    sort_order INTEGER NOT NULL DEFAULT 0
);

-- A/B-Tests
CREATE TABLE IF NOT EXISTS ab_tests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL DEFAULT '',
    variant_a_subject TEXT NOT NULL DEFAULT '',
    variant_a_body TEXT NOT NULL DEFAULT '',
    variant_b_subject TEXT NOT NULL DEFAULT '',
    variant_b_body TEXT NOT NULL DEFAULT '',
    variant_a_sent INTEGER NOT NULL DEFAULT 0,
    variant_a_opened INTEGER NOT NULL DEFAULT 0,
    variant_a_replied INTEGER NOT NULL DEFAULT 0,
    variant_b_sent INTEGER NOT NULL DEFAULT 0,
    variant_b_opened INTEGER NOT NULL DEFAULT 0,
    variant_b_replied INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'running',
    winner TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Webhooks
CREATE TABLE IF NOT EXISTS webhooks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    event_type TEXT NOT NULL,
    secret TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 1,
    last_triggered TEXT,
    fail_count INTEGER NOT NULL DEFAULT 0
);

-- API-Keys
CREATE TABLE IF NOT EXISTS api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    key_hash TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL DEFAULT '',
    last_used TEXT,
    requests_today INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- DSGVO-Log
CREATE TABLE IF NOT EXISTS dsgvo_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    target_email TEXT,
    target_name TEXT,
    channel TEXT NOT NULL DEFAULT 'email',
    content_summary TEXT,
    legal_basis TEXT NOT NULL DEFAULT 'berechtigtes_interesse',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Warmup-Status
CREATE TABLE IF NOT EXISTS warmup_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    smtp_user TEXT NOT NULL,
    day_number INTEGER NOT NULL DEFAULT 1,
    daily_limit INTEGER NOT NULL DEFAULT 5,
    emails_sent_today INTEGER NOT NULL DEFAULT 0,
    last_send_date TEXT,
    reputation_score REAL NOT NULL DEFAULT 50.0,
    UNIQUE(user_id, smtp_user)
);

-- Geplante Jobs
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    job_type TEXT NOT NULL,
    config_json TEXT NOT NULL DEFAULT '{}',
    cron_expression TEXT NOT NULL DEFAULT '',
    next_run TEXT,
    last_run TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    run_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Subscriptions
CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    plan_id TEXT NOT NULL REFERENCES plans(id),
    status TEXT NOT NULL DEFAULT 'active',
    stripe_session_id TEXT,
    order_id TEXT,
    period_start TEXT,
    period_end TEXT,
    amount INTEGER NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'EUR',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Team-Mitglieder
CREATE TABLE IF NOT EXISTS team_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    team_owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    member_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role TEXT NOT NULL DEFAULT 'member',
    invited_at TEXT NOT NULL DEFAULT (datetime('now')),
    accepted_at TEXT
);

-- Notifications
CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    type TEXT NOT NULL DEFAULT 'info',
    title TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL DEFAULT '',
    read INTEGER NOT NULL DEFAULT 0,
    link TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Digistore24 IPN-Log
CREATE TABLE IF NOT EXISTS digistore_ipn_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event TEXT NOT NULL,
    product_id TEXT,
    order_id TEXT,
    transaction_id TEXT,
    email TEXT,
    custom TEXT,
    raw_params TEXT,
    signature_valid INTEGER NOT NULL DEFAULT 0,
    processed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

INDICES_SQL = """
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_session ON users(session_token);
CREATE INDEX IF NOT EXISTS idx_leads_user ON leads(user_id);
CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email);
CREATE INDEX IF NOT EXISTS idx_leads_kanban ON leads(user_id, kanban_stage);
CREATE INDEX IF NOT EXISTS idx_emails_user ON emails(user_id);
CREATE INDEX IF NOT EXISTS idx_emails_tracking ON emails(tracking_id);
CREATE INDEX IF NOT EXISTS idx_emails_status ON emails(status);
CREATE INDEX IF NOT EXISTS idx_emails_lead ON emails(lead_id);
CREATE INDEX IF NOT EXISTS idx_tracking_events_tid ON tracking_events(tracking_id);
CREATE INDEX IF NOT EXISTS idx_tracking_events_user ON tracking_events(user_id);
CREATE INDEX IF NOT EXISTS idx_blacklist_user ON blacklist(user_id);
CREATE INDEX IF NOT EXISTS idx_searches_user ON searches(user_id);
CREATE INDEX IF NOT EXISTS idx_followups_user ON followups(user_id);
CREATE INDEX IF NOT EXISTS idx_followups_status ON followups(status);
CREATE INDEX IF NOT EXISTS idx_tags_user ON tags(user_id);
CREATE INDEX IF NOT EXISTS idx_kanban_stages_user ON kanban_stages(user_id);
CREATE INDEX IF NOT EXISTS idx_ab_tests_user ON ab_tests(user_id);
CREATE INDEX IF NOT EXISTS idx_webhooks_user ON webhooks(user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_dsgvo_log_user ON dsgvo_log(user_id);
CREATE INDEX IF NOT EXISTS idx_warmup_user ON warmup_status(user_id);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_user ON scheduled_jobs(user_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON subscriptions(user_id);
CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id);
"""

SEED_PLANS_SQL = """
INSERT OR IGNORE INTO plans (id, name, price_monthly, searches_per_month, emails_per_day, max_leads_stored,
    has_followups, has_tracking, has_pdf, has_multi_city, has_api, has_team,
    has_ab_testing, has_webhooks, has_kanban, has_imap, has_scheduler)
VALUES
    ('free', 'Free', 0, 5, 10, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    ('pro', 'Pro', 1900, 999999, 100, 10000, 1, 1, 1, 1, 0, 0, 1, 0, 1, 1, 1),
    ('business', 'Business', 4900, 999999, 999999, 999999, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
"""


def _migrate_oauth_columns():
    """Fuegt oauth_provider/oauth_id Spalten hinzu, falls sie fehlen."""
    conn = get_db()
    cols = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    if "oauth_provider" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN oauth_provider TEXT NOT NULL DEFAULT ''")
    if "oauth_id" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN oauth_id TEXT NOT NULL DEFAULT ''")
    conn.commit()


def init_db():
    """Erstellt alle Tabellen, Indizes und Seed-Daten."""
    conn = get_db()
    conn.executescript(SCHEMA_SQL)
    conn.executescript(INDICES_SQL)
    conn.executescript(SEED_PLANS_SQL)
    conn.commit()
    _migrate_oauth_columns()
    log.info("Datenbank initialisiert: %s", DB_PATH)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PLAN-CRUD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_plan(plan_id: str) -> Optional[Dict[str, Any]]:
    """Gibt einen Plan zurueck."""
    row = get_db().execute("SELECT * FROM plans WHERE id = ?", (plan_id,)).fetchone()
    return dict_from_row(row)


def get_all_plans() -> List[Dict[str, Any]]:
    """Gibt alle Plaene zurueck."""
    return rows_to_dicts(get_db().execute("SELECT * FROM plans ORDER BY price_monthly").fetchall())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  USER-CRUD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def create_user(email: str, password_hash: str, name: str = "",
                company: str = "", phone: str = "") -> Optional[int]:
    """Erstellt einen neuen User und gibt dessen ID zurueck."""
    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO users (email, password_hash, name, company, phone)
                   VALUES (?, ?, ?, ?, ?)""",
                (email.lower().strip(), password_hash, name.strip(),
                 company.strip(), phone.strip()),
            )
            user_id = cur.lastrowid
            # Default-Kanban-Spalten
            for idx, (stage_name, color) in enumerate([
                ("Neu", "#3B82F6"), ("Kontaktiert", "#FBBF24"),
                ("Angebot", "#A78BFA"), ("Gewonnen", "#34D399"), ("Verloren", "#F87171"),
            ]):
                cur.execute(
                    "INSERT INTO kanban_stages (user_id, name, color, sort_order) VALUES (?, ?, ?, ?)",
                    (user_id, stage_name, color, idx),
                )
            # Default-Tags
            for tag_name, color in [
                ("Hot", "#F87171"), ("Warm", "#FBBF24"), ("Kalt", "#60A5FA"),
                ("VIP", "#A78BFA"), ("Follow-Up", "#34D399"),
            ]:
                cur.execute(
                    "INSERT INTO tags (user_id, name, color) VALUES (?, ?, ?)",
                    (user_id, tag_name, color),
                )
        return user_id
    except sqlite3.IntegrityError:
        log.warning("User mit E-Mail %s existiert bereits", email)
        return None


def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    """Findet einen User ueber seine E-Mail."""
    row = get_db().execute(
        "SELECT * FROM users WHERE email = ?", (email.lower().strip(),)
    ).fetchone()
    return dict_from_row(row)


def get_user_by_oauth(provider: str, oauth_id: str) -> Optional[Dict[str, Any]]:
    """Findet einen User ueber OAuth-Provider + ID."""
    row = get_db().execute(
        "SELECT * FROM users WHERE oauth_provider = ? AND oauth_id = ?",
        (provider, oauth_id),
    ).fetchone()
    return dict_from_row(row)


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    """Findet einen User ueber seine ID."""
    row = get_db().execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    return dict_from_row(row)


def get_user_by_session(token: str) -> Optional[Dict[str, Any]]:
    """Findet einen User ueber sein Session-Token (prueft Ablauf)."""
    row = get_db().execute(
        "SELECT * FROM users WHERE session_token = ? AND session_expires > datetime('now')",
        (token,),
    ).fetchone()
    return dict_from_row(row)


def update_user(user_id: int, **kwargs) -> bool:
    """Aktualisiert User-Felder dynamisch."""
    if not kwargs:
        return False
    allowed = {
        "email", "password_hash", "name", "company", "phone", "plan_id",
        "email_verified", "verify_token", "reset_token", "reset_token_expires",
        "session_token", "session_expires",
        "smtp_host", "smtp_port", "smtp_user", "smtp_pass_encrypted",
        "smtp_from_name", "smtp_from_email",
        "imap_host", "imap_port", "imap_user", "imap_pass_encrypted",
        "telegram_chat_id", "telegram_bot_token", "telegram_enabled",
        "oauth_provider", "oauth_id",
        "stripe_customer_id", "stripe_subscription_id",
        "searches_this_month", "emails_today", "emails_today_date",
        "last_login", "updated_at",
    }
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return False
    fields["updated_at"] = datetime.utcnow().isoformat()
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [user_id]
    try:
        with get_cursor() as cur:
            cur.execute(f"UPDATE users SET {set_clause} WHERE id = ?", values)
        return True
    except sqlite3.Error as exc:
        log.error("User-Update fehlgeschlagen: %s", exc)
        return False


def increment_user_searches(user_id: int) -> None:
    """Erhoeht den monatlichen Such-Zaehler."""
    with get_cursor() as cur:
        cur.execute(
            "UPDATE users SET searches_this_month = searches_this_month + 1 WHERE id = ?",
            (user_id,),
        )


def increment_user_emails(user_id: int) -> None:
    """Erhoeht den taeglichen E-Mail-Zaehler (Reset bei neuem Tag)."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    with get_cursor() as cur:
        cur.execute(
            """UPDATE users SET
                emails_today = CASE WHEN emails_today_date = ? THEN emails_today + 1 ELSE 1 END,
                emails_today_date = ?
               WHERE id = ?""",
            (today, today, user_id),
        )


def reset_monthly_searches() -> None:
    """Setzt alle monatlichen Such-Zaehler zurueck (Monatsanfang)."""
    with get_cursor() as cur:
        cur.execute("UPDATE users SET searches_this_month = 0")


def get_user_with_plan(user_id: int) -> Optional[Dict[str, Any]]:
    """Gibt User mit Plan-Details zurueck."""
    row = get_db().execute(
        """SELECT u.*, p.name AS plan_name, p.price_monthly, p.searches_per_month,
                  p.emails_per_day, p.max_leads_stored,
                  p.has_followups, p.has_tracking, p.has_pdf, p.has_multi_city,
                  p.has_api, p.has_team, p.has_ab_testing, p.has_webhooks,
                  p.has_kanban, p.has_imap, p.has_scheduler
           FROM users u JOIN plans p ON u.plan_id = p.id
           WHERE u.id = ?""",
        (user_id,),
    ).fetchone()
    return dict_from_row(row)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LEAD-CRUD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def create_lead(user_id: int, data: Dict[str, Any]) -> Optional[int]:
    """Erstellt einen neuen Lead. Gibt die Lead-ID zurueck."""
    fields = [
        "user_id", "name", "email", "phone", "address", "website",
        "whatsapp_url", "google_maps_url", "owner_name", "owner_title",
        "company_type", "tech_stack", "tech_details",
        "site_score", "site_score_details", "site_last_updated",
        "site_has_ssl", "site_is_mobile", "site_has_cookie_banner", "site_load_time",
        "google_rating", "google_reviews", "opening_hours",
        "lead_score", "lead_score_value", "kanban_stage", "notes",
        "source", "search_query", "search_city",
    ]
    data["user_id"] = user_id
    # JSON-Felder serialisieren
    for json_field in ["tech_details", "site_score_details", "opening_hours"]:
        if json_field in data and isinstance(data[json_field], (dict, list)):
            data[json_field] = json.dumps(data[json_field], ensure_ascii=False)
    if "tech_stack" in data and isinstance(data["tech_stack"], list):
        data["tech_stack"] = json.dumps(data["tech_stack"], ensure_ascii=False)

    present = {k: data.get(k) for k in fields if k in data}
    cols = ", ".join(present.keys())
    placeholders = ", ".join("?" for _ in present)
    vals = list(present.values())

    try:
        with get_cursor() as cur:
            cur.execute(f"INSERT INTO leads ({cols}) VALUES ({placeholders})", vals)
            return cur.lastrowid
    except sqlite3.Error as exc:
        log.error("Lead-Erstellung fehlgeschlagen: %s", exc)
        return None


def get_lead(lead_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    """Gibt einen Lead zurueck (prueft Ownership)."""
    row = get_db().execute(
        "SELECT * FROM leads WHERE id = ? AND user_id = ?", (lead_id, user_id)
    ).fetchone()
    result = dict_from_row(row)
    if result:
        _deserialize_lead_json(result)
    return result


def get_leads_by_user(user_id: int, limit: int = 500, offset: int = 0,
                      kanban_stage: str = None, search: str = None) -> List[Dict[str, Any]]:
    """Gibt Leads eines Users zurueck (mit optionalem Filter)."""
    query = "SELECT * FROM leads WHERE user_id = ?"
    params: List[Any] = [user_id]
    if kanban_stage:
        query += " AND kanban_stage = ?"
        params.append(kanban_stage)
    if search:
        query += " AND (name LIKE ? OR email LIKE ? OR website LIKE ?)"
        pat = f"%{search}%"
        params.extend([pat, pat, pat])
    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    rows = get_db().execute(query, params).fetchall()
    results = rows_to_dicts(rows)
    for r in results:
        _deserialize_lead_json(r)
    return results


def get_leads_by_stage(user_id: int) -> Dict[str, List[Dict[str, Any]]]:
    """Gibt Leads gruppiert nach Kanban-Stage zurueck."""
    rows = get_db().execute(
        "SELECT * FROM leads WHERE user_id = ? ORDER BY updated_at DESC",
        (user_id,),
    ).fetchall()
    stages: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        d = dict(row)
        _deserialize_lead_json(d)
        stage = d.get("kanban_stage", "neu")
        stages.setdefault(stage, []).append(d)
    return stages


def update_lead(lead_id: int, user_id: int, **kwargs) -> bool:
    """Aktualisiert Lead-Felder (prueft Ownership)."""
    allowed = {
        "name", "email", "phone", "address", "website",
        "whatsapp_url", "google_maps_url", "owner_name", "owner_title",
        "company_type", "tech_stack", "tech_details",
        "site_score", "site_score_details",
        "google_rating", "google_reviews", "opening_hours",
        "lead_score", "lead_score_value", "kanban_stage", "notes",
        "contacted", "contacted_date", "responded", "responded_date",
        "converted", "converted_date", "revenue",
    }
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return False
    # JSON serialisieren
    for jf in ["tech_details", "site_score_details", "opening_hours"]:
        if jf in fields and isinstance(fields[jf], (dict, list)):
            fields[jf] = json.dumps(fields[jf], ensure_ascii=False)
    if "tech_stack" in fields and isinstance(fields["tech_stack"], list):
        fields["tech_stack"] = json.dumps(fields["tech_stack"], ensure_ascii=False)

    fields["updated_at"] = datetime.utcnow().isoformat()
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [lead_id, user_id]
    try:
        with get_cursor() as cur:
            cur.execute(
                f"UPDATE leads SET {set_clause} WHERE id = ? AND user_id = ?", values
            )
            return cur.rowcount > 0
    except sqlite3.Error as exc:
        log.error("Lead-Update fehlgeschlagen: %s", exc)
        return False


def delete_lead(lead_id: int, user_id: int) -> bool:
    """Loescht einen Lead (prueft Ownership)."""
    try:
        with get_cursor() as cur:
            cur.execute("DELETE FROM leads WHERE id = ? AND user_id = ?", (lead_id, user_id))
            return cur.rowcount > 0
    except sqlite3.Error as exc:
        log.error("Lead-Loeschung fehlgeschlagen: %s", exc)
        return False


def count_leads(user_id: int) -> int:
    """Zaehlt die Leads eines Users."""
    row = get_db().execute(
        "SELECT COUNT(*) AS cnt FROM leads WHERE user_id = ?", (user_id,)
    ).fetchone()
    return row["cnt"] if row else 0


def lead_exists(user_id: int, email: str = None, website: str = None) -> bool:
    """Prueft ob ein Lead bereits existiert (Duplikat-Check)."""
    if email:
        row = get_db().execute(
            "SELECT id FROM leads WHERE user_id = ? AND email = ?",
            (user_id, email.lower()),
        ).fetchone()
        if row:
            return True
    if website:
        from urllib.parse import urlparse
        domain = urlparse(website).netloc.replace("www.", "")
        row = get_db().execute(
            "SELECT id FROM leads WHERE user_id = ? AND website LIKE ?",
            (user_id, f"%{domain}%"),
        ).fetchone()
        if row:
            return True
    return False


def _deserialize_lead_json(d: Dict[str, Any]) -> None:
    """Deserialisiert JSON-Felder eines Lead-Dicts."""
    for field in ["tech_details", "site_score_details", "opening_hours"]:
        val = d.get(field)
        if isinstance(val, str):
            try:
                d[field] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                pass
    val = d.get("tech_stack")
    if isinstance(val, str):
        try:
            d["tech_stack"] = json.loads(val)
        except (json.JSONDecodeError, TypeError):
            d["tech_stack"] = [val] if val else []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EMAIL-CRUD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def create_email_record(user_id: int, data: Dict[str, Any]) -> Optional[int]:
    """Erstellt einen E-Mail-Eintrag."""
    data["user_id"] = user_id
    fields = [
        "user_id", "lead_id", "to_email", "from_email", "subject", "body",
        "body_html", "template_key", "status", "followup_number",
        "ab_test_id", "ab_variant", "tracking_id", "scheduled_at", "pdf_path",
    ]
    present = {k: data.get(k) for k in fields if data.get(k) is not None}
    cols = ", ".join(present.keys())
    placeholders = ", ".join("?" for _ in present)
    vals = list(present.values())
    try:
        with get_cursor() as cur:
            cur.execute(f"INSERT INTO emails ({cols}) VALUES ({placeholders})", vals)
            return cur.lastrowid
    except sqlite3.Error as exc:
        log.error("E-Mail-Erstellung fehlgeschlagen: %s", exc)
        return None


def update_email(email_id: int, user_id: int, **kwargs) -> bool:
    """Aktualisiert E-Mail-Felder."""
    allowed = {
        "status", "opened", "opened_at", "opened_count",
        "clicked", "clicked_at", "bounced", "bounce_reason",
        "replied", "replied_at", "unsubscribed", "unsubscribed_at",
        "sent_at", "error_message", "scheduled_at",
    }
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return False
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [email_id, user_id]
    try:
        with get_cursor() as cur:
            cur.execute(
                f"UPDATE emails SET {set_clause} WHERE id = ? AND user_id = ?", values
            )
            return cur.rowcount > 0
    except sqlite3.Error as exc:
        log.error("E-Mail-Update fehlgeschlagen: %s", exc)
        return False


def get_emails_by_user(user_id: int, status: str = None,
                       limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """Gibt E-Mails eines Users zurueck."""
    query = "SELECT * FROM emails WHERE user_id = ?"
    params: List[Any] = [user_id]
    if status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    return rows_to_dicts(get_db().execute(query, params).fetchall())


def get_email_by_tracking_id(tracking_id: str) -> Optional[Dict[str, Any]]:
    """Findet eine E-Mail ueber ihre Tracking-ID."""
    row = get_db().execute(
        "SELECT * FROM emails WHERE tracking_id = ?", (tracking_id,)
    ).fetchone()
    return dict_from_row(row)


def get_email_by_id(email_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    """Gibt eine einzelne E-Mail zurueck."""
    row = get_db().execute(
        "SELECT * FROM emails WHERE id = ? AND user_id = ?", (email_id, user_id)
    ).fetchone()
    return dict_from_row(row)


def count_emails_sent_today(user_id: int) -> int:
    """Zaehlt heute gesendete E-Mails."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    row = get_db().execute(
        "SELECT COUNT(*) AS cnt FROM emails WHERE user_id = ? AND status = 'sent' AND date(sent_at) = ?",
        (user_id, today),
    ).fetchone()
    return row["cnt"] if row else 0


def get_pending_scheduled_emails(user_id: int = None) -> List[Dict[str, Any]]:
    """Gibt geplante E-Mails zurueck, die faellig sind."""
    query = """SELECT * FROM emails
               WHERE status = 'pending' AND scheduled_at IS NOT NULL
               AND scheduled_at <= datetime('now')"""
    params: List[Any] = []
    if user_id:
        query += " AND user_id = ?"
        params.append(user_id)
    query += " ORDER BY scheduled_at"
    return rows_to_dicts(get_db().execute(query, params).fetchall())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TRACKING-EVENTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def record_tracking_event(tracking_id: str, email_id: int, user_id: int,
                          event_type: str, ip: str = "", ua: str = "") -> Optional[int]:
    """Speichert ein Tracking-Event."""
    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO tracking_events (tracking_id, email_id, user_id, event_type, ip_address, user_agent)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (tracking_id, email_id, user_id, event_type, ip, ua),
            )
            return cur.lastrowid
    except sqlite3.Error as exc:
        log.error("Tracking-Event fehlgeschlagen: %s", exc)
        return None


def get_tracking_events(user_id: int, limit: int = 200) -> List[Dict[str, Any]]:
    """Gibt Tracking-Events eines Users zurueck."""
    return rows_to_dicts(get_db().execute(
        """SELECT te.*, e.to_email, e.subject
           FROM tracking_events te
           LEFT JOIN emails e ON te.email_id = e.id
           WHERE te.user_id = ?
           ORDER BY te.created_at DESC LIMIT ?""",
        (user_id, limit),
    ).fetchall())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BLACKLIST
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def add_to_blacklist(user_id: int, email: str, reason: str = "") -> bool:
    """Fuegt eine E-Mail zur Blacklist hinzu."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "INSERT OR IGNORE INTO blacklist (user_id, email, reason) VALUES (?, ?, ?)",
                (user_id, email.lower().strip(), reason),
            )
            return cur.rowcount > 0
    except sqlite3.Error as exc:
        log.error("Blacklist-Eintrag fehlgeschlagen: %s", exc)
        return False


def remove_from_blacklist(user_id: int, email: str) -> bool:
    """Entfernt eine E-Mail von der Blacklist."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "DELETE FROM blacklist WHERE user_id = ? AND email = ?",
                (user_id, email.lower().strip()),
            )
            return cur.rowcount > 0
    except sqlite3.Error as exc:
        log.error("Blacklist-Entfernung fehlgeschlagen: %s", exc)
        return False


def get_blacklist(user_id: int) -> List[Dict[str, Any]]:
    """Gibt die Blacklist eines Users zurueck."""
    return rows_to_dicts(get_db().execute(
        "SELECT * FROM blacklist WHERE user_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall())


def is_blacklisted(user_id: int, email: str) -> bool:
    """Prueft ob eine E-Mail auf der Blacklist steht."""
    row = get_db().execute(
        "SELECT id FROM blacklist WHERE user_id = ? AND email = ?",
        (user_id, email.lower().strip()),
    ).fetchone()
    return row is not None


def get_blacklist_set(user_id: int) -> set:
    """Gibt die Blacklist als Set zurueck (fuer schnelle Lookups)."""
    rows = get_db().execute(
        "SELECT email FROM blacklist WHERE user_id = ?", (user_id,)
    ).fetchall()
    return {r["email"] for r in rows}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SUCHHISTORIE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def record_search(user_id: int, query: str, city: str,
                  result_count: int = 0, competition_count: int = 0,
                  duration: float = 0, cities_json: str = None) -> Optional[int]:
    """Speichert eine Suchanfrage."""
    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO searches (user_id, query, city, cities_json, result_count,
                   competition_count, duration_seconds)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (user_id, query, city, cities_json, result_count, competition_count, duration),
            )
            return cur.lastrowid
    except sqlite3.Error as exc:
        log.error("Such-Aufzeichnung fehlgeschlagen: %s", exc)
        return None


def get_search_history(user_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    """Gibt die Suchhistorie zurueck."""
    return rows_to_dicts(get_db().execute(
        "SELECT * FROM searches WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
        (user_id, limit),
    ).fetchall())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FOLLOW-UPS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def create_followup(user_id: int, lead_id: int, original_email_id: int,
                    followup_number: int, scheduled_date: str) -> Optional[int]:
    """Plant einen Follow-Up."""
    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO followups (user_id, lead_id, original_email_id,
                   followup_number, scheduled_date) VALUES (?, ?, ?, ?, ?)""",
                (user_id, lead_id, original_email_id, followup_number, scheduled_date),
            )
            return cur.lastrowid
    except sqlite3.Error as exc:
        log.error("Follow-Up-Erstellung fehlgeschlagen: %s", exc)
        return None


def get_pending_followups(user_id: int = None) -> List[Dict[str, Any]]:
    """Gibt faellige Follow-Ups zurueck."""
    query = """SELECT f.*, l.name AS lead_name, l.email AS lead_email
               FROM followups f
               JOIN leads l ON f.lead_id = l.id
               WHERE f.status = 'pending' AND f.scheduled_date <= datetime('now')"""
    params: List[Any] = []
    if user_id:
        query += " AND f.user_id = ?"
        params.append(user_id)
    query += " ORDER BY f.scheduled_date"
    return rows_to_dicts(get_db().execute(query, params).fetchall())


def get_followups_by_user(user_id: int) -> List[Dict[str, Any]]:
    """Gibt alle Follow-Ups eines Users zurueck."""
    return rows_to_dicts(get_db().execute(
        """SELECT f.*, l.name AS lead_name, l.email AS lead_email
           FROM followups f
           JOIN leads l ON f.lead_id = l.id
           WHERE f.user_id = ?
           ORDER BY f.scheduled_date DESC""",
        (user_id,),
    ).fetchall())


def update_followup(followup_id: int, **kwargs) -> bool:
    """Aktualisiert einen Follow-Up."""
    allowed = {"status", "sent_email_id", "sent_at"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return False
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [followup_id]
    try:
        with get_cursor() as cur:
            cur.execute(f"UPDATE followups SET {set_clause} WHERE id = ?", values)
            return cur.rowcount > 0
    except sqlite3.Error as exc:
        log.error("Follow-Up-Update fehlgeschlagen: %s", exc)
        return False


def cancel_followups_for_lead(user_id: int, lead_id: int) -> int:
    """Bricht alle ausstehenden Follow-Ups fuer einen Lead ab."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "UPDATE followups SET status = 'cancelled' WHERE user_id = ? AND lead_id = ? AND status = 'pending'",
                (user_id, lead_id),
            )
            return cur.rowcount
    except sqlite3.Error as exc:
        log.error("Follow-Up-Abbruch fehlgeschlagen: %s", exc)
        return 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TAGS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_tags(user_id: int) -> List[Dict[str, Any]]:
    """Gibt alle Tags eines Users zurueck."""
    return rows_to_dicts(get_db().execute(
        "SELECT * FROM tags WHERE user_id = ? ORDER BY name", (user_id,)
    ).fetchall())


def create_tag(user_id: int, name: str, color: str = "#7C6FFF") -> Optional[int]:
    """Erstellt einen neuen Tag."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "INSERT INTO tags (user_id, name, color) VALUES (?, ?, ?)",
                (user_id, name.strip(), color),
            )
            return cur.lastrowid
    except sqlite3.IntegrityError:
        return None


def delete_tag(tag_id: int, user_id: int) -> bool:
    """Loescht einen Tag."""
    try:
        with get_cursor() as cur:
            cur.execute("DELETE FROM tags WHERE id = ? AND user_id = ?", (tag_id, user_id))
            return cur.rowcount > 0
    except sqlite3.Error:
        return False


def add_lead_tag(lead_id: int, tag_id: int) -> bool:
    """Fuegt einen Tag zu einem Lead hinzu."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "INSERT OR IGNORE INTO lead_tags (lead_id, tag_id) VALUES (?, ?)",
                (lead_id, tag_id),
            )
            return True
    except sqlite3.Error:
        return False


def remove_lead_tag(lead_id: int, tag_id: int) -> bool:
    """Entfernt einen Tag von einem Lead."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "DELETE FROM lead_tags WHERE lead_id = ? AND tag_id = ?",
                (lead_id, tag_id),
            )
            return cur.rowcount > 0
    except sqlite3.Error:
        return False


def get_lead_tags(lead_id: int) -> List[Dict[str, Any]]:
    """Gibt die Tags eines Leads zurueck."""
    return rows_to_dicts(get_db().execute(
        """SELECT t.* FROM tags t
           JOIN lead_tags lt ON t.id = lt.tag_id
           WHERE lt.lead_id = ?""",
        (lead_id,),
    ).fetchall())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  KANBAN-STAGES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_kanban_stages(user_id: int) -> List[Dict[str, Any]]:
    """Gibt die Kanban-Spalten eines Users zurueck."""
    return rows_to_dicts(get_db().execute(
        "SELECT * FROM kanban_stages WHERE user_id = ? ORDER BY sort_order",
        (user_id,),
    ).fetchall())


def create_kanban_stage(user_id: int, name: str, color: str, sort_order: int) -> Optional[int]:
    """Erstellt eine neue Kanban-Spalte."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "INSERT INTO kanban_stages (user_id, name, color, sort_order) VALUES (?, ?, ?, ?)",
                (user_id, name, color, sort_order),
            )
            return cur.lastrowid
    except sqlite3.Error:
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  A/B-TESTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def create_ab_test(user_id: int, name: str,
                   variant_a_subject: str, variant_a_body: str,
                   variant_b_subject: str, variant_b_body: str) -> Optional[int]:
    """Erstellt einen neuen A/B-Test."""
    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO ab_tests (user_id, name, variant_a_subject, variant_a_body,
                   variant_b_subject, variant_b_body) VALUES (?, ?, ?, ?, ?, ?)""",
                (user_id, name, variant_a_subject, variant_a_body,
                 variant_b_subject, variant_b_body),
            )
            return cur.lastrowid
    except sqlite3.Error as exc:
        log.error("A/B-Test-Erstellung fehlgeschlagen: %s", exc)
        return None


def get_ab_tests(user_id: int) -> List[Dict[str, Any]]:
    """Gibt A/B-Tests eines Users zurueck."""
    return rows_to_dicts(get_db().execute(
        "SELECT * FROM ab_tests WHERE user_id = ? ORDER BY created_at DESC",
        (user_id,),
    ).fetchall())


def get_ab_test(test_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    """Gibt einen A/B-Test zurueck."""
    row = get_db().execute(
        "SELECT * FROM ab_tests WHERE id = ? AND user_id = ?", (test_id, user_id)
    ).fetchone()
    return dict_from_row(row)


def update_ab_test(test_id: int, user_id: int, **kwargs) -> bool:
    """Aktualisiert A/B-Test-Felder."""
    allowed = {
        "variant_a_sent", "variant_a_opened", "variant_a_replied",
        "variant_b_sent", "variant_b_opened", "variant_b_replied",
        "status", "winner",
    }
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return False
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [test_id, user_id]
    try:
        with get_cursor() as cur:
            cur.execute(
                f"UPDATE ab_tests SET {set_clause} WHERE id = ? AND user_id = ?", values
            )
            return cur.rowcount > 0
    except sqlite3.Error:
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  WEBHOOKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def create_webhook(user_id: int, url: str, event_type: str, secret: str = "") -> Optional[int]:
    """Erstellt einen neuen Webhook."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "INSERT INTO webhooks (user_id, url, event_type, secret) VALUES (?, ?, ?, ?)",
                (user_id, url, event_type, secret),
            )
            return cur.lastrowid
    except sqlite3.Error:
        return None


def get_webhooks(user_id: int) -> List[Dict[str, Any]]:
    """Gibt Webhooks eines Users zurueck."""
    return rows_to_dicts(get_db().execute(
        "SELECT * FROM webhooks WHERE user_id = ? ORDER BY id", (user_id,)
    ).fetchall())


def get_active_webhooks(user_id: int, event_type: str) -> List[Dict[str, Any]]:
    """Gibt aktive Webhooks fuer einen Event-Typ zurueck."""
    return rows_to_dicts(get_db().execute(
        "SELECT * FROM webhooks WHERE user_id = ? AND event_type = ? AND active = 1",
        (user_id, event_type),
    ).fetchall())


def update_webhook(webhook_id: int, user_id: int, **kwargs) -> bool:
    """Aktualisiert einen Webhook."""
    allowed = {"url", "event_type", "secret", "active", "last_triggered", "fail_count"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return False
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [webhook_id, user_id]
    try:
        with get_cursor() as cur:
            cur.execute(
                f"UPDATE webhooks SET {set_clause} WHERE id = ? AND user_id = ?", values
            )
            return cur.rowcount > 0
    except sqlite3.Error:
        return False


def delete_webhook(webhook_id: int, user_id: int) -> bool:
    """Loescht einen Webhook."""
    try:
        with get_cursor() as cur:
            cur.execute("DELETE FROM webhooks WHERE id = ? AND user_id = ?", (webhook_id, user_id))
            return cur.rowcount > 0
    except sqlite3.Error:
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  API-KEYS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def create_api_key(user_id: int, key_hash: str, name: str = "") -> Optional[int]:
    """Erstellt einen neuen API-Key."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "INSERT INTO api_keys (user_id, key_hash, name) VALUES (?, ?, ?)",
                (user_id, key_hash, name),
            )
            return cur.lastrowid
    except sqlite3.Error:
        return None


def get_api_keys(user_id: int) -> List[Dict[str, Any]]:
    """Gibt API-Keys eines Users zurueck."""
    return rows_to_dicts(get_db().execute(
        "SELECT id, user_id, name, last_used, requests_today, active, created_at FROM api_keys WHERE user_id = ?",
        (user_id,),
    ).fetchall())


def verify_api_key(key_hash: str) -> Optional[Dict[str, Any]]:
    """Verifiziert einen API-Key und gibt den User zurueck."""
    row = get_db().execute(
        "SELECT ak.*, u.id AS uid FROM api_keys ak JOIN users u ON ak.user_id = u.id WHERE ak.key_hash = ? AND ak.active = 1",
        (key_hash,),
    ).fetchone()
    if row:
        with get_cursor() as cur:
            cur.execute(
                "UPDATE api_keys SET last_used = datetime('now'), requests_today = requests_today + 1 WHERE id = ?",
                (row["id"],),
            )
    return dict_from_row(row)


def delete_api_key(key_id: int, user_id: int) -> bool:
    """Loescht einen API-Key."""
    try:
        with get_cursor() as cur:
            cur.execute("DELETE FROM api_keys WHERE id = ? AND user_id = ?", (key_id, user_id))
            return cur.rowcount > 0
    except sqlite3.Error:
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DSGVO-LOG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def log_dsgvo(user_id: int, action: str, target_email: str = "",
              target_name: str = "", channel: str = "email",
              content_summary: str = "", legal_basis: str = "berechtigtes_interesse") -> Optional[int]:
    """Loggt eine DSGVO-relevante Aktion."""
    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO dsgvo_log (user_id, action, target_email, target_name,
                   channel, content_summary, legal_basis) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (user_id, action, target_email, target_name, channel,
                 content_summary, legal_basis),
            )
            return cur.lastrowid
    except sqlite3.Error:
        return None


def get_dsgvo_log(user_id: int, limit: int = 200) -> List[Dict[str, Any]]:
    """Gibt das DSGVO-Log zurueck."""
    return rows_to_dicts(get_db().execute(
        "SELECT * FROM dsgvo_log WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
        (user_id, limit),
    ).fetchall())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  WARMUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_warmup_status(user_id: int, smtp_user: str) -> Optional[Dict[str, Any]]:
    """Gibt den Warmup-Status zurueck."""
    row = get_db().execute(
        "SELECT * FROM warmup_status WHERE user_id = ? AND smtp_user = ?",
        (user_id, smtp_user),
    ).fetchone()
    return dict_from_row(row)


def upsert_warmup_status(user_id: int, smtp_user: str, **kwargs) -> bool:
    """Erstellt oder aktualisiert den Warmup-Status."""
    existing = get_warmup_status(user_id, smtp_user)
    if existing:
        allowed = {"day_number", "daily_limit", "emails_sent_today", "last_send_date", "reputation_score"}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return False
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [user_id, smtp_user]
        try:
            with get_cursor() as cur:
                cur.execute(
                    f"UPDATE warmup_status SET {set_clause} WHERE user_id = ? AND smtp_user = ?",
                    values,
                )
                return True
        except sqlite3.Error:
            return False
    else:
        day = kwargs.get("day_number", 1)
        limit = kwargs.get("daily_limit", 5)
        try:
            with get_cursor() as cur:
                cur.execute(
                    "INSERT INTO warmup_status (user_id, smtp_user, day_number, daily_limit) VALUES (?, ?, ?, ?)",
                    (user_id, smtp_user, day, limit),
                )
                return True
        except sqlite3.Error:
            return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SCHEDULED JOBS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def create_scheduled_job(user_id: int, job_type: str, config: Dict = None,
                         cron: str = "", next_run: str = None) -> Optional[int]:
    """Erstellt einen geplanten Job."""
    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO scheduled_jobs (user_id, job_type, config_json, cron_expression, next_run)
                   VALUES (?, ?, ?, ?, ?)""",
                (user_id, job_type, json.dumps(config or {}), cron,
                 next_run or datetime.utcnow().isoformat()),
            )
            return cur.lastrowid
    except sqlite3.Error:
        return None


def get_due_jobs() -> List[Dict[str, Any]]:
    """Gibt faellige Jobs zurueck."""
    return rows_to_dicts(get_db().execute(
        "SELECT * FROM scheduled_jobs WHERE status = 'active' AND next_run <= datetime('now')"
    ).fetchall())


def update_job(job_id: int, **kwargs) -> bool:
    """Aktualisiert einen Job."""
    allowed = {"next_run", "last_run", "status", "run_count"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return False
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [job_id]
    try:
        with get_cursor() as cur:
            cur.execute(f"UPDATE scheduled_jobs SET {set_clause} WHERE id = ?", values)
            return True
    except sqlite3.Error:
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SUBSCRIPTIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def create_subscription(user_id: int, plan_id: str, order_id: str = "",
                        amount: int = 0, period_start: str = None,
                        period_end: str = None) -> Optional[int]:
    """Erstellt ein Abo."""
    now = datetime.utcnow().isoformat()
    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO subscriptions (user_id, plan_id, order_id, amount,
                   period_start, period_end) VALUES (?, ?, ?, ?, ?, ?)""",
                (user_id, plan_id, order_id, amount,
                 period_start or now, period_end or (datetime.utcnow() + timedelta(days=30)).isoformat()),
            )
            return cur.lastrowid
    except sqlite3.Error:
        return None


def get_subscriptions(user_id: int) -> List[Dict[str, Any]]:
    """Gibt Abos eines Users zurueck."""
    return rows_to_dicts(get_db().execute(
        "SELECT s.*, p.name AS plan_name FROM subscriptions s JOIN plans p ON s.plan_id = p.id WHERE s.user_id = ? ORDER BY s.created_at DESC",
        (user_id,),
    ).fetchall())


def update_subscription(sub_id: int, **kwargs) -> bool:
    """Aktualisiert ein Abo."""
    allowed = {"status", "period_end", "amount"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return False
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [sub_id]
    try:
        with get_cursor() as cur:
            cur.execute(f"UPDATE subscriptions SET {set_clause} WHERE id = ?", values)
            return True
    except sqlite3.Error:
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  NOTIFICATIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def create_notification(user_id: int, title: str, message: str = "",
                        ntype: str = "info", link: str = None) -> Optional[int]:
    """Erstellt eine Notification."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "INSERT INTO notifications (user_id, type, title, message, link) VALUES (?, ?, ?, ?, ?)",
                (user_id, ntype, title, message, link),
            )
            return cur.lastrowid
    except sqlite3.Error:
        return None


def get_notifications(user_id: int, unread_only: bool = False,
                      limit: int = 50) -> List[Dict[str, Any]]:
    """Gibt Notifications zurueck."""
    query = "SELECT * FROM notifications WHERE user_id = ?"
    params: List[Any] = [user_id]
    if unread_only:
        query += " AND read = 0"
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    return rows_to_dicts(get_db().execute(query, params).fetchall())


def mark_notification_read(notif_id: int, user_id: int) -> bool:
    """Markiert eine Notification als gelesen."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "UPDATE notifications SET read = 1 WHERE id = ? AND user_id = ?",
                (notif_id, user_id),
            )
            return cur.rowcount > 0
    except sqlite3.Error:
        return False


def mark_all_notifications_read(user_id: int) -> int:
    """Markiert alle Notifications als gelesen."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "UPDATE notifications SET read = 1 WHERE user_id = ? AND read = 0",
                (user_id,),
            )
            return cur.rowcount
    except sqlite3.Error:
        return 0


def count_unread_notifications(user_id: int) -> int:
    """Zaehlt ungelesene Notifications."""
    row = get_db().execute(
        "SELECT COUNT(*) AS cnt FROM notifications WHERE user_id = ? AND read = 0",
        (user_id,),
    ).fetchone()
    return row["cnt"] if row else 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TEAM MEMBERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def invite_team_member(owner_id: int, member_user_id: int, role: str = "member") -> Optional[int]:
    """Laedt ein Team-Mitglied ein."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "INSERT INTO team_members (team_owner_id, member_user_id, role) VALUES (?, ?, ?)",
                (owner_id, member_user_id, role),
            )
            return cur.lastrowid
    except sqlite3.Error:
        return None


def get_team_members(owner_id: int) -> List[Dict[str, Any]]:
    """Gibt Team-Mitglieder zurueck."""
    return rows_to_dicts(get_db().execute(
        """SELECT tm.*, u.email AS member_email, u.name AS member_name
           FROM team_members tm
           JOIN users u ON tm.member_user_id = u.id
           WHERE tm.team_owner_id = ?""",
        (owner_id,),
    ).fetchall())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DIGISTORE IPN LOG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def log_digistore_ipn(event: str, product_id: str, order_id: str,
                      transaction_id: str, email: str, custom: str,
                      raw_params: str, signature_valid: bool, processed: bool) -> Optional[int]:
    """Loggt einen Digistore24-IPN-Call."""
    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO digistore_ipn_log (event, product_id, order_id, transaction_id,
                   email, custom, raw_params, signature_valid, processed)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (event, product_id, order_id, transaction_id, email, custom,
                 raw_params, int(signature_valid), int(processed)),
            )
            return cur.lastrowid
    except sqlite3.Error:
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  STATISTIK-HILFSFUNKTIONEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_lead_stats(user_id: int) -> Dict[str, int]:
    """Basis-Statistiken fuer Leads."""
    db = get_db()
    total = db.execute("SELECT COUNT(*) AS c FROM leads WHERE user_id = ?", (user_id,)).fetchone()["c"]
    with_email = db.execute("SELECT COUNT(*) AS c FROM leads WHERE user_id = ? AND email IS NOT NULL AND email != ''", (user_id,)).fetchone()["c"]
    contacted = db.execute("SELECT COUNT(*) AS c FROM leads WHERE user_id = ? AND contacted = 1", (user_id,)).fetchone()["c"]
    responded = db.execute("SELECT COUNT(*) AS c FROM leads WHERE user_id = ? AND responded = 1", (user_id,)).fetchone()["c"]
    converted = db.execute("SELECT COUNT(*) AS c FROM leads WHERE user_id = ? AND converted = 1", (user_id,)).fetchone()["c"]
    return {
        "total": total, "with_email": with_email, "contacted": contacted,
        "responded": responded, "converted": converted,
    }


def get_email_stats(user_id: int) -> Dict[str, int]:
    """Basis-Statistiken fuer E-Mails."""
    db = get_db()
    sent = db.execute("SELECT COUNT(*) AS c FROM emails WHERE user_id = ? AND status = 'sent'", (user_id,)).fetchone()["c"]
    opened = db.execute("SELECT COUNT(*) AS c FROM emails WHERE user_id = ? AND opened = 1", (user_id,)).fetchone()["c"]
    replied = db.execute("SELECT COUNT(*) AS c FROM emails WHERE user_id = ? AND replied = 1", (user_id,)).fetchone()["c"]
    bounced = db.execute("SELECT COUNT(*) AS c FROM emails WHERE user_id = ? AND bounced = 1", (user_id,)).fetchone()["c"]
    return {"sent": sent, "opened": opened, "replied": replied, "bounced": bounced}


def get_kanban_counts(user_id: int) -> Dict[str, int]:
    """Zaehlt Leads pro Kanban-Stage."""
    rows = get_db().execute(
        "SELECT kanban_stage, COUNT(*) AS c FROM leads WHERE user_id = ? GROUP BY kanban_stage",
        (user_id,),
    ).fetchall()
    return {r["kanban_stage"]: r["c"] for r in rows}


def get_contacted_emails(user_id: int) -> set:
    """Gibt alle kontaktierten E-Mail-Adressen zurueck."""
    rows = get_db().execute(
        "SELECT DISTINCT to_email FROM emails WHERE user_id = ? AND status = 'sent'",
        (user_id,),
    ).fetchall()
    return {r["to_email"] for r in rows}
