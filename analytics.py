"""
analytics.py — Statistiken, ROI-Berechnung, Reports, A/B-Test-Auswertung

Alle Analyse-Funktionen fuer das LeadFinder Pro Dashboard.
"""

import math
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

import database as db

log = logging.getLogger("leadfinder.analytics")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  USER-STATISTIKEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_user_stats(user_id: int) -> Dict[str, Any]:
    """Umfassende Statistiken fuer einen User."""
    conn = db.get_db()
    lead_stats = db.get_lead_stats(user_id)
    email_stats = db.get_email_stats(user_id)
    kanban_counts = db.get_kanban_counts(user_id)

    # Oeffnungsrate
    sent = email_stats["sent"]
    opened = email_stats["opened"]
    open_rate = round((opened / sent * 100), 1) if sent > 0 else 0

    # Antwortrate
    replied = email_stats["replied"]
    reply_rate = round((replied / sent * 100), 1) if sent > 0 else 0

    # Bounce-Rate
    bounced = email_stats["bounced"]
    bounce_rate = round((bounced / sent * 100), 1) if sent > 0 else 0

    # Faellige Follow-Ups
    followups_due = conn.execute(
        "SELECT COUNT(*) AS c FROM followups WHERE user_id = ? AND status = 'pending' AND scheduled_date <= datetime('now')",
        (user_id,),
    ).fetchone()["c"]

    # Suchen heute
    today = datetime.utcnow().strftime("%Y-%m-%d")
    searches_today = conn.execute(
        "SELECT COUNT(*) AS c FROM searches WHERE user_id = ? AND date(created_at) = ?",
        (user_id, today),
    ).fetchone()["c"]

    # Revenue
    revenue = conn.execute(
        "SELECT COALESCE(SUM(revenue), 0) AS total FROM leads WHERE user_id = ? AND converted = 1",
        (user_id,),
    ).fetchone()["total"]

    return {
        "leads": lead_stats,
        "emails": {
            **email_stats,
            "open_rate": open_rate,
            "reply_rate": reply_rate,
            "bounce_rate": bounce_rate,
        },
        "kanban": kanban_counts,
        "followups_due": followups_due,
        "searches_today": searches_today,
        "total_revenue": revenue,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  WOCHEN-STATISTIK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_weekly_stats(user_id: int, weeks: int = 8) -> List[Dict[str, Any]]:
    """Gibt woechentliche Statistiken zurueck (letzte N Wochen)."""
    conn = db.get_db()
    result = []
    now = datetime.utcnow()

    for w in range(weeks - 1, -1, -1):
        week_start = now - timedelta(weeks=w, days=now.weekday())
        week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        week_end = week_start + timedelta(days=7)
        ws = week_start.isoformat()
        we = week_end.isoformat()
        label = week_start.strftime("KW%V")

        leads_count = conn.execute(
            "SELECT COUNT(*) AS c FROM leads WHERE user_id = ? AND created_at >= ? AND created_at < ?",
            (user_id, ws, we),
        ).fetchone()["c"]

        emails_sent = conn.execute(
            "SELECT COUNT(*) AS c FROM emails WHERE user_id = ? AND status = 'sent' AND sent_at >= ? AND sent_at < ?",
            (user_id, ws, we),
        ).fetchone()["c"]

        emails_opened = conn.execute(
            "SELECT COUNT(*) AS c FROM emails WHERE user_id = ? AND opened = 1 AND opened_at >= ? AND opened_at < ?",
            (user_id, ws, we),
        ).fetchone()["c"]

        emails_replied = conn.execute(
            "SELECT COUNT(*) AS c FROM emails WHERE user_id = ? AND replied = 1 AND replied_at >= ? AND replied_at < ?",
            (user_id, ws, we),
        ).fetchone()["c"]

        result.append({
            "label": label,
            "week_start": ws,
            "leads": leads_count,
            "sent": emails_sent,
            "opened": emails_opened,
            "replied": emails_replied,
        })

    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MONATS-STATISTIK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_monthly_stats(user_id: int, months: int = 6) -> List[Dict[str, Any]]:
    """Gibt monatliche Statistiken zurueck."""
    conn = db.get_db()
    result = []
    now = datetime.utcnow()

    for m in range(months - 1, -1, -1):
        # Monatsanfang berechnen
        month_date = now.replace(day=1) - timedelta(days=m * 28)
        month_date = month_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        year = month_date.year
        month = month_date.month
        if month == 12:
            next_month = month_date.replace(year=year + 1, month=1)
        else:
            next_month = month_date.replace(month=month + 1)
        ms = month_date.isoformat()
        me = next_month.isoformat()
        label = month_date.strftime("%b %Y")

        leads_count = conn.execute(
            "SELECT COUNT(*) AS c FROM leads WHERE user_id = ? AND created_at >= ? AND created_at < ?",
            (user_id, ms, me),
        ).fetchone()["c"]

        emails_sent = conn.execute(
            "SELECT COUNT(*) AS c FROM emails WHERE user_id = ? AND status = 'sent' AND sent_at >= ? AND sent_at < ?",
            (user_id, ms, me),
        ).fetchone()["c"]

        converted = conn.execute(
            "SELECT COUNT(*) AS c FROM leads WHERE user_id = ? AND converted = 1 AND converted_date >= ? AND converted_date < ?",
            (user_id, ms, me),
        ).fetchone()["c"]

        revenue = conn.execute(
            "SELECT COALESCE(SUM(revenue), 0) AS total FROM leads WHERE user_id = ? AND converted = 1 AND converted_date >= ? AND converted_date < ?",
            (user_id, ms, me),
        ).fetchone()["total"]

        result.append({
            "label": label,
            "month_start": ms,
            "leads": leads_count,
            "sent": emails_sent,
            "converted": converted,
            "revenue": revenue,
        })

    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONVERSION-FUNNEL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_funnel(user_id: int) -> Dict[str, Any]:
    """Berechnet den Conversion-Funnel mit Prozenten."""
    stats = db.get_lead_stats(user_id)
    email_stats = db.get_email_stats(user_id)

    total = stats["total"]
    with_email = stats["with_email"]
    contacted = stats["contacted"]
    replied = stats["responded"]
    converted = stats["converted"]

    def pct(val, base):
        return round((val / base * 100), 1) if base > 0 else 0

    return {
        "stages": [
            {"name": "Leads gefunden", "count": total, "percent": 100},
            {"name": "Mit E-Mail", "count": with_email, "percent": pct(with_email, total)},
            {"name": "Kontaktiert", "count": contacted, "percent": pct(contacted, total)},
            {"name": "E-Mail geoeffnet", "count": email_stats["opened"], "percent": pct(email_stats["opened"], email_stats["sent"])},
            {"name": "Geantwortet", "count": replied, "percent": pct(replied, contacted)},
            {"name": "Konvertiert", "count": converted, "percent": pct(converted, contacted)},
        ],
        "total_leads": total,
        "conversion_rate": pct(converted, total),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ROI-BERECHNUNG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def calculate_roi(user_id: int, customer_value: float = 1500) -> Dict[str, Any]:
    """Berechnet den ROI (Return on Investment)."""
    stats = db.get_lead_stats(user_id)
    email_stats = db.get_email_stats(user_id)
    conn = db.get_db()

    converted = stats["converted"]
    total_revenue = conn.execute(
        "SELECT COALESCE(SUM(revenue), 0) AS total FROM leads WHERE user_id = ? AND converted = 1",
        (user_id,),
    ).fetchone()["total"]

    # Wenn kein Revenue erfasst, mit customer_value schaetzen
    if total_revenue == 0 and converted > 0:
        total_revenue = converted * customer_value

    # Kosten: Plan-Kosten (pro Monat seit Registrierung)
    user = db.get_user_with_plan(user_id)
    if not user:
        return {"roi": 0, "revenue": 0, "cost": 0}

    created = datetime.fromisoformat(user.get("created_at", datetime.utcnow().isoformat()))
    months_active = max(1, (datetime.utcnow() - created).days / 30)
    plan_cost = (user.get("price_monthly", 0) / 100) * months_active  # Cent -> Euro
    time_cost = email_stats["sent"] * 0.05  # ~3 Min pro E-Mail gespart bei 1 Euro/Stunde
    total_cost = plan_cost + time_cost

    roi = round(((total_revenue - total_cost) / max(total_cost, 1)) * 100, 1)

    # Cost per Lead / Customer
    total_leads = stats["total"]
    cost_per_lead = round(total_cost / max(total_leads, 1), 2)
    cost_per_customer = round(total_cost / max(converted, 1), 2)

    return {
        "roi": roi,
        "revenue": total_revenue,
        "cost": round(total_cost, 2),
        "plan_cost": round(plan_cost, 2),
        "converted": converted,
        "customer_value": customer_value,
        "cost_per_lead": cost_per_lead,
        "cost_per_customer": cost_per_customer,
        "months_active": round(months_active, 1),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BRANCHEN- UND STADT-PERFORMANCE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_branch_city_performance(user_id: int) -> Dict[str, Any]:
    """Gibt Heatmap-Daten fuer Branchen x Staedte zurueck."""
    conn = db.get_db()

    # Branchen-Performance
    branches = conn.execute(
        """SELECT search_query, COUNT(*) AS total,
                  SUM(CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END) AS with_email,
                  SUM(CASE WHEN contacted = 1 THEN 1 ELSE 0 END) AS contacted,
                  SUM(CASE WHEN responded = 1 THEN 1 ELSE 0 END) AS responded,
                  SUM(CASE WHEN converted = 1 THEN 1 ELSE 0 END) AS converted
           FROM leads WHERE user_id = ? AND search_query IS NOT NULL
           GROUP BY search_query ORDER BY total DESC LIMIT 20""",
        (user_id,),
    ).fetchall()

    # Stadt-Performance
    cities = conn.execute(
        """SELECT search_city, COUNT(*) AS total,
                  SUM(CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END) AS with_email,
                  SUM(CASE WHEN contacted = 1 THEN 1 ELSE 0 END) AS contacted,
                  SUM(CASE WHEN responded = 1 THEN 1 ELSE 0 END) AS responded,
                  SUM(CASE WHEN converted = 1 THEN 1 ELSE 0 END) AS converted
           FROM leads WHERE user_id = ? AND search_city IS NOT NULL AND search_city != ''
           GROUP BY search_city ORDER BY total DESC LIMIT 20""",
        (user_id,),
    ).fetchall()

    # Heatmap: Branche x Stadt
    heatmap = conn.execute(
        """SELECT search_query, search_city, COUNT(*) AS total,
                  SUM(CASE WHEN responded = 1 THEN 1 ELSE 0 END) AS responded
           FROM leads WHERE user_id = ? AND search_query IS NOT NULL AND search_city IS NOT NULL AND search_city != ''
           GROUP BY search_query, search_city
           ORDER BY total DESC LIMIT 100""",
        (user_id,),
    ).fetchall()

    return {
        "branches": [dict(r) for r in branches],
        "cities": [dict(r) for r in cities],
        "heatmap": [dict(r) for r in heatmap],
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  A/B-TEST AUSWERTUNG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def evaluate_ab_test(test_id: int, user_id: int) -> Dict[str, Any]:
    """Wertet einen A/B-Test aus (Chi-Quadrat, Winner bei p<0.05 und 30+ Sends)."""
    test = db.get_ab_test(test_id, user_id)
    if not test:
        return {"error": "Test nicht gefunden"}

    a_sent = test["variant_a_sent"]
    a_opened = test["variant_a_opened"]
    a_replied = test["variant_a_replied"]
    b_sent = test["variant_b_sent"]
    b_opened = test["variant_b_opened"]
    b_replied = test["variant_b_replied"]

    total_sent = a_sent + b_sent
    min_sends = 30

    # Oeffnungsraten
    a_open_rate = round((a_opened / a_sent * 100), 1) if a_sent > 0 else 0
    b_open_rate = round((b_opened / b_sent * 100), 1) if b_sent > 0 else 0

    # Antwortraten
    a_reply_rate = round((a_replied / a_sent * 100), 1) if a_sent > 0 else 0
    b_reply_rate = round((b_replied / b_sent * 100), 1) if b_sent > 0 else 0

    # Chi-Quadrat-Test fuer Oeffnungsrate
    winner = None
    p_value = None
    significant = False

    if total_sent >= min_sends and a_sent > 0 and b_sent > 0:
        # Chi-Quadrat fuer 2x2 Tabelle (opened/not-opened x variant-A/variant-B)
        a_not = a_sent - a_opened
        b_not = b_sent - b_opened
        table = [[a_opened, a_not], [b_opened, b_not]]
        chi2 = _chi_squared_2x2(table)
        # p-Wert aus Chi-Quadrat (1 df)
        p_value = _chi2_to_p(chi2)
        significant = p_value < 0.05

        if significant:
            if a_open_rate > b_open_rate:
                winner = "A"
            elif b_open_rate > a_open_rate:
                winner = "B"

            # Winner in DB speichern
            if winner:
                db.update_ab_test(test_id, user_id, winner=winner, status="completed")

    return {
        "test_id": test_id,
        "name": test.get("name", ""),
        "status": test.get("status", "running"),
        "variant_a": {
            "subject": test.get("variant_a_subject", ""),
            "sent": a_sent,
            "opened": a_opened,
            "replied": a_replied,
            "open_rate": a_open_rate,
            "reply_rate": a_reply_rate,
        },
        "variant_b": {
            "subject": test.get("variant_b_subject", ""),
            "sent": b_sent,
            "opened": b_opened,
            "replied": b_replied,
            "open_rate": b_open_rate,
            "reply_rate": b_reply_rate,
        },
        "total_sent": total_sent,
        "min_sends_reached": total_sent >= min_sends,
        "chi_squared": round(chi2, 4) if p_value is not None else None,
        "p_value": round(p_value, 4) if p_value is not None else None,
        "significant": significant,
        "winner": winner,
    }


def _chi_squared_2x2(table: List[List[int]]) -> float:
    """Berechnet Chi-Quadrat fuer eine 2x2-Tabelle."""
    a, b = table[0]
    c, d = table[1]
    n = a + b + c + d
    if n == 0:
        return 0.0
    expected = [
        [(a + b) * (a + c) / n, (a + b) * (b + d) / n],
        [(c + d) * (a + c) / n, (c + d) * (b + d) / n],
    ]
    chi2 = 0.0
    for i in range(2):
        for j in range(2):
            if expected[i][j] > 0:
                chi2 += (table[i][j] - expected[i][j]) ** 2 / expected[i][j]
    return chi2


def _chi2_to_p(chi2: float, df: int = 1) -> float:
    """Approximiert den p-Wert aus Chi-Quadrat (1 df) via Normalapproximation."""
    if chi2 <= 0:
        return 1.0
    # Wilson-Hilfinger Approximation
    z = math.sqrt(chi2)
    # Approximation der Standard-Normal-CDF
    p = 0.5 * math.erfc(z / math.sqrt(2))
    return max(p, 0.0001)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  WOCHENREPORT-DATEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_weekly_report(user_id: int) -> Dict[str, Any]:
    """Sammelt alle Daten fuer den Wochenreport."""
    conn = db.get_db()
    now = datetime.utcnow()
    week_start = (now - timedelta(days=7)).isoformat()

    # Wochendaten
    new_leads = conn.execute(
        "SELECT COUNT(*) AS c FROM leads WHERE user_id = ? AND created_at >= ?",
        (user_id, week_start),
    ).fetchone()["c"]

    emails_sent = conn.execute(
        "SELECT COUNT(*) AS c FROM emails WHERE user_id = ? AND status = 'sent' AND sent_at >= ?",
        (user_id, week_start),
    ).fetchone()["c"]

    emails_opened = conn.execute(
        "SELECT COUNT(*) AS c FROM emails WHERE user_id = ? AND opened = 1 AND opened_at >= ?",
        (user_id, week_start),
    ).fetchone()["c"]

    replies = conn.execute(
        "SELECT COUNT(*) AS c FROM emails WHERE user_id = ? AND replied = 1 AND replied_at >= ?",
        (user_id, week_start),
    ).fetchone()["c"]

    new_conversions = conn.execute(
        "SELECT COUNT(*) AS c FROM leads WHERE user_id = ? AND converted = 1 AND converted_date >= ?",
        (user_id, week_start),
    ).fetchone()["c"]

    revenue_week = conn.execute(
        "SELECT COALESCE(SUM(revenue), 0) AS total FROM leads WHERE user_id = ? AND converted = 1 AND converted_date >= ?",
        (user_id, week_start),
    ).fetchone()["total"]

    # Top-Leads (diese Woche geantwortet)
    top_leads = conn.execute(
        "SELECT name, email, website FROM leads WHERE user_id = ? AND responded = 1 AND responded_date >= ? LIMIT 5",
        (user_id, week_start),
    ).fetchall()

    stats = get_user_stats(user_id)
    funnel = get_funnel(user_id)

    return {
        "period": f"{(now - timedelta(days=7)).strftime('%d.%m.%Y')} - {now.strftime('%d.%m.%Y')}",
        "new_leads": new_leads,
        "emails_sent": emails_sent,
        "emails_opened": emails_opened,
        "open_rate": round((emails_opened / emails_sent * 100), 1) if emails_sent > 0 else 0,
        "replies": replies,
        "new_conversions": new_conversions,
        "revenue_week": revenue_week,
        "top_leads": [dict(r) for r in top_leads],
        "overall_stats": stats,
        "funnel": funnel,
    }
