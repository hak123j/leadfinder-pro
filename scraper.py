"""
scraper.py — Web-Scraping Engine fuer LeadFinder Pro v3

Google/DuckDuckGo-Suche, Kontakt-Extraktion, Impressum-Parser,
Technologie-Erkennung, Website-Score, Google-Bewertungen.
"""

import re
import json
import time
import random
import logging
from typing import Optional, Dict, List, Any, Set, Tuple
from urllib.parse import urljoin, urlparse, quote_plus, unquote

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    raise SystemExit("Fehlende Pakete: pip install requests beautifulsoup4 lxml")

log = logging.getLogger("leadfinder.scraper")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  KONSTANTEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/123.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 Safari/605.1.15",
]

SKIP_DOMAINS = [
    "google.", "youtube.", "facebook.", "twitter.", "instagram.",
    "linkedin.", "wikipedia.", "amazon.", "ebay.", "yelp.",
    "tripadvisor.", "pinterest.", "reddit.", "apple.com",
    "tiktok.com", "xing.", "kununu.",
]

CONTACT_PATHS = [
    "/impressum", "/kontakt", "/contact", "/imprint",
    "/ueber-uns", "/about", "/about-us",
]

EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', re.IGNORECASE
)

EMAIL_DOMAIN_BLACKLIST: Set[str] = {
    "example.com", "schema.org", "w3.org", "wixpress.com", "googleapis.com",
    "wordpress.org", "sentry.io", "gravatar.com", "cloudflare.com",
    "google.com", "facebook.com", "twitter.com", "instagram.com",
    "youtube.com", "github.com", "placeholder.com",
}

PHONE_PATTERNS = [
    re.compile(r'\+49[\s\-./]*\(?\d{2,5}\)?[\s\-./]*[\d\s\-./]{4,12}'),
    re.compile(r'\(0\d{2,5}\)[\s\-./]*[\d\s\-./]{4,12}'),
    re.compile(r'0\d{2,5}[\s\-./]+[\d\s\-./]{4,12}'),
    re.compile(r'01[5-7]\d[\s\-./]*[\d\s\-./]{6,10}'),
]

PLZ_PATTERN = re.compile(
    r'(\d{5})\s+([A-ZÄÖÜ][a-zäöüß]+(?:\s+[a-zäöüß]+)*)', re.UNICODE,
)

# Branchenbegriffe mit Synonymen (25+ deutsche Branchen)
BRANCH_SYNONYMS: Dict[str, List[str]] = {
    "pizzeria": ["ristorante", "trattoria", "italienisches restaurant", "pizza lieferservice"],
    "friseur": ["friseursalon", "hairstylist", "barbershop", "barber shop", "coiffeur"],
    "zahnarzt": ["zahnarztpraxis", "zahnklinik", "dental", "kieferorthopäde"],
    "anwalt": ["rechtsanwalt", "kanzlei", "rechtsanwälte", "anwaltskanzlei"],
    "steuerberater": ["steuerkanzlei", "steuerberatung", "buchhalter", "wirtschaftsprüfer"],
    "restaurant": ["gasthaus", "gasthof", "gaststätte", "speiselokal", "bistro"],
    "hotel": ["pension", "gasthof", "gasthaus", "unterkunft", "ferienhaus"],
    "arzt": ["arztpraxis", "hausarzt", "allgemeinarzt", "facharzt", "praxis"],
    "physiotherapie": ["physiotherapeut", "krankengymnastik", "physiotherapiepraxis"],
    "autowerkstatt": ["kfz werkstatt", "autohaus", "kfz service", "autoreparatur"],
    "bäckerei": ["backhaus", "brotladen", "konditorei", "bäcker"],
    "metzger": ["metzgerei", "fleischerei", "schlachterei"],
    "apotheke": ["pharmazie"],
    "optiker": ["augenoptiker", "brillenladen", "optikgeschäft"],
    "fotograf": ["fotostudio", "fotografin", "fotografie"],
    "dachdecker": ["dachdeckerei", "bedachungen", "dacharbeiten"],
    "elektriker": ["elektrotechnik", "elektroinstallation", "elektromeister"],
    "maler": ["malerbetrieb", "malermeister", "anstreicher"],
    "schreiner": ["schreinerei", "tischlerei", "tischler", "möbelbau"],
    "sanitär": ["sanitärtechnik", "klempner", "heizung sanitär", "installateur"],
    "immobilien": ["immobilienmakler", "makler", "hausverwaltung"],
    "versicherung": ["versicherungsmakler", "versicherungsagentur"],
    "fahrschule": ["führerschein", "fahrausbildung"],
    "tierarzt": ["tierarztpraxis", "tierklinik", "veterinär"],
    "kosmetik": ["kosmetikstudio", "beauty salon", "nagelstudio", "kosmetikerin"],
    "fitnessstudio": ["fitness", "gym", "sportstudio", "sportverein"],
    "yoga": ["yogastudio", "yoga kurs", "yoga schule"],
    "nachhilfe": ["nachhilfelehrer", "lernhilfe", "tutoring"],
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HTTP-HILFSFUNKTIONEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _random_ua() -> str:
    """Gibt einen zufaelligen User-Agent zurueck."""
    return random.choice(USER_AGENTS)


def _headers() -> Dict[str, str]:
    """Standard-Request-Header."""
    return {
        "User-Agent": _random_ua(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
    }


def fetch(url: str, timeout: int = 12) -> Optional[requests.Response]:
    """Fetched eine URL. Gibt Response oder None zurueck."""
    try:
        resp = requests.get(url, headers=_headers(), timeout=timeout,
                            allow_redirects=True, verify=True)
        resp.raise_for_status()
        return resp
    except requests.exceptions.RequestException as exc:
        log.debug("Fetch fehlgeschlagen fuer %s: %s", url, exc)
        return None


def fetch_text(url: str, timeout: int = 12) -> Optional[str]:
    """Fetched eine URL und gibt den HTML-Text zurueck."""
    resp = fetch(url, timeout)
    return resp.text if resp else None


def _is_valid_search_result(url: str) -> bool:
    """Prueft ob eine URL ein brauchbares Suchergebnis ist."""
    return not any(d in url.lower() for d in SKIP_DOMAINS)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SUCHMASCHINEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def google_search(query: str, count: int = 15) -> List[str]:
    """Sucht via Google und gibt URLs zurueck. Fallback auf DuckDuckGo."""
    urls: List[str] = []
    html = fetch_text(
        f"https://www.google.com/search?q={quote_plus(query)}&hl=de&num={count + 5}"
    )
    if not html:
        return duckduckgo_search(query, count)

    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/url?q="):
            u = href.split("/url?q=")[1].split("&")[0]
            if u.startswith("http") and _is_valid_search_result(u):
                urls.append(u)
        elif href.startswith("http") and _is_valid_search_result(href):
            urls.append(href)
        if len(urls) >= count:
            break

    return urls[:count] if len(urls) >= 3 else duckduckgo_search(query, count)


def duckduckgo_search(query: str, count: int = 15) -> List[str]:
    """Sucht via DuckDuckGo HTML-Version."""
    urls: List[str] = []
    html = fetch_text(f"https://html.duckduckgo.com/html/?q={quote_plus(query)}")
    if not html:
        return urls
    for a in BeautifulSoup(html, "lxml").find_all("a", class_="result__a", href=True):
        href = a["href"]
        if "uddg=" in href:
            u = unquote(href.split("uddg=")[1].split("&")[0])
        else:
            u = href
        if u.startswith("http") and _is_valid_search_result(u):
            urls.append(u)
        if len(urls) >= count:
            break
    return urls[:count]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  KONTAKTDATEN-EXTRAKTION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def extract_emails(html: str) -> List[str]:
    """Extrahiert E-Mail-Adressen aus HTML (mit De-Obfuscation)."""
    text = (
        html.replace("&#64;", "@")
        .replace("&#46;", ".")
        .replace("[at]", "@")
        .replace("(at)", "@")
        .replace("[dot]", ".")
        .replace("(dot)", ".")
        .replace(" AT ", "@")
        .replace(" DOT ", ".")
    )
    results: List[str] = []
    seen: Set[str] = set()
    for email in EMAIL_PATTERN.findall(text):
        email = email.lower().strip().rstrip(".")
        if email in seen:
            continue
        seen.add(email)
        domain = email.split("@")[1] if "@" in email else ""
        if domain in EMAIL_DOMAIN_BLACKLIST:
            continue
        if any(email.endswith(ext) for ext in [".png", ".jpg", ".svg", ".css", ".js", ".gif", ".webp"]):
            continue
        if any(x in email for x in ["noreply", "no-reply", "mailer-daemon", "postmaster", "webmaster"]):
            continue
        results.append(email)
    return results


def extract_phones(html: str) -> List[str]:
    """Extrahiert Telefonnummern aus HTML."""
    results: List[str] = []
    seen: Set[str] = set()
    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    for pattern in PHONE_PATTERNS:
        for match in pattern.findall(text):
            cleaned = re.sub(r"[\s\-./()]+", " ", match).strip()
            digits = re.sub(r"[^\d+]", "", cleaned)
            if digits in seen or len(digits) < 8:
                continue
            seen.add(digits)
            results.append(cleaned)
    return results


def extract_name(html: str, url: str) -> str:
    """Extrahiert den Firmennamen aus HTML."""
    soup = BeautifulSoup(html, "lxml")
    # 1. og:title / og:site_name
    for meta_name in ["og:site_name", "og:title"]:
        tag = soup.find("meta", property=meta_name)
        if tag and tag.get("content"):
            name = tag["content"].strip()
            if 3 <= len(name) <= 80:
                return name
    # 2. title-Tag
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
        # Trenne am ersten Separator
        for sep in [" - ", " | ", " – ", " :: ", " › "]:
            if sep in title:
                title = title.split(sep)[0].strip()
                break
        if 3 <= len(title) <= 80:
            return title
    # 3. h1
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        name = h1.get_text(strip=True)[:80]
        if len(name) >= 3:
            return name
    # 4. Domain als Fallback
    return urlparse(url).netloc.replace("www.", "")


def extract_address(html: str) -> Optional[str]:
    """Extrahiert eine deutsche Adresse (PLZ + Ort)."""
    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    match = PLZ_PATTERN.search(text)
    if match:
        plz, ort = match.groups()
        # Suche Strasse vor PLZ
        before = text[:match.start()]
        street_pattern = re.compile(
            r'([A-ZÄÖÜ][a-zäöüß]+(?:str\.|straße|stra[sß]e|weg|gasse|platz|allee|ring|damm|ufer)\s*\d+\s*[a-zA-Z]?)',
            re.UNICODE,
        )
        street_match = street_pattern.search(before[-200:])
        if street_match:
            return f"{street_match.group(1)}, {plz} {ort}"
        return f"{plz} {ort}"
    return None


def extract_opening_hours(html: str) -> Optional[str]:
    """Extrahiert Oeffnungszeiten (Schema.org + Text-Patterns)."""
    soup = BeautifulSoup(html, "lxml")
    # Schema.org openingHours
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list):
                data = data[0] if data else {}
            hours = data.get("openingHours") or data.get("openingHoursSpecification")
            if hours:
                if isinstance(hours, list):
                    return "; ".join(str(h) if isinstance(h, str) else
                                     f"{h.get('dayOfWeek', '')}: {h.get('opens', '')}-{h.get('closes', '')}"
                                     for h in hours)
                return str(hours)
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue
    # Text-Pattern: "Mo-Fr 9-18"
    text = soup.get_text(" ", strip=True)
    hours_pattern = re.compile(
        r'(?:Öffnungszeiten|Oeffnungszeiten|Geschäftszeiten|Sprechzeiten)[:\s]*'
        r'((?:Mo|Di|Mi|Do|Fr|Sa|So)[\s\-–bis]+(?:Mo|Di|Mi|Do|Fr|Sa|So)?[:\s]*\d{1,2}[:.]\d{2}\s*[-–bis]+\s*\d{1,2}[:.]\d{2}.*?)(?:\n|$)',
        re.IGNORECASE,
    )
    match = hours_pattern.search(text)
    if match:
        return match.group(1).strip()[:200]
    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  IMPRESSUM-PARSER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def parse_impressum(html: str) -> Dict[str, Optional[str]]:
    """Parst ein Impressum und extrahiert Inhaber, Rechtsform, HRB, USt-IdNr."""
    result: Dict[str, Optional[str]] = {
        "owner_name": None,
        "owner_title": None,
        "company_type": None,
        "hrb_number": None,
        "ust_id": None,
    }
    text = BeautifulSoup(html, "lxml").get_text("\n", strip=True)

    # Geschaeftsfuehrer / Inhaber
    owner_patterns = [
        re.compile(r'(?:Geschäftsführer(?:in)?|Geschaeftsfuehrer(?:in)?)[:\s]+([A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+)', re.UNICODE),
        re.compile(r'(?:Inhaber(?:in)?)[:\s]+([A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+)', re.UNICODE),
        re.compile(r'(?:Vertreten durch)[:\s]+([A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+)', re.UNICODE),
        re.compile(r'(?:Verantwortlich(?:\s+(?:gem\.|i\.S\.d\.).*?)?)[:\s]+([A-ZÄÖÜ][a-zäöüß]+\s+[A-ZÄÖÜ][a-zäöüß]+)', re.UNICODE),
    ]
    for pat in owner_patterns:
        match = pat.search(text)
        if match:
            result["owner_name"] = match.group(1).strip()
            break

    # Titel (Dr., Prof., etc.)
    if result["owner_name"]:
        title_pat = re.compile(
            r'(?:Dr\.\s*(?:med\.|jur\.|rer\.\s*nat\.)?|Prof\.\s*(?:Dr\.)?\s*|Dipl\.-\w+\s+)' +
            re.escape(result["owner_name"]),
            re.UNICODE,
        )
        match = title_pat.search(text)
        if match:
            result["owner_title"] = match.group(0).split(result["owner_name"])[0].strip()

    # Rechtsform
    for rform in ["GmbH & Co. KG", "GmbH & Co. KGaA", "GmbH", "UG (haftungsbeschränkt)",
                   "UG", "AG", "eK", "e.K.", "GbR", "OHG", "KG", "e.V.", "gGmbH"]:
        if rform in text:
            result["company_type"] = rform
            break

    # Handelsregister
    hrb_pat = re.compile(r'(HR[AB]\s*\d+(?:\s*[A-Z])?)', re.IGNORECASE)
    match = hrb_pat.search(text)
    if match:
        result["hrb_number"] = match.group(1).strip()

    # USt-IdNr
    ust_pat = re.compile(r'(DE\s*\d{9})')
    match = ust_pat.search(text)
    if match:
        result["ust_id"] = match.group(1).replace(" ", "")

    return result


def extract_owner(html: str) -> Optional[str]:
    """Shortcut: Extrahiert den Inhaber-Namen."""
    data = parse_impressum(html)
    return data.get("owner_name")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TECHNOLOGIE-ERKENNUNG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def detect_technology(html: str, headers: Dict[str, str] = None) -> Dict[str, Any]:
    """Erkennt CMS, Frameworks und Analytics."""
    headers = headers or {}
    result: Dict[str, Any] = {
        "cms": None,
        "frameworks": [],
        "analytics": [],
        "server": headers.get("Server", ""),
        "details": {},
    }
    html_lower = html.lower()
    powered = headers.get("X-Powered-By", "").lower()

    # CMS-Erkennung
    cms_checks = [
        ("WordPress", ["/wp-content/", "/wp-includes/", "wp-json", 'name="generator" content="WordPress']),
        ("Wix", ["wix.com", "_wixCIDX", "X-Wix-"]),
        ("Squarespace", ["squarespace.com", "sqsp.net", "static.squarespace"]),
        ("Jimdo", ["jimdo.com", "jimdosite.com"]),
        ("Shopify", ["shopify.com", "cdn.shopify", "myshopify"]),
        ("Typo3", ["typo3", "/typo3conf/", "/typo3temp/"]),
        ("Joomla", ["joomla", "/media/jui/", "/components/com_"]),
        ("Drupal", ["drupal", "/sites/default/", "/modules/"]),
        ("Webflow", ["webflow.com", "wf-cdn"]),
    ]
    for cms_name, indicators in cms_checks:
        if any(ind.lower() in html_lower or ind.lower() in powered for ind in indicators):
            result["cms"] = cms_name
            break

    # Meta-Generator
    soup = BeautifulSoup(html, "lxml")
    gen_tag = soup.find("meta", attrs={"name": "generator"})
    if gen_tag and gen_tag.get("content"):
        gen = gen_tag["content"]
        result["details"]["generator"] = gen
        if not result["cms"]:
            for cms_name in ["WordPress", "Joomla", "Drupal", "Typo3", "Wix"]:
                if cms_name.lower() in gen.lower():
                    result["cms"] = cms_name
                    break

    if not result["cms"]:
        result["cms"] = "Custom"

    # Frameworks
    framework_checks = [
        ("Bootstrap", ["bootstrap.min.css", "bootstrap.min.js", "bootstrap.css"]),
        ("Tailwind CSS", ["tailwindcss", "tailwind.min.css"]),
        ("jQuery", ["jquery.min.js", "jquery.js", "jquery-"]),
        ("React", ["react.production.min.js", "react-dom", "__NEXT_DATA__"]),
        ("Vue.js", ["vue.min.js", "vue.js", "vue-router"]),
        ("Angular", ["angular.min.js", "ng-version", "ng-app"]),
    ]
    for fw_name, indicators in framework_checks:
        if any(ind.lower() in html_lower for ind in indicators):
            result["frameworks"].append(fw_name)

    # Analytics
    analytics_checks = [
        ("Google Analytics", ["google-analytics.com", "gtag(", "ga.js", "analytics.js", "googletagmanager"]),
        ("Matomo", ["matomo", "piwik"]),
        ("Hotjar", ["hotjar.com", "hjSiteSettings"]),
        ("Facebook Pixel", ["fbevents.js", "facebook.com/tr"]),
    ]
    for an_name, indicators in analytics_checks:
        if any(ind.lower() in html_lower for ind in indicators):
            result["analytics"].append(an_name)

    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  WEBSITE-SCORE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def score_website(url: str, html: str, response: requests.Response) -> Dict[str, Any]:
    """Bewertet eine Webseite (0-10) mit Detail-Issues und Positives."""
    score = 0
    issues: List[str] = []
    positives: List[str] = []
    details: Dict[str, Any] = {}

    # 1. SSL
    if url.startswith("https://"):
        score += 1
        positives.append("SSL-Verschluesselung aktiv")
        details["ssl"] = True
    else:
        issues.append("Keine SSL-Verschluesselung (kein HTTPS)")
        details["ssl"] = False

    # 2. Mobile-Friendly (Viewport)
    has_viewport = 'name="viewport"' in html.lower() or "name='viewport'" in html.lower()
    if has_viewport:
        score += 1
        positives.append("Mobile-optimiert (Viewport vorhanden)")
        details["mobile"] = True
    else:
        issues.append("Nicht mobile-optimiert (kein Viewport-Tag)")
        details["mobile"] = False

    # 3. Ladezeit
    load_time = response.elapsed.total_seconds()
    details["load_time"] = round(load_time, 2)
    if load_time < 2:
        score += 1
        positives.append(f"Schnelle Ladezeit ({load_time:.1f}s)")
    elif load_time < 5:
        score += 0.5
        issues.append(f"Mittlere Ladezeit ({load_time:.1f}s)")
    else:
        issues.append(f"Langsame Ladezeit ({load_time:.1f}s)")

    # 4. Cookie-Banner
    html_lower = html.lower()
    cookie_indicators = ["cookie", "datenschutz", "consent", "gdpr", "dsgvo"]
    has_cookie = any(ind in html_lower for ind in cookie_indicators)
    details["cookie_banner"] = has_cookie
    if has_cookie:
        score += 1
        positives.append("Cookie-Banner / Datenschutz-Hinweis vorhanden")
    else:
        issues.append("Kein Cookie-Banner erkannt")

    # 5. Design (modernes CSS)
    modern_css = any(x in html_lower for x in ["flexbox", "display:flex", "display: flex",
                                                 "display:grid", "display: grid", "tailwind",
                                                 "bootstrap", "modern", "font-family"])
    if modern_css:
        score += 1
        positives.append("Modernes CSS/Design erkannt")
    else:
        issues.append("Veraltetes Design moeglich")

    # 6. Social Media Links
    social_platforms = ["facebook.com", "instagram.com", "twitter.com", "linkedin.com",
                        "youtube.com", "tiktok.com", "xing.com"]
    social_count = sum(1 for s in social_platforms if s in html_lower)
    details["social_count"] = social_count
    if social_count >= 2:
        score += 1
        positives.append(f"Social-Media-Praesenz ({social_count} Plattformen)")
    elif social_count == 0:
        issues.append("Keine Social-Media-Links gefunden")

    # 7. Kontaktinformationen
    has_contact = bool(extract_emails(html) or extract_phones(html))
    if has_contact:
        score += 1
        positives.append("Kontaktdaten auf der Seite vorhanden")
    else:
        issues.append("Keine Kontaktdaten auf der Hauptseite")

    # 8. Schema.org / Structured Data
    has_schema = "application/ld+json" in html_lower or "itemtype" in html_lower
    details["schema_org"] = has_schema
    if has_schema:
        score += 0.5
        positives.append("Strukturierte Daten (Schema.org) vorhanden")
    else:
        issues.append("Keine strukturierten Daten (Schema.org) gefunden")

    # 9. Alt-Texte
    soup = BeautifulSoup(html, "lxml")
    images = soup.find_all("img")
    imgs_with_alt = sum(1 for img in images if img.get("alt", "").strip())
    if images:
        alt_ratio = imgs_with_alt / len(images)
        details["alt_text_ratio"] = round(alt_ratio, 2)
        if alt_ratio >= 0.7:
            score += 0.5
            positives.append(f"Gute Alt-Text-Abdeckung ({int(alt_ratio*100)}%)")
        else:
            issues.append(f"Alt-Texte fehlen bei {int((1-alt_ratio)*100)}% der Bilder")
    else:
        details["alt_text_ratio"] = 1.0

    # 10. Indexierbarkeit
    robots = soup.find("meta", attrs={"name": "robots"})
    noindex = robots and "noindex" in (robots.get("content", "").lower()) if robots else False
    details["indexable"] = not noindex
    if noindex:
        issues.append("Seite ist auf 'noindex' gesetzt")
    else:
        score += 0.5
        positives.append("Seite ist indexierbar")

    final_score = min(10, round(score))
    return {
        "score": final_score,
        "issues": issues,
        "positives": positives,
        "details": details,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LETZTE AKTUALISIERUNG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def detect_last_update(html: str, response: requests.Response = None) -> Optional[str]:
    """Erkennt wann die Seite zuletzt aktualisiert wurde."""
    soup = BeautifulSoup(html, "lxml")
    # 1. Meta-Tags
    for meta_name in ["last-modified", "date", "article:modified_time",
                       "article:published_time", "og:updated_time"]:
        tag = soup.find("meta", property=meta_name) or soup.find("meta", attrs={"name": meta_name})
        if tag and tag.get("content"):
            return tag["content"][:10]
    # 2. Last-Modified Header
    if response and "Last-Modified" in response.headers:
        return response.headers["Last-Modified"][:16]
    # 3. Copyright-Jahr
    text = soup.get_text(" ", strip=True)
    copyright_pat = re.compile(r'(?:©|Copyright|&copy;)\s*(\d{4})')
    match = copyright_pat.search(text)
    if match:
        return match.group(1)
    # 4. Blog-Datum
    date_pat = re.compile(r'(20[12]\d[-/]\d{1,2}[-/]\d{1,2})')
    match = date_pat.search(text)
    if match:
        return match.group(1)
    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  GOOGLE-BEWERTUNGEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_google_rating(business_name: str, city: str = "") -> Optional[Dict[str, Any]]:
    """Scrapt Google Knowledge Panel fuer Bewertung + Anzahl."""
    try:
        query = f"{business_name} {city}".strip()
        html = fetch_text(
            f"https://www.google.com/search?q={quote_plus(query)}&hl=de"
        )
        if not html:
            return None
        # Schema.org aggregateRating
        rating = None
        reviews = 0
        for match in re.finditer(r'"aggregateRating"\s*:\s*\{[^}]+\}', html):
            try:
                snippet = "{" + match.group(0) + "}"
                snippet = snippet.replace("'", '"')
                data = json.loads("{" + match.group(0) + "}")
                ar = data.get("aggregateRating", {})
                if ar.get("ratingValue"):
                    rating = float(ar["ratingValue"])
                if ar.get("reviewCount"):
                    reviews = int(ar["reviewCount"])
            except (json.JSONDecodeError, ValueError, TypeError):
                continue
        # Fallback: Text-Pattern
        if not rating:
            pat = re.compile(r'(\d[,\.]\d)\s*(?:von\s*5\s*)?(?:Sternen?)?\s*[–·]\s*(\d+)\s*(?:Rezension|Bewertung|Google)', re.IGNORECASE)
            match = pat.search(html)
            if match:
                rating = float(match.group(1).replace(",", "."))
                reviews = int(match.group(2))
        if rating:
            return {"rating": round(rating, 1), "reviews": reviews}
    except (requests.exceptions.RequestException, ValueError) as exc:
        log.debug("Google-Rating-Fehler fuer %s: %s", business_name, exc)
    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  WHATSAPP + GOOGLE MAPS LINKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_whatsapp_link(phone: str) -> Optional[str]:
    """Generiert einen WhatsApp-Link aus einer Telefonnummer."""
    if not phone:
        return None
    clean = re.sub(r"[^\d+]", "", phone)
    if clean.startswith("0"):
        clean = "+49" + clean[1:]
    elif not clean.startswith("+"):
        clean = "+49" + clean
    return f"https://wa.me/{clean.replace('+', '')}"


def generate_maps_link(name: str, city: str = "") -> str:
    """Generiert einen Google-Maps-Such-Link."""
    query = f"{name} {city}".strip()
    return f"https://www.google.com/maps/search/{quote_plus(query)}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BRANCHENCODE / SYNONYME
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_related_terms(profession: str) -> List[str]:
    """Gibt verwandte Suchbegriffe zurueck."""
    terms = [profession]
    key = profession.lower().strip()
    if key in BRANCH_SYNONYMS:
        terms.extend(BRANCH_SYNONYMS[key])
    else:
        for k, synonyms in BRANCH_SYNONYMS.items():
            if k in key or key in k:
                terms.extend(synonyms)
                break
    return terms


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  KONKURRENZ-ANALYSE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def competitor_analysis(profession: str, city: str) -> Dict[str, Any]:
    """Schaetzt die Konkurrenz-Dichte ab."""
    query = f"{profession} {city}".strip()
    html = fetch_text(f"https://www.google.com/search?q={quote_plus(query)}&hl=de")
    total = 0
    if html:
        match = re.search(r'(?:Ungefähr|About)\s+([\d.,]+)\s+(?:Ergebnisse|results)', html)
        if match:
            total = int(match.group(1).replace(".", "").replace(",", ""))
    level = "niedrig" if total < 50000 else ("mittel" if total < 500000 else "hoch")
    return {"total_estimated": total, "competition_level": level, "query": query}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HAUPT: SEITE SCRAPEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def scrape_lead(url: str, city: str = "") -> Optional[Dict[str, Any]]:
    """Scrapt eine Webseite und extrahiert alle Kontaktdaten, Score usw."""
    if not url.startswith("http"):
        url = "https://" + url
    url = url.rstrip("/")
    domain = urlparse(url).netloc.replace("www.", "")

    result: Dict[str, Any] = {
        "name": domain,
        "email": None,
        "phone": None,
        "address": None,
        "website": url,
        "all_emails": [],
        "all_phones": [],
        "score": None,
        "rating": None,
        "whatsapp_url": None,
        "google_maps_url": None,
        "owner_name": None,
        "owner_title": None,
        "company_type": None,
        "tech_stack": [],
        "tech_details": {},
        "opening_hours": None,
        "site_last_updated": None,
    }

    all_emails: Set[str] = set()
    all_phones: Set[str] = set()

    response = fetch(url)
    if not response:
        return None
    html = response.text

    result["name"] = extract_name(html, url)
    for e in extract_emails(html):
        all_emails.add(e)
    for p in extract_phones(html):
        all_phones.add(p)
    result["address"] = extract_address(html)

    # Website-Score
    result["score"] = score_website(url, html, response)

    # Technologie
    tech = detect_technology(html, dict(response.headers))
    result["tech_stack"] = [tech["cms"]] + tech["frameworks"] + tech["analytics"]
    result["tech_stack"] = [t for t in result["tech_stack"] if t]
    result["tech_details"] = tech

    # Oeffnungszeiten
    result["opening_hours"] = extract_opening_hours(html)

    # Letzte Aktualisierung
    result["site_last_updated"] = detect_last_update(html, response)

    # Kontakt-Unterseiten scrapen
    soup = BeautifulSoup(html, "lxml")
    subpages: Set[str] = set()
    for a in soup.find_all("a", href=True):
        href_lower = a["href"].lower()
        text_lower = a.get_text(strip=True).lower()
        if any(k in href_lower or k in text_lower
               for k in ["impressum", "kontakt", "contact", "imprint", "ueber-uns", "about"]):
            full_url = urljoin(url, a["href"])
            if domain in full_url:
                subpages.add(full_url)
    for path in CONTACT_PATHS:
        subpages.add(url + path)

    for sub_url in list(subpages)[:4]:
        time.sleep(random.uniform(0.8, 2))
        sub_html = fetch_text(sub_url)
        if sub_html:
            for e in extract_emails(sub_html):
                all_emails.add(e)
            for p in extract_phones(sub_html):
                all_phones.add(p)
            if not result["address"]:
                result["address"] = extract_address(sub_html)
            # Impressum-Daten
            if not result["owner_name"]:
                imp = parse_impressum(sub_html)
                if imp["owner_name"]:
                    result["owner_name"] = imp["owner_name"]
                    result["owner_title"] = imp["owner_title"]
                    result["company_type"] = imp["company_type"]

    result["all_emails"] = sorted(list(all_emails))
    result["all_phones"] = sorted(list(all_phones))
    result["email"] = result["all_emails"][0] if result["all_emails"] else None
    result["phone"] = result["all_phones"][0] if result["all_phones"] else None

    # WhatsApp + Maps
    result["whatsapp_url"] = generate_whatsapp_link(result["phone"])
    result["google_maps_url"] = generate_maps_link(result["name"], city)

    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HAUPT: LEADS FINDEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _normalize_domain(url: str) -> str:
    """Normalisiert eine Domain fuer Duplikat-Erkennung."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower().replace("www.", "")
    path = parsed.path.lower().rstrip("/")
    for suffix in ["/de", "/en", "/index.html", "/index.php", "/home"]:
        if path.endswith(suffix):
            path = path[:-len(suffix)]
    return domain + path.rstrip("/")


def find_leads(profession: str, city: str, count: int = 10,
               use_synonyms: bool = True, blacklist_set: Set[str] = None) -> List[Dict[str, Any]]:
    """Sucht nach Leads fuer eine Branche und Stadt."""
    search_terms = get_related_terms(profession) if use_synonyms else [profession]
    all_urls: List[str] = []

    for term in search_terms[:3]:
        query = f"{term} {city}" if city else term
        urls = google_search(f"{query} Kontakt", count=count + 5)
        all_urls.extend(urls)
        if len(all_urls) >= count * 3:
            break
        if len(search_terms) > 1:
            time.sleep(random.uniform(1, 2))

    # Duplikate entfernen
    seen_raw: Set[str] = set()
    unique_urls: List[str] = []
    for u in all_urls:
        norm = _normalize_domain(u)
        if norm not in seen_raw:
            seen_raw.add(norm)
            unique_urls.append(u)

    leads: List[Dict[str, Any]] = []
    blacklist_set = blacklist_set or set()

    for url in unique_urls:
        if len(leads) >= count:
            break
        result = scrape_lead(url, city)
        if result and (result["email"] or result["phone"]):
            if result.get("email") and result["email"] in blacklist_set:
                continue
            seen_domains = {_normalize_domain(l["website"]) for l in leads}
            if _normalize_domain(result["website"]) not in seen_domains:
                result["profession"] = profession
                result["category"] = profession
                result["city"] = city
                leads.append(result)
        time.sleep(random.uniform(1, 2.5))

    # Google-Bewertungen
    for lead in leads:
        try:
            rating = get_google_rating(lead["name"], city)
            if rating:
                lead["rating"] = rating
        except (requests.exceptions.RequestException, ValueError) as exc:
            log.debug("Rating-Fehler: %s", exc)
        time.sleep(random.uniform(0.5, 1.5))

    return leads
