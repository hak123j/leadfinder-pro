/**
 * LeadFinder Pro v3 — Auth utilities & shared helpers
 *
 * Provides:
 *   checkAuth()           – verify session via /api/auth/me
 *   onPageLoad()          – bootstrap page (auth + theme)
 *   formatDate(iso)       – German date (dd.mm.yyyy HH:mm)
 *   formatNumber(n)       – German number (1.234)
 *   debounce(fn, ms)      – standard debounce
 *   copyToClipboard(txt)  – clipboard write with fallback
 *   downloadCSV(csv, fn)  – trigger CSV download
 *   generateVCard(lead)   – build vCard 3.0 string
 *   Dark-theme toggle     – persisted in localStorage
 *   Keyboard shortcuts    – Ctrl+K search, Escape close modal
 */

/* ------------------------------------------------------------------ */
/*  Auth                                                              */
/* ------------------------------------------------------------------ */

/**
 * Fetch /api/auth/me. If the response is 401 or the request fails,
 * redirect to /login. Returns the user object on success.
 */
async function checkAuth() {
  try {
    const res = await fetch('/api/auth/me', { credentials: 'same-origin' });
    if (res.status === 401 || res.status === 403) {
      window.location.href = '/login';
      return null;
    }
    if (!res.ok) {
      console.error('[auth] Unexpected status', res.status);
      window.location.href = '/login';
      return null;
    }
    const data = await res.json();
    return data;
  } catch (err) {
    console.error('[auth] Network error', err);
    window.location.href = '/login';
    return null;
  }
}

/* ------------------------------------------------------------------ */
/*  Page bootstrap                                                    */
/* ------------------------------------------------------------------ */

/**
 * Call on every protected page's DOMContentLoaded.
 * Verifies auth, applies saved theme, registers keyboard shortcuts.
 * Returns the authenticated user object (or null if redirected).
 */
async function onPageLoad() {
  applyStoredTheme();
  registerKeyboardShortcuts();
  const user = await checkAuth();
  return user;
}

/* ------------------------------------------------------------------ */
/*  Formatting                                                        */
/* ------------------------------------------------------------------ */

/**
 * Convert an ISO date string to German format: dd.mm.yyyy HH:mm
 * @param {string} isoString
 * @returns {string}
 */
function formatDate(isoString) {
  if (!isoString) return '—';
  const d = new Date(isoString);
  if (isNaN(d.getTime())) return '—';
  const dd   = String(d.getDate()).padStart(2, '0');
  const mm   = String(d.getMonth() + 1).padStart(2, '0');
  const yyyy = d.getFullYear();
  const HH   = String(d.getHours()).padStart(2, '0');
  const MM   = String(d.getMinutes()).padStart(2, '0');
  return `${dd}.${mm}.${yyyy} ${HH}:${MM}`;
}

/**
 * Format a number using German locale (1.234,56).
 * @param {number} n
 * @returns {string}
 */
function formatNumber(n) {
  if (n == null || isNaN(n)) return '—';
  return Number(n).toLocaleString('de-DE');
}

/* ------------------------------------------------------------------ */
/*  Utilities                                                         */
/* ------------------------------------------------------------------ */

/**
 * Classic debounce: delays `fn` execution until `ms` milliseconds
 * after the last invocation.
 * @param {Function} fn
 * @param {number} ms
 * @returns {Function}
 */
function debounce(fn, ms) {
  let timer = null;
  return function (...args) {
    clearTimeout(timer);
    timer = setTimeout(() => fn.apply(this, args), ms);
  };
}

/**
 * Copy text to clipboard using the Clipboard API.
 * Falls back to a hidden textarea for older browsers.
 * @param {string} text
 * @returns {Promise<boolean>}
 */
async function copyToClipboard(text) {
  if (navigator.clipboard && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (_) {
      // fall through to fallback
    }
  }
  // Fallback
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.style.cssText = 'position:fixed;left:-9999px;top:-9999px;opacity:0;';
  document.body.appendChild(ta);
  ta.select();
  let ok = false;
  try {
    ok = document.execCommand('copy');
  } catch (_) {
    // ignore
  }
  document.body.removeChild(ta);
  return ok;
}

/**
 * Trigger a CSV file download in the browser.
 * @param {string} csvString  – full CSV content including header row
 * @param {string} filename   – e.g. "leads-export.csv"
 */
function downloadCSV(csvString, filename) {
  const BOM = '\uFEFF'; // for proper Excel/German encoding
  const blob = new Blob([BOM + csvString], { type: 'text/csv;charset=utf-8;' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = filename || 'export.csv';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 5000);
}

/**
 * Generate a vCard 3.0 string for a single lead object.
 *
 * Expected lead shape:
 *   { firstName, lastName, email, phone, company, title, url, street,
 *     city, zip, country, notes }
 *
 * @param {object} lead
 * @returns {string}
 */
function generateVCard(lead) {
  const l = lead || {};
  const esc = (s) => (s || '').replace(/[;,\\]/g, (m) => '\\' + m);

  const lines = [
    'BEGIN:VCARD',
    'VERSION:3.0',
    `N:${esc(l.lastName)};${esc(l.firstName)};;;`,
    `FN:${[l.firstName, l.lastName].filter(Boolean).join(' ')}`,
  ];

  if (l.company)  lines.push(`ORG:${esc(l.company)}`);
  if (l.title)    lines.push(`TITLE:${esc(l.title)}`);
  if (l.email)    lines.push(`EMAIL;TYPE=INTERNET:${l.email}`);
  if (l.phone)    lines.push(`TEL;TYPE=WORK,VOICE:${l.phone}`);
  if (l.url)      lines.push(`URL:${l.url}`);

  if (l.street || l.city || l.zip || l.country) {
    lines.push(
      `ADR;TYPE=WORK:;;${esc(l.street)};${esc(l.city)};;${esc(l.zip)};${esc(l.country)}`
    );
  }

  if (l.notes) lines.push(`NOTE:${esc(l.notes)}`);

  lines.push(
    `REV:${new Date().toISOString()}`,
    'END:VCARD',
  );

  return lines.join('\r\n');
}

/* ------------------------------------------------------------------ */
/*  Dark theme toggle                                                 */
/* ------------------------------------------------------------------ */

const THEME_KEY = 'leadfinder_theme';

function applyStoredTheme() {
  const stored = localStorage.getItem(THEME_KEY);
  // Default to dark; only switch if explicitly set to 'light'
  if (stored === 'light') {
    document.documentElement.classList.remove('dark');
    document.documentElement.classList.add('light');
  } else {
    document.documentElement.classList.remove('light');
    document.documentElement.classList.add('dark');
  }
}

/**
 * Toggle between dark and light theme.
 * Persists choice in localStorage.
 * @returns {'dark'|'light'} the new active theme
 */
function toggleTheme() {
  const isDark = document.documentElement.classList.contains('dark');
  const next = isDark ? 'light' : 'dark';
  localStorage.setItem(THEME_KEY, next);
  applyStoredTheme();
  return next;
}

/* ------------------------------------------------------------------ */
/*  Keyboard shortcuts                                                */
/* ------------------------------------------------------------------ */

function registerKeyboardShortcuts() {
  document.addEventListener('keydown', (e) => {
    // Ctrl+K  or  Cmd+K  — focus search input
    if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
      e.preventDefault();
      const searchInput =
        document.getElementById('global-search') ||
        document.querySelector('[data-role="search"]') ||
        document.querySelector('input[type="search"]');
      if (searchInput) {
        searchInput.focus();
        searchInput.select();
      }
    }

    // Escape — close topmost open modal
    if (e.key === 'Escape') {
      const modal =
        document.querySelector('.modal.open') ||
        document.querySelector('.modal[data-open="true"]') ||
        document.querySelector('[data-modal-open="true"]');
      if (modal) {
        modal.classList.remove('open');
        modal.removeAttribute('data-open');
        modal.removeAttribute('data-modal-open');
        modal.style.display = 'none';
        // dispatch a custom event so page-level code can react
        document.dispatchEvent(new CustomEvent('modal:closed', { detail: { modal } }));
      }
    }
  });
}

/* ------------------------------------------------------------------ */
/*  Exports (global)                                                  */
/* ------------------------------------------------------------------ */

window.checkAuth       = checkAuth;
window.onPageLoad      = onPageLoad;
window.formatDate      = formatDate;
window.formatNumber    = formatNumber;
window.debounce        = debounce;
window.copyToClipboard = copyToClipboard;
window.downloadCSV     = downloadCSV;
window.generateVCard   = generateVCard;
window.toggleTheme     = toggleTheme;
