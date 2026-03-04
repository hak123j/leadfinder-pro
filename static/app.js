/* ============================================================
   LeadFinder Pro v3 — Dashboard JavaScript
   static/app.js
   Design tokens: --accent:#7C6FFF  --green:#34D399  --red:#F87171
   ============================================================ */

"use strict";

/* ----------------------------------------------------------
   1. GLOBAL STATE
   ---------------------------------------------------------- */
let currentUser = null;
let currentLeads = [];
let currentTab = "search";
let _toastTimer = null;
let _searchTimer = null;
let _searchStart = 0;

/* ----------------------------------------------------------
   2. CORE UTILITIES
   ---------------------------------------------------------- */

/**
 * Fetch wrapper.  Adds credentials, reads JSON, handles 401.
 * @param {string} url
 * @param {RequestInit} [opts]
 * @returns {Promise<any>}
 */
async function api(url, opts = {}) {
  opts.credentials = "include";
  if (opts.body && typeof opts.body === "object" && !(opts.body instanceof FormData)) {
    opts.headers = Object.assign({ "Content-Type": "application/json" }, opts.headers || {});
    opts.body = JSON.stringify(opts.body);
  }
  const res = await fetch(url, opts);
  if (res.status === 401) {
    window.location.href = "/login";
    throw new Error("Unauthorized");
  }
  if (!res.ok) {
    let msg = "Request failed";
    try {
      const j = await res.json();
      msg = j.detail || j.error || msg;
    } catch (_) { /* ignore */ }
    throw new Error(msg);
  }
  if (res.status === 204) return null;
  return res.json();
}

/**
 * HTML-escape a string.
 * @param {string} str
 * @returns {string}
 */
function esc(str) {
  if (str == null) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

/**
 * Sanitise a URL — allow only http, https, mailto.
 * @param {string} url
 * @returns {string} safe href or empty string
 */
function safeUrl(url) {
  if (!url) return "";
  const s = String(url).trim();
  if (/^https?:\/\//i.test(s)) return s;
  if (/^mailto:/i.test(s)) return s;
  return "";
}

/**
 * Format a number with thousand separators.
 */
function fmtNum(n) {
  if (n == null) return "0";
  return Number(n).toLocaleString("de-DE");
}

/**
 * Format a percentage value.
 */
function fmtPct(n) {
  if (n == null) return "0 %";
  return Number(n).toFixed(1) + " %";
}

/**
 * Simple date formatter (ISO -> dd.mm.yyyy HH:MM).
 */
function fmtDate(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  if (isNaN(d)) return "-";
  const pad = (v) => String(v).padStart(2, "0");
  return (
    pad(d.getDate()) + "." + pad(d.getMonth() + 1) + "." + d.getFullYear() +
    " " + pad(d.getHours()) + ":" + pad(d.getMinutes())
  );
}

/**
 * Debounce helper.
 */
function debounce(fn, ms) {
  let t;
  return function (...args) {
    clearTimeout(t);
    t = setTimeout(() => fn.apply(this, args), ms);
  };
}

/* ----------------------------------------------------------
   3. TOAST NOTIFICATIONS
   ---------------------------------------------------------- */

/**
 * Show a small toast message in the top-right corner.
 * @param {string} message
 * @param {"success"|"error"|"info"} [type]
 */
function showToast(message, type) {
  type = type || "info";
  let container = document.getElementById("toast-container");
  if (!container) {
    container = document.createElement("div");
    container.id = "toast-container";
    container.style.cssText =
      "position:fixed;top:16px;right:16px;z-index:9999;display:flex;flex-direction:column;gap:8px;pointer-events:none;";
    document.body.appendChild(container);
  }

  const colors = {
    success: "var(--green, #34D399)",
    error: "var(--red, #F87171)",
    info: "var(--accent, #7C6FFF)",
  };

  const toast = document.createElement("div");
  toast.style.cssText =
    "pointer-events:auto;padding:12px 20px;border-radius:8px;color:#fff;" +
    "font-size:14px;box-shadow:0 4px 14px rgba(0,0,0,.25);transform:translateX(120%);transition:transform .3s ease;" +
    "background:" + (colors[type] || colors.info) + ";max-width:360px;word-break:break-word;";
  toast.textContent = message;
  container.appendChild(toast);

  requestAnimationFrame(() => {
    toast.style.transform = "translateX(0)";
  });

  setTimeout(() => {
    toast.style.transform = "translateX(120%)";
    setTimeout(() => toast.remove(), 350);
  }, 3000);
}

/* ----------------------------------------------------------
   4. MODAL SYSTEM
   ---------------------------------------------------------- */

function openModal(title, html) {
  closeModal();

  const overlay = document.createElement("div");
  overlay.id = "modal-overlay";
  overlay.style.cssText =
    "position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:8000;display:flex;align-items:center;justify-content:center;";
  overlay.addEventListener("click", function (e) {
    if (e.target === overlay) closeModal();
  });

  const box = document.createElement("div");
  box.style.cssText =
    "background:#1E1E2E;border-radius:12px;padding:24px;max-width:600px;width:90%;max-height:80vh;" +
    "overflow-y:auto;box-shadow:0 8px 32px rgba(0,0,0,.5);color:#E0E0E0;position:relative;";

  const hdr = document.createElement("div");
  hdr.style.cssText = "display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;";
  const h2 = document.createElement("h2");
  h2.style.cssText = "margin:0;font-size:18px;color:#fff;";
  h2.textContent = title;

  const closeBtn = document.createElement("button");
  closeBtn.innerHTML = "&times;";
  closeBtn.style.cssText =
    "background:none;border:none;color:#aaa;font-size:24px;cursor:pointer;line-height:1;padding:0 4px;";
  closeBtn.onclick = closeModal;

  hdr.appendChild(h2);
  hdr.appendChild(closeBtn);
  box.appendChild(hdr);

  const body = document.createElement("div");
  body.id = "modal-body";
  body.innerHTML = html;
  box.appendChild(body);

  overlay.appendChild(box);
  document.body.appendChild(overlay);
  document.body.style.overflow = "hidden";
}

function closeModal() {
  const el = document.getElementById("modal-overlay");
  if (el) {
    el.remove();
    document.body.style.overflow = "";
  }
}

/* ----------------------------------------------------------
   5. TAB SWITCHING
   ---------------------------------------------------------- */

function showTab(name) {
  if (!name) name = "search";
  name = name.replace(/^#/, "");

  const validTabs = [
    "search", "kanban", "email", "emails", "followup", "followups", "analytics",
    "tracking", "blacklist", "dsgvo", "abo", "settings", "admin",
  ];
  if (!validTabs.includes(name)) name = "search";

  // Stop admin auto-refresh when leaving admin tab
  if (currentTab === "admin" && name !== "admin" && window._adminRefresh) {
    clearInterval(window._adminRefresh);
    window._adminRefresh = null;
  }

  currentTab = name;
  location.hash = name;

  document.querySelectorAll(".tab-pane, .tab-content").forEach((pane) => {
    pane.style.display = pane.id === "tab-" + name ? "block" : "none";
    pane.classList.toggle("active", pane.id === "tab-" + name);
  });
  document.querySelectorAll(".tab-btn, .nav-btn").forEach((btn) => {
    const isActive = btn.dataset.tab === name;
    btn.classList.toggle("active", isActive);
  });

  // Lazy-load data per tab
  switch (name) {
    case "search":
      break;
    case "email":
      loadTemplates();
      loadEmailLog();
      break;
    case "followup":
      loadFollowups();
      break;
    case "analytics":
      loadAnalytics();
      break;
    case "tracking":
      loadTracking();
      break;
    case "blacklist":
      loadBlacklist();
      break;
    case "dsgvo":
      loadDsgvo();
      break;
    case "abo":
      loadAbo();
      break;
    case "settings":
      loadSettings();
      break;
    case "admin":
      loadAdmin();
      break;
  }
}

/* ----------------------------------------------------------
   6. HEADER — USER, NOTIFICATIONS, LOGOUT
   ---------------------------------------------------------- */

async function loadUser() {
  try {
    currentUser = await api("/api/auth/me");
    const el = document.getElementById("user-display");
    if (!el) return;

    const planColors = { free: "#6B7280", pro: "#7C6FFF", business: "#F59E0B" };
    const plan = (currentUser.plan || "free").toLowerCase();
    const color = planColors[plan] || planColors.free;
    const planLabel = plan.charAt(0).toUpperCase() + plan.slice(1);

    el.innerHTML =
      '<span style="color:#fff;font-weight:600;">' + esc(currentUser.name || currentUser.email) + "</span> " +
      '<span style="background:' + color + ";padding:2px 8px;border-radius:10px;font-size:11px;color:#fff;" +
      'margin-left:6px;">' + esc(planLabel) + "</span>";

    loadNotifications();
  } catch (e) {
    console.error("loadUser:", e);
  }
}

async function loadNotifications() {
  try {
    const data = await api("/api/notifications");
    const items = Array.isArray(data) ? data : data.items || [];
    const unread = items.filter((n) => !n.read).length;

    const bell = document.getElementById("notification-bell");
    if (bell) {
      bell.innerHTML =
        '<svg width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">' +
        '<path d="M18 8a6 6 0 10-12 0c0 7-3 9-3 9h18s-3-2-3-9"></path><path d="M13.73 21a2 2 0 01-3.46 0"></path></svg>' +
        (unread > 0
          ? '<span style="position:absolute;top:-4px;right:-4px;background:#F87171;color:#fff;font-size:10px;' +
            'border-radius:50%;width:18px;height:18px;display:flex;align-items:center;justify-content:center;">' +
            unread + "</span>"
          : "");
    }

    const dropdown = document.getElementById("notification-dropdown");
    if (!dropdown) return;

    if (items.length === 0) {
      dropdown.innerHTML = '<div style="padding:12px;color:#888;">No notifications</div>';
      return;
    }

    let html = "";
    items.slice(0, 20).forEach((n) => {
      html +=
        '<div style="padding:8px 12px;border-bottom:1px solid #333;' +
        (n.read ? "opacity:.6;" : "") + '">' +
        '<div style="font-size:13px;">' + esc(n.message || n.text) + "</div>" +
        '<div style="font-size:11px;color:#888;margin-top:2px;">' + fmtDate(n.created_at) + "</div>" +
        "</div>";
    });
    html +=
      '<div style="padding:8px;text-align:center;">' +
      '<button onclick="markAllNotificationsRead()" style="background:none;border:none;color:#7C6FFF;' +
      'cursor:pointer;font-size:12px;">Mark all as read</button></div>';
    dropdown.innerHTML = html;
  } catch (e) {
    console.error("loadNotifications:", e);
  }
}

async function markAllNotificationsRead() {
  try {
    await api("/api/notifications/read-all", { method: "POST" });
    showToast("Notifications marked as read", "success");
    loadNotifications();
  } catch (e) {
    showToast(e.message, "error");
  }
}

function toggleNotificationDropdown() {
  const dd = document.getElementById("notification-dropdown");
  if (!dd) return;
  dd.style.display = dd.style.display === "block" ? "none" : "block";
}

async function doLogout() {
  try {
    await api("/api/auth/logout", { method: "POST" });
  } catch (_) { /* ignore */ }
  window.location.href = "/login";
}

/* ----------------------------------------------------------
   7. SEARCH TAB
   ---------------------------------------------------------- */

async function doSearch() {
  const q = (document.getElementById("search-q") || {}).value || "";
  const city = (document.getElementById("search-city") || {}).value || "";
  const n = (document.getElementById("search-n") || {}).value || "20";

  if (!q.trim()) {
    showToast("Please enter a search query", "error");
    return;
  }

  const params = new URLSearchParams({ q: q.trim(), city: city.trim(), n });
  showSearchProgress(true);
  _searchStart = Date.now();
  startSearchTimer();

  try {
    const data = await api("/api/search?" + params.toString());
    currentLeads = Array.isArray(data) ? data : data.leads || data.results || [];
    renderLeads(currentLeads);
    showToast(currentLeads.length + " leads found", "success");
  } catch (e) {
    showToast(e.message, "error");
    renderLeads([]);
  } finally {
    showSearchProgress(false);
    stopSearchTimer();
  }
}

async function doMultiSearch() {
  const q = (document.getElementById("search-q") || {}).value || "";
  const citiesRaw = (document.getElementById("search-cities") || {}).value || "";
  const n = (document.getElementById("search-n") || {}).value || "20";

  if (!q.trim()) {
    showToast("Please enter a search query", "error");
    return;
  }

  const cities = citiesRaw
    .split(",")
    .map((c) => c.trim())
    .filter(Boolean);
  if (cities.length === 0) {
    showToast("Please enter at least one city", "error");
    return;
  }

  showSearchProgress(true);
  _searchStart = Date.now();
  startSearchTimer();

  try {
    const data = await api("/api/search-multi", {
      method: "POST",
      body: { query: q.trim(), cities, n: parseInt(n, 10) },
    });
    currentLeads = Array.isArray(data) ? data : data.leads || data.results || [];
    renderLeads(currentLeads);
    showToast(currentLeads.length + " leads found across " + cities.length + " cities", "success");
  } catch (e) {
    showToast(e.message, "error");
    renderLeads([]);
  } finally {
    showSearchProgress(false);
    stopSearchTimer();
  }
}

function showSearchProgress(show) {
  const bar = document.getElementById("search-progress");
  if (bar) bar.style.display = show ? "block" : "none";
}

function startSearchTimer() {
  const el = document.getElementById("search-timer");
  if (!el) return;
  el.style.display = "inline";
  _searchTimer = setInterval(() => {
    const elapsed = ((Date.now() - _searchStart) / 1000).toFixed(1);
    el.textContent = elapsed + "s";
  }, 100);
}

function stopSearchTimer() {
  if (_searchTimer) {
    clearInterval(_searchTimer);
    _searchTimer = null;
  }
}

/* ----------------------------------------------------------
   7a. LEAD RENDERING
   ---------------------------------------------------------- */

function scoreColor(score) {
  if (score == null) return "#6B7280";
  if (score >= 80) return "#34D399";
  if (score >= 50) return "#FBBF24";
  return "#F87171";
}

function ratingStars(rating) {
  if (rating == null) return "";
  const full = Math.floor(rating);
  const half = rating - full >= 0.5 ? 1 : 0;
  let s = "";
  for (let i = 0; i < full; i++) s += '<span style="color:#FBBF24;">&#9733;</span>';
  if (half) s += '<span style="color:#FBBF24;">&#9734;</span>';
  for (let i = full + half; i < 5; i++) s += '<span style="color:#555;">&#9734;</span>';
  return s + ' <span style="color:#aaa;font-size:12px;">(' + Number(rating).toFixed(1) + ")</span>";
}

function renderLeads(leads) {
  const container = document.getElementById("leads-container");
  if (!container) return;

  if (!leads || leads.length === 0) {
    container.innerHTML =
      '<div style="text-align:center;padding:40px;color:#888;">No leads found. Try a different search.</div>';
    updateLeadCount(0);
    return;
  }

  updateLeadCount(leads.length);

  let html = "";
  leads.forEach((lead, idx) => {
    const sc = lead.score != null ? lead.score : "-";
    const sColor = scoreColor(lead.score);
    const phone = lead.phone || lead.formatted_phone_number || "";
    const email = lead.email || "";
    const website = lead.website || "";
    const address = lead.address || lead.formatted_address || "";
    const ownerName = lead.owner || "";
    const techStack = lead.tech_stack || lead.technologies || [];

    // Badges
    let badges = "";
    if (lead.is_duplicate || lead.duplicate) {
      badges += '<span style="background:#F59E0B;color:#000;padding:1px 6px;border-radius:4px;font-size:10px;margin-right:4px;">Duplicate</span>';
    }
    if (lead.contacted) {
      badges += '<span style="background:#7C6FFF;color:#fff;padding:1px 6px;border-radius:4px;font-size:10px;margin-right:4px;">Contacted</span>';
    }

    // Tech stack badges
    let techHtml = "";
    if (Array.isArray(techStack) && techStack.length > 0) {
      techStack.forEach((t) => {
        techHtml +=
          '<span style="background:#2A2A3E;color:#A5B4FC;padding:2px 6px;border-radius:4px;font-size:11px;margin:2px;">' +
          esc(t) + "</span>";
      });
    }

    // Action links
    let actions = "";
    if (phone) {
      const waLink = "https://wa.me/" + phone.replace(/[^0-9+]/g, "").replace(/^\+/, "");
      actions +=
        '<a href="' + esc(waLink) + '" target="_blank" rel="noopener" ' +
        'style="color:#34D399;text-decoration:none;font-size:12px;margin-right:10px;" title="WhatsApp">' +
        "WhatsApp</a>";
    }
    if (address) {
      const mapsLink = "https://www.google.com/maps/search/" + encodeURIComponent(address);
      actions +=
        '<a href="' + esc(mapsLink) + '" target="_blank" rel="noopener" ' +
        'style="color:#60A5FA;text-decoration:none;font-size:12px;margin-right:10px;" title="Google Maps">' +
        "Maps</a>";
    }

    html +=
      '<div class="lead-card" style="background:#1E1E2E;border-radius:10px;padding:16px;margin-bottom:12px;' +
      'border:1px solid #2A2A3E;transition:border-color .2s;" ' +
      'onmouseenter="this.style.borderColor=\'#7C6FFF\'" onmouseleave="this.style.borderColor=\'#2A2A3E\'">' +
      '<div style="display:flex;justify-content:space-between;align-items:flex-start;">' +
        '<div style="flex:1;">' +
          '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">' +
            '<span style="font-size:16px;font-weight:600;color:#fff;">' + esc(lead.name || "Unnamed") + "</span>" +
            badges +
          "</div>" +
          '<div style="margin-bottom:4px;">' + ratingStars(lead.rating) + "</div>" +
          (email
            ? '<div style="font-size:13px;color:#ccc;margin-bottom:2px;"><span style="color:#888;">Email:</span> ' +
              '<a href="mailto:' + esc(email) + '" style="color:#7C6FFF;text-decoration:none;">' + esc(email) + "</a></div>"
            : "") +
          (phone
            ? '<div style="font-size:13px;color:#ccc;margin-bottom:2px;"><span style="color:#888;">Phone:</span> ' + esc(phone) + "</div>"
            : "") +
          (address
            ? '<div style="font-size:13px;color:#ccc;margin-bottom:2px;"><span style="color:#888;">Address:</span> ' + esc(address) + "</div>"
            : "") +
          (website
            ? '<div style="font-size:13px;margin-bottom:2px;"><span style="color:#888;">Web:</span> ' +
              '<a href="' + esc(safeUrl(website)) + '" target="_blank" rel="noopener" style="color:#60A5FA;text-decoration:none;">' +
              esc(website) + "</a></div>"
            : "") +
          (ownerName
            ? '<div style="font-size:13px;color:#ccc;margin-bottom:2px;"><span style="color:#888;">Owner:</span> ' + esc(ownerName) + "</div>"
            : "") +
          (techHtml ? '<div style="margin-top:6px;display:flex;flex-wrap:wrap;gap:2px;">' + techHtml + "</div>" : "") +
          '<div style="margin-top:8px;">' + actions + "</div>" +
        "</div>" +
        '<div style="text-align:center;min-width:60px;">' +
          '<div style="font-size:28px;font-weight:700;color:' + sColor + ';">' + esc(String(sc)) + "</div>" +
          '<div style="font-size:10px;color:#888;">Score</div>' +
          '<button onclick="saveOneLead(' + idx + ')" style="margin-top:8px;background:#7C6FFF;color:#fff;' +
          'border:none;border-radius:6px;padding:6px 12px;cursor:pointer;font-size:12px;">Save</button>' +
        "</div>" +
      "</div>" +
      "</div>";
  });

  container.innerHTML = html;
}

function updateLeadCount(count) {
  const el = document.getElementById("lead-count");
  if (el) el.textContent = count + " lead" + (count !== 1 ? "s" : "");
}

/* ----------------------------------------------------------
   7b. SAVE LEADS
   ---------------------------------------------------------- */

async function saveOneLead(idx) {
  if (!currentLeads[idx]) return;
  try {
    await api("/api/leads", { method: "POST", body: currentLeads[idx] });
    showToast("Lead saved", "success");
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function saveAllLeads() {
  if (currentLeads.length === 0) {
    showToast("No leads to save", "error");
    return;
  }
  try {
    await api("/api/leads/save-all", { method: "POST", body: { leads: currentLeads } });
    showToast(currentLeads.length + " leads saved", "success");
  } catch (e) {
    showToast(e.message, "error");
  }
}

/* ----------------------------------------------------------
   7c. CSV CLIPBOARD COPY
   ---------------------------------------------------------- */

function copyLeadsCsv() {
  if (currentLeads.length === 0) {
    showToast("No leads to copy", "error");
    return;
  }

  const headers = ["Name", "Score", "Rating", "Email", "Phone", "Address", "Website", "Owner"];
  const rows = currentLeads.map((l) => [
    l.name || "",
    l.score != null ? l.score : "",
    l.rating != null ? l.rating : "",
    l.email || "",
    l.phone || l.formatted_phone_number || "",
    l.address || l.formatted_address || "",
    l.website || "",
    l.owner || "",
  ]);

  const csvLine = (arr) => arr.map((v) => '"' + String(v).replace(/"/g, '""') + '"').join(",");
  const csv = [csvLine(headers)].concat(rows.map(csvLine)).join("\n");

  navigator.clipboard.writeText(csv).then(
    () => showToast("CSV copied to clipboard", "success"),
    () => showToast("Failed to copy CSV", "error")
  );
}

/* ----------------------------------------------------------
   8. EMAIL TAB
   ---------------------------------------------------------- */

let emailTemplates = [];
let selectedTemplateId = null;
let abTestEnabled = false;

async function loadTemplates() {
  try {
    const data = await api("/api/emails/templates");
    emailTemplates = Array.isArray(data) ? data : data.templates || [];
    renderTemplateChips();
  } catch (e) {
    console.error("loadTemplates:", e);
  }
}

function renderTemplateChips() {
  const container = document.getElementById("template-chips");
  if (!container) return;

  let html =
    '<button onclick="selectCustomTemplate()" class="tmpl-chip" ' +
    'style="padding:6px 14px;border-radius:16px;border:1px solid #444;cursor:pointer;font-size:13px;' +
    'margin:4px;background:' + (selectedTemplateId === null ? "#7C6FFF" : "#2A2A3E") + ";color:#fff;" +
    '">Custom</button>';

  emailTemplates.forEach((t) => {
    const active = selectedTemplateId === t.id;
    html +=
      '<button onclick="selectTemplate(\'' + esc(t.id) + '\')" class="tmpl-chip" ' +
      'style="padding:6px 14px;border-radius:16px;border:1px solid #444;cursor:pointer;font-size:13px;' +
      "margin:4px;background:" + (active ? "#7C6FFF" : "#2A2A3E") + ';color:#fff;">' +
      esc(t.name) + "</button>";
  });

  container.innerHTML = html;
}

function selectTemplate(id) {
  const tpl = emailTemplates.find((t) => String(t.id) === String(id));
  if (!tpl) return;
  selectedTemplateId = tpl.id;
  renderTemplateChips();

  const subjectEl = document.getElementById("email-subject");
  const bodyEl = document.getElementById("email-body");
  if (subjectEl) subjectEl.value = tpl.subject || "";
  if (bodyEl) bodyEl.value = tpl.body || "";

  updateEmailPreview();
}

function selectCustomTemplate() {
  selectedTemplateId = null;
  renderTemplateChips();
  const subjectEl = document.getElementById("email-subject");
  const bodyEl = document.getElementById("email-body");
  if (subjectEl) subjectEl.value = "";
  if (bodyEl) bodyEl.value = "";
  updateEmailPreview();
}

function insertPlaceholder(ph) {
  const bodyEl = document.getElementById("email-body");
  if (!bodyEl) return;
  const start = bodyEl.selectionStart;
  const end = bodyEl.selectionEnd;
  const text = bodyEl.value;
  bodyEl.value = text.substring(0, start) + "{{" + ph + "}}" + text.substring(end);
  bodyEl.focus();
  bodyEl.selectionStart = bodyEl.selectionEnd = start + ph.length + 4;
  updateEmailPreview();
}

function updateEmailPreview() {
  const preview = document.getElementById("email-preview");
  if (!preview) return;
  const subject = (document.getElementById("email-subject") || {}).value || "";
  const body = (document.getElementById("email-body") || {}).value || "";

  // Replace placeholders with sample values for preview
  const sampleData = {
    name: "Max Mustermann",
    company: "Beispiel GmbH",
    city: "Berlin",
    email: "max@beispiel.de",
    phone: "+49 30 12345678",
    website: "www.beispiel.de",
  };

  let previewSubject = subject;
  let previewBody = body;
  Object.keys(sampleData).forEach((key) => {
    const re = new RegExp("\\{\\{" + key + "\\}\\}", "gi");
    previewSubject = previewSubject.replace(re, sampleData[key]);
    previewBody = previewBody.replace(re, sampleData[key]);
  });

  preview.innerHTML =
    '<div style="border:1px solid #333;border-radius:8px;padding:16px;background:#151521;">' +
    '<div style="font-weight:600;color:#fff;margin-bottom:8px;">Subject: ' + esc(previewSubject) + "</div>" +
    '<div style="color:#ccc;white-space:pre-wrap;font-size:13px;">' + esc(previewBody) + "</div>" +
    "</div>";
}

function toggleAbTest() {
  abTestEnabled = !abTestEnabled;
  const container = document.getElementById("ab-test-fields");
  if (container) container.style.display = abTestEnabled ? "block" : "none";
  const btn = document.getElementById("ab-toggle-btn");
  if (btn) {
    btn.style.background = abTestEnabled ? "#7C6FFF" : "#2A2A3E";
    btn.textContent = abTestEnabled ? "A/B Test: ON" : "A/B Test: OFF";
  }
}

async function sendEmail(lead) {
  const subject = (document.getElementById("email-subject") || {}).value || "";
  const body = (document.getElementById("email-body") || {}).value || "";

  if (!subject || !body) {
    showToast("Subject and body are required", "error");
    return;
  }
  if (!lead.email) {
    showToast("This lead has no email address", "error");
    return;
  }

  try {
    const payload = {
      to: lead.email,
      subject,
      body,
      lead_id: lead.id,
      template_id: selectedTemplateId,
    };

    if (abTestEnabled) {
      payload.ab_subject = (document.getElementById("ab-subject") || {}).value || "";
      payload.ab_body = (document.getElementById("ab-body") || {}).value || "";
    }

    await api("/api/emails/send", { method: "POST", body: payload });
    showToast("Email sent to " + lead.email, "success");
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function sendAllEmails() {
  const subject = (document.getElementById("email-subject") || {}).value || "";
  const body = (document.getElementById("email-body") || {}).value || "";

  if (!subject || !body) {
    showToast("Subject and body are required", "error");
    return;
  }

  const leadsWithEmail = currentLeads.filter((l) => l.email);
  if (leadsWithEmail.length === 0) {
    showToast("No leads with email addresses", "error");
    return;
  }

  const progressEl = document.getElementById("email-send-progress");
  if (progressEl) progressEl.style.display = "block";

  try {
    const payload = {
      leads: leadsWithEmail.map((l) => ({ id: l.id, email: l.email, name: l.name })),
      subject,
      body,
      template_id: selectedTemplateId,
    };

    if (abTestEnabled) {
      payload.ab_subject = (document.getElementById("ab-subject") || {}).value || "";
      payload.ab_body = (document.getElementById("ab-body") || {}).value || "";
    }

    const result = await api("/api/emails/send-all", { method: "POST", body: payload });
    const sent = result.sent || result.count || leadsWithEmail.length;
    showToast(sent + " emails sent", "success");
    loadEmailLog();
  } catch (e) {
    showToast(e.message, "error");
  } finally {
    if (progressEl) progressEl.style.display = "none";
  }
}

async function loadEmailLog() {
  try {
    const data = await api("/api/emails");
    const emails = Array.isArray(data) ? data : data.emails || data.items || [];
    const container = document.getElementById("email-log");
    if (!container) return;

    if (emails.length === 0) {
      container.innerHTML = '<div style="color:#888;padding:12px;">No emails sent yet.</div>';
      return;
    }

    let html =
      '<table style="width:100%;border-collapse:collapse;font-size:13px;">' +
      "<thead><tr>" +
      '<th style="text-align:left;padding:8px;color:#aaa;border-bottom:1px solid #333;">To</th>' +
      '<th style="text-align:left;padding:8px;color:#aaa;border-bottom:1px solid #333;">Subject</th>' +
      '<th style="text-align:left;padding:8px;color:#aaa;border-bottom:1px solid #333;">Status</th>' +
      '<th style="text-align:left;padding:8px;color:#aaa;border-bottom:1px solid #333;">Sent</th>' +
      "</tr></thead><tbody>";

    emails.forEach((em) => {
      const statusColor = em.status === "delivered" || em.status === "sent"
        ? "#34D399"
        : em.status === "bounced" || em.status === "failed"
          ? "#F87171"
          : "#FBBF24";
      html +=
        "<tr>" +
        '<td style="padding:8px;color:#ccc;border-bottom:1px solid #222;">' + esc(em.to || em.recipient) + "</td>" +
        '<td style="padding:8px;color:#ccc;border-bottom:1px solid #222;">' + esc(em.subject) + "</td>" +
        '<td style="padding:8px;border-bottom:1px solid #222;"><span style="color:' + statusColor + ';">' +
        esc(em.status || "sent") + "</span></td>" +
        '<td style="padding:8px;color:#888;border-bottom:1px solid #222;">' + fmtDate(em.sent_at || em.created_at) + "</td>" +
        "</tr>";
    });

    html += "</tbody></table>";
    container.innerHTML = html;
  } catch (e) {
    console.error("loadEmailLog:", e);
  }
}

/* ----------------------------------------------------------
   9. FOLLOW-UP TAB
   ---------------------------------------------------------- */

async function loadFollowups() {
  try {
    const data = await api("/api/followups");
    const items = Array.isArray(data) ? data : data.followups || data.items || [];
    const container = document.getElementById("followup-list");
    if (!container) return;

    if (items.length === 0) {
      container.innerHTML = '<div style="color:#888;padding:12px;">No follow-ups scheduled.</div>';
      return;
    }

    let html = "";
    items.forEach((fu) => {
      const statusLabel = fu.status === "sent" ? "Sent" : fu.status === "cancelled" ? "Cancelled" : "Pending";
      const statusColor = fu.status === "sent" ? "#34D399" : fu.status === "cancelled" ? "#F87171" : "#FBBF24";

      html +=
        '<div style="background:#1E1E2E;border-radius:8px;padding:14px;margin-bottom:8px;border:1px solid #2A2A3E;' +
        'display:flex;justify-content:space-between;align-items:center;">' +
          '<div>' +
            '<div style="font-weight:600;color:#fff;">' + esc(fu.lead_name || fu.to || "Lead") + "</div>" +
            '<div style="font-size:12px;color:#888;margin-top:2px;">Due: ' + fmtDate(fu.scheduled_at || fu.due_date) + "</div>" +
            '<div style="font-size:12px;color:' + statusColor + ';margin-top:2px;">' + statusLabel + "</div>" +
          "</div>" +
          '<div style="display:flex;gap:6px;">' +
            (fu.status === "pending"
              ? '<button onclick="sendFollowup(\'' + esc(fu.id) + '\')" ' +
                'style="background:#34D399;color:#000;border:none;border-radius:6px;padding:6px 12px;cursor:pointer;font-size:12px;">Send</button>' +
                '<button onclick="cancelFollowup(\'' + esc(fu.id) + '\')" ' +
                'style="background:#F87171;color:#fff;border:none;border-radius:6px;padding:6px 12px;cursor:pointer;font-size:12px;">Cancel</button>'
              : "") +
          "</div>" +
        "</div>";
    });

    container.innerHTML = html;
  } catch (e) {
    console.error("loadFollowups:", e);
  }
}

async function sendFollowup(id) {
  try {
    await api("/api/followups/" + encodeURIComponent(id) + "/send", { method: "POST" });
    showToast("Follow-up sent", "success");
    loadFollowups();
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function cancelFollowup(id) {
  try {
    await api("/api/followups/" + encodeURIComponent(id) + "/cancel", { method: "POST" });
    showToast("Follow-up cancelled", "info");
    loadFollowups();
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function scheduleAllFollowups() {
  try {
    const result = await api("/api/followups/schedule", { method: "POST" });
    const count = result.count || result.scheduled || 0;
    showToast(count + " follow-ups scheduled", "success");
    loadFollowups();
  } catch (e) {
    showToast(e.message, "error");
  }
}

/* ----------------------------------------------------------
   10. ANALYTICS TAB
   ---------------------------------------------------------- */

let _trendChartInstance = null;

async function loadAnalytics() {
  try {
    const data = await api("/api/analytics/stats");
    renderStatCards(data);
    renderFunnel(data.funnel || data);
    renderWeeklyTrend(data.weekly || data.trend || []);
    renderRoiCalculator(data);
    renderHeatmap(data.heatmap || []);
    renderAbResults(data.ab_results || data.ab_tests || []);
  } catch (e) {
    console.error("loadAnalytics:", e);
    showToast("Failed to load analytics", "error");
  }
}

function renderStatCards(data) {
  const container = document.getElementById("analytics-cards");
  if (!container) return;

  const cards = [
    { label: "Total Leads", value: fmtNum(data.total_leads || data.leads || 0), color: "#7C6FFF" },
    { label: "Emails Sent", value: fmtNum(data.emails_sent || 0), color: "#60A5FA" },
    { label: "Open Rate", value: fmtPct(data.open_rate || 0), color: "#34D399" },
    { label: "Reply Rate", value: fmtPct(data.reply_rate || 0), color: "#A78BFA" },
    { label: "Bounce Rate", value: fmtPct(data.bounce_rate || 0), color: "#F87171" },
    { label: "Conversions", value: fmtNum(data.conversions || 0), color: "#FBBF24" },
    { label: "Revenue", value: (data.revenue != null ? fmtNum(data.revenue) + " \u20AC" : "0 \u20AC"), color: "#34D399" },
    { label: "Follow-ups Due", value: fmtNum(data.followups_due || data.followups || 0), color: "#F59E0B" },
  ];

  let html = '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px;">';
  cards.forEach((c) => {
    html +=
      '<div style="background:#1E1E2E;border-radius:10px;padding:18px;border:1px solid #2A2A3E;">' +
      '<div style="font-size:12px;color:#888;margin-bottom:4px;">' + c.label + "</div>" +
      '<div style="font-size:24px;font-weight:700;color:' + c.color + ';">' + c.value + "</div>" +
      "</div>";
  });
  html += "</div>";
  container.innerHTML = html;
}

function renderFunnel(funnel) {
  const container = document.getElementById("analytics-funnel");
  if (!container) return;

  const stages = [
    { label: "Searched", value: funnel.searched || funnel.total_leads || 0 },
    { label: "Saved", value: funnel.saved || funnel.leads_saved || 0 },
    { label: "Emailed", value: funnel.emailed || funnel.emails_sent || 0 },
    { label: "Opened", value: funnel.opened || 0 },
    { label: "Replied", value: funnel.replied || 0 },
    { label: "Converted", value: funnel.converted || funnel.conversions || 0 },
  ];

  const max = Math.max(1, ...stages.map((s) => s.value));

  let html =
    '<h3 style="color:#fff;font-size:16px;margin-bottom:12px;">Conversion Funnel</h3>';
  stages.forEach((s) => {
    const pct = ((s.value / max) * 100).toFixed(1);
    html +=
      '<div style="margin-bottom:8px;">' +
      '<div style="display:flex;justify-content:space-between;margin-bottom:2px;">' +
      '<span style="color:#ccc;font-size:13px;">' + s.label + "</span>" +
      '<span style="color:#aaa;font-size:13px;">' + fmtNum(s.value) + " (" + pct + "%)</span>" +
      "</div>" +
      '<div style="background:#2A2A3E;border-radius:4px;height:20px;overflow:hidden;">' +
      '<div style="background:linear-gradient(90deg,#7C6FFF,#A78BFA);height:100%;width:' + pct + '%;' +
      'border-radius:4px;transition:width .6s ease;"></div>' +
      "</div></div>";
  });

  container.innerHTML = html;
}

function renderWeeklyTrend(weeklyData) {
  const canvas = document.getElementById("trend-chart");
  if (!canvas) return;

  if (typeof Chart === "undefined") {
    canvas.parentElement.innerHTML =
      '<div style="color:#888;padding:12px;">Chart.js not loaded. Include it to see trend charts.</div>';
    return;
  }

  if (_trendChartInstance) {
    _trendChartInstance.destroy();
    _trendChartInstance = null;
  }

  const labels = weeklyData.map((w) => w.week || w.label || w.date || "");
  const leadsData = weeklyData.map((w) => w.leads || 0);
  const emailsData = weeklyData.map((w) => w.emails || w.emails_sent || 0);
  const repliesData = weeklyData.map((w) => w.replies || 0);

  _trendChartInstance = new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Leads",
          data: leadsData,
          borderColor: "#7C6FFF",
          backgroundColor: "rgba(124,111,255,0.1)",
          tension: 0.3,
          fill: true,
        },
        {
          label: "Emails",
          data: emailsData,
          borderColor: "#34D399",
          backgroundColor: "rgba(52,211,153,0.1)",
          tension: 0.3,
          fill: true,
        },
        {
          label: "Replies",
          data: repliesData,
          borderColor: "#F59E0B",
          backgroundColor: "rgba(245,158,11,0.1)",
          tension: 0.3,
          fill: true,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: "#ccc" } },
      },
      scales: {
        x: { ticks: { color: "#888" }, grid: { color: "#2A2A3E" } },
        y: { ticks: { color: "#888" }, grid: { color: "#2A2A3E" }, beginAtZero: true },
      },
    },
  });
}

function renderRoiCalculator(data) {
  const container = document.getElementById("roi-calculator");
  if (!container) return;

  const emailsSent = data.emails_sent || 0;
  const conversions = data.conversions || 0;
  const revenue = data.revenue || 0;
  const costPerEmail = data.cost_per_email || 0.01;
  const totalCost = emailsSent * costPerEmail;
  const roi = totalCost > 0 ? (((revenue - totalCost) / totalCost) * 100).toFixed(0) : 0;

  container.innerHTML =
    '<h3 style="color:#fff;font-size:16px;margin-bottom:12px;">ROI Calculator</h3>' +
    '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">' +
      '<div style="background:#1E1E2E;padding:14px;border-radius:8px;border:1px solid #2A2A3E;">' +
        '<label style="color:#888;font-size:12px;">Cost per Email (\u20AC)</label>' +
        '<input id="roi-cost" type="number" step="0.01" value="' + costPerEmail + '" ' +
        'onchange="recalcRoi()" style="width:100%;margin-top:4px;background:#12121C;border:1px solid #333;' +
        'color:#fff;padding:6px;border-radius:4px;">' +
      "</div>" +
      '<div style="background:#1E1E2E;padding:14px;border-radius:8px;border:1px solid #2A2A3E;">' +
        '<label style="color:#888;font-size:12px;">Avg Revenue per Conversion (\u20AC)</label>' +
        '<input id="roi-revenue" type="number" step="1" value="' +
        (conversions > 0 ? Math.round(revenue / conversions) : 0) + '" ' +
        'onchange="recalcRoi()" style="width:100%;margin-top:4px;background:#12121C;border:1px solid #333;' +
        'color:#fff;padding:6px;border-radius:4px;">' +
      "</div>" +
    "</div>" +
    '<div id="roi-result" style="margin-top:12px;background:#1E1E2E;padding:16px;border-radius:8px;border:1px solid #2A2A3E;' +
    'text-align:center;">' +
      '<div style="font-size:12px;color:#888;">Estimated ROI</div>' +
      '<div style="font-size:32px;font-weight:700;color:' + (roi > 0 ? "#34D399" : "#F87171") + ';">' + roi + " %</div>" +
      '<div style="font-size:12px;color:#888;margin-top:4px;">Total cost: ' + fmtNum(totalCost.toFixed(2)) +
      " \u20AC | Revenue: " + fmtNum(revenue) + " \u20AC</div>" +
    "</div>";
}

function recalcRoi() {
  const costPerEmail = parseFloat((document.getElementById("roi-cost") || {}).value) || 0.01;
  const revenuePerConv = parseFloat((document.getElementById("roi-revenue") || {}).value) || 0;

  const emailsSent = currentUser ? (currentUser.emails_sent || 0) : 0;
  const conversions = currentUser ? (currentUser.conversions || 0) : 0;
  const totalCost = emailsSent * costPerEmail;
  const totalRevenue = conversions * revenuePerConv;
  const roi = totalCost > 0 ? (((totalRevenue - totalCost) / totalCost) * 100).toFixed(0) : 0;

  const el = document.getElementById("roi-result");
  if (el) {
    el.innerHTML =
      '<div style="font-size:12px;color:#888;">Estimated ROI</div>' +
      '<div style="font-size:32px;font-weight:700;color:' + (roi > 0 ? "#34D399" : "#F87171") + ';">' + roi + " %</div>" +
      '<div style="font-size:12px;color:#888;margin-top:4px;">Total cost: ' + fmtNum(totalCost.toFixed(2)) +
      " \u20AC | Revenue: " + fmtNum(totalRevenue) + " \u20AC</div>";
  }
}

function renderHeatmap(heatmapData) {
  const container = document.getElementById("analytics-heatmap");
  if (!container) return;

  if (!Array.isArray(heatmapData) || heatmapData.length === 0) {
    container.innerHTML = '<div style="color:#888;padding:12px;">No heatmap data available.</div>';
    return;
  }

  // Collect unique branches and cities
  const branches = [...new Set(heatmapData.map((h) => h.branch || h.category || ""))];
  const cities = [...new Set(heatmapData.map((h) => h.city || ""))];

  // Build lookup
  const lookup = {};
  heatmapData.forEach((h) => {
    const key = (h.branch || h.category || "") + "|" + (h.city || "");
    lookup[key] = h.count || h.value || 0;
  });

  const maxVal = Math.max(1, ...Object.values(lookup));

  let html =
    '<h3 style="color:#fff;font-size:16px;margin-bottom:12px;">Branch x City Heatmap</h3>' +
    '<div style="overflow-x:auto;">' +
    '<table style="border-collapse:collapse;width:100%;font-size:12px;">' +
    "<thead><tr><th style=\"padding:6px;color:#888;text-align:left;\"></th>";

  cities.forEach((c) => {
    html += '<th style="padding:6px;color:#aaa;text-align:center;">' + esc(c) + "</th>";
  });
  html += "</tr></thead><tbody>";

  branches.forEach((b) => {
    html += '<tr><td style="padding:6px;color:#ccc;font-weight:600;">' + esc(b) + "</td>";
    cities.forEach((c) => {
      const val = lookup[b + "|" + c] || 0;
      const intensity = val / maxVal;
      const r = Math.round(124 * intensity);
      const g = Math.round(111 * intensity);
      const bl = Math.round(255 * intensity);
      html +=
        '<td style="padding:6px;text-align:center;color:#fff;background:rgba(' +
        r + "," + g + "," + bl + "," + (0.2 + intensity * 0.8).toFixed(2) + ');">' +
        fmtNum(val) + "</td>";
    });
    html += "</tr>";
  });

  html += "</tbody></table></div>";
  container.innerHTML = html;
}

function renderAbResults(results) {
  const container = document.getElementById("analytics-ab");
  if (!container) return;

  if (!Array.isArray(results) || results.length === 0) {
    container.innerHTML = '<div style="color:#888;padding:12px;">No A/B test results yet.</div>';
    return;
  }

  let html =
    '<h3 style="color:#fff;font-size:16px;margin-bottom:12px;">A/B Test Results</h3>';

  results.forEach((test) => {
    const winnerA = (test.a_rate || 0) >= (test.b_rate || 0);
    html +=
      '<div style="background:#1E1E2E;border-radius:8px;padding:14px;margin-bottom:8px;border:1px solid #2A2A3E;">' +
        '<div style="font-weight:600;color:#fff;margin-bottom:8px;">' + esc(test.name || test.subject || "Test") + "</div>" +
        '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">' +
          '<div style="padding:10px;border-radius:6px;border:2px solid ' + (winnerA ? "#34D399" : "#333") + ';">' +
            '<div style="font-size:12px;color:#888;">Variant A</div>' +
            '<div style="font-size:20px;font-weight:700;color:' + (winnerA ? "#34D399" : "#ccc") + ';">' +
            fmtPct(test.a_rate || 0) + "</div>" +
            '<div style="font-size:11px;color:#888;">' + fmtNum(test.a_sent || 0) + " sent</div>" +
          "</div>" +
          '<div style="padding:10px;border-radius:6px;border:2px solid ' + (!winnerA ? "#34D399" : "#333") + ';">' +
            '<div style="font-size:12px;color:#888;">Variant B</div>' +
            '<div style="font-size:20px;font-weight:700;color:' + (!winnerA ? "#34D399" : "#ccc") + ';">' +
            fmtPct(test.b_rate || 0) + "</div>" +
            '<div style="font-size:11px;color:#888;">' + fmtNum(test.b_sent || 0) + " sent</div>" +
          "</div>" +
        "</div>" +
      "</div>";
  });

  container.innerHTML = html;
}

/* ----------------------------------------------------------
   11. TRACKING TAB
   ---------------------------------------------------------- */

let trackingSortField = "date";
let trackingSortAsc = false;

async function loadTracking() {
  try {
    const data = await api("/api/tracking");
    const events = Array.isArray(data) ? data : data.events || data.items || [];
    const stats = data.stats || {};

    renderTrackingStats(stats, events);
    renderTrackingEvents(events);
  } catch (e) {
    console.error("loadTracking:", e);
    showToast("Failed to load tracking data", "error");
  }
}

function renderTrackingStats(stats, events) {
  const container = document.getElementById("tracking-stats");
  if (!container) return;

  // Compute from events if stats object is empty
  const total = stats.sent || events.filter((e) => e.type === "sent").length || events.length;
  const opened = stats.opened || events.filter((e) => e.type === "opened" || e.type === "open").length;
  const replied = stats.replied || events.filter((e) => e.type === "replied" || e.type === "reply").length;
  const bounced = stats.bounced || events.filter((e) => e.type === "bounced" || e.type === "bounce").length;

  const pctO = total > 0 ? ((opened / total) * 100).toFixed(1) : "0.0";
  const pctR = total > 0 ? ((replied / total) * 100).toFixed(1) : "0.0";
  const pctB = total > 0 ? ((bounced / total) * 100).toFixed(1) : "0.0";

  container.innerHTML =
    '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:16px;">' +
      statPill("Sent", fmtNum(total), "#60A5FA") +
      statPill("Opened", fmtNum(opened) + " (" + pctO + "%)", "#34D399") +
      statPill("Replied", fmtNum(replied) + " (" + pctR + "%)", "#A78BFA") +
      statPill("Bounced", fmtNum(bounced) + " (" + pctB + "%)", "#F87171") +
    "</div>";
}

function statPill(label, value, color) {
  return (
    '<div style="flex:1;min-width:140px;background:#1E1E2E;border-radius:8px;padding:14px;border-left:3px solid ' +
    color + ';">' +
    '<div style="font-size:11px;color:#888;">' + label + "</div>" +
    '<div style="font-size:18px;font-weight:700;color:' + color + ';">' + value + "</div>" +
    "</div>"
  );
}

function renderTrackingEvents(events) {
  const container = document.getElementById("tracking-events");
  if (!container) return;

  if (events.length === 0) {
    container.innerHTML = '<div style="color:#888;padding:12px;">No tracking events.</div>';
    return;
  }

  // Sort
  const sorted = [...events].sort((a, b) => {
    let va, vb;
    if (trackingSortField === "date") {
      va = new Date(a.date || a.created_at || 0).getTime();
      vb = new Date(b.date || b.created_at || 0).getTime();
    } else {
      va = (a[trackingSortField] || "").toString().toLowerCase();
      vb = (b[trackingSortField] || "").toString().toLowerCase();
    }
    if (va < vb) return trackingSortAsc ? -1 : 1;
    if (va > vb) return trackingSortAsc ? 1 : -1;
    return 0;
  });

  const sortIcon = trackingSortAsc ? " &#9650;" : " &#9660;";
  const colStyle = 'style="padding:8px;color:#aaa;border-bottom:1px solid #333;cursor:pointer;user-select:none;"';

  let html =
    '<table style="width:100%;border-collapse:collapse;font-size:13px;">' +
    "<thead><tr>" +
    "<th " + colStyle + ' onclick="sortTracking(\'type\')">Type' +
    (trackingSortField === "type" ? sortIcon : "") + "</th>" +
    "<th " + colStyle + ' onclick="sortTracking(\'recipient\')">Recipient' +
    (trackingSortField === "recipient" ? sortIcon : "") + "</th>" +
    "<th " + colStyle + ' onclick="sortTracking(\'subject\')">Subject' +
    (trackingSortField === "subject" ? sortIcon : "") + "</th>" +
    "<th " + colStyle + ' onclick="sortTracking(\'date\')">Date' +
    (trackingSortField === "date" ? sortIcon : "") + "</th>" +
    "</tr></thead><tbody>";

  sorted.forEach((ev) => {
    const typeColor =
      ev.type === "opened" || ev.type === "open" ? "#34D399" :
      ev.type === "replied" || ev.type === "reply" ? "#A78BFA" :
      ev.type === "bounced" || ev.type === "bounce" ? "#F87171" :
      ev.type === "clicked" || ev.type === "click" ? "#60A5FA" :
      "#888";

    html +=
      "<tr>" +
      '<td style="padding:8px;border-bottom:1px solid #222;"><span style="color:' + typeColor + ';">' +
      esc(ev.type) + "</span></td>" +
      '<td style="padding:8px;border-bottom:1px solid #222;color:#ccc;">' + esc(ev.recipient || ev.to || ev.email || "") + "</td>" +
      '<td style="padding:8px;border-bottom:1px solid #222;color:#ccc;">' + esc(ev.subject || "") + "</td>" +
      '<td style="padding:8px;border-bottom:1px solid #222;color:#888;">' + fmtDate(ev.date || ev.created_at) + "</td>" +
      "</tr>";
  });

  html += "</tbody></table>";
  container.innerHTML = html;
}

function sortTracking(field) {
  if (trackingSortField === field) {
    trackingSortAsc = !trackingSortAsc;
  } else {
    trackingSortField = field;
    trackingSortAsc = true;
  }
  loadTracking();
}

/* ----------------------------------------------------------
   12. BLACKLIST TAB
   ---------------------------------------------------------- */

async function loadBlacklist() {
  try {
    const data = await api("/api/blacklist");
    const items = Array.isArray(data) ? data : data.items || data.entries || [];
    const container = document.getElementById("blacklist-list");
    if (!container) return;

    let html =
      '<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;">' +
        '<input id="bl-value" placeholder="Email or domain" style="flex:1;min-width:200px;background:#12121C;' +
        'border:1px solid #333;color:#fff;padding:8px 12px;border-radius:6px;">' +
        '<input id="bl-reason" placeholder="Reason (optional)" style="flex:1;min-width:150px;background:#12121C;' +
        'border:1px solid #333;color:#fff;padding:8px 12px;border-radius:6px;">' +
        '<button onclick="addBlacklist()" style="background:#7C6FFF;color:#fff;border:none;border-radius:6px;' +
        'padding:8px 18px;cursor:pointer;white-space:nowrap;">Add</button>' +
      "</div>";

    if (items.length === 0) {
      html += '<div style="color:#888;padding:12px;">Blacklist is empty.</div>';
    } else {
      html +=
        '<table style="width:100%;border-collapse:collapse;font-size:13px;">' +
        "<thead><tr>" +
        '<th style="text-align:left;padding:8px;color:#aaa;border-bottom:1px solid #333;">Value</th>' +
        '<th style="text-align:left;padding:8px;color:#aaa;border-bottom:1px solid #333;">Reason</th>' +
        '<th style="text-align:left;padding:8px;color:#aaa;border-bottom:1px solid #333;">Added</th>' +
        '<th style="text-align:right;padding:8px;color:#aaa;border-bottom:1px solid #333;"></th>' +
        "</tr></thead><tbody>";

      items.forEach((entry) => {
        html +=
          "<tr>" +
          '<td style="padding:8px;color:#ccc;border-bottom:1px solid #222;">' + esc(entry.value || entry.email || entry.domain || "") + "</td>" +
          '<td style="padding:8px;color:#888;border-bottom:1px solid #222;">' + esc(entry.reason || "-") + "</td>" +
          '<td style="padding:8px;color:#888;border-bottom:1px solid #222;">' + fmtDate(entry.created_at) + "</td>" +
          '<td style="padding:8px;text-align:right;border-bottom:1px solid #222;">' +
          '<button onclick="removeBlacklist(\'' + esc(entry.id) + '\')" style="background:#F87171;color:#fff;' +
          'border:none;border-radius:4px;padding:4px 10px;cursor:pointer;font-size:12px;">Remove</button></td>' +
          "</tr>";
      });

      html += "</tbody></table>";
    }

    container.innerHTML = html;
  } catch (e) {
    console.error("loadBlacklist:", e);
  }
}

async function addBlacklist() {
  const value = (document.getElementById("bl-value") || {}).value || "";
  const reason = (document.getElementById("bl-reason") || {}).value || "";

  if (!value.trim()) {
    showToast("Please enter an email or domain", "error");
    return;
  }

  try {
    await api("/api/blacklist", { method: "POST", body: { value: value.trim(), reason: reason.trim() } });
    showToast("Added to blacklist", "success");
    loadBlacklist();
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function removeBlacklist(id) {
  try {
    await api("/api/blacklist/" + encodeURIComponent(id), { method: "DELETE" });
    showToast("Removed from blacklist", "info");
    loadBlacklist();
  } catch (e) {
    showToast(e.message, "error");
  }
}

/* ----------------------------------------------------------
   13. DSGVO TAB
   ---------------------------------------------------------- */

let dsgvoFilterAction = "";

async function loadDsgvo() {
  try {
    const data = await api("/api/dsgvo");
    const items = Array.isArray(data) ? data : data.items || data.events || [];
    renderDsgvoTimeline(items);
  } catch (e) {
    console.error("loadDsgvo:", e);
  }
}

function renderDsgvoTimeline(items) {
  const container = document.getElementById("dsgvo-timeline");
  if (!container) return;

  // Collect unique action types
  const actionTypes = [...new Set(items.map((i) => i.action || i.type || "unknown"))];

  let html =
    '<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;align-items:center;">' +
      '<span style="color:#888;font-size:13px;">Filter:</span>' +
      '<button onclick="filterDsgvo(\'\')" style="padding:4px 12px;border-radius:12px;border:1px solid #444;' +
      'cursor:pointer;font-size:12px;background:' + (dsgvoFilterAction === "" ? "#7C6FFF" : "#2A2A3E") + ';color:#fff;">All</button>';

  actionTypes.forEach((a) => {
    html +=
      '<button onclick="filterDsgvo(\'' + esc(a) + '\')" style="padding:4px 12px;border-radius:12px;' +
      "border:1px solid #444;cursor:pointer;font-size:12px;background:" +
      (dsgvoFilterAction === a ? "#7C6FFF" : "#2A2A3E") + ';color:#fff;">' + esc(a) + "</button>";
  });

  html +=
    '<button onclick="exportDsgvoCsv()" style="margin-left:auto;padding:4px 14px;border-radius:6px;' +
    'border:1px solid #444;cursor:pointer;font-size:12px;background:#2A2A3E;color:#fff;">Export CSV</button>' +
    "</div>";

  const filtered = dsgvoFilterAction
    ? items.filter((i) => (i.action || i.type) === dsgvoFilterAction)
    : items;

  if (filtered.length === 0) {
    html += '<div style="color:#888;padding:12px;">No DSGVO events found.</div>';
  } else {
    html += '<div style="position:relative;padding-left:24px;">';

    // Vertical timeline line
    html +=
      '<div style="position:absolute;left:10px;top:0;bottom:0;width:2px;background:#2A2A3E;"></div>';

    filtered.forEach((item) => {
      const actionColor =
        item.action === "consent" || item.type === "consent" ? "#34D399" :
        item.action === "delete" || item.type === "delete" ? "#F87171" :
        item.action === "export" || item.type === "export" ? "#60A5FA" :
        item.action === "opt-out" || item.type === "opt-out" ? "#FBBF24" :
        "#7C6FFF";

      html +=
        '<div style="position:relative;margin-bottom:16px;padding-left:20px;">' +
          '<div style="position:absolute;left:-7px;top:4px;width:14px;height:14px;border-radius:50%;' +
          'background:' + actionColor + ';border:2px solid #12121C;"></div>' +
          '<div style="background:#1E1E2E;border-radius:8px;padding:12px;border:1px solid #2A2A3E;">' +
            '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">' +
              '<span style="color:' + actionColor + ';font-weight:600;font-size:13px;">' +
              esc(item.action || item.type || "event") + "</span>" +
              '<span style="color:#888;font-size:11px;">' + fmtDate(item.date || item.created_at) + "</span>" +
            "</div>" +
            '<div style="font-size:13px;color:#ccc;">' + esc(item.description || item.details || item.email || "") + "</div>" +
            (item.user || item.subject_name
              ? '<div style="font-size:11px;color:#888;margin-top:2px;">User: ' + esc(item.user || item.subject_name) + "</div>"
              : "") +
          "</div>" +
        "</div>";
    });

    html += "</div>";
  }

  container.innerHTML = html;
}

function filterDsgvo(action) {
  dsgvoFilterAction = action;
  loadDsgvo();
}

function exportDsgvoCsv() {
  api("/api/dsgvo")
    .then((data) => {
      const items = Array.isArray(data) ? data : data.items || data.events || [];
      const headers = ["Date", "Action", "Description", "User"];
      const rows = items.map((i) => [
        i.date || i.created_at || "",
        i.action || i.type || "",
        i.description || i.details || i.email || "",
        i.user || i.subject_name || "",
      ]);

      const csvLine = (arr) => arr.map((v) => '"' + String(v).replace(/"/g, '""') + '"').join(",");
      const csv = [csvLine(headers)].concat(rows.map(csvLine)).join("\n");

      const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = "dsgvo_export_" + new Date().toISOString().slice(0, 10) + ".csv";
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
      showToast("DSGVO data exported", "success");
    })
    .catch((e) => showToast(e.message, "error"));
}

/* ----------------------------------------------------------
   14. ABO (SUBSCRIPTION) TAB
   ---------------------------------------------------------- */

async function loadAbo() {
  try {
    const [subData, historyRes] = await Promise.all([
      api("/api/auth/me"),
      api("/api/subscriptions").catch(() => ({ payments: [] })),
    ]);

    renderPlanCards(subData);
    renderPaymentHistory(historyRes);
    renderLimitsDisplay(subData);
  } catch (e) {
    console.error("loadAbo:", e);
  }
}

function renderPlanCards(userData) {
  const container = document.getElementById("plan-cards");
  if (!container) return;

  const currentPlan = (userData.plan || "free").toLowerCase();

  const plans = [
    {
      id: "free",
      name: "Free",
      price: "0 \u20AC / mo",
      color: "#6B7280",
      features: [
        { text: "50 searches / month", included: true },
        { text: "10 emails / month", included: true },
        { text: "Basic templates", included: true },
        { text: "Email tracking", included: false },
        { text: "Analytics", included: false },
        { text: "API access", included: false },
        { text: "A/B testing", included: false },
        { text: "Priority support", included: false },
      ],
    },
    {
      id: "pro",
      name: "Pro",
      price: "29 \u20AC / mo",
      color: "#7C6FFF",
      features: [
        { text: "500 searches / month", included: true },
        { text: "200 emails / month", included: true },
        { text: "All templates", included: true },
        { text: "Email tracking", included: true },
        { text: "Analytics", included: true },
        { text: "API access", included: false },
        { text: "A/B testing", included: true },
        { text: "Priority support", included: false },
      ],
    },
    {
      id: "business",
      name: "Business",
      price: "99 \u20AC / mo",
      color: "#F59E0B",
      features: [
        { text: "Unlimited searches", included: true },
        { text: "Unlimited emails", included: true },
        { text: "All templates", included: true },
        { text: "Email tracking", included: true },
        { text: "Advanced analytics", included: true },
        { text: "API access", included: true },
        { text: "A/B testing", included: true },
        { text: "Priority support", included: true },
      ],
    },
  ];

  let html = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px;">';

  plans.forEach((plan) => {
    const isCurrent = plan.id === currentPlan;
    html +=
      '<div style="background:#1E1E2E;border-radius:12px;padding:24px;border:2px solid ' +
      (isCurrent ? plan.color : "#2A2A3E") + ';position:relative;">' +
        (isCurrent
          ? '<div style="position:absolute;top:-10px;right:16px;background:' + plan.color +
            ';color:#fff;padding:2px 10px;border-radius:10px;font-size:11px;font-weight:600;">Current Plan</div>'
          : "") +
        '<div style="text-align:center;margin-bottom:16px;">' +
          '<div style="font-size:20px;font-weight:700;color:' + plan.color + ';">' + plan.name + "</div>" +
          '<div style="font-size:28px;font-weight:700;color:#fff;margin-top:4px;">' + plan.price + "</div>" +
        "</div>" +
        '<ul style="list-style:none;padding:0;margin:0 0 16px 0;">';

    plan.features.forEach((f) => {
      html +=
        '<li style="padding:6px 0;font-size:13px;color:' + (f.included ? "#ccc" : "#555") + ';">' +
        '<span style="color:' + (f.included ? "#34D399" : "#F87171") + ';margin-right:6px;">' +
        (f.included ? "&#10003;" : "&#10007;") + "</span>" + esc(f.text) + "</li>";
    });

    html += "</ul>";

    if (isCurrent) {
      if (plan.id !== "free") {
        html +=
          '<button onclick="cancelSubscription()" style="width:100%;padding:10px;border-radius:8px;' +
          'border:1px solid #F87171;background:transparent;color:#F87171;cursor:pointer;font-size:13px;">' +
          "Cancel Plan</button>";
      }
    } else {
      const isUpgrade =
        (currentPlan === "free" && (plan.id === "pro" || plan.id === "business")) ||
        (currentPlan === "pro" && plan.id === "business");
      if (isUpgrade) {
        html +=
          '<button onclick="upgradePlan(\'' + plan.id + '\')" style="width:100%;padding:10px;border-radius:8px;' +
          "border:none;background:" + plan.color + ';color:#fff;cursor:pointer;font-size:13px;font-weight:600;">' +
          "Upgrade to " + plan.name + "</button>";
      }
    }

    html += "</div>";
  });

  html += "</div>";
  container.innerHTML = html;
}

function renderPaymentHistory(data) {
  const container = document.getElementById("payment-history");
  if (!container) return;

  const payments = data.payments || data.items || (Array.isArray(data) ? data : []);
  if (payments.length === 0) {
    container.innerHTML =
      '<h3 style="color:#fff;font-size:16px;margin-bottom:12px;">Payment History</h3>' +
      '<div style="color:#888;padding:12px;">No payment history.</div>';
    return;
  }

  let html =
    '<h3 style="color:#fff;font-size:16px;margin-bottom:12px;">Payment History</h3>' +
    '<table style="width:100%;border-collapse:collapse;font-size:13px;">' +
    "<thead><tr>" +
    '<th style="text-align:left;padding:8px;color:#aaa;border-bottom:1px solid #333;">Date</th>' +
    '<th style="text-align:left;padding:8px;color:#aaa;border-bottom:1px solid #333;">Plan</th>' +
    '<th style="text-align:left;padding:8px;color:#aaa;border-bottom:1px solid #333;">Amount</th>' +
    '<th style="text-align:left;padding:8px;color:#aaa;border-bottom:1px solid #333;">Status</th>' +
    "</tr></thead><tbody>";

  payments.forEach((p) => {
    const statusColor = p.status === "paid" || p.status === "succeeded" ? "#34D399" : "#FBBF24";
    html +=
      "<tr>" +
      '<td style="padding:8px;color:#ccc;border-bottom:1px solid #222;">' + fmtDate(p.date || p.created_at) + "</td>" +
      '<td style="padding:8px;color:#ccc;border-bottom:1px solid #222;">' + esc(p.plan || p.plan_name || "") + "</td>" +
      '<td style="padding:8px;color:#ccc;border-bottom:1px solid #222;">' + fmtNum(p.amount || 0) + " \u20AC</td>" +
      '<td style="padding:8px;border-bottom:1px solid #222;"><span style="color:' + statusColor + ';">' +
      esc(p.status || "pending") + "</span></td>" +
      "</tr>";
  });

  html += "</tbody></table>";
  container.innerHTML = html;
}

function renderLimitsDisplay(userData) {
  const container = document.getElementById("limits-display");
  if (!container) return;

  const limits = userData.limits || {};
  const searchUsed = limits.searches_used || userData.searches_used || 0;
  const searchMax = limits.searches_max || userData.searches_max || 50;
  const emailUsed = limits.emails_used || userData.emails_used || 0;
  const emailMax = limits.emails_max || userData.emails_max || 10;

  const searchPct = Math.min(100, (searchUsed / Math.max(1, searchMax)) * 100);
  const emailPct = Math.min(100, (emailUsed / Math.max(1, emailMax)) * 100);

  container.innerHTML =
    '<h3 style="color:#fff;font-size:16px;margin-bottom:12px;">Usage Limits</h3>' +
    '<div style="margin-bottom:12px;">' +
      '<div style="display:flex;justify-content:space-between;margin-bottom:4px;">' +
        '<span style="color:#ccc;font-size:13px;">Searches</span>' +
        '<span style="color:#888;font-size:13px;">' + fmtNum(searchUsed) + " / " + fmtNum(searchMax) + "</span>" +
      "</div>" +
      '<div style="background:#2A2A3E;border-radius:4px;height:8px;overflow:hidden;">' +
        '<div style="background:' + (searchPct > 90 ? "#F87171" : "#7C6FFF") +
        ";height:100%;width:" + searchPct + '%;transition:width .4s;"></div>' +
      "</div>" +
    "</div>" +
    '<div style="margin-bottom:12px;">' +
      '<div style="display:flex;justify-content:space-between;margin-bottom:4px;">' +
        '<span style="color:#ccc;font-size:13px;">Emails</span>' +
        '<span style="color:#888;font-size:13px;">' + fmtNum(emailUsed) + " / " + fmtNum(emailMax) + "</span>" +
      "</div>" +
      '<div style="background:#2A2A3E;border-radius:4px;height:8px;overflow:hidden;">' +
        '<div style="background:' + (emailPct > 90 ? "#F87171" : "#34D399") +
        ";height:100%;width:" + emailPct + '%;transition:width .4s;"></div>' +
      "</div>" +
    "</div>";
}

async function upgradePlan(planId) {
  try {
    const result = await api("/api/checkout/" + encodeURIComponent(planId), { method: "POST" });
    if (result.url) {
      window.location.href = result.url;
    } else {
      showToast("Upgrade initiated", "success");
      loadAbo();
    }
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function cancelSubscription() {
  if (!confirm("Are you sure you want to cancel your subscription? You will keep access until the end of the billing period.")) {
    return;
  }
  try {
    await api("/api/subscriptions/cancel", { method: "POST" });
    showToast("Subscription cancelled", "info");
    loadUser();
    loadAbo();
  } catch (e) {
    showToast(e.message, "error");
  }
}

/* ----------------------------------------------------------
   15. SETTINGS TAB
   ---------------------------------------------------------- */

async function loadSettings() {
  await Promise.all([
    loadSmtpSettings(),
    loadImapSettings(),
    loadTelegramSettings(),
    loadApiKeys(),
    loadWebhooks(),
    loadWarmupStatus(),
  ]);
}

/* -- SMTP -- */
async function loadSmtpSettings() {
  try {
    const data = await api("/api/settings/smtp");
    const form = document.getElementById("smtp-form");
    if (!form) return;
    setFormValues(form, {
      "smtp-host": data.host || "",
      "smtp-port": data.port || 587,
      "smtp-user": data.username || data.user || "",
      "smtp-pass": "",
      "smtp-from": data.from_email || data.from || "",
      "smtp-tls": data.tls !== false,
    });
  } catch (_) { /* no settings yet */ }
}

async function saveSmtp() {
  const payload = {
    host: val("smtp-host"),
    port: parseInt(val("smtp-port"), 10) || 587,
    username: val("smtp-user"),
    password: val("smtp-pass"),
    from_email: val("smtp-from"),
    tls: document.getElementById("smtp-tls") ? document.getElementById("smtp-tls").checked : true,
  };
  try {
    await api("/api/settings/smtp", { method: "PUT", body: payload });
    showToast("SMTP settings saved", "success");
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function testSmtp() {
  try {
    const result = await api("/api/settings/smtp/test", { method: "POST" });
    showToast(result.message || "SMTP test successful", "success");
  } catch (e) {
    showToast("SMTP test failed: " + e.message, "error");
  }
}

/* -- IMAP -- */
async function loadImapSettings() {
  try {
    const data = await api("/api/settings/imap");
    setFormValues(document.getElementById("imap-form"), {
      "imap-host": data.host || "",
      "imap-port": data.port || 993,
      "imap-user": data.username || data.user || "",
      "imap-pass": "",
      "imap-tls": data.tls !== false,
    });
  } catch (_) { /* no settings yet */ }
}

async function saveImap() {
  const payload = {
    host: val("imap-host"),
    port: parseInt(val("imap-port"), 10) || 993,
    username: val("imap-user"),
    password: val("imap-pass"),
    tls: document.getElementById("imap-tls") ? document.getElementById("imap-tls").checked : true,
  };
  try {
    await api("/api/settings/imap", { method: "PUT", body: payload });
    showToast("IMAP settings saved", "success");
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function testImap() {
  try {
    const result = await api("/api/settings/imap/test", { method: "POST" });
    showToast(result.message || "IMAP test successful", "success");
  } catch (e) {
    showToast("IMAP test failed: " + e.message, "error");
  }
}

/* -- Telegram -- */
async function loadTelegramSettings() {
  try {
    const data = await api("/api/settings/telegram");
    const el = document.getElementById("telegram-chat-id");
    if (el) el.value = data.chat_id || "";
    const statusEl = document.getElementById("telegram-status");
    if (statusEl) {
      statusEl.innerHTML = data.connected
        ? '<span style="color:#34D399;">Connected</span>'
        : '<span style="color:#888;">Not connected</span>';
    }
  } catch (_) { /* ignore */ }
}

async function saveTelegram() {
  const chatId = val("telegram-chat-id");
  if (!chatId) {
    showToast("Please enter a Telegram Chat ID", "error");
    return;
  }
  try {
    await api("/api/settings/telegram", { method: "PUT", body: { chat_id: chatId } });
    showToast("Telegram settings saved", "success");
    loadTelegramSettings();
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function testTelegram() {
  try {
    const result = await api("/api/settings/telegram/test", { method: "POST" });
    showToast(result.message || "Telegram test message sent", "success");
  } catch (e) {
    showToast("Telegram test failed: " + e.message, "error");
  }
}

/* -- API Keys -- */
async function loadApiKeys() {
  try {
    const data = await api("/api/settings/api-keys");
    const keys = Array.isArray(data) ? data : data.keys || data.items || [];
    const container = document.getElementById("api-keys-list");
    if (!container) return;

    let html =
      '<div style="display:flex;gap:8px;margin-bottom:12px;">' +
        '<input id="api-key-name" placeholder="Key name" style="flex:1;background:#12121C;border:1px solid #333;' +
        'color:#fff;padding:8px 12px;border-radius:6px;">' +
        '<button onclick="createApiKey()" style="background:#7C6FFF;color:#fff;border:none;border-radius:6px;' +
        'padding:8px 18px;cursor:pointer;white-space:nowrap;">Create</button>' +
      "</div>";

    if (keys.length === 0) {
      html += '<div style="color:#888;padding:8px;">No API keys created.</div>';
    } else {
      keys.forEach((k) => {
        html +=
          '<div style="display:flex;justify-content:space-between;align-items:center;padding:8px;' +
          'background:#1E1E2E;border-radius:6px;margin-bottom:6px;border:1px solid #2A2A3E;">' +
            '<div>' +
              '<div style="color:#fff;font-size:13px;font-weight:600;">' + esc(k.name || "Unnamed") + "</div>" +
              '<div style="color:#888;font-size:11px;font-family:monospace;">' +
              esc(k.key_preview || k.prefix || (k.key ? k.key.slice(0, 12) + "..." : "")) + "</div>" +
              '<div style="color:#666;font-size:11px;">Created: ' + fmtDate(k.created_at) + "</div>" +
            "</div>" +
            '<button onclick="deleteApiKey(\'' + esc(k.id) + '\')" style="background:#F87171;color:#fff;' +
            'border:none;border-radius:4px;padding:4px 10px;cursor:pointer;font-size:12px;">Delete</button>' +
          "</div>";
      });
    }

    container.innerHTML = html;
  } catch (e) {
    console.error("loadApiKeys:", e);
  }
}

async function createApiKey() {
  const name = val("api-key-name");
  if (!name) {
    showToast("Please enter a key name", "error");
    return;
  }
  try {
    const result = await api("/api/settings/api-keys", { method: "POST", body: { name } });
    if (result.key) {
      openModal("API Key Created", '<div style="text-align:center;">' +
        '<p style="color:#ccc;margin-bottom:12px;">Copy this key now. It will not be shown again.</p>' +
        '<div style="background:#12121C;padding:12px;border-radius:6px;font-family:monospace;color:#34D399;' +
        'word-break:break-all;font-size:14px;">' + esc(result.key) + "</div>" +
        '<button onclick="navigator.clipboard.writeText(\'' + esc(result.key) +
        '\');showToast(\'Copied!\',\'success\')" style="margin-top:12px;background:#7C6FFF;color:#fff;' +
        'border:none;border-radius:6px;padding:8px 20px;cursor:pointer;">Copy</button></div>');
    }
    showToast("API key created", "success");
    loadApiKeys();
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function deleteApiKey(id) {
  if (!confirm("Delete this API key?")) return;
  try {
    await api("/api/settings/api-keys/" + encodeURIComponent(id), { method: "DELETE" });
    showToast("API key deleted", "info");
    loadApiKeys();
  } catch (e) {
    showToast(e.message, "error");
  }
}

/* -- Webhooks -- */
async function loadWebhooks() {
  try {
    const data = await api("/api/settings/webhooks");
    const hooks = Array.isArray(data) ? data : data.webhooks || data.items || [];
    const container = document.getElementById("webhooks-list");
    if (!container) return;

    let html =
      '<div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;">' +
        '<input id="webhook-url" placeholder="Webhook URL" style="flex:2;min-width:200px;background:#12121C;' +
        'border:1px solid #333;color:#fff;padding:8px 12px;border-radius:6px;">' +
        '<select id="webhook-event" style="flex:1;min-width:120px;background:#12121C;border:1px solid #333;' +
        'color:#fff;padding:8px 12px;border-radius:6px;">' +
          '<option value="lead.created">lead.created</option>' +
          '<option value="email.sent">email.sent</option>' +
          '<option value="email.opened">email.opened</option>' +
          '<option value="email.replied">email.replied</option>' +
          '<option value="email.bounced">email.bounced</option>' +
        "</select>" +
        '<button onclick="createWebhook()" style="background:#7C6FFF;color:#fff;border:none;border-radius:6px;' +
        'padding:8px 18px;cursor:pointer;white-space:nowrap;">Add</button>' +
      "</div>";

    if (hooks.length === 0) {
      html += '<div style="color:#888;padding:8px;">No webhooks configured.</div>';
    } else {
      hooks.forEach((h) => {
        html +=
          '<div style="display:flex;justify-content:space-between;align-items:center;padding:8px;' +
          'background:#1E1E2E;border-radius:6px;margin-bottom:6px;border:1px solid #2A2A3E;">' +
            '<div>' +
              '<div style="color:#fff;font-size:13px;">' + esc(h.url) + "</div>" +
              '<div style="color:#888;font-size:11px;">Event: ' + esc(h.event || h.events || "") + "</div>" +
            "</div>" +
            '<button onclick="deleteWebhook(\'' + esc(h.id) + '\')" style="background:#F87171;color:#fff;' +
            'border:none;border-radius:4px;padding:4px 10px;cursor:pointer;font-size:12px;">Delete</button>' +
          "</div>";
      });
    }

    container.innerHTML = html;
  } catch (e) {
    console.error("loadWebhooks:", e);
  }
}

async function createWebhook() {
  const url = val("webhook-url");
  const event = val("webhook-event");
  if (!url) {
    showToast("Please enter a webhook URL", "error");
    return;
  }
  try {
    await api("/api/settings/webhooks", { method: "POST", body: { url, event } });
    showToast("Webhook created", "success");
    loadWebhooks();
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function deleteWebhook(id) {
  if (!confirm("Delete this webhook?")) return;
  try {
    await api("/api/settings/webhooks/" + encodeURIComponent(id), { method: "DELETE" });
    showToast("Webhook deleted", "info");
    loadWebhooks();
  } catch (e) {
    showToast(e.message, "error");
  }
}

/* -- Warmup Status -- */
async function loadWarmupStatus() {
  try {
    const data = await api("/api/settings/warmup");
    const container = document.getElementById("warmup-status");
    if (!container) return;

    const active = data.active || data.enabled || false;
    const day = data.current_day || data.day || 0;
    const totalDays = data.total_days || 30;
    const dailyLimit = data.daily_limit || 0;
    const pct = ((day / Math.max(1, totalDays)) * 100).toFixed(0);

    container.innerHTML =
      '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">' +
        '<span style="color:#ccc;font-size:13px;">Email Warmup</span>' +
        '<span style="color:' + (active ? "#34D399" : "#F87171") + ';font-size:13px;font-weight:600;">' +
        (active ? "Active" : "Inactive") + "</span>" +
      "</div>" +
      '<div style="margin-bottom:8px;">' +
        '<div style="display:flex;justify-content:space-between;margin-bottom:4px;">' +
          '<span style="color:#888;font-size:12px;">Day ' + day + " of " + totalDays + "</span>" +
          '<span style="color:#888;font-size:12px;">' + pct + "%</span>" +
        "</div>" +
        '<div style="background:#2A2A3E;border-radius:4px;height:6px;overflow:hidden;">' +
          '<div style="background:#7C6FFF;height:100%;width:' + pct + '%;transition:width .4s;"></div>' +
        "</div>" +
      "</div>" +
      '<div style="color:#888;font-size:12px;">Daily limit: ' + fmtNum(dailyLimit) + " emails</div>";
  } catch (_) {
    const container = document.getElementById("warmup-status");
    if (container) {
      container.innerHTML = '<div style="color:#888;font-size:13px;">Warmup status unavailable.</div>';
    }
  }
}

/* -- Change Password -- */
async function changePassword() {
  const current = val("pw-current");
  const newPw = val("pw-new");
  const confirm = val("pw-confirm");

  if (!current || !newPw) {
    showToast("Please fill in all password fields", "error");
    return;
  }
  if (newPw.length < 8) {
    showToast("Password must be at least 8 characters", "error");
    return;
  }
  if (newPw !== confirm) {
    showToast("Passwords do not match", "error");
    return;
  }

  try {
    await api("/api/auth/password", { method: "PUT", body: { current_password: current, new_password: newPw } });
    showToast("Password changed successfully", "success");
    document.getElementById("pw-current").value = "";
    document.getElementById("pw-new").value = "";
    document.getElementById("pw-confirm").value = "";
  } catch (e) {
    showToast(e.message, "error");
  }
}

/* ----------------------------------------------------------
   16. FORM HELPERS
   ---------------------------------------------------------- */

function val(id) {
  const el = document.getElementById(id);
  return el ? el.value : "";
}

function setFormValues(form, map) {
  if (!form) return;
  Object.keys(map).forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    if (el.type === "checkbox") {
      el.checked = !!map[id];
    } else {
      el.value = map[id];
    }
  });
}

/* ----------------------------------------------------------
   17. KEYBOARD SHORTCUTS & HASH ROUTING
   ---------------------------------------------------------- */

function handleHashChange() {
  const hash = location.hash.replace(/^#/, "");
  if (hash) showTab(hash);
}

document.addEventListener("keydown", function (e) {
  // Escape to close modal
  if (e.key === "Escape") {
    closeModal();
  }
  // Ctrl+Enter in search box triggers search
  if (e.ctrlKey && e.key === "Enter") {
    if (currentTab === "search") {
      e.preventDefault();
      doSearch();
    }
  }
});

window.addEventListener("hashchange", handleHashChange);

/* ----------------------------------------------------------
   18. CLICK-OUTSIDE HANDLERS
   ---------------------------------------------------------- */

document.addEventListener("click", function (e) {
  // Close notification dropdown when clicking outside
  const bell = document.getElementById("notification-bell");
  const dropdown = document.getElementById("notification-dropdown");
  if (dropdown && dropdown.style.display === "block") {
    if (bell && !bell.contains(e.target) && !dropdown.contains(e.target)) {
      dropdown.style.display = "none";
    }
  }
});

/* ----------------------------------------------------------
   19. SEND EMAIL FOR SPECIFIC LEAD (from lead card)
   ---------------------------------------------------------- */

function openSendEmailModal(idx) {
  const lead = currentLeads[idx];
  if (!lead) return;
  if (!lead.email) {
    showToast("This lead has no email address", "error");
    return;
  }

  const subject = (document.getElementById("email-subject") || {}).value || "";
  const body = (document.getElementById("email-body") || {}).value || "";

  openModal("Send Email to " + (lead.name || lead.email),
    '<div>' +
      '<div style="margin-bottom:12px;">' +
        '<label style="color:#888;font-size:12px;">To</label>' +
        '<div style="color:#fff;font-size:14px;">' + esc(lead.email) + "</div>" +
      "</div>" +
      '<div style="margin-bottom:12px;">' +
        '<label style="color:#888;font-size:12px;">Subject</label>' +
        '<input id="modal-email-subject" value="' + esc(subject) + '" ' +
        'style="width:100%;margin-top:4px;background:#12121C;border:1px solid #333;color:#fff;padding:8px;border-radius:4px;">' +
      "</div>" +
      '<div style="margin-bottom:12px;">' +
        '<label style="color:#888;font-size:12px;">Body</label>' +
        '<textarea id="modal-email-body" rows="8" ' +
        'style="width:100%;margin-top:4px;background:#12121C;border:1px solid #333;color:#fff;padding:8px;' +
        'border-radius:4px;resize:vertical;">' + esc(body) + "</textarea>" +
      "</div>" +
      '<button onclick="sendEmailFromModal(' + idx + ')" style="background:#7C6FFF;color:#fff;border:none;' +
      'border-radius:6px;padding:10px 24px;cursor:pointer;font-size:14px;width:100%;">Send</button>' +
    "</div>"
  );
}

async function sendEmailFromModal(idx) {
  const lead = currentLeads[idx];
  if (!lead) return;

  const subject = (document.getElementById("modal-email-subject") || {}).value || "";
  const body = (document.getElementById("modal-email-body") || {}).value || "";

  if (!subject || !body) {
    showToast("Subject and body are required", "error");
    return;
  }

  try {
    await api("/api/emails/send", {
      method: "POST",
      body: {
        to: lead.email,
        subject,
        body,
        lead_id: lead.id,
      },
    });
    showToast("Email sent to " + lead.email, "success");
    closeModal();
  } catch (e) {
    showToast(e.message, "error");
  }
}

/* ----------------------------------------------------------
   20. LEAD DETAIL MODAL
   ---------------------------------------------------------- */

function openLeadDetail(idx) {
  const lead = currentLeads[idx];
  if (!lead) return;

  const phone = lead.phone || lead.formatted_phone_number || "";
  const email = lead.email || "";
  const website = lead.website || "";
  const address = lead.address || lead.formatted_address || "";
  const ownerName = lead.owner || "";
  const techStack = lead.tech_stack || lead.technologies || [];
  const sc = lead.score != null ? lead.score : "-";

  let techHtml = "";
  if (Array.isArray(techStack) && techStack.length > 0) {
    techStack.forEach((t) => {
      techHtml +=
        '<span style="background:#2A2A3E;color:#A5B4FC;padding:3px 8px;border-radius:4px;font-size:12px;margin:2px;">' +
        esc(t) + "</span>";
    });
  }

  let html =
    '<div style="text-align:center;margin-bottom:16px;">' +
      '<div style="font-size:48px;font-weight:700;color:' + scoreColor(lead.score) + ';">' + esc(String(sc)) + "</div>" +
      '<div style="color:#888;font-size:12px;">Lead Score</div>' +
      '<div style="margin-top:4px;">' + ratingStars(lead.rating) + "</div>" +
    "</div>" +
    '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:16px;">' +
      detailRow("Email", email ? '<a href="mailto:' + esc(email) + '" style="color:#7C6FFF;">' + esc(email) + "</a>" : "-") +
      detailRow("Phone", phone || "-") +
      detailRow("Address", address || "-") +
      detailRow("Website", website ? '<a href="' + esc(safeUrl(website)) + '" target="_blank" style="color:#60A5FA;">' + esc(website) + "</a>" : "-") +
      detailRow("Owner", ownerName || "-") +
    "</div>" +
    (techHtml ? '<div style="margin-bottom:16px;"><span style="color:#888;font-size:12px;">Tech Stack:</span><br>' + techHtml + "</div>" : "") +
    '<div style="display:flex;gap:8px;">' +
      '<button onclick="saveOneLead(' + idx + ');closeModal();" style="flex:1;background:#7C6FFF;color:#fff;border:none;' +
      'border-radius:6px;padding:8px;cursor:pointer;">Save Lead</button>' +
      (email
        ? '<button onclick="closeModal();openSendEmailModal(' + idx + ');" style="flex:1;background:#34D399;color:#000;border:none;' +
          'border-radius:6px;padding:8px;cursor:pointer;">Send Email</button>'
        : "") +
    "</div>";

  openModal(lead.name || "Lead Details", html);
}

function detailRow(label, value) {
  return (
    '<div style="padding:6px 0;">' +
    '<div style="font-size:11px;color:#888;">' + label + "</div>" +
    '<div style="font-size:13px;color:#ccc;">' + value + "</div>" +
    "</div>"
  );
}

/* ----------------------------------------------------------
   21. MULTI-SEARCH CITY CHIPS UI
   ---------------------------------------------------------- */

let multiSearchCities = [];

function addCityChip() {
  const input = document.getElementById("city-chip-input");
  if (!input) return;
  const city = input.value.trim();
  if (!city) return;
  if (multiSearchCities.includes(city)) {
    showToast("City already added", "info");
    return;
  }
  multiSearchCities.push(city);
  input.value = "";
  renderCityChips();
  syncCitiesToField();
}

function removeCityChip(idx) {
  multiSearchCities.splice(idx, 1);
  renderCityChips();
  syncCitiesToField();
}

function renderCityChips() {
  const container = document.getElementById("city-chips");
  if (!container) return;

  if (multiSearchCities.length === 0) {
    container.innerHTML = "";
    return;
  }

  let html = "";
  multiSearchCities.forEach((city, idx) => {
    html +=
      '<span style="display:inline-flex;align-items:center;background:#2A2A3E;color:#ccc;padding:4px 10px;' +
      'border-radius:12px;font-size:12px;margin:2px;">' +
      esc(city) +
      '<span onclick="removeCityChip(' + idx + ')" style="margin-left:6px;cursor:pointer;color:#F87171;' +
      'font-weight:bold;">&times;</span></span>';
  });
  container.innerHTML = html;
}

function syncCitiesToField() {
  const field = document.getElementById("search-cities");
  if (field) field.value = multiSearchCities.join(",");
}

function handleCityChipKey(e) {
  if (e.key === "Enter") {
    e.preventDefault();
    addCityChip();
  }
}

/* ----------------------------------------------------------
   22. PLACEHOLDER HELPERS FOR EMAIL TAB
   ---------------------------------------------------------- */

function renderPlaceholders() {
  const container = document.getElementById("placeholder-list");
  if (!container) return;

  const placeholders = ["name", "company", "city", "email", "phone", "website", "address", "owner"];

  let html = '<span style="color:#888;font-size:12px;margin-right:4px;">Insert:</span>';
  placeholders.forEach((ph) => {
    html +=
      '<button onclick="insertPlaceholder(\'' + ph + '\')" style="background:#2A2A3E;color:#A5B4FC;' +
      'border:1px solid #333;border-radius:4px;padding:2px 8px;cursor:pointer;font-size:11px;margin:2px;">' +
      "{{" + ph + "}}</button>";
  });

  container.innerHTML = html;
}

/* ----------------------------------------------------------
   23. EMAIL TEMPLATE LIVE PREVIEW (input listeners)
   ---------------------------------------------------------- */

function initEmailListeners() {
  const subjectEl = document.getElementById("email-subject");
  const bodyEl = document.getElementById("email-body");

  if (subjectEl) {
    subjectEl.addEventListener("input", debounce(updateEmailPreview, 300));
  }
  if (bodyEl) {
    bodyEl.addEventListener("input", debounce(updateEmailPreview, 300));
  }
}

/* ----------------------------------------------------------
   24. EXPORT / IMPORT UTILITIES
   ---------------------------------------------------------- */

function exportLeadsJson() {
  if (currentLeads.length === 0) {
    showToast("No leads to export", "error");
    return;
  }
  const blob = new Blob([JSON.stringify(currentLeads, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "leads_" + new Date().toISOString().slice(0, 10) + ".json";
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
  showToast("Leads exported as JSON", "success");
}

/* ----------------------------------------------------------
   25. SEARCH RESULT COUNT & SUMMARY
   ---------------------------------------------------------- */

function renderSearchSummary() {
  const container = document.getElementById("search-summary");
  if (!container) return;

  if (currentLeads.length === 0) {
    container.innerHTML = "";
    return;
  }

  const withEmail = currentLeads.filter((l) => l.email).length;
  const withPhone = currentLeads.filter((l) => l.phone || l.formatted_phone_number).length;
  const avgScore =
    currentLeads.reduce((sum, l) => sum + (l.score || 0), 0) / currentLeads.length;

  container.innerHTML =
    '<div style="display:flex;gap:16px;flex-wrap:wrap;padding:8px 0;font-size:13px;color:#aaa;">' +
      "<span>Total: <strong style=\"color:#fff;\">" + currentLeads.length + "</strong></span>" +
      "<span>With email: <strong style=\"color:#34D399;\">" + withEmail + "</strong></span>" +
      "<span>With phone: <strong style=\"color:#60A5FA;\">" + withPhone + "</strong></span>" +
      "<span>Avg score: <strong style=\"color:" + scoreColor(avgScore) + ";\">" + avgScore.toFixed(1) + "</strong></span>" +
    "</div>";
}

/* ----------------------------------------------------------
   26. AUTO-REFRESH INTERVALS
   ---------------------------------------------------------- */

let _notificationInterval = null;

function startAutoRefresh() {
  // Refresh notifications every 60 seconds
  _notificationInterval = setInterval(loadNotifications, 60000);
}

function stopAutoRefresh() {
  if (_notificationInterval) {
    clearInterval(_notificationInterval);
    _notificationInterval = null;
  }
}

/* ----------------------------------------------------------
   27. VISIBILITY CHANGE HANDLER
   ---------------------------------------------------------- */

document.addEventListener("visibilitychange", function () {
  if (document.hidden) {
    stopAutoRefresh();
  } else {
    startAutoRefresh();
    // Refresh current tab data when user returns
    if (currentTab === "tracking") loadTracking();
    if (currentTab === "followup") loadFollowups();
  }
});

/* ----------------------------------------------------------
   28. RESPONSIVE HELPERS
   ---------------------------------------------------------- */

function isMobile() {
  return window.innerWidth < 768;
}

/* ----------------------------------------------------------
   29. ERROR BOUNDARY
   ---------------------------------------------------------- */

window.addEventListener("unhandledrejection", function (e) {
  console.error("Unhandled promise rejection:", e.reason);
  if (e.reason && e.reason.message && e.reason.message !== "Unauthorized") {
    showToast("An error occurred: " + e.reason.message, "error");
  }
});

/* ----------------------------------------------------------
   30. INITIALISATION
   ---------------------------------------------------------- */

document.addEventListener("DOMContentLoaded", function () {
  // Load user and stats
  loadUser();
  loadStats();

  // Set up email listeners
  initEmailListeners();
  renderPlaceholders();

  // Determine active tab from URL hash
  const hash = location.hash.replace(/^#/, "");
  showTab(hash || "search");

  // Start notification auto-refresh
  startAutoRefresh();

  // Attach tab button listeners (for tabs rendered server-side)
  document.querySelectorAll(".tab-btn").forEach(function (btn) {
    btn.addEventListener("click", function () {
      showTab(btn.dataset.tab);
    });
  });

  // Attach form event listeners where elements exist
  attachListener("search-btn", "click", doSearch);
  attachListener("multi-search-btn", "click", doMultiSearch);
  attachListener("save-all-btn", "click", saveAllLeads);
  attachListener("copy-csv-btn", "click", copyLeadsCsv);
  attachListener("export-json-btn", "click", exportLeadsJson);
  attachListener("send-all-btn", "click", sendAllEmails);
  attachListener("schedule-all-btn", "click", scheduleAllFollowups);
  attachListener("ab-toggle-btn", "click", toggleAbTest);
  attachListener("notification-bell", "click", toggleNotificationDropdown);
  attachListener("logout-btn", "click", doLogout);
  attachListener("smtp-save-btn", "click", saveSmtp);
  attachListener("smtp-test-btn", "click", testSmtp);
  attachListener("imap-save-btn", "click", saveImap);
  attachListener("imap-test-btn", "click", testImap);
  attachListener("telegram-save-btn", "click", saveTelegram);
  attachListener("telegram-test-btn", "click", testTelegram);
  attachListener("change-pw-btn", "click", changePassword);
  attachListener("add-city-btn", "click", addCityChip);
  attachListener("city-chip-input", "keydown", handleCityChipKey);

  // Search on Enter key in search inputs
  attachListener("search-q", "keydown", function (e) {
    if (e.key === "Enter") { e.preventDefault(); doSearch(); }
  });
  attachListener("search-city", "keydown", function (e) {
    if (e.key === "Enter") { e.preventDefault(); doSearch(); }
  });
});

function attachListener(id, event, handler) {
  const el = document.getElementById(id);
  if (el) el.addEventListener(event, handler);
}

async function loadStats() {
  try {
    const data = await api("/api/analytics/stats");
    const el = document.getElementById("dashboard-stats");
    if (!el) return;

    el.innerHTML =
      '<div style="display:flex;gap:12px;flex-wrap:wrap;">' +
        miniStat("Leads", fmtNum(data.total_leads || data.leads || 0), "#7C6FFF") +
        miniStat("Emails", fmtNum(data.emails_sent || 0), "#34D399") +
        miniStat("Open Rate", fmtPct(data.open_rate || 0), "#60A5FA") +
        miniStat("Replies", fmtNum(data.replies || data.replied || 0), "#A78BFA") +
      "</div>";
  } catch (_) {
    // Stats may not be available yet
  }
}

function miniStat(label, value, color) {
  return (
    '<div style="background:#1E1E2E;border-radius:8px;padding:10px 16px;border:1px solid #2A2A3E;min-width:100px;">' +
    '<div style="font-size:10px;color:#888;">' + label + "</div>" +
    '<div style="font-size:18px;font-weight:700;color:' + color + ';">' + value + "</div>" +
    "</div>"
  );
}

/* ----------------------------------------------------------
   ADMIN — Besucher-Tracking
   ---------------------------------------------------------- */
let _adminMap = null;
let _adminMarkers = null;

function formatDuration(seconds) {
  if (seconds < 60) return seconds + "s";
  if (seconds < 3600) return Math.floor(seconds / 60) + "m " + (seconds % 60) + "s";
  return Math.floor(seconds / 3600) + "h " + Math.floor((seconds % 3600) / 60) + "m";
}

async function loadAdmin() {
  try {
    const data = await api("/api/admin/visitors");
    renderAdminKPIs(data.kpis);
    renderAdminMap(data.active_sessions);
    renderAdminVisitors(data.active_sessions);
    renderAdminTimeline(data.hourly_timeline);
    renderAdminSections(data.section_counts);
  } catch (e) {
    console.error("loadAdmin:", e);
  }

  // Auto-refresh alle 15s
  if (!window._adminRefresh) {
    window._adminRefresh = setInterval(function () {
      if (currentTab === "admin") loadAdmin();
    }, 15000);
  }
}

function renderAdminKPIs(kpis) {
  const el = (id, v) => { const e = document.getElementById(id); if (e) e.textContent = v; };
  el("admin-active", kpis.active_now);
  el("admin-today", kpis.today_total);
  el("admin-duration", formatDuration(kpis.avg_duration));
  el("admin-bounce", kpis.bounce_rate + "%");
}

function renderAdminMap(sessions) {
  const container = document.getElementById("admin-map");
  if (!container) return;

  if (!_adminMap) {
    _adminMap = L.map(container, { zoomControl: true, attributionControl: false }).setView([30, 0], 2);
    L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
      maxZoom: 18,
    }).addTo(_adminMap);
    _adminMarkers = L.layerGroup().addTo(_adminMap);
    // Fix tile rendering after tab switch
    setTimeout(function () { _adminMap.invalidateSize(); }, 200);
  } else {
    _adminMap.invalidateSize();
  }

  _adminMarkers.clearLayers();
  sessions.forEach(function (s) {
    if (!s.lat && !s.lon) return;
    L.circleMarker([s.lat, s.lon], {
      radius: 7,
      color: "#7C6FFF",
      fillColor: "#7C6FFF",
      fillOpacity: 0.7,
      weight: 2,
    })
      .bindPopup(
        "<b>" + esc(s.city) + ", " + esc(s.country) + "</b><br>" +
        "Bereich: " + esc(s.section) + "<br>" +
        "Dauer: " + formatDuration(s.duration)
      )
      .addTo(_adminMarkers);
  });
}

function renderAdminVisitors(sessions) {
  const el = document.getElementById("admin-visitor-list");
  if (!el) return;
  if (!sessions.length) {
    el.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text3)">Keine aktiven Besucher</div>';
    return;
  }
  el.innerHTML = sessions
    .sort(function (a, b) { return b.duration - a.duration; })
    .map(function (s) {
      return (
        '<div class="admin-visitor-row">' +
        '<span class="pulse-dot"></span>' +
        '<span style="flex:1;color:var(--text0)">' + esc(s.city) + ", " + esc(s.country) + "</span>" +
        '<span class="badge badge-purple">' + esc(s.section) + "</span>" +
        '<span style="color:var(--text3);min-width:60px;text-align:right">' + formatDuration(s.duration) + "</span>" +
        "</div>"
      );
    })
    .join("");
}

function renderAdminTimeline(timeline) {
  const el = document.getElementById("admin-timeline");
  if (!el) return;

  const hours = [];
  for (let h = 0; h < 24; h++) hours.push(String(h).padStart(2, "0") + ":00");

  const values = hours.map(function (h) { return timeline[h] || 0; });
  const max = Math.max.apply(null, values) || 1;

  el.innerHTML = hours
    .map(function (h, i) {
      var pct = Math.max((values[i] / max) * 100, 2);
      return (
        '<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:2px" title="' + h + ": " + values[i] + ' Besucher">' +
        '<div style="width:100%;background:var(--accent);border-radius:2px;opacity:' + (values[i] ? 1 : 0.15) + ';height:' + pct + '%"></div>' +
        '<span style="font-size:9px;color:var(--text3)">' + (i % 3 === 0 ? h.slice(0, 2) : "") + "</span>" +
        "</div>"
      );
    })
    .join("");
}

function renderAdminSections(sections) {
  const el = document.getElementById("admin-sections");
  if (!el) return;

  var entries = Object.entries(sections).sort(function (a, b) { return b[1] - a[1]; });
  var total = entries.reduce(function (s, e) { return s + e[1]; }, 0) || 1;

  if (!entries.length) {
    el.innerHTML = '<div style="padding:12px;color:var(--text3);text-align:center">Keine Daten</div>';
    return;
  }

  el.innerHTML = entries
    .map(function (e) {
      var pct = Math.round((e[1] / total) * 100);
      return (
        '<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">' +
        '<span style="min-width:80px;font-size:12px;color:var(--text2)">' + esc(e[0]) + "</span>" +
        '<div style="flex:1;height:6px;background:var(--bg4);border-radius:3px;overflow:hidden">' +
        '<div class="admin-section-bar" style="width:' + pct + '%"></div></div>' +
        '<span style="min-width:30px;font-size:11px;color:var(--text3);text-align:right">' + e[1] + "</span>" +
        "</div>"
      );
    })
    .join("");
}

async function adminCleanup() {
  try {
    var res = await api("/api/admin/cleanup", { method: "POST" });
    showToast(res.removed + " alte Sessions entfernt", "success");
  } catch (e) {
    showToast("Fehler: " + e.message, "error");
  }
}
