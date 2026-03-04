/* =========================================================
   LeadFinder Pro v3 — Kanban Board
   Dark-theme drag-and-drop pipeline board.
   Accent: #7C6FFF
   Depends on app.js globals: api(), esc(), showToast(),
     openModal(), closeModal()
   ========================================================= */

(function () {
  "use strict";

  /* ----- state ------------------------------------------ */
  let stages = [];
  let leadsByStage = {};          // { stageId: [lead, ...] }
  let allTags = [];               // cached tag list
  let draggedLeadId = null;
  let draggedCard = null;         // DOM ref while dragging
  let touchClone = null;          // ghost element for touch drag
  let touchOriginStage = null;

  const BOARD_SEL = "#kanban-board";
  const ACCENT = "#7C6FFF";

  /* =========================================================
     Public bootstrap
     ========================================================= */

  window.initKanban = async function initKanban() {
    try {
      const [stagesRes, leadsRes, tagsRes] = await Promise.all([
        api("GET", "/api/kanban/stages"),
        api("GET", "/api/kanban"),
        api("GET", "/api/tags").catch(() => []),
      ]);

      stages = stagesRes || [];
      allTags = tagsRes || [];

      // Bucket leads by stage id
      leadsByStage = {};
      stages.forEach((s) => (leadsByStage[s.id] = []));
      (leadsRes || []).forEach((lead) => {
        const sid = lead.stage_id || (stages[0] && stages[0].id);
        if (!leadsByStage[sid]) leadsByStage[sid] = [];
        leadsByStage[sid].push(lead);
      });

      renderKanbanBoard(stages, leadsByStage);
    } catch (err) {
      console.error("[kanban] init failed", err);
      showToast("Failed to load Kanban board", "error");
    }
  };

  /* =========================================================
     Render full board
     ========================================================= */

  function renderKanbanBoard(stages, leadsByStage) {
    const board = document.querySelector(BOARD_SEL);
    if (!board) return;
    board.innerHTML = "";
    board.classList.add("kanban-board");

    stages.forEach((stage) => {
      const leads = leadsByStage[stage.id] || [];
      const col = createColumn(stage, leads);
      board.appendChild(col);
    });

    injectKanbanStyles();
  }

  /* ----- column ----------------------------------------- */

  function createColumn(stage, leads) {
    const col = document.createElement("div");
    col.className = "kanban-col";
    col.dataset.stageId = stage.id;

    const color = stage.color || ACCENT;

    // Header
    const header = document.createElement("div");
    header.className = "kanban-col-header";
    header.innerHTML =
      '<span class="kanban-col-color" style="background:' + esc(color) + '"></span>' +
      '<span class="kanban-col-name">' + esc(stage.name) + "</span>" +
      '<span class="kanban-col-count">' + leads.length + "</span>" +
      '<button class="kanban-col-toggle" aria-label="Collapse column">' +
      '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">' +
      '<polyline points="6 9 12 15 18 9"></polyline></svg></button>';
    col.appendChild(header);

    // Toggle collapse (mobile-friendly)
    const toggleBtn = header.querySelector(".kanban-col-toggle");
    toggleBtn.addEventListener("click", () => {
      col.classList.toggle("collapsed");
    });

    // Drop zone
    const zone = document.createElement("div");
    zone.className = "kanban-drop-zone";
    zone.dataset.stageId = stage.id;

    // Mouse drag events
    zone.addEventListener("dragover", onDragOver);
    zone.addEventListener("dragenter", onDragEnter);
    zone.addEventListener("dragleave", onDragLeave);
    zone.addEventListener("drop", onDrop);

    // Touch drag events
    zone.addEventListener("touchmove", onTouchMoveZone, { passive: false });
    zone.addEventListener("touchend", onTouchEndZone);

    leads.forEach((lead) => zone.appendChild(createCard(lead)));
    col.appendChild(zone);

    return col;
  }

  /* ----- card ------------------------------------------- */

  function createCard(lead) {
    const card = document.createElement("div");
    card.className = "kanban-card";
    card.draggable = true;
    card.dataset.leadId = lead.id;

    const scoreCls = lead.score >= 80 ? "high" : lead.score >= 50 ? "med" : "low";
    const tagsHtml = (lead.tags || [])
      .map((t) => '<span class="kanban-tag" style="background:' + esc(t.color || "#444") + '">' + esc(t.name) + "</span>")
      .join("");

    card.innerHTML =
      '<div class="kanban-card-top">' +
        '<span class="kanban-card-name">' + esc(lead.name || "Unnamed") + "</span>" +
        '<span class="kanban-score ' + scoreCls + '">' + (lead.score != null ? lead.score : "--") + "</span>" +
      "</div>" +
      (lead.email ? '<div class="kanban-card-email">' + esc(lead.email) + "</div>" : "") +
      (tagsHtml ? '<div class="kanban-card-tags">' + tagsHtml + "</div>" : "") +
      '<div class="kanban-card-actions">' +
        '<button class="kanban-btn-detail" title="Details" data-lead-id="' + lead.id + '">' +
          '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">' +
          '<circle cx="12" cy="12" r="10"></circle><line x1="12" y1="16" x2="12" y2="12"></line>' +
          '<line x1="12" y1="8" x2="12.01" y2="8"></line></svg>' +
        "</button>" +
        '<button class="kanban-btn-delete" title="Delete" data-lead-id="' + lead.id + '">' +
          '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">' +
          '<polyline points="3 6 5 6 21 6"></polyline><path d="M19 6l-1 14H6L5 6"></path>' +
          '<path d="M10 11v6"></path><path d="M14 11v6"></path>' +
          '<path d="M9 6V4h6v2"></path></svg>' +
        "</button>" +
      "</div>";

    // Mouse drag
    card.addEventListener("dragstart", onDragStart);
    card.addEventListener("dragend", onDragEnd);

    // Touch drag
    card.addEventListener("touchstart", onTouchStart, { passive: false });
    card.addEventListener("touchmove", onTouchMove, { passive: false });
    card.addEventListener("touchend", onTouchEnd);

    // Card click -> detail
    card.querySelector(".kanban-btn-detail").addEventListener("click", (e) => {
      e.stopPropagation();
      openLeadDetail(lead.id);
    });
    card.addEventListener("click", () => openLeadDetail(lead.id));

    // Delete
    card.querySelector(".kanban-btn-delete").addEventListener("click", (e) => {
      e.stopPropagation();
      confirmDeleteLead(lead.id, card);
    });

    return card;
  }

  /* =========================================================
     HTML5 Drag & Drop (mouse)
     ========================================================= */

  function onDragStart(e) {
    draggedLeadId = this.dataset.leadId;
    draggedCard = this;
    e.dataTransfer.effectAllowed = "move";
    e.dataTransfer.setData("text/plain", draggedLeadId);
    requestAnimationFrame(() => this.classList.add("dragging"));
  }

  function onDragEnd() {
    this.classList.remove("dragging");
    clearHighlights();
    draggedLeadId = null;
    draggedCard = null;
  }

  function onDragEnter(e) {
    e.preventDefault();
    this.classList.add("drop-highlight");
  }

  function onDragOver(e) {
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    this.classList.add("drop-highlight");
  }

  function onDragLeave(e) {
    // Only remove if truly leaving the zone (not entering a child)
    if (!this.contains(e.relatedTarget)) {
      this.classList.remove("drop-highlight");
    }
  }

  async function onDrop(e) {
    e.preventDefault();
    this.classList.remove("drop-highlight");

    const leadId = e.dataTransfer.getData("text/plain");
    const newStageId = this.dataset.stageId;
    if (!leadId || !newStageId) return;

    await moveLead(leadId, newStageId, draggedCard, this);
  }

  /* =========================================================
     Touch Drag support (mobile)
     ========================================================= */

  function onTouchStart(e) {
    if (e.target.closest(".kanban-btn-detail, .kanban-btn-delete")) return;
    const touch = e.touches[0];
    draggedLeadId = this.dataset.leadId;
    draggedCard = this;
    touchOriginStage = this.closest(".kanban-drop-zone")?.dataset.stageId;

    // Create ghost clone
    touchClone = this.cloneNode(true);
    touchClone.classList.add("kanban-touch-ghost");
    const rect = this.getBoundingClientRect();
    touchClone.style.width = rect.width + "px";
    touchClone.style.left = touch.clientX - rect.width / 2 + "px";
    touchClone.style.top = touch.clientY - 20 + "px";
    document.body.appendChild(touchClone);

    this.classList.add("dragging");
    e.preventDefault();
  }

  function onTouchMove(e) {
    if (!touchClone) return;
    e.preventDefault();
    const touch = e.touches[0];
    const rect = touchClone.getBoundingClientRect();
    touchClone.style.left = touch.clientX - rect.width / 2 + "px";
    touchClone.style.top = touch.clientY - 20 + "px";

    // Highlight the zone under the finger
    clearHighlights();
    const el = document.elementFromPoint(touch.clientX, touch.clientY);
    const zone = el?.closest(".kanban-drop-zone");
    if (zone) zone.classList.add("drop-highlight");
  }

  function onTouchEnd(e) {
    if (!touchClone) return;
    const touch = e.changedTouches[0];
    const el = document.elementFromPoint(touch.clientX, touch.clientY);
    const zone = el?.closest(".kanban-drop-zone");

    if (zone && draggedLeadId) {
      const newStageId = zone.dataset.stageId;
      moveLead(draggedLeadId, newStageId, draggedCard, zone);
    }

    cleanupTouch();
  }

  // Zone-level touch handlers (fallback for older browsers)
  function onTouchMoveZone(e) {
    if (touchClone) e.preventDefault();
  }

  function onTouchEndZone() {
    // handled by card touchend
  }

  function cleanupTouch() {
    if (touchClone) {
      touchClone.remove();
      touchClone = null;
    }
    if (draggedCard) {
      draggedCard.classList.remove("dragging");
    }
    clearHighlights();
    draggedLeadId = null;
    draggedCard = null;
    touchOriginStage = null;
  }

  function clearHighlights() {
    document.querySelectorAll(".drop-highlight").forEach((el) =>
      el.classList.remove("drop-highlight")
    );
  }

  /* =========================================================
     Move lead between stages
     ========================================================= */

  async function moveLead(leadId, newStageId, card, zone) {
    // Find current stage
    const oldZone = card?.closest(".kanban-drop-zone");
    const oldStageId = oldZone?.dataset.stageId;

    if (oldStageId === newStageId) return; // same column, no-op

    try {
      await api("POST", "/api/kanban/move", {
        lead_id: parseInt(leadId, 10),
        stage_id: parseInt(newStageId, 10),
      });

      // Move card in DOM
      if (card && zone) {
        zone.appendChild(card);
      }

      // Update internal state
      if (oldStageId && leadsByStage[oldStageId]) {
        leadsByStage[oldStageId] = leadsByStage[oldStageId].filter(
          (l) => String(l.id) !== String(leadId)
        );
      }
      const movedLead = findLeadInState(leadId);
      if (movedLead) {
        movedLead.stage_id = parseInt(newStageId, 10);
        if (!leadsByStage[newStageId]) leadsByStage[newStageId] = [];
        // Avoid duplicates
        if (!leadsByStage[newStageId].some((l) => String(l.id) === String(leadId))) {
          leadsByStage[newStageId].push(movedLead);
        }
      }

      updateColumnCounts();
      showToast("Lead moved successfully", "success");
    } catch (err) {
      console.error("[kanban] move failed", err);
      showToast("Failed to move lead", "error");
      // Revert: put card back
      if (card && oldZone) {
        oldZone.appendChild(card);
      }
    }
  }

  function findLeadInState(leadId) {
    for (const sid in leadsByStage) {
      const found = leadsByStage[sid].find((l) => String(l.id) === String(leadId));
      if (found) return found;
    }
    return null;
  }

  function updateColumnCounts() {
    document.querySelectorAll(".kanban-col").forEach((col) => {
      const sid = col.dataset.stageId;
      const zone = col.querySelector(".kanban-drop-zone");
      const count = zone ? zone.querySelectorAll(".kanban-card").length : 0;
      const badge = col.querySelector(".kanban-col-count");
      if (badge) badge.textContent = count;
    });
  }

  /* =========================================================
     Lead Detail Modal
     ========================================================= */

  window.openLeadDetail = async function openLeadDetail(leadId) {
    try {
      const lead = await api("GET", "/api/leads/" + leadId);
      renderDetailModal(lead);
    } catch (err) {
      console.error("[kanban] detail fetch failed", err);
      showToast("Failed to load lead details", "error");
    }
  };

  function renderDetailModal(lead) {
    const stageName = (sid) => {
      const s = stages.find((st) => String(st.id) === String(sid));
      return s ? s.name : "Unknown";
    };

    const stageOptions = stages
      .map(
        (s) =>
          '<option value="' + s.id + '"' +
          (String(s.id) === String(lead.stage_id) ? " selected" : "") +
          ">" + esc(s.name) + "</option>"
      )
      .join("");

    const currentTagIds = new Set((lead.tags || []).map((t) => String(t.id)));

    const tagChips = (lead.tags || [])
      .map(
        (t) =>
          '<span class="detail-tag" style="background:' + esc(t.color || "#444") + '">' +
          esc(t.name) +
          '<button class="detail-tag-rm" data-tag-id="' + t.id + '" title="Remove tag">&times;</button>' +
          "</span>"
      )
      .join("");

    const availableTags = allTags
      .filter((t) => !currentTagIds.has(String(t.id)))
      .map(
        (t) =>
          '<option value="' + t.id + '">' + esc(t.name) + "</option>"
      )
      .join("");

    const scoreCls = lead.score >= 80 ? "high" : lead.score >= 50 ? "med" : "low";

    const html =
      '<div class="lead-detail">' +
        '<div class="lead-detail-header">' +
          '<h2>' + esc(lead.name || "Unnamed Lead") + "</h2>" +
          '<span class="kanban-score ' + scoreCls + ' big">' +
            (lead.score != null ? lead.score : "--") +
          "</span>" +
        "</div>" +

        '<div class="lead-detail-grid">' +
          '<div class="lead-detail-field"><label>Email</label><span>' + esc(lead.email || "--") + "</span></div>" +
          '<div class="lead-detail-field"><label>Phone</label><span>' + esc(lead.phone || "--") + "</span></div>" +
          '<div class="lead-detail-field"><label>Company</label><span>' + esc(lead.company || "--") + "</span></div>" +
          '<div class="lead-detail-field"><label>Source</label><span>' + esc(lead.source || "--") + "</span></div>" +
          '<div class="lead-detail-field"><label>Created</label><span>' + formatDate(lead.created_at) + "</span></div>" +
          '<div class="lead-detail-field"><label>Updated</label><span>' + formatDate(lead.updated_at) + "</span></div>" +
        "</div>" +

        '<div class="lead-detail-section">' +
          "<label>Stage</label>" +
          '<select id="detail-stage-select" class="detail-select">' + stageOptions + "</select>" +
        "</div>" +

        '<div class="lead-detail-section">' +
          "<label>Tags</label>" +
          '<div id="detail-tag-list" class="detail-tag-list">' + tagChips + "</div>" +
          (availableTags
            ? '<div class="detail-tag-add">' +
              '<select id="detail-tag-add-select" class="detail-select">' +
                '<option value="">Add tag...</option>' + availableTags +
              "</select>" +
              '<button id="detail-tag-add-btn" class="btn-accent btn-sm">Add</button>' +
              "</div>"
            : "") +
        "</div>" +

        '<div class="lead-detail-section">' +
          "<label>Notes</label>" +
          '<textarea id="detail-notes" class="detail-textarea" rows="4" placeholder="Add notes...">' +
            esc(lead.notes || "") +
          "</textarea>" +
          '<button id="detail-save-notes" class="btn-accent btn-sm">Save Notes</button>' +
        "</div>" +

        '<div class="lead-detail-footer">' +
          '<button id="detail-convert-btn" class="btn-accent">' +
            (lead.converted ? "Converted" : "Mark as Converted") +
          "</button>" +
          '<button id="detail-delete-btn" class="btn-danger">Delete Lead</button>' +
        "</div>" +
      "</div>";

    openModal("Lead Details", html);

    // -- Wire up events inside modal --

    const leadId = lead.id;

    // Stage change
    const stageSelect = document.getElementById("detail-stage-select");
    if (stageSelect) {
      stageSelect.addEventListener("change", async () => {
        const newStageId = stageSelect.value;
        try {
          await api("POST", "/api/kanban/move", {
            lead_id: parseInt(leadId, 10),
            stage_id: parseInt(newStageId, 10),
          });
          showToast("Stage updated", "success");
          closeModal();
          await window.initKanban(); // refresh board
        } catch (err) {
          showToast("Failed to update stage", "error");
        }
      });
    }

    // Save notes
    const saveNotesBtn = document.getElementById("detail-save-notes");
    if (saveNotesBtn) {
      saveNotesBtn.addEventListener("click", () => {
        const notes = document.getElementById("detail-notes").value;
        updateLeadNotes(leadId, notes);
      });
    }

    // Add tag
    const addTagBtn = document.getElementById("detail-tag-add-btn");
    if (addTagBtn) {
      addTagBtn.addEventListener("click", () => {
        const sel = document.getElementById("detail-tag-add-select");
        const tagId = sel?.value;
        if (tagId) addTagToLead(leadId, tagId);
      });
    }

    // Remove tag buttons
    document.querySelectorAll(".detail-tag-rm").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        removeTagFromLead(leadId, btn.dataset.tagId);
      });
    });

    // Convert
    const convertBtn = document.getElementById("detail-convert-btn");
    if (convertBtn && !lead.converted) {
      convertBtn.addEventListener("click", () => markConverted(leadId));
    } else if (convertBtn) {
      convertBtn.disabled = true;
      convertBtn.classList.add("btn-disabled");
    }

    // Delete
    const deleteBtn = document.getElementById("detail-delete-btn");
    if (deleteBtn) {
      deleteBtn.addEventListener("click", () => {
        confirmDeleteLead(leadId, null);
      });
    }
  }

  /* =========================================================
     Lead CRUD helpers
     ========================================================= */

  async function updateLeadNotes(leadId, notes) {
    try {
      await api("PUT", "/api/leads/" + leadId, { notes: notes });
      // Update local state
      const lead = findLeadInState(leadId);
      if (lead) lead.notes = notes;
      showToast("Notes saved", "success");
    } catch (err) {
      console.error("[kanban] save notes failed", err);
      showToast("Failed to save notes", "error");
    }
  }

  async function addTagToLead(leadId, tagId) {
    try {
      await api("POST", "/api/leads/" + leadId + "/tags", {
        tag_id: parseInt(tagId, 10),
      });
      showToast("Tag added", "success");
      closeModal();
      // Refresh the board to reflect tag changes
      await window.initKanban();
      openLeadDetail(leadId);
    } catch (err) {
      console.error("[kanban] add tag failed", err);
      showToast("Failed to add tag", "error");
    }
  }

  async function removeTagFromLead(leadId, tagId) {
    try {
      await api("DELETE", "/api/leads/" + leadId + "/tags/" + tagId);
      showToast("Tag removed", "success");
      closeModal();
      await window.initKanban();
      openLeadDetail(leadId);
    } catch (err) {
      console.error("[kanban] remove tag failed", err);
      showToast("Failed to remove tag", "error");
    }
  }

  async function deleteLead(leadId) {
    try {
      await api("DELETE", "/api/leads/" + leadId);

      // Remove from local state
      for (const sid in leadsByStage) {
        leadsByStage[sid] = leadsByStage[sid].filter(
          (l) => String(l.id) !== String(leadId)
        );
      }

      // Remove card from DOM
      const card = document.querySelector('.kanban-card[data-lead-id="' + leadId + '"]');
      if (card) card.remove();

      updateColumnCounts();
      closeModal();
      showToast("Lead deleted", "success");
    } catch (err) {
      console.error("[kanban] delete failed", err);
      showToast("Failed to delete lead", "error");
    }
  }

  function confirmDeleteLead(leadId, card) {
    const html =
      '<div class="confirm-dialog">' +
        "<p>Are you sure you want to delete this lead? This action cannot be undone.</p>" +
        '<div class="confirm-actions">' +
          '<button id="confirm-del-yes" class="btn-danger">Delete</button>' +
          '<button id="confirm-del-no" class="btn-accent">Cancel</button>' +
        "</div>" +
      "</div>";

    openModal("Confirm Delete", html);

    document.getElementById("confirm-del-yes").addEventListener("click", () => {
      deleteLead(leadId);
    });
    document.getElementById("confirm-del-no").addEventListener("click", () => {
      closeModal();
    });
  }

  async function markConverted(leadId) {
    try {
      await api("PUT", "/api/leads/" + leadId, { converted: true });
      showToast("Lead marked as converted", "success");
      closeModal();
      await window.initKanban();
    } catch (err) {
      console.error("[kanban] convert failed", err);
      showToast("Failed to mark as converted", "error");
    }
  }

  /* =========================================================
     Utilities
     ========================================================= */

  function formatDate(iso) {
    if (!iso) return "--";
    try {
      const d = new Date(iso);
      return d.toLocaleDateString(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
      });
    } catch {
      return iso;
    }
  }

  /* =========================================================
     Scoped styles (injected once)
     ========================================================= */

  let stylesInjected = false;

  function injectKanbanStyles() {
    if (stylesInjected) return;
    stylesInjected = true;

    const css = `
/* ---- Kanban Board ---- */
.kanban-board {
  display: flex;
  gap: 16px;
  padding: 16px 0;
  overflow-x: auto;
  min-height: 70vh;
  -webkit-overflow-scrolling: touch;
}

/* ---- Column ---- */
.kanban-col {
  flex: 0 0 290px;
  min-width: 260px;
  background: #1a1a2e;
  border-radius: 10px;
  display: flex;
  flex-direction: column;
  max-height: calc(100vh - 160px);
  transition: opacity .2s;
}
.kanban-col.collapsed .kanban-drop-zone {
  display: none;
}
.kanban-col.collapsed .kanban-col-toggle svg {
  transform: rotate(-90deg);
}

.kanban-col-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 14px 14px 10px;
  border-bottom: 1px solid rgba(255,255,255,0.06);
  user-select: none;
}
.kanban-col-color {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  flex-shrink: 0;
}
.kanban-col-name {
  font-weight: 600;
  font-size: 14px;
  color: #e0e0e0;
  flex: 1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.kanban-col-count {
  background: ${ACCENT};
  color: #fff;
  font-size: 11px;
  font-weight: 700;
  padding: 2px 8px;
  border-radius: 12px;
  min-width: 22px;
  text-align: center;
}
.kanban-col-toggle {
  background: none;
  border: none;
  color: #888;
  cursor: pointer;
  padding: 2px;
  display: none;
}

/* ---- Drop Zone ---- */
.kanban-drop-zone {
  flex: 1;
  padding: 8px 10px 12px;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-height: 60px;
  transition: border .15s;
  border: 2px solid transparent;
  border-radius: 0 0 10px 10px;
}
.kanban-drop-zone.drop-highlight {
  border: 2px dashed ${ACCENT};
  background: rgba(124, 111, 255, 0.06);
}

/* ---- Card ---- */
.kanban-card {
  background: #16162a;
  border: 1px solid rgba(255,255,255,0.06);
  border-radius: 8px;
  padding: 10px 12px;
  cursor: grab;
  transition: transform .15s, box-shadow .15s, opacity .15s;
  position: relative;
}
.kanban-card:hover {
  border-color: ${ACCENT}44;
  box-shadow: 0 2px 12px rgba(124,111,255,0.12);
}
.kanban-card.dragging {
  opacity: 0.35;
  transform: scale(0.96);
}
.kanban-card-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
  margin-bottom: 4px;
}
.kanban-card-name {
  font-weight: 600;
  font-size: 13px;
  color: #e8e8f0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  flex: 1;
}
.kanban-card-email {
  font-size: 11px;
  color: #888;
  margin-bottom: 6px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

/* Score Badge */
.kanban-score {
  font-size: 11px;
  font-weight: 700;
  padding: 2px 7px;
  border-radius: 6px;
  white-space: nowrap;
}
.kanban-score.high { background: #1b4332; color: #95d5b2; }
.kanban-score.med  { background: #3e2c00; color: #ffd166; }
.kanban-score.low  { background: #3c1518; color: #f4978e; }
.kanban-score.big  { font-size: 16px; padding: 4px 12px; }

/* Tag Chips */
.kanban-card-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  margin-bottom: 6px;
}
.kanban-tag {
  font-size: 10px;
  padding: 2px 7px;
  border-radius: 4px;
  color: #fff;
  white-space: nowrap;
}

/* Mini Action Buttons */
.kanban-card-actions {
  display: flex;
  justify-content: flex-end;
  gap: 6px;
  opacity: 0;
  transition: opacity .15s;
}
.kanban-card:hover .kanban-card-actions {
  opacity: 1;
}
.kanban-card-actions button {
  background: none;
  border: 1px solid rgba(255,255,255,0.1);
  border-radius: 4px;
  color: #999;
  cursor: pointer;
  padding: 3px 5px;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: color .15s, border-color .15s;
}
.kanban-card-actions button:hover {
  color: ${ACCENT};
  border-color: ${ACCENT};
}
.kanban-btn-delete:hover {
  color: #f4978e !important;
  border-color: #f4978e !important;
}

/* ---- Touch Ghost ---- */
.kanban-touch-ghost {
  position: fixed;
  z-index: 10000;
  pointer-events: none;
  opacity: 0.85;
  transform: rotate(2deg) scale(1.04);
  box-shadow: 0 8px 30px rgba(0,0,0,0.45);
}

/* ---- Detail Modal ---- */
.lead-detail {
  color: #d0d0dd;
}
.lead-detail-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 18px;
}
.lead-detail-header h2 {
  margin: 0;
  font-size: 20px;
  color: #f0f0f0;
}
.lead-detail-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px 20px;
  margin-bottom: 18px;
}
.lead-detail-field label {
  display: block;
  font-size: 11px;
  color: #777;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 2px;
}
.lead-detail-field span {
  font-size: 14px;
  color: #ccc;
}
.lead-detail-section {
  margin-bottom: 16px;
}
.lead-detail-section > label {
  display: block;
  font-size: 11px;
  color: #777;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 6px;
}

.detail-select {
  width: 100%;
  padding: 8px 10px;
  background: #1a1a2e;
  border: 1px solid rgba(255,255,255,0.1);
  border-radius: 6px;
  color: #d0d0dd;
  font-size: 13px;
  outline: none;
}
.detail-select:focus {
  border-color: ${ACCENT};
}

.detail-tag-list {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-bottom: 8px;
}
.detail-tag {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: 12px;
  padding: 3px 10px;
  border-radius: 5px;
  color: #fff;
}
.detail-tag-rm {
  background: none;
  border: none;
  color: rgba(255,255,255,0.7);
  cursor: pointer;
  font-size: 14px;
  line-height: 1;
  padding: 0 0 0 2px;
}
.detail-tag-rm:hover {
  color: #fff;
}
.detail-tag-add {
  display: flex;
  gap: 8px;
  align-items: center;
}
.detail-tag-add .detail-select {
  flex: 1;
}

.detail-textarea {
  width: 100%;
  padding: 10px;
  background: #1a1a2e;
  border: 1px solid rgba(255,255,255,0.1);
  border-radius: 6px;
  color: #d0d0dd;
  font-size: 13px;
  font-family: inherit;
  resize: vertical;
  outline: none;
  margin-bottom: 8px;
  box-sizing: border-box;
}
.detail-textarea:focus {
  border-color: ${ACCENT};
}

.lead-detail-footer {
  display: flex;
  gap: 10px;
  justify-content: flex-end;
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid rgba(255,255,255,0.06);
}

/* ---- Buttons ---- */
.btn-accent {
  background: ${ACCENT};
  color: #fff;
  border: none;
  border-radius: 6px;
  padding: 8px 18px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  transition: background .15s;
}
.btn-accent:hover {
  background: #6b5ce7;
}
.btn-accent.btn-sm {
  padding: 5px 12px;
  font-size: 12px;
}
.btn-accent.btn-disabled {
  opacity: 0.5;
  cursor: default;
  pointer-events: none;
}
.btn-danger {
  background: #c0392b;
  color: #fff;
  border: none;
  border-radius: 6px;
  padding: 8px 18px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  transition: background .15s;
}
.btn-danger:hover {
  background: #a93226;
}

/* ---- Confirm Dialog ---- */
.confirm-dialog p {
  font-size: 14px;
  color: #c0c0cc;
  margin: 0 0 18px;
  line-height: 1.5;
}
.confirm-actions {
  display: flex;
  gap: 10px;
  justify-content: flex-end;
}

/* ---- Mobile ---- */
@media (max-width: 768px) {
  .kanban-board {
    flex-direction: column;
    gap: 12px;
  }
  .kanban-col {
    flex: none;
    min-width: 0;
    max-height: none;
  }
  .kanban-col-toggle {
    display: flex;
  }
  .kanban-drop-zone {
    max-height: 50vh;
  }
  .kanban-card-actions {
    opacity: 1;
  }
  .lead-detail-grid {
    grid-template-columns: 1fr;
  }
  .lead-detail-footer {
    flex-direction: column;
  }
}

/* ---- Scrollbar (Webkit) ---- */
.kanban-drop-zone::-webkit-scrollbar {
  width: 5px;
}
.kanban-drop-zone::-webkit-scrollbar-track {
  background: transparent;
}
.kanban-drop-zone::-webkit-scrollbar-thumb {
  background: rgba(255,255,255,0.08);
  border-radius: 3px;
}
.kanban-drop-zone::-webkit-scrollbar-thumb:hover {
  background: rgba(255,255,255,0.16);
}
`;

    const style = document.createElement("style");
    style.setAttribute("data-kanban", "");
    style.textContent = css;
    document.head.appendChild(style);
  }
})();
