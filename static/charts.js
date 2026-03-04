/**
 * LeadFinder Pro v3 — Canvas-based charts (no external dependencies)
 *
 * Dark theme palette
 *   background : #131318
 *   grid       : #24242e
 *   accent     : #7C6FFF
 *   green      : #34D399
 *   red        : #F87171
 *   orange     : #FBBF24
 *   cyan       : #22D3EE
 *
 * All numbers are rendered in German locale (1.234 instead of 1,234).
 */

/* ------------------------------------------------------------------ */
/*  Helpers                                                           */
/* ------------------------------------------------------------------ */

const CHART_COLORS = {
  accent: '#7C6FFF',
  green:  '#34D399',
  red:    '#F87171',
  orange: '#FBBF24',
  cyan:   '#22D3EE',
};

const CHART_BG    = '#131318';
const CHART_GRID  = '#24242e';
const CHART_TEXT  = '#a1a1aa';
const CHART_WHITE = '#e4e4e7';

function fmtNum(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString('de-DE');
}

function hexToRgba(hex, alpha) {
  const h = hex.replace('#', '');
  const r = parseInt(h.substring(0, 2), 16);
  const g = parseInt(h.substring(2, 4), 16);
  const b = parseInt(h.substring(4, 6), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

function getDevicePixelRatio() {
  return window.devicePixelRatio || 1;
}

function sizeCanvas(canvas) {
  const dpr = getDevicePixelRatio();
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width  = rect.width  * dpr;
  canvas.height = rect.height * dpr;
  canvas.style.width  = rect.width  + 'px';
  canvas.style.height = rect.height + 'px';
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  return { width: rect.width, height: rect.height, ctx, dpr };
}

/* ------------------------------------------------------------------ */
/*  1. LineChart                                                      */
/* ------------------------------------------------------------------ */

class LineChart {
  /**
   * @param {string} canvasId
   * @param {{
   *   labels: string[],
   *   datasets: {label: string, data: number[], color: string}[],
   *   yLabel?: string
   * }} config
   */
  constructor(canvasId, config) {
    this.canvas = document.getElementById(canvasId);
    if (!this.canvas) throw new Error(`Canvas #${canvasId} not found`);
    this.config = config;
    this._tooltip = null;
    this._points  = [];

    this._createTooltipEl();
    this._bindEvents();
    this.draw();

    this._ro = new ResizeObserver(() => this.draw());
    this._ro.observe(this.canvas.parentElement);
  }

  _createTooltipEl() {
    this._tooltipEl = document.createElement('div');
    Object.assign(this._tooltipEl.style, {
      position: 'absolute', pointerEvents: 'none', opacity: '0',
      background: '#1e1e26', border: '1px solid #34344a', borderRadius: '6px',
      padding: '6px 10px', fontSize: '12px', color: CHART_WHITE,
      transition: 'opacity 0.15s', zIndex: '100', whiteSpace: 'nowrap',
    });
    this.canvas.parentElement.style.position = 'relative';
    this.canvas.parentElement.appendChild(this._tooltipEl);
  }

  _bindEvents() {
    this.canvas.addEventListener('mousemove', (e) => this._onMouseMove(e));
    this.canvas.addEventListener('mouseleave', () => {
      this._tooltipEl.style.opacity = '0';
    });
  }

  _onMouseMove(e) {
    const rect = this.canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    let closest = null;
    let minDist = 20;
    for (const p of this._points) {
      const d = Math.hypot(p.x - mx, p.y - my);
      if (d < minDist) { minDist = d; closest = p; }
    }
    if (closest) {
      this._tooltipEl.innerHTML =
        `<strong style="color:${closest.color}">${closest.dataset}</strong><br>` +
        `${closest.label}: <strong>${fmtNum(closest.value)}</strong>`;
      this._tooltipEl.style.left = closest.x + 12 + 'px';
      this._tooltipEl.style.top  = closest.y - 10 + 'px';
      this._tooltipEl.style.opacity = '1';
    } else {
      this._tooltipEl.style.opacity = '0';
    }
  }

  draw() {
    const { width, height, ctx } = sizeCanvas(this.canvas);
    const { labels, datasets, yLabel } = this.config;
    if (!labels || !labels.length || !datasets || !datasets.length) return;

    const pad = { top: 20, right: 20, bottom: 40, left: yLabel ? 65 : 55 };
    const w = width  - pad.left - pad.right;
    const h = height - pad.top  - pad.bottom;

    // background
    ctx.fillStyle = CHART_BG;
    ctx.fillRect(0, 0, width, height);

    // value range
    let allVals = datasets.flatMap(ds => ds.data);
    let minVal  = Math.min(0, ...allVals);
    let maxVal  = Math.max(...allVals);
    if (maxVal === minVal) maxVal = minVal + 1;
    const range = maxVal - minVal;
    const yTicks = 5;

    // grid + y-axis labels
    ctx.strokeStyle = CHART_GRID;
    ctx.lineWidth   = 1;
    ctx.fillStyle   = CHART_TEXT;
    ctx.font        = '11px system-ui, sans-serif';
    ctx.textAlign   = 'right';
    ctx.textBaseline = 'middle';
    for (let i = 0; i <= yTicks; i++) {
      const v = minVal + (range * i) / yTicks;
      const y = pad.top + h - (h * i) / yTicks;
      ctx.beginPath();
      ctx.moveTo(pad.left, y);
      ctx.lineTo(pad.left + w, y);
      ctx.stroke();
      ctx.fillText(fmtNum(Math.round(v)), pad.left - 8, y);
    }

    // y-axis label
    if (yLabel) {
      ctx.save();
      ctx.translate(14, pad.top + h / 2);
      ctx.rotate(-Math.PI / 2);
      ctx.textAlign = 'center';
      ctx.fillStyle = CHART_TEXT;
      ctx.font = '11px system-ui, sans-serif';
      ctx.fillText(yLabel, 0, 0);
      ctx.restore();
    }

    // x-axis labels
    ctx.textAlign    = 'center';
    ctx.textBaseline = 'top';
    ctx.fillStyle    = CHART_TEXT;
    const step = labels.length > 1 ? w / (labels.length - 1) : 0;
    labels.forEach((lbl, i) => {
      const x = pad.left + i * step;
      ctx.fillText(lbl, x, pad.top + h + 8);
    });

    // data mapping helpers
    const mapX = (i) => pad.left + i * step;
    const mapY = (v) => pad.top + h - ((v - minVal) / range) * h;

    this._points = [];

    // draw datasets
    datasets.forEach((ds) => {
      const color = ds.color || CHART_COLORS.accent;

      // filled area
      ctx.beginPath();
      ctx.moveTo(mapX(0), mapY(0));
      ds.data.forEach((v, i) => ctx.lineTo(mapX(i), mapY(v)));
      ctx.lineTo(mapX(ds.data.length - 1), mapY(0));
      ctx.closePath();
      ctx.fillStyle = hexToRgba(color, 0.1);
      ctx.fill();

      // line
      ctx.beginPath();
      ds.data.forEach((v, i) => {
        const x = mapX(i);
        const y = mapY(v);
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      });
      ctx.strokeStyle = color;
      ctx.lineWidth   = 2;
      ctx.lineJoin    = 'round';
      ctx.stroke();

      // data points
      ds.data.forEach((v, i) => {
        const x = mapX(i);
        const y = mapY(v);
        ctx.beginPath();
        ctx.arc(x, y, 3.5, 0, Math.PI * 2);
        ctx.fillStyle = color;
        ctx.fill();
        this._points.push({ x, y, value: v, label: labels[i], dataset: ds.label, color });
      });
    });

    // legend
    this._drawLegend(ctx, datasets, width, pad);
  }

  _drawLegend(ctx, datasets, totalWidth, pad) {
    ctx.font = '11px system-ui, sans-serif';
    ctx.textBaseline = 'middle';
    const items = datasets.map(ds => ({
      label: ds.label,
      color: ds.color || CHART_COLORS.accent,
      width: ctx.measureText(ds.label).width + 22,
    }));
    const totalLen = items.reduce((s, i) => s + i.width + 12, 0);
    let cx = (totalWidth - totalLen) / 2;
    const cy = pad.top - 2;
    items.forEach((item) => {
      ctx.fillStyle = item.color;
      ctx.fillRect(cx, cy - 4, 12, 8);
      ctx.fillStyle = CHART_WHITE;
      ctx.textAlign = 'left';
      ctx.fillText(item.label, cx + 16, cy);
      cx += item.width + 12;
    });
  }

  update(config) {
    Object.assign(this.config, config);
    this.draw();
  }

  destroy() {
    this._ro.disconnect();
    if (this._tooltipEl && this._tooltipEl.parentElement) {
      this._tooltipEl.parentElement.removeChild(this._tooltipEl);
    }
  }
}

/* ------------------------------------------------------------------ */
/*  2. BarChart (horizontal bars — funnel style)                      */
/* ------------------------------------------------------------------ */

class BarChart {
  /**
   * @param {string} canvasId
   * @param {{
   *   labels: string[],
   *   data: number[],
   *   colors: string[],
   *   maxValue?: number
   * }} config
   */
  constructor(canvasId, config) {
    this.canvas = document.getElementById(canvasId);
    if (!this.canvas) throw new Error(`Canvas #${canvasId} not found`);
    this.config = config;
    this.draw();

    this._ro = new ResizeObserver(() => this.draw());
    this._ro.observe(this.canvas.parentElement);
  }

  draw() {
    const { width, height, ctx } = sizeCanvas(this.canvas);
    const { labels, data, colors, maxValue } = this.config;
    if (!labels || !labels.length) return;

    ctx.fillStyle = CHART_BG;
    ctx.fillRect(0, 0, width, height);

    const max = maxValue || Math.max(...data);
    const n = labels.length;
    const pad = { left: 120, right: 70, top: 16, bottom: 16 };
    const barH = Math.min(30, (height - pad.top - pad.bottom - (n - 1) * 8) / n);
    const gap  = 8;
    const barAreaW = width - pad.left - pad.right;

    labels.forEach((label, i) => {
      const y = pad.top + i * (barH + gap);
      const val = data[i] || 0;
      const pct = max > 0 ? (val / max) * 100 : 0;
      const bw  = max > 0 ? (val / max) * barAreaW : 0;
      const color = colors[i % colors.length] || CHART_COLORS.accent;

      // label
      ctx.fillStyle   = CHART_WHITE;
      ctx.font        = '12px system-ui, sans-serif';
      ctx.textAlign   = 'right';
      ctx.textBaseline = 'middle';
      ctx.fillText(label, pad.left - 10, y + barH / 2);

      // bar background
      ctx.fillStyle = CHART_GRID;
      ctx.beginPath();
      ctx.roundRect(pad.left, y, barAreaW, barH, 4);
      ctx.fill();

      // bar fill
      if (bw > 0) {
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.roundRect(pad.left, y, bw, barH, 4);
        ctx.fill();
      }

      // value + pct
      ctx.fillStyle = CHART_TEXT;
      ctx.textAlign = 'left';
      ctx.fillText(`${fmtNum(val)}  (${pct.toFixed(1)}%)`, pad.left + barAreaW + 8, y + barH / 2);
    });
  }

  update(config) {
    Object.assign(this.config, config);
    this.draw();
  }

  destroy() {
    this._ro.disconnect();
  }
}

/* ------------------------------------------------------------------ */
/*  3. HeatmapTable (DOM-based, color-coded cells)                    */
/* ------------------------------------------------------------------ */

class HeatmapTable {
  /**
   * @param {string} containerId
   * @param {{
   *   rows: {label: string, values: number[]}[],
   *   columns: string[],
   *   colorScale: 'green'|'red'
   * }} config
   */
  constructor(containerId, config) {
    this.container = document.getElementById(containerId);
    if (!this.container) throw new Error(`Container #${containerId} not found`);
    this.config = config;
    this.render();
  }

  _intensityColor(ratio, scale) {
    // ratio 0..1
    const clamped = Math.max(0, Math.min(1, ratio));
    if (scale === 'red') {
      return `rgba(248,113,113,${0.1 + clamped * 0.7})`;   // #F87171
    }
    return `rgba(52,211,153,${0.1 + clamped * 0.7})`;       // #34D399
  }

  render() {
    const { rows, columns, colorScale } = this.config;
    if (!rows || !rows.length || !columns || !columns.length) return;

    // find global min/max for normalisation
    const allVals = rows.flatMap(r => r.values);
    const minV = Math.min(...allVals);
    const maxV = Math.max(...allVals);
    const rangeV = maxV - minV || 1;

    const table = document.createElement('table');
    table.style.cssText =
      'width:100%;border-collapse:collapse;font-size:13px;color:#e4e4e7;';

    // header
    const thead = document.createElement('thead');
    const headRow = document.createElement('tr');
    const cornerTh = document.createElement('th');
    cornerTh.style.cssText = 'padding:6px 10px;text-align:left;color:#a1a1aa;font-weight:500;';
    headRow.appendChild(cornerTh);
    columns.forEach(col => {
      const th = document.createElement('th');
      th.textContent = col;
      th.style.cssText = 'padding:6px 10px;text-align:center;color:#a1a1aa;font-weight:500;';
      headRow.appendChild(th);
    });
    thead.appendChild(headRow);
    table.appendChild(thead);

    // body
    const tbody = document.createElement('tbody');
    rows.forEach(row => {
      const tr = document.createElement('tr');
      const labelTd = document.createElement('td');
      labelTd.textContent = row.label;
      labelTd.style.cssText =
        'padding:6px 10px;font-weight:500;white-space:nowrap;border-bottom:1px solid #24242e;';
      tr.appendChild(labelTd);

      row.values.forEach(v => {
        const td = document.createElement('td');
        td.textContent = fmtNum(v);
        const ratio = (v - minV) / rangeV;
        td.style.cssText =
          `padding:6px 10px;text-align:center;border-bottom:1px solid #24242e;` +
          `background:${this._intensityColor(ratio, colorScale)};`;
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    this.container.innerHTML = '';
    this.container.appendChild(table);
  }

  update(config) {
    Object.assign(this.config, config);
    this.render();
  }
}

/* ------------------------------------------------------------------ */
/*  4. DonutChart (animated arcs + center total + legend)             */
/* ------------------------------------------------------------------ */

class DonutChart {
  /**
   * @param {string} canvasId
   * @param {{
   *   labels: string[],
   *   data: number[],
   *   colors: string[]
   * }} config
   */
  constructor(canvasId, config) {
    this.canvas = document.getElementById(canvasId);
    if (!this.canvas) throw new Error(`Canvas #${canvasId} not found`);
    this.config = config;
    this._animProgress = 0;
    this._animId = null;

    this._ro = new ResizeObserver(() => this._drawFrame(1));
    this._ro.observe(this.canvas.parentElement);

    this.animate();
  }

  animate() {
    const duration = 700; // ms
    const start = performance.now();

    const tick = (now) => {
      const elapsed = now - start;
      this._animProgress = Math.min(1, elapsed / duration);
      // ease out cubic
      const t = 1 - Math.pow(1 - this._animProgress, 3);
      this._drawFrame(t);
      if (this._animProgress < 1) {
        this._animId = requestAnimationFrame(tick);
      }
    };
    if (this._animId) cancelAnimationFrame(this._animId);
    this._animId = requestAnimationFrame(tick);
  }

  _drawFrame(t) {
    const { width, height, ctx } = sizeCanvas(this.canvas);
    const { labels, data, colors } = this.config;
    if (!data || !data.length) return;

    ctx.fillStyle = CHART_BG;
    ctx.fillRect(0, 0, width, height);

    const total = data.reduce((s, v) => s + v, 0);
    if (total === 0) return;

    const legendH = labels.length * 20 + 10;
    const available = Math.min(width, height - legendH);
    const radius = available * 0.36;
    const innerRadius = radius * 0.6;
    const cx = width / 2;
    const cy = (height - legendH) / 2;

    let startAngle = -Math.PI / 2;

    data.forEach((val, i) => {
      const sliceAngle = (val / total) * Math.PI * 2 * t;
      const endAngle = startAngle + sliceAngle;
      const color = colors[i % colors.length] || CHART_COLORS.accent;

      ctx.beginPath();
      ctx.arc(cx, cy, radius, startAngle, endAngle);
      ctx.arc(cx, cy, innerRadius, endAngle, startAngle, true);
      ctx.closePath();
      ctx.fillStyle = color;
      ctx.fill();

      startAngle = endAngle;
    });

    // center text
    ctx.fillStyle   = CHART_WHITE;
    ctx.font        = 'bold 22px system-ui, sans-serif';
    ctx.textAlign   = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(fmtNum(total), cx, cy - 8);
    ctx.font = '11px system-ui, sans-serif';
    ctx.fillStyle = CHART_TEXT;
    ctx.fillText('Gesamt', cx, cy + 12);

    // legend below
    this._drawLegend(ctx, labels, data, colors, total, width, height - legendH + 10);
  }

  _drawLegend(ctx, labels, data, colors, total, totalWidth, startY) {
    ctx.font = '11px system-ui, sans-serif';
    ctx.textBaseline = 'middle';

    const colWidth = 180;
    const cols = Math.max(1, Math.floor(totalWidth / colWidth));
    const rows = Math.ceil(labels.length / cols);
    const blockWidth = cols * colWidth;
    const offsetX = (totalWidth - blockWidth) / 2;

    labels.forEach((label, i) => {
      const col = i % cols;
      const row = Math.floor(i / cols);
      const x = offsetX + col * colWidth + 10;
      const y = startY + row * 20;

      const color = colors[i % colors.length] || CHART_COLORS.accent;
      ctx.fillStyle = color;
      ctx.beginPath();
      ctx.arc(x + 5, y, 4, 0, Math.PI * 2);
      ctx.fill();

      const pct = total > 0 ? ((data[i] / total) * 100).toFixed(1) : '0.0';
      ctx.fillStyle = CHART_WHITE;
      ctx.textAlign = 'left';
      ctx.fillText(`${label}  ${fmtNum(data[i])} (${pct}%)`, x + 14, y);
    });
  }

  update(config) {
    Object.assign(this.config, config);
    this.animate();
  }

  destroy() {
    if (this._animId) cancelAnimationFrame(this._animId);
    this._ro.disconnect();
  }
}

/* ------------------------------------------------------------------ */
/*  5. drawFunnel (HTML-based funnel visualisation)                    */
/* ------------------------------------------------------------------ */

/**
 * Renders an HTML funnel into the given container.
 *
 * @param {string} containerId
 * @param {{label: string, value: number, color?: string}[]} stages
 */
function drawFunnel(containerId, stages) {
  const container = document.getElementById(containerId);
  if (!container) throw new Error(`Container #${containerId} not found`);
  if (!stages || !stages.length) return;

  const maxVal = Math.max(...stages.map(s => s.value));
  const defaultColors = [
    CHART_COLORS.accent, CHART_COLORS.cyan, CHART_COLORS.green,
    CHART_COLORS.orange, CHART_COLORS.red,
  ];

  container.innerHTML = '';
  container.style.cssText = 'display:flex;flex-direction:column;gap:6px;';

  stages.forEach((stage, i) => {
    const pct = maxVal > 0 ? (stage.value / maxVal) * 100 : 0;
    const convPct = i === 0
      ? '100%'
      : (stages[i - 1].value > 0
        ? ((stage.value / stages[i - 1].value) * 100).toFixed(1) + '%'
        : '0%');
    const color = stage.color || defaultColors[i % defaultColors.length];

    const row = document.createElement('div');
    row.style.cssText = 'display:flex;align-items:center;gap:10px;';

    // label
    const labelEl = document.createElement('span');
    labelEl.textContent = stage.label;
    labelEl.style.cssText =
      'width:120px;text-align:right;font-size:13px;color:#e4e4e7;flex-shrink:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;';
    row.appendChild(labelEl);

    // bar wrapper
    const barWrapper = document.createElement('div');
    barWrapper.style.cssText =
      'flex:1;height:28px;background:#24242e;border-radius:4px;position:relative;overflow:hidden;';

    const barFill = document.createElement('div');
    barFill.style.cssText =
      `width:${pct}%;height:100%;background:${color};border-radius:4px;` +
      'transition:width 0.5s ease;';
    barWrapper.appendChild(barFill);
    row.appendChild(barWrapper);

    // stats
    const statsEl = document.createElement('span');
    statsEl.style.cssText =
      'width:110px;font-size:12px;color:#a1a1aa;flex-shrink:0;white-space:nowrap;';
    statsEl.textContent = `${fmtNum(stage.value)}  (${convPct})`;
    row.appendChild(statsEl);

    container.appendChild(row);
  });
}

/* ------------------------------------------------------------------ */
/*  Exports (global)                                                  */
/* ------------------------------------------------------------------ */

window.LineChart    = LineChart;
window.BarChart     = BarChart;
window.HeatmapTable = HeatmapTable;
window.DonutChart   = DonutChart;
window.drawFunnel   = drawFunnel;
