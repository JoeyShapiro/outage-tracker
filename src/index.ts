interface RawOutage {
  affected: number;
  cause: string;
  city: string;
  lat: number;
  lng: number;
  reported: string;
  restore: string | null;
  stormMode: boolean;
  status: string;
  zip: string;
  maxAffected: number;
  comment: string;
}

interface FeedResponse {
  outageList: RawOutage[];
}

interface OpenState {
  outage_id: string;
  status: string;
  cause: string | null;
  comment: string | null;
  affected: number;
  max_affected: number;
  restore_estimate: string | null;
  storm_mode: number;
}

function buildOutageId(rec: RawOutage): string {
  return `${rec.lat}_${rec.lng}_${rec.reported}`;
}

function hasChanged(open: OpenState | undefined, rec: RawOutage, stormMode: number): boolean {
  if (!open) return true;
  return (
    open.status !== rec.status ||
    open.cause !== rec.cause ||
    open.comment !== rec.comment ||
    open.affected !== rec.affected ||
    open.max_affected !== rec.maxAffected ||
    open.restore_estimate !== rec.restore ||
    open.storm_mode !== stormMode
  );
}

async function runInBatches(db: D1Database, stmts: D1PreparedStatement[], size = 100): Promise<void> {
  for (let i = 0; i < stmts.length; i += size) {
    await db.batch(stmts.slice(i, i + size));
  }
}

async function syncOutages(db: D1Database, records: RawOutage[]): Promise<void> {
  const now = new Date().toISOString();

  const openRows = await db
    .prepare(
      `SELECT outage_id, status, cause, comment, affected, max_affected, restore_estimate, storm_mode
       FROM outage_states WHERE valid_to IS NULL`
    )
    .all<OpenState>();
  const openByOutageId = new Map(openRows.results.map((r) => [r.outage_id, r]));
  const seenIds = new Set<string>();

  const newOutageStmts: D1PreparedStatement[] = [];
  const closeStmts: D1PreparedStatement[] = [];
  const insertStateStmts: D1PreparedStatement[] = [];

  for (const rec of records) {
    const outageId = buildOutageId(rec);
    seenIds.add(outageId);

    const stormMode = rec.stormMode ? 1 : 0;
    const open = openByOutageId.get(outageId);
    if (!hasChanged(open, rec, stormMode)) continue;

    if (!open) {
      newOutageStmts.push(
        db
          .prepare(
            `INSERT INTO outages (outage_id, lat, lng, city, zip, reported, first_seen)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(outage_id) DO NOTHING`
          )
          .bind(outageId, rec.lat, rec.lng, rec.city, rec.zip, rec.reported, now)
      );
    } else {
      closeStmts.push(
        db
          .prepare(`UPDATE outage_states SET valid_to = ? WHERE outage_id = ? AND valid_to IS NULL`)
          .bind(now, outageId)
      );
    }

    insertStateStmts.push(
      db
        .prepare(
          `INSERT INTO outage_states
             (outage_id, valid_from, status, cause, comment, affected, max_affected, restore_estimate, storm_mode)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .bind(
          outageId,
          now,
          rec.status,
          rec.cause,
          rec.comment,
          rec.affected,
          rec.maxAffected,
          rec.restore,
          stormMode
        )
    );
  }

  const resolveStmts: D1PreparedStatement[] = [];
  for (const outageId of openByOutageId.keys()) {
    if (seenIds.has(outageId)) continue;
    resolveStmts.push(
      db
        .prepare(`UPDATE outage_states SET valid_to = ? WHERE outage_id = ? AND valid_to IS NULL`)
        .bind(now, outageId)
    );
    resolveStmts.push(db.prepare(`UPDATE outages SET resolved_at = ? WHERE outage_id = ?`).bind(now, outageId));
  }

  // Order matters: for any given outage, its close-old-row statement must run
  // before its insert-new-row statement, or it'll end up with two open rows.
  await runInBatches(db, [...newOutageStmts, ...closeStmts, ...insertStateStmts, ...resolveStmts]);
}

interface NearestOutage {
  outage_id: string;
  lat: number;
  lng: number;
  city: string;
  zip: string;
}

interface TimelineRow {
  status: string;
  cause: string | null;
  comment: string | null;
  affected: number;
  max_affected: number;
  restore_estimate: string | null;
  valid_from: string;
  valid_to: string | null;
}

interface AffectedSnapshot {
  ts: string;
  total_affected: number;
}

// One row per (city, status) currently open. Live query, used both to render
// the "now" numbers and to persist a snapshot at ingest time.
const LIVE_CITY_STATUS_SQL = `
  SELECT o.city AS city, os.status AS status, COUNT(*) AS count, SUM(os.affected) AS affected
  FROM outage_states os
  JOIN outages o ON o.outage_id = os.outage_id
  WHERE os.valid_to IS NULL
  GROUP BY o.city, os.status`;

async function recordSnapshot(db: D1Database): Promise<void> {
  const now = new Date().toISOString();
  const { results } = await db.prepare(LIVE_CITY_STATUS_SQL).all<SnapshotRow>();
  const stmts = results.map((r) =>
    db
      .prepare(`INSERT INTO outage_snapshots (ts, city, status, count, affected) VALUES (?, ?, ?, ?, ?)`)
      .bind(now, r.city, r.status, r.count, r.affected)
  );
  await runInBatches(db, stmts);
}

function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]!));
}

function formatTs(iso: string): string {
  return iso.replace("T", " ").replace(/\.\d+Z?$/, "").replace(/Z$/, "");
}

function statusPillClass(status: string): string {
  return /restor/i.test(status) ? "pill pill-resolved" : "pill pill-active";
}

const PAGE_STYLE = `
  :root {
    --bg: #f5f6f8;
    --card-bg: #ffffff;
    --text: #1a1d21;
    --muted: #6b7280;
    --border: #e5e7eb;
    --accent: #2563eb;
    --accent-contrast: #ffffff;
    --error: #dc2626;
    --pill-active-bg: #fff4e5;
    --pill-active-text: #9a5b00;
    --pill-resolved-bg: #e6f4ea;
    --pill-resolved-text: #1e7e34;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0f1115;
      --card-bg: #171a21;
      --text: #e5e7eb;
      --muted: #9aa1ab;
      --border: #2a2e37;
      --accent: #3b82f6;
      --accent-contrast: #0f1115;
      --error: #f87171;
      --pill-active-bg: #3a2a10;
      --pill-active-text: #f3b45c;
      --pill-resolved-bg: #16311f;
      --pill-resolved-text: #6bd48a;
    }
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; }
  body {
    margin: 0;
    display: flex;
    flex-direction: column;
    height: 100vh;
    overflow: hidden;
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  }
  header { flex: none; padding: 1.25rem 1.5rem 0.5rem; display: flex; align-items: flex-start; justify-content: space-between; gap: 1rem; }
  header h1 { margin: 0; font-size: 1.25rem; }
  header p { margin: 0.2rem 0 0; color: var(--muted); font-size: 0.85rem; }
  .donate-wrap { flex: none; }
  /* The bmc-button widget injects its own <style> with fixed px sizing;
     these need higher specificity to win the cascade and shrink it ~30%. */
  .donate-wrap .bmc-btn {
    min-width: 147px !important;
    height: 42px !important;
    padding: 0 17px !important;
    border-radius: 8px !important;
    font-size: 20px !important;
    line-height: 19px !important;
  }
  .donate-wrap .bmc-btn svg { height: 22px !important; }
  .donate-wrap .bmc-btn-text { margin-left: 6px !important; }
  main.layout {
    flex: 1;
    min-height: 0;
    display: grid;
    grid-template-columns: minmax(320px, 1fr) minmax(360px, 1.2fr);
    gap: 1rem;
    padding: 0.75rem 1.5rem 1.5rem;
  }
  .stack {
    display: flex;
    flex-direction: column;
    gap: 1rem;
    min-height: 0;
  }
  .stack .card:first-child { flex: 1 1 auto; }
  .stack .card:last-child { flex: 0 0 42%; }
  .chart-wrap { flex: 1; min-height: 0; position: relative; }
  .chart-wrap canvas { width: 100% !important; height: 100% !important; }
  .card {
    min-height: 0;
    display: flex;
    flex-direction: column;
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.1rem 1.25rem;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
  }
  .card h2 {
    margin: 0 0 0.85rem;
    font-size: 0.75rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--muted);
  }
  .card-scroll { flex: 1 1 auto; overflow-y: auto; min-height: 0; }
  form.search { display: flex; gap: 0.6rem; align-items: end; flex-wrap: wrap; margin-bottom: 1rem; }
  form.search label { flex: 1 1 320px; }
  label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.75rem; color: var(--muted); }
  input {
    padding: 0.45rem 0.6rem;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--bg);
    color: var(--text);
    font-size: 0.9rem;
    width: 100%;
  }
  button {
    padding: 0.5rem 1rem;
    border: none;
    border-radius: 8px;
    background: var(--accent);
    color: var(--accent-contrast);
    font-weight: 600;
    font-size: 0.85rem;
    cursor: pointer;
  }
  button:hover { opacity: 0.92; }
  table { border-collapse: collapse; width: 100%; }
  th, td { text-align: left; padding: 0.45rem 0.6rem; border-bottom: 1px solid var(--border); font-size: 0.85rem; }
  th { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.03em; color: var(--muted); position: sticky; top: 0; background: var(--card-bg); }
  .pill { display: inline-block; padding: 0.15rem 0.55rem; border-radius: 999px; font-size: 0.72rem; font-weight: 600; }
  .pill-active { background: var(--pill-active-bg); color: var(--pill-active-text); }
  .pill-resolved { background: var(--pill-resolved-bg); color: var(--pill-resolved-text); }
  .stats { display: flex; gap: 0.6rem; margin-bottom: 0.85rem; }
  .stat { flex: 1; background: var(--bg); border-radius: 8px; padding: 0.55rem 0.75rem; }
  .stat .value { font-size: 1.25rem; font-weight: 700; line-height: 1.2; }
  .stat .label { font-size: 0.68rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.03em; }
  .delta { font-size: 0.72rem; font-weight: 600; white-space: nowrap; }
  .delta-up { color: var(--pill-active-text); }
  .delta-down { color: var(--pill-resolved-text); }
  .error { color: var(--error); font-size: 0.85rem; }
  .muted { color: var(--muted); font-size: 0.85rem; }
  .match { font-size: 0.85rem; margin: 0 0 0.85rem; color: var(--muted); }
  .match strong { color: var(--text); }
`;

function pageShell(body: string): string {
  return `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Outage tracker</title>
<style>${PAGE_STYLE}</style>
</head>
<body>
${body}
</body>
</html>`;
}

function renderSearchForm(address: string): string {
  return `<form class="search" id="address-form">
  <label>Address <input type="text" name="address" id="address-input" value="${escapeHtml(address)}" placeholder="123 Main St, City, ST" required></label>
  <button type="submit">Search</button>
</form>
<script>
(function () {
  const form = document.getElementById("address-form");
  const input = document.getElementById("address-input");
  const button = form.querySelector("button");

  form.addEventListener("submit", async function (event) {
    event.preventDefault();
    const address = input.value.trim();
    if (!address) return;

    button.disabled = true;
    button.textContent = "Searching…";

    try {
      const geoUrl = "https://nominatim.openstreetmap.org/search?format=json&limit=1&q=" + encodeURIComponent(address);
      const geoRes = await fetch(geoUrl, { headers: { Accept: "application/json" } });
      if (!geoRes.ok) throw new Error("geocode failed");
      const geoResults = await geoRes.json();
      const first = geoResults[0];
      if (!first) {
        alert("Couldn't find that address. Try adding a city and state.");
        return;
      }

      const params = new URLSearchParams({ address: address, lat: first.lat, lng: first.lon });
      window.location.href = "/?" + params.toString();
    } catch (err) {
      alert("Something went wrong looking up that address. Please try again.");
    } finally {
      button.disabled = false;
      button.textContent = "Search";
    }
  });
})();
</script>`;
}

function renderTimelinePanel(
  addressParam: string,
  searchError: boolean,
  nearest: NearestOutage | null,
  distanceMiles: number | null,
  timeline: TimelineRow[]
): string {
  let content: string;

  if (searchError) {
    content = `<p class="error">Couldn't find that address. Try adding a city and state.</p>`;
  } else if (!addressParam) {
    content = `<p class="muted">Enter your address to see your outage history.</p>`;
  } else if (!nearest) {
    content = `<p class="muted">No outages have been recorded yet.</p>`;
  } else {
    const rows = timeline
      .map(
        (r) => `<tr>
          <td><span class="${statusPillClass(r.status)}">${escapeHtml(r.status)}</span></td>
          <td>${escapeHtml(r.cause ?? "")}</td>
          <td>${r.affected}</td>
          <td>${formatTs(r.valid_from)}</td>
          <td>${r.valid_to ? formatTs(r.valid_to) : "ongoing"}</td>
        </tr>`
      )
      .join("\n");

    const distanceLabel = distanceMiles !== null ? ` (${distanceMiles.toFixed(1)} mi away)` : "";

    content = `<p class="match">Nearest known location: <strong>${escapeHtml(nearest.city)}, ${escapeHtml(nearest.zip)}</strong>${distanceLabel}</p>
<div class="card-scroll">
<table>
<thead><tr><th>Status</th><th>Cause</th><th>Affected</th><th>From</th><th>To</th></tr></thead>
<tbody>${rows}</tbody>
</table>
</div>`;
  }

  return `<section class="card">
${renderSearchForm(addressParam)}
${content}
</section>`;
}

interface SnapshotRow {
  city: string;
  status: string;
  count: number;
  affected: number;
}

function renderAffectedChart(points: AffectedSnapshot[]): string {
  if (points.length === 0) {
    return `<div class="chart-wrap"><p class="muted">Not enough data yet.</p></div>`;
  }

  const labels = points.map((p) => formatTs(p.ts));
  const totals = points.map((p) => p.total_affected);

  return `<div class="chart-wrap"><canvas id="affectedChart"></canvas></div>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<script>
(function () {
  const styles = getComputedStyle(document.documentElement);
  const textColor = styles.getPropertyValue("--muted").trim() || "#6b7280";
  const gridColor = styles.getPropertyValue("--border").trim() || "#e5e7eb";
  const accent = styles.getPropertyValue("--accent").trim() || "#2563eb";

  new Chart(document.getElementById("affectedChart"), {
    type: "line",
    data: {
      labels: ${JSON.stringify(labels)},
      datasets: [{
        label: "Customers affected",
        data: ${JSON.stringify(totals)},
        borderColor: accent,
        backgroundColor: accent + "26",
        fill: true,
        pointRadius: 0,
        borderWidth: 2,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: { ticks: { color: textColor, maxRotation: 0, autoSkip: true, maxTicksLimit: 6 }, grid: { color: gridColor } },
        y: { ticks: { color: textColor }, grid: { color: gridColor }, beginAtZero: true },
      },
      plugins: { legend: { display: false } },
    },
  });
})();
</script>`;
}

// Covers every status the feed emits: a freshly reported outage starts
// with an empty status before the utility assesses it.
const STATUS_COLUMNS = [
  { label: "Reported", test: /^$/ },
  { label: "Assessing", test: /assess/i },
  { label: "Assigned", test: /assign/i },
  { label: "Onsite", test: /on-?site/i },
];

function renderAffectedChartCard(points: AffectedSnapshot[]): string {
  return `<section class="card">
<h2>Total customers affected</h2>
${renderAffectedChart(points)}
</section>`;
}

interface CityAgg {
  outageCount: number;
  affected: number;
  statusCounts: number[];
}

function aggregateByCity(rows: SnapshotRow[]): Map<string, CityAgg> {
  const byCity = new Map<string, CityAgg>();
  for (const r of rows) {
    const agg = byCity.get(r.city) ?? { outageCount: 0, affected: 0, statusCounts: STATUS_COLUMNS.map(() => 0) };
    agg.outageCount += r.count;
    agg.affected += r.affected;
    const colIndex = STATUS_COLUMNS.findIndex((c) => c.test.test(r.status));
    if (colIndex !== -1) agg.statusCounts[colIndex] += r.count;
    byCity.set(r.city, agg);
  }
  return byCity;
}

function aggregateTotals(rows: SnapshotRow[]): { outages: number; affected: number; onSite: number } {
  const onSiteTest = STATUS_COLUMNS.find((c) => c.label === "Onsite")!.test;
  let outages = 0;
  let affected = 0;
  let onSite = 0;
  for (const r of rows) {
    outages += r.count;
    affected += r.affected;
    if (onSiteTest.test(r.status)) onSite += r.count;
  }
  return { outages, affected, onSite };
}

function renderDelta(delta: number): string {
  if (delta === 0) return "";
  const cls = delta > 0 ? "delta-up" : "delta-down";
  const arrow = delta > 0 ? "↑" : "↓";
  return ` <span class="delta ${cls}">${arrow}${Math.abs(delta).toLocaleString()}</span>`;
}

function renderCityPanel(current: SnapshotRow[], baseline: SnapshotRow[]): string {
  const currentTotals = aggregateTotals(current);
  const baselineTotals = aggregateTotals(baseline);
  const currentByCity = aggregateByCity(current);
  const baselineByCity = aggregateByCity(baseline);

  const cities = Array.from(currentByCity.keys()).sort();

  const header = `<th>City</th><th>Affected</th><th>Outages</th>${STATUS_COLUMNS.map(
    (c) => `<th>${c.label}</th>`
  ).join("")}`;
  const bodyRows = cities
    .map((city) => {
      const agg = currentByCity.get(city)!;
      const prevAffected = baselineByCity.get(city)?.affected ?? 0;
      const statusCells = agg.statusCounts.map((c) => `<td>${c || "–"}</td>`).join("");
      return `<tr><td>${escapeHtml(city)}</td><td>${agg.affected.toLocaleString()}${renderDelta(
        agg.affected - prevAffected
      )}</td><td>${agg.outageCount}</td>${statusCells}</tr>`;
    })
    .join("\n");

  const table =
    cities.length === 0
      ? `<p class="muted">No active outages.</p>`
      : `<div class="card-scroll">
<table>
<thead><tr>${header}</tr></thead>
<tbody>${bodyRows}</tbody>
</table>
</div>`;

  return `<section class="card">
<h2>Outages by city</h2>
<div class="stats">
  <div class="stat"><div class="value">${currentTotals.affected.toLocaleString()}${renderDelta(
    currentTotals.affected - baselineTotals.affected
  )}</div><div class="label">Customers affected</div></div>
  <div class="stat"><div class="value">${currentTotals.outages.toLocaleString()}${renderDelta(
    currentTotals.outages - baselineTotals.outages
  )}</div><div class="label">Active outages</div></div>
  <div class="stat"><div class="value">${currentTotals.onSite.toLocaleString()}${renderDelta(
    currentTotals.onSite - baselineTotals.onSite
  )}</div><div class="label">Crew on-site</div></div>
</div>
${table}
</section>`;
}

function html(body: string, status = 200): Response {
  return new Response(body, { status, headers: { "content-type": "text/html; charset=utf-8" } });
}

function haversineMiles(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const earthRadiusMiles = 3958.8;
  const toRad = (deg: number) => (deg * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return 2 * earthRadiusMiles * Math.asin(Math.sqrt(a));
}

async function handleHome(env: Env, url: URL): Promise<Response> {
  // The address the user typed is geocoded client-side (see renderSearchForm's
  // script) and arrives here as lat/lng; `address` is only echoed back into the
  // input so the box doesn't show raw coordinates after a search.
  const addressParam = url.searchParams.get("address") ?? "";
  const latParam = url.searchParams.get("lat") ?? "";
  const lngParam = url.searchParams.get("lng") ?? "";
  const searched = latParam !== "" || lngParam !== "";
  const lat = Number(latParam);
  const lng = Number(lngParam);
  const searchError = searched && (latParam === "" || lngParam === "" || !Number.isFinite(lat) || !Number.isFinite(lng));

  let nearest: NearestOutage | null = null;
  let timeline: TimelineRow[] = [];
  let distanceMiles: number | null = null;

  if (searched && !searchError) {
    // Nearest-neighbor by plain squared distance in degree space -- fine at the
    // scale of a single utility's service territory, no need for haversine here.
    nearest = await env.DB.prepare(
      `SELECT outage_id, lat, lng, city, zip
       FROM outages
       ORDER BY (lat - ?) * (lat - ?) + (lng - ?) * (lng - ?) ASC
       LIMIT 1`
    )
      .bind(lat, lat, lng, lng)
      .first<NearestOutage>();

    if (nearest) {
      distanceMiles = haversineMiles(lat, lng, nearest.lat, nearest.lng);

      const result = await env.DB.prepare(
        `SELECT os.status, os.cause, os.comment, os.affected, os.max_affected, os.restore_estimate,
                os.valid_from, os.valid_to
         FROM outage_states os
         JOIN outages o ON o.outage_id = os.outage_id
         WHERE o.lat = ? AND o.lng = ?
         ORDER BY os.valid_from ASC`
      )
        .bind(nearest.lat, nearest.lng)
        .all<TimelineRow>();
      timeline = result.results;
    }
  }

  const { results: currentSnapshot } = await env.DB.prepare(LIVE_CITY_STATUS_SQL).all<SnapshotRow>();

  const hourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  const baselineTs = await env.DB.prepare(`SELECT MAX(ts) AS ts FROM outage_snapshots WHERE ts <= ?`)
    .bind(hourAgo)
    .first<{ ts: string | null }>();
  const baselineSnapshot = baselineTs?.ts
    ? (
        await env.DB.prepare(`SELECT city, status, count, affected FROM outage_snapshots WHERE ts = ?`)
          .bind(baselineTs.ts)
          .all<SnapshotRow>()
      ).results
    : [];

  const { results: affectedSnapshotsDesc } = await env.DB.prepare(
    `SELECT ts, SUM(affected) AS total_affected FROM outage_snapshots GROUP BY ts ORDER BY ts DESC LIMIT 500`
  ).all<AffectedSnapshot>();
  const affectedSnapshots = affectedSnapshotsDesc.slice().reverse();

  const body = `<header>
  <div>
    <h1>Outage tracker</h1>
    <p>Live status pulled from the utility feed</p>
  </div>
  <div class="donate-wrap">
    <script type="text/javascript" src="https://cdnjs.buymeacoffee.com/1.0.0/button.prod.min.js" data-name="bmc-button" data-slug="JoeyShapiro" data-color="#FFDD00" data-emoji=""  data-font="Cookie" data-text="Buy me a &lt;s&gt;Coffee&lt;/s&gt; Pastry" data-outline-color="#000000" data-font-color="#000000" data-coffee-color="#ffffff" ></script>
  </div>
</header>
<main class="layout">
${renderTimelinePanel(addressParam, searchError, nearest, distanceMiles, timeline)}
<div class="stack">
${renderCityPanel(currentSnapshot, baselineSnapshot)}
${renderAffectedChartCard(affectedSnapshots)}
</div>
</main>`;

  return html(pageShell(body), searchError ? 400 : 200);
}

async function handleIngest(request: Request, env: Env & { INGEST_TOKEN?: string }): Promise<Response> {
  const auth = request.headers.get("Authorization");
  if (!env.INGEST_TOKEN || auth !== `Bearer ${env.INGEST_TOKEN}`) {
    return new Response("Unauthorized", { status: 401 });
  }

  let data: FeedResponse;
  try {
    data = await request.json<FeedResponse>();
  } catch {
    return new Response("Invalid JSON", { status: 400 });
  }

  await syncOutages(env.DB, data.outageList);
  await recordSnapshot(env.DB);
  return new Response("ok", { status: 200 });
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/ingest" && request.method === "POST") {
      return handleIngest(request, env);
    }
    return handleHome(env, url);
  },
} satisfies ExportedHandler<Env>;
