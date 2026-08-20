// TODO: replace with the real feed URL once you have it.
const FEED_URL = "https://www.nipsco.com/nisource-api/ldc/GetPowerOutages";

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

interface CityTotal {
  city: string;
  outage_count: number;
  total_affected: number;
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

async function recordAffectedSnapshot(db: D1Database): Promise<void> {
  const now = new Date().toISOString();
  const row = await db
    .prepare(`SELECT COALESCE(SUM(affected), 0) AS total FROM outage_states WHERE valid_to IS NULL`)
    .first<{ total: number }>();
  await db
    .prepare(`INSERT INTO affected_snapshots (ts, total_affected) VALUES (?, ?)`)
    .bind(now, row?.total ?? 0)
    .run();
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
  header { flex: none; padding: 1.25rem 1.5rem 0.5rem; }
  header h1 { margin: 0; font-size: 1.25rem; }
  header p { margin: 0.2rem 0 0; color: var(--muted); font-size: 0.85rem; }
  main.layout {
    flex: 1;
    min-height: 0;
    display: grid;
    grid-template-columns: minmax(320px, 1fr) minmax(340px, 1.1fr);
    grid-template-rows: 2fr 3fr;
    gap: 1rem;
    padding: 0.75rem 1.5rem 1.5rem;
  }
  .full-span { grid-column: 1 / -1; }
  .city-content { display: flex; gap: 1.25rem; flex: 1; min-height: 0; }
  .city-content .chart-wrap { flex: 1 1 45%; min-height: 0; position: relative; }
  .city-content .chart-wrap canvas { width: 100% !important; height: 100% !important; }
  .city-content .card-scroll { flex: 1 1 55%; }
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
  .card-scroll { overflow-y: auto; min-height: 0; }
  form.search { display: flex; gap: 0.6rem; align-items: end; flex-wrap: wrap; margin-bottom: 1rem; }
  label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.75rem; color: var(--muted); }
  input {
    padding: 0.45rem 0.6rem;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--bg);
    color: var(--text);
    font-size: 0.9rem;
    width: 9rem;
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

function renderSearchForm(lat: string, lng: string): string {
  return `<form class="search" method="GET" action="/">
  <label>Latitude <input type="number" step="any" name="lat" value="${escapeHtml(lat)}" required></label>
  <label>Longitude <input type="number" step="any" name="lng" value="${escapeHtml(lng)}" required></label>
  <button type="submit">Find my outages</button>
</form>`;
}

function renderTimelinePanel(
  latParam: string,
  lngParam: string,
  searchError: boolean,
  nearest: NearestOutage | null,
  timeline: TimelineRow[]
): string {
  let content: string;

  if (searchError) {
    content = `<p class="error">Enter a valid latitude and longitude.</p>`;
  } else if (!latParam && !lngParam) {
    content = `<p class="muted">Enter your coordinates to see your outage history.</p>`;
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

    content = `<p class="match">Nearest known location: <strong>${escapeHtml(nearest.city)}, ${escapeHtml(nearest.zip)}</strong> (${nearest.lat}, ${nearest.lng})</p>
<div class="card-scroll">
<table>
<thead><tr><th>Status</th><th>Cause</th><th>Affected</th><th>From</th><th>To</th></tr></thead>
<tbody>${rows}</tbody>
</table>
</div>`;
  }

  return `<section class="card">
${renderSearchForm(latParam, lngParam)}
${content}
</section>`;
}

interface CityStatusCount {
  city: string;
  status: string;
  count: number;
}

function renderCrewPanel(rows: CityStatusCount[]): string {
  const onSite = rows.filter((r) => /on-site/i.test(r.status)).reduce((sum, r) => sum + r.count, 0);

  const statusTotals = new Map<string, number>();
  for (const r of rows) statusTotals.set(r.status, (statusTotals.get(r.status) ?? 0) + r.count);
  const statuses = [...statusTotals.keys()].sort((a, b) => statusTotals.get(b)! - statusTotals.get(a)!);

  const cityMap = new Map<string, Map<string, number>>();
  for (const r of rows) {
    if (!cityMap.has(r.city)) cityMap.set(r.city, new Map());
    cityMap.get(r.city)!.set(r.status, r.count);
  }
  const cities = [...cityMap.keys()].sort();

  const header = `<th>City</th>${statuses.map((s) => `<th>${escapeHtml(s || "Unknown")}</th>`).join("")}`;
  const bodyRows = cities
    .map((city) => {
      const counts = cityMap.get(city)!;
      const cells = statuses.map((s) => `<td>${counts.get(s) ?? "–"}</td>`).join("");
      return `<tr><td>${escapeHtml(city)}</td>${cells}</tr>`;
    })
    .join("\n");

  const table =
    rows.length === 0
      ? `<p class="muted">No active outages.</p>`
      : `<div class="card-scroll">
<table>
<thead><tr>${header}</tr></thead>
<tbody>${bodyRows}</tbody>
</table>
</div>`;

  return `<section class="card">
<h2>Crew status (${onSite} on-site)</h2>
${table}
</section>`;
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

function renderCityPanel(rows: CityTotal[], affectedSnapshots: AffectedSnapshot[]): string {
  const totalOutages = rows.reduce((sum, r) => sum + r.outage_count, 0);
  const totalAffected = rows.reduce((sum, r) => sum + r.total_affected, 0);

  const items = rows
    .map((r) => `<tr><td>${escapeHtml(r.city)}</td><td>${r.outage_count}</td><td>${r.total_affected}</td></tr>`)
    .join("\n");

  return `<section class="card full-span">
<h2>Total outages by city</h2>
<div class="stats">
  <div class="stat"><div class="value">${totalOutages}</div><div class="label">Active outages</div></div>
  <div class="stat"><div class="value">${totalAffected}</div><div class="label">Customers affected</div></div>
</div>
<div class="city-content">
${renderAffectedChart(affectedSnapshots)}
<div class="card-scroll">
<table>
<thead><tr><th>City</th><th>Outages</th><th>Affected</th></tr></thead>
<tbody>${items}</tbody>
</table>
</div>
</div>
</section>`;
}

function html(body: string, status = 200): Response {
  return new Response(body, { status, headers: { "content-type": "text/html; charset=utf-8" } });
}

async function handleHome(env: Env, url: URL): Promise<Response> {
  const latParam = url.searchParams.get("lat") ?? "";
  const lngParam = url.searchParams.get("lng") ?? "";
  const searched = latParam !== "" || lngParam !== "";
  const lat = Number(latParam);
  const lng = Number(lngParam);
  const searchError = searched && (latParam === "" || lngParam === "" || !Number.isFinite(lat) || !Number.isFinite(lng));

  let nearest: NearestOutage | null = null;
  let timeline: TimelineRow[] = [];

  if (searched && !searchError) {
    // Nearest-neighbor by plain squared distance in degree space -- fine at the
    // scale of a single utility's service territory, no need for haversine.
    nearest = await env.DB.prepare(
      `SELECT outage_id, lat, lng, city, zip
       FROM outages
       ORDER BY (lat - ?) * (lat - ?) + (lng - ?) * (lng - ?) ASC
       LIMIT 1`
    )
      .bind(lat, lat, lng, lng)
      .first<NearestOutage>();

    if (nearest) {
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

  const { results: cityTotals } = await env.DB.prepare(
    `SELECT o.city AS city, COUNT(*) AS outage_count, SUM(os.affected) AS total_affected
     FROM outage_states os
     JOIN outages o ON o.outage_id = os.outage_id
     WHERE os.valid_to IS NULL
     GROUP BY o.city
     ORDER BY o.city ASC`
  ).all<CityTotal>();

  const { results: cityStatusCounts } = await env.DB.prepare(
    `SELECT o.city AS city, os.status AS status, COUNT(*) AS count
     FROM outage_states os
     JOIN outages o ON o.outage_id = os.outage_id
     WHERE os.valid_to IS NULL
     GROUP BY o.city, os.status
     ORDER BY o.city ASC`
  ).all<CityStatusCount>();

  const { results: affectedSnapshotsDesc } = await env.DB.prepare(
    `SELECT ts, total_affected FROM affected_snapshots ORDER BY ts DESC LIMIT 500`
  ).all<AffectedSnapshot>();
  const affectedSnapshots = affectedSnapshotsDesc.slice().reverse();

  const body = `<header>
  <h1>Outage tracker</h1>
  <p>Live status pulled from the utility feed</p>
</header>
<main class="layout">
${renderTimelinePanel(latParam, lngParam, searchError, nearest, timeline)}
${renderCrewPanel(cityStatusCounts)}
${renderCityPanel(cityTotals, affectedSnapshots)}
</main>`;

  return html(pageShell(body), searchError ? 400 : 200);
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url);
    return handleHome(env, url);
  },

  async scheduled(event, env, ctx): Promise<void> {
    const res = await fetch(FEED_URL);
    if (!res.ok) {
      console.error(`outage-tracker: feed fetch failed with ${res.status}`);
      return;
    }
    const data = await res.json<FeedResponse>();
    await syncOutages(env.DB, data.outageList);
    await recordAffectedSnapshot(env.DB);
  },
} satisfies ExportedHandler<Env>;
