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

function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]!));
}

function pageShell(title: string, body: string): string {
  return `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>${escapeHtml(title)}</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 2rem; }
  table { border-collapse: collapse; width: 100%; max-width: 600px; }
  th, td { text-align: left; padding: 0.4rem 0.8rem; border-bottom: 1px solid #ddd; }
  th { font-weight: 600; }
  form { margin: 1rem 0 2rem; display: flex; gap: 0.5rem; align-items: end; }
  label { display: flex; flex-direction: column; font-size: 0.85rem; }
  input { padding: 0.3rem; }
  .error { color: #b00020; }
  .muted { color: #666; }
</style>
</head>
<body>
${body}
</body>
</html>`;
}

function renderSearchForm(lat?: string, lng?: string): string {
  return `<form method="GET" action="/search">
  <label>Latitude <input type="number" step="any" name="lat" value="${escapeHtml(lat ?? "")}" required></label>
  <label>Longitude <input type="number" step="any" name="lng" value="${escapeHtml(lng ?? "")}" required></label>
  <button type="submit">Find my outages</button>
</form>`;
}

function renderCityList(rows: CityTotal[]): string {
  const items = rows
    .map(
      (r) =>
        `<tr><td>${escapeHtml(r.city)}</td><td>${r.outage_count}</td><td>${r.total_affected}</td></tr>`
    )
    .join("\n");

  return pageShell(
    "Current outages",
    `<h1>Find your outage</h1>
${renderSearchForm()}
<h2>Current outages by city</h2>
<table>
<thead><tr><th>City</th><th>Outages</th><th>Affected</th></tr></thead>
<tbody>${items}</tbody>
</table>`
  );
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

function renderSearchResults(lat: string, lng: string, nearest: NearestOutage | null, timeline: TimelineRow[]): string {
  if (!nearest) {
    return pageShell(
      "No outages found",
      `<h1>Find your outage</h1>
${renderSearchForm(lat, lng)}
<p class="muted">No outages have been recorded yet.</p>`
    );
  }

  const rows = timeline
    .map(
      (r) =>
        `<tr>
          <td>${escapeHtml(r.status)}</td>
          <td>${escapeHtml(r.cause ?? "")}</td>
          <td>${r.affected}</td>
          <td>${escapeHtml(r.valid_from)}</td>
          <td>${r.valid_to ? escapeHtml(r.valid_to) : "ongoing"}</td>
        </tr>`
    )
    .join("\n");

  return pageShell(
    "Your outage timeline",
    `<h1>Find your outage</h1>
${renderSearchForm(lat, lng)}
<p>Nearest known location: <strong>${escapeHtml(nearest.city)}, ${escapeHtml(nearest.zip)}</strong>
(${nearest.lat}, ${nearest.lng})</p>
<table>
<thead><tr><th>Status</th><th>Cause</th><th>Affected</th><th>From</th><th>To</th></tr></thead>
<tbody>${rows}</tbody>
</table>`
  );
}

function html(body: string, status = 200): Response {
  return new Response(body, { status, headers: { "content-type": "text/html; charset=utf-8" } });
}

async function handleHome(env: Env): Promise<Response> {
  const { results } = await env.DB.prepare(
    `SELECT o.city AS city, COUNT(*) AS outage_count, SUM(os.affected) AS total_affected
     FROM outage_states os
     JOIN outages o ON o.outage_id = os.outage_id
     WHERE os.valid_to IS NULL
     GROUP BY o.city
     ORDER BY o.city ASC`
  ).all<CityTotal>();

  return html(renderCityList(results));
}

async function handleSearch(env: Env, url: URL): Promise<Response> {
  const latParam = url.searchParams.get("lat") ?? "";
  const lngParam = url.searchParams.get("lng") ?? "";
  const lat = Number(latParam);
  const lng = Number(lngParam);

  if (latParam === "" || lngParam === "" || !Number.isFinite(lat) || !Number.isFinite(lng)) {
    return html(
      pageShell(
        "Invalid search",
        `<h1>Find your outage</h1>
${renderSearchForm(latParam, lngParam)}
<p class="error">Enter a valid latitude and longitude.</p>`
      ),
      400
    );
  }

  // Nearest-neighbor by plain squared distance in degree space -- fine at the
  // scale of a single utility's service territory, no need for haversine.
  const nearest = await env.DB.prepare(
    `SELECT outage_id, lat, lng, city, zip
     FROM outages
     ORDER BY (lat - ?) * (lat - ?) + (lng - ?) * (lng - ?) ASC
     LIMIT 1`
  )
    .bind(lat, lat, lng, lng)
    .first<NearestOutage>();

  if (!nearest) {
    return html(renderSearchResults(latParam, lngParam, null, []));
  }

  const { results: timeline } = await env.DB.prepare(
    `SELECT os.status, os.cause, os.comment, os.affected, os.max_affected, os.restore_estimate,
            os.valid_from, os.valid_to
     FROM outage_states os
     JOIN outages o ON o.outage_id = os.outage_id
     WHERE o.lat = ? AND o.lng = ?
     ORDER BY os.valid_from ASC`
  )
    .bind(nearest.lat, nearest.lng)
    .all<TimelineRow>();

  return html(renderSearchResults(latParam, lngParam, nearest, timeline));
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/search") return handleSearch(env, url);
    return handleHome(env);
  },

  async scheduled(event, env, ctx): Promise<void> {
    const res = await fetch(FEED_URL);
    if (!res.ok) {
      console.error(`outage-tracker: feed fetch failed with ${res.status}`);
      return;
    }
    const data = await res.json<FeedResponse>();
    await syncOutages(env.DB, data.outageList);
  },
} satisfies ExportedHandler<Env>;
