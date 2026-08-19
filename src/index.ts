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

export default {
  async fetch(request, env, ctx): Promise<Response> {
    return new Response("outage-tracker: viewer worker is up\n");
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
