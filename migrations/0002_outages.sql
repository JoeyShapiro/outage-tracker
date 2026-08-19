-- Identity registry: one row per distinct outage.
-- outage_id is a stable natural key built in app code from (lat, lng, reported),
-- e.g. `${lat}_${lng}_${reported}` -- this trio was verified unique across a
-- full snapshot (3980/3981 distinct), and including `reported` means a new
-- incident at the same address gets a new id instead of merging into the old one.
CREATE TABLE IF NOT EXISTS outages (
  outage_id   TEXT PRIMARY KEY,
  lat         REAL NOT NULL,
  lng         REAL NOT NULL,
  city        TEXT NOT NULL,
  zip         TEXT NOT NULL,
  reported    TEXT NOT NULL,
  first_seen  TEXT NOT NULL DEFAULT (datetime('now')),
  resolved_at TEXT
);

-- Change log: one row per distinct state an outage was in, for as long as it held.
-- Only written when a poll's tracked fields differ from the currently open row
-- (valid_to IS NULL) for that outage -- not one row per poll.
CREATE TABLE IF NOT EXISTS outage_states (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  outage_id         TEXT NOT NULL REFERENCES outages(outage_id),
  valid_from        TEXT NOT NULL,
  valid_to          TEXT,
  status            TEXT NOT NULL,
  cause             TEXT,
  comment           TEXT,
  affected          INTEGER NOT NULL,
  max_affected      INTEGER NOT NULL,
  restore_estimate  TEXT,
  storm_mode        INTEGER NOT NULL DEFAULT 0
);

-- Per-house timeline lookups.
CREATE INDEX IF NOT EXISTS idx_outage_states_outage_id_valid_from
  ON outage_states (outage_id, valid_from);

-- Fast "what's the current state" lookup (open rows only).
CREATE INDEX IF NOT EXISTS idx_outage_states_open
  ON outage_states (outage_id)
  WHERE valid_to IS NULL;
