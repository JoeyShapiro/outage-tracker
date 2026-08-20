-- Replaces affected_snapshots: one row per (city, status) per ingest cycle,
-- recording how many outages were open and how many customers were affected.
-- Summing affected across a ts reproduces the old total-affected chart, and
-- grouping by city gives the per-city/per-status history needed to show
-- "change since an hour ago" without a time-travel query over outage_states.
DROP TABLE IF EXISTS affected_snapshots;

CREATE TABLE IF NOT EXISTS outage_snapshots (
  ts       TEXT NOT NULL,
  city     TEXT NOT NULL,
  status   TEXT NOT NULL,
  count    INTEGER NOT NULL,
  affected INTEGER NOT NULL,
  PRIMARY KEY (ts, city, status)
);

CREATE INDEX IF NOT EXISTS idx_outage_snapshots_ts ON outage_snapshots (ts);
CREATE INDEX IF NOT EXISTS idx_outage_snapshots_city_ts ON outage_snapshots (city, ts);
