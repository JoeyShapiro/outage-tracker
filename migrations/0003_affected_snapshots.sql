-- One row per scheduled feed sync: the total customers affected across all
-- currently open outages at that point in time. Used to chart the trend.
-- not sure why this needs its own table. this should be computable from other tables
CREATE TABLE IF NOT EXISTS affected_snapshots (
  ts             TEXT PRIMARY KEY,
  total_affected INTEGER NOT NULL
);
