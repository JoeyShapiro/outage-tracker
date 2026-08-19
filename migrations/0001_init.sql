-- Placeholder table so the D1 pipeline (migrate -> write -> read) can be verified end-to-end.
CREATE TABLE IF NOT EXISTS pings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
