-- Lets the nearest-outage lookup pre-filter with a bounding box instead of
-- ordering the whole table by a computed distance expression -- outages is
-- append-only (never pruned), so that scan was growing with the tracker's
-- entire history instead of just what's near the search point.
CREATE INDEX IF NOT EXISTS idx_outages_lat ON outages (lat);
CREATE INDEX IF NOT EXISTS idx_outages_lng ON outages (lng);
