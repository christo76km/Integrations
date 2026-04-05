CREATE TABLE IF NOT EXISTS scrobble_updates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scrobble_id INTEGER NOT NULL,
    record_type TEXT NOT NULL CHECK (record_type IN ('artist','album','track')),
    updated_value TEXT NOT NULL,
    update_date INTEGER NOT NULL,

    FOREIGN KEY (scrobble_id) REFERENCES scrobbles(id)
);

CREATE INDEX idx_updates_scrobble_type
    ON scrobble_updates(scrobble_id, record_type, update_date);

commit