#!/usr/bin/env python3
import os
import time
import sqlite3
import requests
from typing import Optional, Dict, Any, List, Tuple

LASTFM_API_URL = "https://ws.audioscrobbler.com/2.0/"

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS scrobbles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL,
    uts INTEGER NOT NULL,                  -- unix timestamp (seconds)
    played_at TEXT,                        -- human-readable from API if present
    artist TEXT NOT NULL,
    album TEXT,
    track TEXT NOT NULL,
    track_mbid TEXT,
    artist_mbid TEXT,
    album_mbid TEXT,
    duration INTEGER,
    streamable INTEGER,
    loved INTEGER,
    raw_json TEXT,

    -- Prevent duplicates; chosen to be stable even when MBIDs missing
    UNIQUE(username, uts, artist, track)
);

CREATE INDEX IF NOT EXISTS idx_scrobbles_user_uts ON scrobbles(username, uts);
CREATE INDEX IF NOT EXISTS idx_scrobbles_user_track ON scrobbles(username, artist, track);
"""

def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(SCHEMA_SQL)
    return conn

def get_latest_uts(conn: sqlite3.Connection, username: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(uts), 0) FROM scrobbles WHERE username = ?",
        (username,)
    ).fetchone()
    return int(row[0] or 0)

def lastfm_get_recent_tracks(
    api_key: str,
    username: str,
    page: int = 1,
    limit: int = 200
) -> Dict[str, Any]:
    params = {
        "method": "user.getrecenttracks",
        "user": username,
        "api_key": api_key,
        "format": "json",
        "limit": str(limit),
        "page": str(page),
        # "extended": "1",  # optional, returns more fields
    }
    r = requests.get(LASTFM_API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def normalize_track_item(item: Dict[str, Any]) -> Optional[Tuple]:
    """
    Convert one Last.fm track item to a tuple matching INSERT statement.
    Returns None for 'now playing' items that lack a date/uts.
    """
    # 'now playing' entries have @attr: {"nowplaying":"true"} and no "date"
    date = item.get("date")
    if not date or "uts" not in date:
        return None

    uts = int(date["uts"])
    played_at = date.get("#text")

    artist = (item.get("artist") or {}).get("#text") or ""
    track = item.get("name") or ""
    album = (item.get("album") or {}).get("#text")

    artist_mbid = (item.get("artist") or {}).get("mbid")
    track_mbid = item.get("mbid")
    album_mbid = (item.get("album") or {}).get("mbid")

    streamable = None
    streamable_field = item.get("streamable")
    if isinstance(streamable_field, dict):
        streamable = streamable_field.get("#text")
    elif streamable_field is not None:
        streamable = streamable_field

    loved = item.get("loved")
    duration = item.get("duration")

    import json
    raw_json = json.dumps(item, ensure_ascii=False)

    return (uts, played_at, artist, album, track, track_mbid, artist_mbid, album_mbid, duration, streamable, loved, raw_json)

def insert_scrobble_rows(
    conn: sqlite3.Connection,
    username: str,
    rows: List[Tuple]
) -> int:
    """
    rows tuples are without username; we add username at insert time.
    Returns number of inserted rows (best-effort).
    """
    sql = """
    INSERT OR IGNORE INTO scrobbles (
        username, uts, played_at, artist, album, track,
        track_mbid, artist_mbid, album_mbid,
        duration, streamable, loved, raw_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    before = conn.total_changes

    cur.executemany(
        sql,
        [(username, *r) for r in rows]
    )
    conn.commit()

    after = conn.total_changes
    return after - before

def import_new_scrobbles(
    db_path: str,
    api_key: str,
    username: str,
    limit_per_page: int = 200,
    sleep_seconds: float = 0.25
) -> Dict[str, int]:
    """
    Incremental importer:
    - Finds latest uts in DB
    - Pages backward through recent tracks
    - Inserts only those with uts > latest_uts
    - Stops once all remaining are older/equal
    """
    conn = get_conn(db_path)
    latest_uts = get_latest_uts(conn, username)

    total_inserted = 0
    total_seen_with_date = 0
    pages_fetched = 0

    page = 1
    total_pages = None

    while True:
        data = lastfm_get_recent_tracks(api_key, username, page=page, limit=limit_per_page)
        pages_fetched += 1

        recent = data.get("recenttracks", {})
        tracks = recent.get("track", [])
        if isinstance(tracks, dict):
            tracks = [tracks]  # defensive: sometimes API returns object for single

        # total pages from @attr if present
        attr = recent.get("@attr") or {}
        if total_pages is None:
            try:
                total_pages = int(attr.get("totalPages"))
            except Exception:
                total_pages = None

        batch = []
        min_uts_in_page = None

        for item in tracks:
            row = normalize_track_item(item)
            if row is None:
                continue
            total_seen_with_date += 1

            uts = row[0]
            if min_uts_in_page is None or uts < min_uts_in_page:
                min_uts_in_page = uts

            if uts > latest_uts:
                batch.append(row)

        if batch:
            inserted = insert_scrobble_rows(conn, username, batch)
            total_inserted += inserted

        # Stop condition:
        # If the oldest scrobble in this page is <= latest_uts,
        # then subsequent pages will only be older.
        if min_uts_in_page is not None and min_uts_in_page <= latest_uts:
            break

        # Also stop if we've fetched all pages (if known)
        if total_pages is not None and page >= total_pages:
            break

        page += 1
        if sleep_seconds:
            time.sleep(sleep_seconds)

    conn.close()
    return {
        "inserted": total_inserted,
        "seen_with_date": total_seen_with_date,
        "pages_fetched": pages_fetched,
        "previous_latest_uts": latest_uts
    }

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Incrementally import Last.fm scrobbles into SQLite.")
    parser.add_argument("--db", default="scrobbles.sqlite", help="Path to SQLite DB file")
    parser.add_argument("--user", required=True, help="Last.fm username")
    parser.add_argument("--api-key", default=os.getenv("LASTFM_API_KEY"), help="Last.fm API key (or env LASTFM_API_KEY)")
    parser.add_argument("--limit", type=int, default=200, help="Tracks per page (max 200)")
    args = parser.parse_args()

    if not args.api_key:
        raise SystemExit("Missing API key. Provide --api-key or set LASTFM_API_KEY env var.")

    stats = import_new_scrobbles(args.db, args.api_key, args.user, limit_per_page=args.limit)
    print("Done.")
    print(stats)

if __name__ == "__main__":
    main()