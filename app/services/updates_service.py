import time
from ..db import get_db

def add_update(scrobble_id, record_type, updated_value):
    db = get_db()
    db.execute("""
        INSERT INTO scrobble_updates
        (scrobble_id, record_type, updated_value, update_date)
        VALUES (?, ?, ?, ?)
    """, (scrobble_id, record_type, updated_value, int(time.time())))
    db.commit()


def bulk_update(record_type, scope, updated_value):
    """
    Bulk update scoped to the *exact group row* (artist+album+track) shown in UI.
    Prevents unintended updates across other albums with same track name.
    """
    db = get_db()
    ts = int(time.time())

    artist = scope.get("artist") or ""
    album  = scope.get("album") or ""   # IMPORTANT: keep empty as empty
    track  = scope.get("track") or ""

    sql = """
    WITH ranked_updates AS (
        SELECT
            scrobble_id,
            record_type,
            updated_value,
            ROW_NUMBER() OVER (
                PARTITION BY scrobble_id, record_type
                ORDER BY update_date DESC
            ) AS rn
        FROM scrobble_updates
    ),
    latest_updates AS (
        SELECT scrobble_id, record_type, updated_value
        FROM ranked_updates
        WHERE rn = 1
    ),
    effective_scrobbles AS (
        SELECT
            s.id AS scrobble_id,
            COALESCE(ua.updated_value, s.artist)           AS artist,
            COALESCE(ual.updated_value, s.album, '')      AS album,
            COALESCE(ut.updated_value, s.track)           AS track
        FROM scrobbles s
        LEFT JOIN latest_updates ua
          ON ua.scrobble_id = s.id AND ua.record_type = 'artist'
        LEFT JOIN latest_updates ual
          ON ual.scrobble_id = s.id AND ual.record_type = 'album'
        LEFT JOIN latest_updates ut
          ON ut.scrobble_id = s.id AND ut.record_type = 'track'
    )
    INSERT INTO scrobble_updates (scrobble_id, record_type, updated_value, update_date)
    SELECT
        scrobble_id,
        :record_type,
        :updated_value,
        :update_date
    FROM effective_scrobbles
    WHERE artist = :scope_artist
      AND album  = :scope_album
      AND track  = :scope_track
      AND (
        CASE :record_type
          WHEN 'artist' THEN artist
          WHEN 'album'  THEN album
          WHEN 'track'  THEN track
        END
      ) <> :updated_value;
    """

    params = {
        "record_type": record_type,
        "updated_value": updated_value,
        "update_date": ts,
        "scope_artist": artist,
        "scope_album": album,
        "scope_track": track,
    }

    before = db.total_changes
    db.execute(sql, params)
    db.commit()
    return db.total_changes - before
