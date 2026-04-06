from ..db import get_db

SQL = """
      WITH latest_updates AS (SELECT scrobble_id,
                                     record_type,
                                     updated_value
                              FROM scrobble_updates u
                              WHERE update_date = (SELECT MAX(update_date)
                                                   FROM scrobble_updates
                                                   WHERE scrobble_id = u.scrobble_id
                                                     AND record_type = u.record_type)),
           effective_scrobbles AS (SELECT s.id                                 AS scrobble_id,
                                          s.uts,
                                          datetime(s.uts, 'unixepoch')         AS played_at,

                                          COALESCE(ua.updated_value, s.artist) AS artist,
                                          COALESCE(ual.updated_value, s.album) AS album,
                                          COALESCE(ut.updated_value, s.track)  AS track
                                   FROM scrobbles s
                                            LEFT JOIN latest_updates ua
                                                      ON ua.scrobble_id = s.id AND ua.record_type = 'artist'
                                            LEFT JOIN latest_updates ual
                                                      ON ual.scrobble_id = s.id AND ual.record_type = 'album'
                                            LEFT JOIN latest_updates ut
                                                      ON ut.scrobble_id = s.id AND ut.record_type = 'track')
      SELECT MIN(scrobble_id)                AS scrobble_id,
             artist,
             album,
             track,
             COUNT(*)                        AS play_count,
             MAX(uts)                        AS last_played_uts,
             datetime(MAX(uts), 'unixepoch') AS last_played
      FROM effective_scrobbles
      WHERE (:artist IS NULL OR artist LIKE :artist)
        AND (:album IS NULL OR album LIKE :album)
        AND (:track IS NULL OR track LIKE :track)
        AND (:date_from IS NULL OR uts >= :date_from)
        AND (:date_to IS NULL OR uts <= :date_to)
      GROUP BY artist, album, track
      ORDER BY {sort_col} {sort_dir}, artist, album, track
      LIMIT :limit OFFSET :offset; \
      """


SORT_COLUMNS = {
    "artist": "artist",
    "album": "album",
    "track": "track",
    "plays": "play_count",
    "last_played": "last_played_uts",
}


def fetch_scrobbles(filters, sort_col="artist", sort_dir="asc", limit=50, offset=0):
    db = get_db()

    sort_col = SORT_COLUMNS.get(sort_col, "artist")
    sort_dir = "DESC" if sort_dir == "desc" else "ASC"

    params = {
        "artist": f"%{filters['artist']}%" if filters.get("artist") else None,
        "album": f"%{filters['album']}%" if filters.get("album") else None,
        "track": f"%{filters['track']}%" if filters.get("track") else None,
        "date_from": int(filters["date_from"]) if filters.get("date_from") else None,
        "date_to": int(filters["date_to"]) if filters.get("date_to") else None,
    }

    params.update({
        "limit": limit,
        "offset": offset,
    })

    sql = SQL.format(sort_col=sort_col, sort_dir=sort_dir)
    rows = db.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def fetch_plays_for_groups(rows, filters):
    """
    Returns:
    {
      (artist, album, track): [
          {"played_at": "...", "uts": ...},
          ...
      ]
    }
    """
    db = get_db()

    result = {}

    for row in rows:
        key = (row["artist"], row["album"], row["track"])

        plays = db.execute("""
                           WITH latest_updates AS (SELECT scrobble_id, record_type, updated_value
                                                   FROM scrobble_updates u
                                                   WHERE update_date = (SELECT MAX(update_date)
                                                                        FROM scrobble_updates
                                                                        WHERE scrobble_id = u.scrobble_id
                                                                          AND record_type = u.record_type))
                           SELECT datetime(s.uts, 'unixepoch') AS played_at,
                                  s.uts
                           FROM scrobbles s
                                    LEFT JOIN latest_updates ua
                                              ON ua.scrobble_id = s.id AND ua.record_type = 'artist'
                                    LEFT JOIN latest_updates ual
                                              ON ual.scrobble_id = s.id AND ual.record_type = 'album'
                                    LEFT JOIN latest_updates ut
                                              ON ut.scrobble_id = s.id AND ut.record_type = 'track'
                           WHERE COALESCE(ua.updated_value, s.artist) = ?
                             AND COALESCE(ual.updated_value, s.album) = ?
                             AND COALESCE(ut.updated_value, s.track) = ?
                           ORDER BY s.uts DESC
                           """, key).fetchall()

        result[key] = [dict(p) for p in plays]

    return result


def fetch_group_plays(artist, album, track):
    db = get_db()
    rows = db.execute("""
                      WITH ranked_updates AS (SELECT scrobble_id,
                                                     record_type,
                                                     updated_value,
                                                     ROW_NUMBER() OVER (
                                                         PARTITION BY scrobble_id, record_type
                                                         ORDER BY update_date DESC
                                                         ) AS rn
                                              FROM scrobble_updates),
                           latest_updates AS (SELECT scrobble_id, record_type, updated_value
                                              FROM ranked_updates
                                              WHERE rn = 1),
                           effective_scrobbles AS (SELECT s.id                                     AS scrobble_id,
                                                          s.uts,
                                                          COALESCE(ua.updated_value, s.artist)     AS artist,
                                                          COALESCE(ual.updated_value, s.album, '') AS album,
                                                          COALESCE(ut.updated_value, s.track)      AS track
                                                   FROM scrobbles s
                                                            LEFT JOIN latest_updates ua
                                                                      ON ua.scrobble_id = s.id AND ua.record_type = 'artist'
                                                            LEFT JOIN latest_updates ual
                                                                      ON ual.scrobble_id = s.id AND ual.record_type = 'album'
                                                            LEFT JOIN latest_updates ut
                                                                      ON ut.scrobble_id = s.id AND ut.record_type = 'track')
                      SELECT datetime(uts, 'unixepoch') AS played_at, uts
                      FROM effective_scrobbles
                      WHERE artist = ?
                        AND album = ?
                        AND track = ?
                      ORDER BY uts DESC
                      """, (artist, album, track)).fetchall()

    return [dict(r) for r in rows]

BASE_CTE = """
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
        s.uts,
        COALESCE(ua.updated_value, s.artist) AS artist,
        COALESCE(ual.updated_value, s.album) AS album,
        COALESCE(ut.updated_value, s.track) AS track
    FROM scrobbles s
    LEFT JOIN latest_updates ua
        ON ua.scrobble_id = s.id AND ua.record_type='artist'
    LEFT JOIN latest_updates ual
        ON ual.scrobble_id = s.id AND ual.record_type='album'
    LEFT JOIN latest_updates ut
        ON ut.scrobble_id = s.id AND ut.record_type='track'
)
"""

def fetch_top_artists(filters, limit=20):
    db = get_db()
    return db.execute(
        BASE_CTE + """
        SELECT artist, COUNT(*) AS scrobbles
        FROM effective_scrobbles
        WHERE
            (:artist IS NULL OR artist LIKE :artist)
        AND (:date_from IS NULL OR uts >= :date_from)
        AND (:date_to IS NULL OR uts <= :date_to)
        GROUP BY artist
        ORDER BY scrobbles DESC
        LIMIT :limit
        """,
        {
            "artist": f"%{filters['artist']}%" if filters["artist"] else None,
            "date_from": filters["date_from"],
            "date_to": filters["date_to"],
            "limit": limit,
        },
    ).fetchall()

def fetch_top_albums(filters, limit=20):
    db = get_db()
    return db.execute(
        BASE_CTE + """
        SELECT album, artist, COUNT(*) AS scrobbles
        FROM effective_scrobbles
        WHERE
            (:artist IS NULL OR artist LIKE :artist)
        AND (:album IS NULL OR album LIKE :album)
        AND (:date_from IS NULL OR uts >= :date_from)
        AND (:date_to IS NULL OR uts <= :date_to)
        GROUP BY artist, album
        ORDER BY scrobbles DESC
        LIMIT :limit
        """,
        {
            "artist": f"%{filters['artist']}%" if filters["artist"] else None,
            "album": f"%{filters['album']}%" if filters["album"] else None,
            "date_from": filters["date_from"],
            "date_to": filters["date_to"],
            "limit": limit,
        },
    ).fetchall()

def fetch_top_tracks(filters, limit=20):
    db = get_db()
    return db.execute(
        BASE_CTE + """
        SELECT track, artist, album, COUNT(*) AS scrobbles
        FROM effective_scrobbles
        WHERE
            (:artist IS NULL OR artist LIKE :artist)
        AND (:album IS NULL OR album LIKE :album)
        AND (:track IS NULL OR track LIKE :track)
        AND (:date_from IS NULL OR uts >= :date_from)
        AND (:date_to IS NULL OR uts <= :date_to)
        GROUP BY artist, album, track
        ORDER BY scrobbles DESC
        LIMIT :limit
        """,
        {
            "artist": f"%{filters['artist']}%" if filters["artist"] else None,
            "album": f"%{filters['album']}%" if filters["album"] else None,
            "track": f"%{filters['track']}%" if filters["track"] else None,
            "date_from": filters["date_from"],
            "date_to": filters["date_to"],
            "limit": limit,
        },
    ).fetchall()


def count_scrobbles(filters):
    db = get_db()
    sql = """
    WITH ranked_updates AS (
        SELECT scrobble_id, record_type, updated_value,
               ROW_NUMBER() OVER (
                   PARTITION BY scrobble_id, record_type
                   ORDER BY update_date DESC
               ) AS rn
        FROM scrobble_updates
    ),
    latest_updates AS (
        SELECT scrobble_id, record_type, updated_value
        FROM ranked_updates WHERE rn = 1
    ),
    effective_scrobbles AS (
        SELECT
            COALESCE(ua.updated_value, s.artist) AS artist,
            COALESCE(ual.updated_value, s.album) AS album,
            COALESCE(ut.updated_value, s.track) AS track,
            s.uts
        FROM scrobbles s
        LEFT JOIN latest_updates ua
          ON ua.scrobble_id = s.id AND ua.record_type='artist'
        LEFT JOIN latest_updates ual
          ON ual.scrobble_id = s.id AND ual.record_type='album'
        LEFT JOIN latest_updates ut
          ON ut.scrobble_id = s.id AND ut.record_type='track'
    )
    SELECT COUNT(*) FROM (
        SELECT 1
        FROM effective_scrobbles
        WHERE
            (:artist IS NULL OR artist LIKE :artist)
        AND (:album IS NULL OR album LIKE :album)
        AND (:track IS NULL OR track LIKE :track)
        AND (:date_from IS NULL OR uts >= :date_from)
        AND (:date_to IS NULL OR uts <= :date_to)
        GROUP BY artist, album, track
    )
    """
    row = db.execute(sql, {
        "artist": f"%{filters['artist']}%" if filters["artist"] else None,
        "album": f"%{filters['album']}%" if filters["album"] else None,
        "track": f"%{filters['track']}%" if filters["track"] else None,
        "date_from": filters["date_from"],
        "date_to": filters["date_to"],
    }).fetchone()
    return row[0]
