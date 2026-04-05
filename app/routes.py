from flask import Blueprint, render_template, request, jsonify
from .services.scrobbles_service import fetch_scrobbles, fetch_plays_for_groups
from .services.scrobbles_service import fetch_group_plays
from .services.updates_service import add_update, bulk_update
from datetime import datetime, date, time as dtime, timedelta, timezone
from flask import redirect, url_for, flash
from .services.import_service import run_lastfm_import
from .config import Config


def to_uts(date_str):
    if not date_str:
        return None
    return int(datetime.strptime(date_str, "%Y-%m-%d").timestamp())


def to_uts_start(date_str):
    if not date_str:
        return None
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())

def to_uts_end(date_str):
    if not date_str:
        return None
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    dt_end = dt + timedelta(days=1) - timedelta(seconds=1)
    return int(dt_end.timestamp())


def get_tz(tz_mode: str):
    # tz_mode: 'local' or 'utc'
    if tz_mode == "utc":
        return timezone.utc
    # Use OS local timezone (works on Windows without extra tz packages)
    return datetime.now().astimezone().tzinfo

def parse_date_ymd(s: str):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def day_start_end_to_uts(d: date, tz):
    start = datetime.combine(d, dtime.min, tzinfo=tz)  # 00:00:00
    end = start + timedelta(days=1) - timedelta(seconds=1)  # 23:59:59
    return int(start.timestamp()), int(end.timestamp())

def apply_presets(args: dict, tz):
    preset = args.get("preset")
    if not preset:
        return args, False

    today = datetime.now(tz).date()

    if preset == "today":
        args["date_from"] = today.strftime("%Y-%m-%d")
        args["date_to"] = today.strftime("%Y-%m-%d")

    elif preset == "last7":
        start = today - timedelta(days=6)  # inclusive 7 days including today
        args["date_from"] = start.strftime("%Y-%m-%d")
        args["date_to"] = today.strftime("%Y-%m-%d")

    elif preset == "last30":
        start = today - timedelta(days=30)  # inclusive 30 days including today
        args["date_from"] = start.strftime("%Y-%m-%d")
        args["date_to"] = today.strftime("%Y-%m-%d")

    elif preset == "last365":
        start = today - timedelta(days=365)  # inclusive 365 days including today
        args["date_from"] = start.strftime("%Y-%m-%d")
        args["date_to"] = today.strftime("%Y-%m-%d")

    elif preset == "clear_dates":
        args["date_from"] = ""
        args["date_to"] = ""

    # remove preset so it doesn't stay in URL
    args.pop("preset", None)
    return args, True


bp = Blueprint("main", __name__)

@bp.route("/")
def index():
    return render_template("index.html")

@bp.route("/api/scrobbles")
def api_scrobbles():
    filters = {
        "artist": request.args.get("artist"),
        "album": request.args.get("album"),
        "track": request.args.get("track"),
        "date_from": request.args.get("date_from"),
        "date_to": request.args.get("date_to"),
    }
    data = fetch_scrobbles(filters)
    return jsonify(data)

@bp.route("/api/update", methods=["POST"])
def api_update():
    payload = request.json
    add_update(
        scrobble_id=payload["scrobble_id"],
        record_type=payload["record_type"],
        updated_value=payload["updated_value"]
    )
    return {"status": "ok"}


@bp.route("/scrobbles")
def scrobbles_view():
    sort = request.args.get("sort", "artist")
    direction = request.args.get("dir", "asc")

    tz_mode = request.args.get("tz", "local")  # 'local' or 'utc'
    tz = get_tz(tz_mode)

    # Work with args as dict (so we can redirect after preset)
    args = request.args.to_dict(flat=True)

    # Apply presets and redirect to clean URL
    args, changed = apply_presets(args, tz)
    if changed:
        return redirect(url_for("main.scrobbles_view", **args))

    # Parse dates (strings)
    d_from = parse_date_ymd(args.get("date_from", ""))
    d_to = parse_date_ymd(args.get("date_to", ""))

    date_from_uts = None
    date_to_uts = None

    if d_from:
        date_from_uts, _ = day_start_end_to_uts(d_from, tz)
    if d_to:
        _, date_to_uts = day_start_end_to_uts(d_to, tz)

    # Active range label for UI
    range_label = None
    if date_from_uts or date_to_uts:
        fmt = "%Y-%m-%d %H:%M:%S"
        left = datetime.fromtimestamp(date_from_uts, tz).strftime(fmt) if date_from_uts else "…"
        right = datetime.fromtimestamp(date_to_uts, tz).strftime(fmt) if date_to_uts else "…"
        tz_label = "UTC" if tz_mode == "utc" else f"Local ({datetime.now(tz).tzname()})"
        range_label = f"{left} → {right} ({tz_label})"

    filters = {
        "artist": args.get("artist") or None,
        "album": args.get("album") or None,
        "track": args.get("track") or None,

        # keep original strings for form inputs
        "date_from_str": args.get("date_from") or "",
        "date_to_str": args.get("date_to") or "",

        # numeric boundaries for SQL
        "date_from": date_from_uts,
        "date_to": date_to_uts,

        "show_plays": args.get("show_plays") == "on",
        "tz": tz_mode,
        "range_label": range_label,

        "sort": sort,
        "dir": direction,
    }

    rows = fetch_scrobbles(filters, sort, direction)
    return render_template("index.html", rows=rows, filters=filters)

@bp.route("/api/bulk_update", methods=["POST"])
def api_bulk_update():
    payload = request.json
    inserted = bulk_update(
        record_type=payload["record_type"],
        scope=payload["scope"],
        updated_value=payload["updated_value"]
    )
    return {"status": "ok", "inserted": inserted}


@bp.route("/api/plays")
def api_plays():
    artist = request.args.get("artist") or ""
    album  = request.args.get("album") or ""
    track  = request.args.get("track") or ""

    plays = fetch_group_plays(artist, album, track)
    return jsonify(plays)

@bp.route("/summary")
def summary_view():
    from .services.scrobbles_service import (
        fetch_top_artists,
        fetch_top_albums,
        fetch_top_tracks,
    )

    tz_mode = request.args.get("tz", "local")  # 'local' or 'utc'
    tz = get_tz(tz_mode)

    # Work with args as dict (so we can redirect after preset)
    args = request.args.to_dict(flat=True)

    # Apply presets and redirect to clean URL
    args, changed = apply_presets(args, tz)
    if changed:
        return redirect(url_for("main.summary_view", **args))

    # Parse dates (strings)
    d_from = parse_date_ymd(args.get("date_from", ""))
    d_to = parse_date_ymd(args.get("date_to", ""))

    date_from_uts = None
    date_to_uts = None

    if d_from:
        date_from_uts, _ = day_start_end_to_uts(d_from, tz)
    if d_to:
        _, date_to_uts = day_start_end_to_uts(d_to, tz)

    # Active range label for UI
    range_label = None
    if date_from_uts or date_to_uts:
        fmt = "%Y-%m-%d %H:%M:%S"
        left = datetime.fromtimestamp(date_from_uts, tz).strftime(fmt) if date_from_uts else "…"
        right = datetime.fromtimestamp(date_to_uts, tz).strftime(fmt) if date_to_uts else "…"
        tz_label = "UTC" if tz_mode == "utc" else f"Local ({datetime.now(tz).tzname()})"
        range_label = f"{left} → {right} ({tz_label})"
    top = request.args.get("top", "20")

    try:
        top = int(top)
        top = max(10, min(top, 1000))
    except ValueError:
        top = 20

    filters = {
        "artist": request.args.get("artist") or None,
        "album": request.args.get("album") or None,
        "track": request.args.get("track") or None,
        # keep original strings for form inputs
        "date_from_str": args.get("date_from") or "",
        "date_to_str": args.get("date_to") or "",

        # numeric boundaries for SQL
        "date_from": date_from_uts,
        "date_to": date_to_uts,
        "tz": tz_mode,
        "range_label": range_label,
        "top": top,
    }

    top_artists = fetch_top_artists(filters, top)
    top_albums  = fetch_top_albums(filters, top)
    top_tracks  = fetch_top_tracks(filters, top)

    return render_template(
        "summary.html",
        filters=filters,
        top_artists=top_artists,
        top_albums=top_albums,
        top_tracks=top_tracks,
        max_artist=max((r["scrobbles"] for r in top_artists), default=1),
        max_album=max((r["scrobbles"] for r in top_albums), default=1),
        max_track=max((r["scrobbles"] for r in top_tracks), default=1),
    )


from flask import current_app
import sqlite3, os

@bp.route("/import", methods=["POST"])
def import_scrobbles():
    print(">>> IMPORT ROUTE HIT <<<")
    db_path = current_app.config["DATABASE"]
    print("IMPORT using DB:", os.path.abspath(db_path))

    # quick peek into DB (counts)
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM scrobbles")
    print("scrobbles total before:", cur.fetchone()[0])
    con.close()

    try:
        stats = run_lastfm_import(db_path)
        print("IMPORT stats:", stats)

        # counts after
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM scrobbles")
        print("scrobbles total after:", cur.fetchone()[0])
        con.close()

        flash(
            f"Imported {stats['inserted']} new scrobbles "
            f"(previous_latest_uts={stats['previous_latest_uts']}, pages={stats['pages_fetched']})",
            "success"
        )
    except Exception as e:
        print("IMPORT error:", repr(e))
        flash(str(e), "error")

    return redirect(url_for("main.scrobbles_view"))
