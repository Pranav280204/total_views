# app.py
# MrBeast Total Views Tracker
# - Stores snapshots in PostgreSQL
# - Updates every 10 minutes on exact :00, :10, :20, :30, :40, :50 (IST)
# - Provides /total, /history?day=YYYY-MM-DD, /daily, /snapshot
# - Adds hourly gain calculation (1 hour before; +/-5s tolerant then best within 10min)
# - No "target" feature (removed)

import os
import time
import threading
import requests
import math
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, render_template, request
from sqlalchemy import create_engine, text

# ---------------- TIMEZONE (IST, WINDOWS SAFE) ----------------
try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception:
    IST = timezone(timedelta(hours=5, minutes=30))
# --------------------------------------------------------------

app = Flask(__name__, static_folder="static", template_folder="templates")

# ---------------- CONFIG ----------------
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
MRBEAST_CHANNEL_ID = os.environ.get(
    "MRBEAST_CHANNEL_ID", "UCX6OQ3DkcsbYNE6H8uQQuVA"
)

DATABASE_URL = os.environ.get("DATABASE_URL")
SNAPSHOT_TOKEN = os.environ.get("SNAPSHOT_TOKEN")

# fetch every 10 minutes on :00, :10, :20, ...
SNAPSHOT_INTERVAL_SECONDS = int(
    os.environ.get("SNAPSHOT_INTERVAL_SECONDS", 10 * 60)
)

DISABLE_INTERNAL_SCHEDULER = os.environ.get(
    "DISABLE_INTERNAL_SCHEDULER", "false"
).lower() in ("1", "true", "yes")

ADVISORY_LOCK_KEY = int(os.environ.get("PG_ADVISORY_LOCK_KEY", "123456789"))

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
# ----------------------------------------

# ---------------- DATABASE ----------------
engine = None
if DATABASE_URL:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)


def init_db():
    if engine is None:
        app.logger.warning("DATABASE_URL not set; DB disabled")
        return

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS view_snapshots (
                id SERIAL PRIMARY KEY,
                ts TIMESTAMPTZ NOT NULL,
                total_views BIGINT NOT NULL,
                view_gain BIGINT
            );
        """))
# ------------------------------------------


# ---------------- YOUTUBE HELPERS ----------------
def get_uploads_playlist_id(channel_id: str) -> str:
    r = requests.get(
        f"{YOUTUBE_API_BASE}/channels",
        params={
            "part": "contentDetails",
            "id": channel_id,
            "key": YOUTUBE_API_KEY
        },
        timeout=20
    )
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items:
        raise RuntimeError("channel not found or no contentDetails")
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_all_video_ids(playlist_id: str):
    ids = []
    page_token = None

    while True:
        params = {
            "part": "contentDetails",
            "playlistId": playlist_id,
            "maxResults": 50,
            "key": YOUTUBE_API_KEY
        }
        if page_token:
            params["pageToken"] = page_token

        r = requests.get(
            f"{YOUTUBE_API_BASE}/playlistItems",
            params=params,
            timeout=20
        )
        r.raise_for_status()
        data = r.json()

        for item in data.get("items", []):
            ids.append(item["contentDetails"]["videoId"])

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return ids


def get_total_views(video_ids):
    total = 0
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        r = requests.get(
            f"{YOUTUBE_API_BASE}/videos",
            params={
                "part": "statistics",
                "id": ",".join(batch),
                "key": YOUTUBE_API_KEY
            },
            timeout=30
        )
        r.raise_for_status()
        for item in r.json().get("items", []):
            total += int(item["statistics"].get("viewCount", 0))
    return total
# -------------------------------------------------


# ---------------- SNAPSHOT LOGIC ----------------
def take_snapshot():
    # preserve existing tracking: if DB or API missing, do nothing
    if not YOUTUBE_API_KEY or engine is None:
        app.logger.debug("Skipping snapshot: missing API key or DB.")
        return

    try:
        uploads = get_uploads_playlist_id(MRBEAST_CHANNEL_ID)
        video_ids = get_all_video_ids(uploads)
        total_views = get_total_views(video_ids)

        with engine.begin() as conn:
            locked = conn.execute(
                text("SELECT pg_try_advisory_lock(:k)"),
                {"k": ADVISORY_LOCK_KEY}
            ).scalar()

            if not locked:
                app.logger.info("Could not acquire advisory lock; skipping snapshot.")
                return

            try:
                prev = conn.execute(text("""
                    SELECT total_views
                    FROM view_snapshots
                    ORDER BY ts DESC
                    LIMIT 1
                """)).fetchone()

                prev_total = prev[0] if prev else None
                gain = None if prev_total is None else total_views - prev_total

                conn.execute(
                    text("""
                        INSERT INTO view_snapshots (ts, total_views, view_gain)
                        VALUES (:ts, :tv, :vg)
                    """),
                    {
                        "ts": datetime.now(IST),
                        "tv": total_views,
                        "vg": gain
                    }
                )
            finally:
                conn.execute(
                    text("SELECT pg_advisory_unlock(:k)"),
                    {"k": ADVISORY_LOCK_KEY}
                )

        app.logger.info("Snapshot stored: %s", total_views)

    except Exception:
        app.logger.exception("Snapshot failed")
# -------------------------------------------------


# ---------------- SCHEDULER (10-minute aligned) ----------------
def seconds_until_next_multiple_of_ten(now_dt):
    if now_dt.minute % 10 == 0 and now_dt.second == 0:
        return 0
    minutes_to_add = 10 - (now_dt.minute % 10)
    next_dt = (now_dt + timedelta(minutes=minutes_to_add)).replace(second=0, microsecond=0)
    delta = (next_dt - now_dt).total_seconds()
    return max(0, int(delta))


def scheduler_loop():
    app.logger.info("Scheduler started (10-minute aligned interval)")
    while True:
        now_ist = datetime.now(IST)
        wait = seconds_until_next_multiple_of_ten(now_ist)
        if wait > 0:
            app.logger.info("Sleeping %ds until next aligned snapshot (:00/:10/:20/:30/:40/:50 IST).", wait)
            time.sleep(wait)
        take_snapshot()
        time.sleep(SNAPSHOT_INTERVAL_SECONDS)
# -------------------------------------------


# ---------------- HELPERS FOR HOURLY GAIN ----------------
def find_snapshot_near(conn, target_ts, tolerance_seconds=5, fallback_seconds=600):
    """
    Attempt to find a snapshot close to target_ts.
    1) Search for ts between target_ts - tolerance_seconds and target_ts + tolerance_seconds.
    2) If not found, search for the nearest snapshot by absolute difference but only accept it
       if it's within fallback_seconds (default 600s = 10min). Return (total_views, ts, approx_flag).
    If nothing, return (None, None, False).
    """
    # 1) exact +/- tolerance
    start = target_ts - timedelta(seconds=tolerance_seconds)
    end = target_ts + timedelta(seconds=tolerance_seconds)
    row = conn.execute(text("""
        SELECT total_views, ts
        FROM view_snapshots
        WHERE ts BETWEEN :start AND :end
        ORDER BY abs(EXTRACT(EPOCH FROM ts - :target)) ASC
        LIMIT 1
    """), {"start": start, "end": end, "target": target_ts}).fetchone()

    if row:
        return int(row[0]), row[1], False  # exact (within tolerance)

    # 2) nearest within fallback_seconds
    row2 = conn.execute(text("""
        SELECT total_views, ts, abs(EXTRACT(EPOCH FROM ts - :target)) AS diff
        FROM view_snapshots
        ORDER BY diff ASC
        LIMIT 1
    """), {"target": target_ts}).fetchone()

    if row2:
        diff = float(row2[2])
        if diff <= fallback_seconds:
            return int(row2[0]), row2[1], True  # approximate
    return None, None, False
# -----------------------------------------------------


# ---------------- ROUTES ----------------
@app.route("/")
def home():
    return render_template("index.html")


@app.route("/total")
def total():
    if engine is None:
        return jsonify({"error": "DB not configured"}), 400

    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT total_views, ts
            FROM view_snapshots
            ORDER BY ts DESC
            LIMIT 1
        """)).fetchone()

    if not row:
        return jsonify({"total_views": None})
    return jsonify({"total_views": int(row[0]), "ts": row[1].isoformat()})


@app.route("/history")
def history():
    """
    Returns recent snapshots or snapshots for a specific day.
    Query param:
      - day (optional): YYYY-MM-DD (IST) to filter snapshots for that calendar day in IST
    Adds hourly_gain and hourly_approx fields for each snapshot.
    """
    if engine is None:
        return jsonify({"error": "DB not configured"}), 400

    day = request.args.get("day")
    with engine.connect() as conn:
        if day:
            rows = conn.execute(text("""
                SELECT ts, total_views, view_gain
                FROM view_snapshots
                WHERE ((ts AT TIME ZONE 'Asia/Kolkata')::date) = :day
                ORDER BY ts DESC
            """), {"day": day}).fetchall()
        else:
            rows = conn.execute(text("""
                SELECT ts, total_views, view_gain
                FROM view_snapshots
                ORDER BY ts DESC
                LIMIT 100
            """)).fetchall()

        out = []
        for r in rows:
            ts = r[0]
            tv = int(r[1])
            vg = int(r[2]) if r[2] is not None else None

            # compute hourly gain: find snapshot approx 1 hour earlier
            target_ts = ts - timedelta(hours=1)
            found_tv, found_ts, approx = find_snapshot_near(conn, target_ts, tolerance_seconds=5, fallback_seconds=600)
            if found_tv is not None:
                hourly_gain = tv - int(found_tv)
            else:
                hourly_gain = None

            out.append({
                "ts": ts.isoformat(),
                "total_views": tv,
                "view_gain": vg,
                "hourly_gain": int(hourly_gain) if hourly_gain is not None else None,
                "hourly_approx": bool(approx)
            })

    return jsonify(out)


@app.route("/daily")
def daily():
    """
    Returns day-wise aggregates:
      - day (YYYY-MM-DD IST)
      - first_total: total_views at first snapshot of the day (IST)
      - last_total: total_views at last snapshot of the day (IST)
      - daily_gain: last_total - first_total
      - snaps: number of snapshots recorded that day
    """
    if engine is None:
        return jsonify({"error": "DB not configured"}), 400

    with engine.connect() as conn:
        rows = conn.execute(text("""
            WITH extremes AS (
                SELECT ((ts AT TIME ZONE 'Asia/Kolkata')::date) AS day,
                       MIN(ts) AS first_ts,
                       MAX(ts) AS last_ts,
                       COUNT(*) as snaps
                FROM view_snapshots
                GROUP BY day
                ORDER BY day DESC
                LIMIT 365
            )
            SELECT
                e.day::text AS day,
                v_first.total_views AS first_total,
                v_last.total_views AS last_total,
                (v_last.total_views - v_first.total_views) AS daily_gain,
                e.snaps
            FROM extremes e
            JOIN view_snapshots v_first ON v_first.ts = e.first_ts
            JOIN view_snapshots v_last ON v_last.ts = e.last_ts
            ORDER BY e.day DESC;
        """)).fetchall()

    return jsonify([
        {
            "day": r[0],
            "first_total": int(r[1]),
            "last_total": int(r[2]),
            "daily_gain": int(r[3]),
            "snaps": int(r[4])
        }
        for r in rows
    ])


@app.route("/snapshot", methods=["POST", "GET"])
def snapshot():
    token = request.args.get("token")
    if SNAPSHOT_TOKEN and token != SNAPSHOT_TOKEN:
        return jsonify({"error": "invalid token"}), 403

    # immediate snapshot run
    take_snapshot()
    return jsonify({"status": "ok"})
# ----------------------------------------

# ---------------- STARTUP ----------------
init_db()

if not DISABLE_INTERNAL_SCHEDULER:
    threading.Thread(target=scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
# ----------------------------------------
