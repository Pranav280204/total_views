# app.py
# MrBeast Total Views Tracker (with Target feature)
# - Stores snapshots in PostgreSQL
# - Updates every 10 minutes on exact :00, :10, :20, :30, :40, :50 (IST)
# - Provides /total, /history?day=YYYY-MM-DD, /daily, /snapshot
# - Adds hourly gain calculation (1 hour before; ±5s or nearest within 10min)
# - Adds day_gain for each snapshot row (= total_views - first_total_of_that_day)
# - Adds persistent targets table and /target (GET/POST/DELETE), /required endpoint

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
        # snapshots table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS view_snapshots (
                id SERIAL PRIMARY KEY,
                ts TIMESTAMPTZ NOT NULL,
                total_views BIGINT NOT NULL,
                view_gain BIGINT
            );
        """))
        # targets table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS targets (
                id SERIAL PRIMARY KEY,
                target_total BIGINT NOT NULL,
                target_ts TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
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
        return int(row[0]), row[1], False

    row2 = conn.execute(text("""
        SELECT total_views, ts, abs(EXTRACT(EPOCH FROM ts - :target)) AS diff
        FROM view_snapshots
        ORDER BY diff ASC
        LIMIT 1
    """), {"target": target_ts}).fetchone()

    if row2:
        diff = float(row2[2])
        if diff <= fallback_seconds:
            return int(row2[0]), row2[1], True
    return None, None, False
# -----------------------------------------------------


# ---------------- TARGET HELPERS ----------------
def get_latest_target(conn):
    row = conn.execute(text("""
        SELECT id, target_total, target_ts
        FROM targets
        ORDER BY created_at DESC
        LIMIT 1
    """)).fetchone()
    if not row:
        return None
    return {"id": int(row[0]), "target_total": int(row[1]), "target_ts": row[2]}


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
    Each returned row includes:
      - ts, total_views, view_gain, hourly_gain, hourly_approx, day_gain
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
            first_row = conn.execute(text("""
                SELECT total_views
                FROM view_snapshots
                WHERE ((ts AT TIME ZONE 'Asia/Kolkata')::date) = :day
                ORDER BY ts ASC
                LIMIT 1
            """), {"day": day}).fetchone()
            first_total_for_day = int(first_row[0]) if first_row else None
        else:
            rows = conn.execute(text("""
                SELECT ts, total_views, view_gain
                FROM view_snapshots
                ORDER BY ts DESC
                LIMIT 100
            """)).fetchall()
            days = set()
            for r in rows:
                ts_ = r[0]
                d = (ts_.astimezone(IST).date()) if hasattr(ts_, "astimezone") else (ts_.date())
                days.add(d.isoformat())
            first_totals_map = {}
            for d in days:
                fr = conn.execute(text("""
                    SELECT total_views
                    FROM view_snapshots
                    WHERE ((ts AT TIME ZONE 'Asia/Kolkata')::date) = :day
                    ORDER BY ts ASC
                    LIMIT 1
                """), {"day": d}).fetchone()
                if fr:
                    first_totals_map[d] = int(fr[0])
            first_total_for_day = None

        out = []
        for r in rows:
            ts = r[0]
            tv = int(r[1])
            vg = int(r[2]) if r[2] is not None else None

            # hourly gain
            target_ts = ts - timedelta(hours=1)
            found_tv, found_ts, approx = find_snapshot_near(conn, target_ts, tolerance_seconds=5, fallback_seconds=600)
            hourly_gain = tv - int(found_tv) if found_tv is not None else None

            # day_gain
            day_of_row = (ts.astimezone(IST).date()).isoformat() if hasattr(ts, "astimezone") else ts.date().isoformat()
            if day:
                first_total = first_total_for_day
            else:
                first_total = first_totals_map.get(day_of_row)
            day_gain = (tv - first_total) if first_total is not None else None

            out.append({
                "ts": ts.isoformat(),
                "total_views": tv,
                "view_gain": vg,
                "hourly_gain": int(hourly_gain) if hourly_gain is not None else None,
                "hourly_approx": bool(approx),
                "day_gain": int(day_gain) if day_gain is not None else None
            })

    return jsonify(out)


@app.route("/daily")
def daily():
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

    take_snapshot()
    return jsonify({"status": "ok"})


# ---------------- TARGET ENDPOINTS ----------------
@app.route("/target", methods=["GET", "POST", "DELETE"])
def target():
    """
    GET: returns latest target or null:
      { "target_total": ..., "target_ts": "ISO" }

    POST JSON: { "target_total": 107000000000, "target_ts": "YYYY-MM-DDTHH:MM" }
      - target_ts naive is interpreted as IST (Asia/Kolkata)
    DELETE: removes the latest saved target (useful to clear)
    """
    if engine is None:
        return jsonify({"error": "DB not configured"}), 400

    if request.method == "POST":
        data = request.get_json(force=True, silent=True)
        if not data:
            return jsonify({"error": "missing json body"}), 400

        try:
            target_total = int(data.get("target_total"))
            target_ts_str = data.get("target_ts")
            if not target_ts_str:
                return jsonify({"error": "target_ts required"}), 400

            # parse ISO-like string; if naive -> treat as IST
            try:
                dt = datetime.fromisoformat(target_ts_str)
            except Exception:
                return jsonify({"error": "invalid datetime format; use YYYY-MM-DDTHH:MM"}), 400

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IST)
            else:
                dt = dt.astimezone(IST)

            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO targets (target_total, target_ts)
                    VALUES (:tt, :ts)
                """), {"tt": target_total, "ts": dt})
            return jsonify({"status": "ok", "target_total": target_total, "target_ts": dt.isoformat()})
        except ValueError:
            return jsonify({"error": "invalid target_total"}), 400

    if request.method == "DELETE":
        with engine.begin() as conn:
            # delete the latest target row if exists
            conn.execute(text("""
                DELETE FROM targets
                WHERE id = (
                    SELECT id FROM targets ORDER BY created_at DESC LIMIT 1
                )
            """))
        return jsonify({"status": "ok", "message": "latest target removed"})

    # GET latest target
    with engine.connect() as conn:
        t = get_latest_target(conn)
    if not t:
        return jsonify(None)
    # ensure ISO string
    ts = t["target_ts"].isoformat() if hasattr(t["target_ts"], "isoformat") else str(t["target_ts"])
    return jsonify({"target_total": t["target_total"], "target_ts": ts})


@app.route("/required")
def required():
    """
    Computes required rates from latest snapshot to latest target.
    Returns:
    {
      status: "ok" | "missing_target" | "no_snapshot" | "passed" | "deadline_passed",
      target_total, target_ts, current_total, remaining_needed,
      seconds_remaining, intervals_remaining_10min, per_10min_required, days_remaining, per_day_required
    }
    """
    if engine is None:
        return jsonify({"error": "DB not configured"}), 400

    with engine.connect() as conn:
        snap = conn.execute(text("""
            SELECT total_views, ts
            FROM view_snapshots
            ORDER BY ts DESC
            LIMIT 1
        """)).fetchone()

        t = get_latest_target(conn)

    if not t:
        return jsonify({"status": "missing_target", "message": "No target set yet."})
    if not snap:
        return jsonify({"status": "no_snapshot", "message": "No snapshots available yet."})

    current_total = int(snap[0])
    target_total = int(t["target_total"])
    target_ts = t["target_ts"]
    if hasattr(target_ts, "astimezone"):
        target_ts = target_ts.astimezone(IST)
    else:
        try:
            target_ts = datetime.fromisoformat(str(target_ts))
            if target_ts.tzinfo is None:
                target_ts = target_ts.replace(tzinfo=IST)
            else:
                target_ts = target_ts.astimezone(IST)
        except Exception:
            target_ts = datetime.now(IST)

    now_ist = datetime.now(IST)
    seconds_remaining = (target_ts - now_ist).total_seconds()
    remaining_needed = target_total - current_total

    if seconds_remaining <= 0:
        return jsonify({
            "status": "passed" if remaining_needed <= 0 else "deadline_passed",
            "target_total": target_total,
            "target_ts": target_ts.isoformat(),
            "current_total": current_total,
            "remaining_needed": remaining_needed,
            "seconds_remaining": int(seconds_remaining)
        })

    intervals_remaining = max(1, math.ceil(seconds_remaining / (10 * 60)))
    days_remaining = max(1, math.ceil(seconds_remaining / 86400.0))

    per_10min = math.ceil(remaining_needed / intervals_remaining) if remaining_needed > 0 else 0
    per_day = math.ceil(remaining_needed / days_remaining) if remaining_needed > 0 else 0

    return jsonify({
        "status": "ok",
        "target_total": target_total,
        "target_ts": target_ts.isoformat(),
        "current_total": current_total,
        "remaining_needed": remaining_needed,
        "seconds_remaining": int(seconds_remaining),
        "intervals_remaining_10min": int(intervals_remaining),
        "per_10min_required": int(per_10min),
        "days_remaining": int(days_remaining),
        "per_day_required": int(per_day)
    })
# ----------------------------------------


# ---------------- STARTUP ----------------
init_db()

if not DISABLE_INTERNAL_SCHEDULER:
    threading.Thread(target=scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
# ----------------------------------------
