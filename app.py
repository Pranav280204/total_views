# app.py
# MrBeast Total Views Tracker
# - Stores snapshots in PostgreSQL
# - Updates every 30 minutes
# - Uses Indian Standard Time (IST)
# - Displays data in table on website

import os
import time
import threading
import requests
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, render_template, request
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ---------------- TIMEZONE (IST, WINDOWS SAFE) ----------------
try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception:
    IST = timezone(timedelta(hours=5, minutes=30))
# --------------------------------------------------------------

app = Flask(__name__)

# ---------------- CONFIG ----------------
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
MRBEAST_CHANNEL_ID = os.environ.get(
    "MRBEAST_CHANNEL_ID", "UCX6OQ3DkcsbYNE6H8uQQuVA"
)

DATABASE_URL = os.environ.get("DATABASE_URL")
SNAPSHOT_TOKEN = os.environ.get("SNAPSHOT_TOKEN")

# 30 minutes
SNAPSHOT_INTERVAL_SECONDS = int(
    os.environ.get("SNAPSHOT_INTERVAL_SECONDS", 30 * 60)
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
    return r.json()["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


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
                        "ts": datetime.now(IST),   # ✅ IST stored
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


# ---------------- SCHEDULER ----------------
def scheduler_loop():
    app.logger.info("Scheduler started (30-minute interval)")
    while True:
        take_snapshot()
        time.sleep(SNAPSHOT_INTERVAL_SECONDS)
# -------------------------------------------


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
            SELECT total_views
            FROM view_snapshots
            ORDER BY ts DESC
            LIMIT 1
        """)).fetchone()

    return jsonify({"total_views": row[0] if row else None})


@app.route("/history")
def history():
    if engine is None:
        return jsonify({"error": "DB not configured"}), 400

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT ts, total_views, view_gain
            FROM view_snapshots
            ORDER BY ts DESC
            LIMIT 100
        """)).fetchall()

    return jsonify([
        {
            "ts": r[0].isoformat(),
            "total_views": int(r[1]),
            "view_gain": int(r[2]) if r[2] is not None else None
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
# ----------------------------------------


# ---------------- STARTUP ----------------
init_db()

if not DISABLE_INTERNAL_SCHEDULER:
    threading.Thread(target=scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
# ----------------------------------------
