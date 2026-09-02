"""
song_cache.py — Persistent song cache backed by Turso (libSQL).

Stores the actual audio/video bytes as a BLOB in Turso, keyed by
(video_id, type). This survives host restarts/redeploys, unlike the
local-disk cache in OpusApi/saved/ which is wiped on platforms like
Render/Railway.

Flow:
    1. /stream request comes in for a video_id.
    2. Check local disk cache first (fastest, no network round-trip).
    3. If not on disk, check Turso — if found, write it to disk and
       serve it (also repopulates the fast local cache).
    4. If not in Turso either, download with yt-dlp as before, then
       save into both local disk AND Turso.
"""

import os
import time
import libsql_client

TURSO_DATABASE_URL = os.environ.get("TURSO_DATABASE_URL")
TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

_client = None


def _get_client():
    """Lazily create the (async) libsql client, reused across calls."""
    global _client
    if _client is None:
        if not TURSO_DATABASE_URL or not TURSO_AUTH_TOKEN:
            raise RuntimeError(
                "TURSO_DATABASE_URL / TURSO_AUTH_TOKEN not set — "
                "check your .env file."
            )
        # libsql_client accepts the libsql:// scheme directly for remote DBs.
        _client = libsql_client.create_client(
            url=TURSO_DATABASE_URL,
            auth_token=TURSO_AUTH_TOKEN,
        )
    return _client


async def init_cache_db() -> None:
    """Create the songs table if it doesn't exist yet. Safe to call
    on every startup."""
    client = _get_client()
    await client.execute(
        """
        CREATE TABLE IF NOT EXISTS songs (
            video_id   TEXT NOT NULL,
            type       TEXT NOT NULL,
            ext        TEXT NOT NULL,
            data       BLOB NOT NULL,
            size_bytes INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (video_id, type)
        )
        """
    )


async def get_cached_song(video_id: str, type: str) -> dict | None:
    """Look up a cached song in Turso. Returns
    {"ext": str, "data": bytes} or None if not cached."""
    client = _get_client()
    result = await client.execute(
        "SELECT ext, data FROM songs WHERE video_id = ? AND type = ? LIMIT 1",
        [video_id, type],
    )
    if not result.rows:
        return None
    row = result.rows[0]
    return {"ext": row[0], "data": bytes(row[1])}


async def save_song(video_id: str, type: str, ext: str, data: bytes) -> None:
    """Save (or overwrite) a song's bytes into Turso."""
    client = _get_client()
    await client.execute(
        """
        INSERT INTO songs (video_id, type, ext, data, size_bytes, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (video_id, type) DO UPDATE SET
            ext = excluded.ext,
            data = excluded.data,
            size_bytes = excluded.size_bytes,
            created_at = excluded.created_at
        """,
        [video_id, type, ext, data, len(data), int(time.time())],
    )


async def get_cache_stats() -> dict:
    """Total songs cached and total bytes stored in Turso."""
    client = _get_client()
    result = await client.execute(
        "SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM songs"
    )
    row = result.rows[0]
    return {
        "total_songs_cached": row[0],
        "total_cache_bytes": row[1],
    }

