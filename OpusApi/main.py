"""
OpusApi - YouTube media streaming/download engine
Self-hosted FastAPI service backed by yt-dlp.
"""

import os
import re
import time
import uuid
import socket
import asyncio
from fastapi import FastAPI, BackgroundTasks, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

from OpusApi.database.stats import init_db, add_download, get_stats

app = FastAPI(title="OpusApi")

ROOT_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR    = os.path.join(ROOT_DIR, "OpusApi", "saved")
COOKIES_FILE = os.path.join(ROOT_DIR, "cookies.txt")
os.makedirs(CACHE_DIR, exist_ok=True)

DEFAULT_PORT = int(os.environ.get("PORT", 8080))

# ── Auto-cleanup config ──────────────────────────────────────
# When free disk space on CACHE_DIR's volume drops below this, delete
# the least-recently-used cached files until we're back above it.
MIN_FREE_BYTES = int(os.environ.get("MIN_FREE_MB", 500)) * 1024 * 1024


def find_free_port(preferred: int) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("0.0.0.0", preferred))
            return preferred
        except OSError:
            pass
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("0.0.0.0", 0))
        return s.getsockname()[1]


@app.on_event("startup")
async def startup():
    await init_db()

TOKENS     = {}
START_TIME = time.time()

# ── Race-condition fix ──────────────────────────────────────
# One asyncio.Lock per video_id so two concurrent requests for the
# same video don't both spawn yt-dlp and stomp on the same temp file.
_video_locks: dict[str, asyncio.Lock] = {}
_locks_guard = asyncio.Lock()


async def get_video_lock(video_id: str) -> asyncio.Lock:
    async with _locks_guard:
        lock = _video_locks.get(video_id)
        if lock is None:
            lock = asyncio.Lock()
            _video_locks[video_id] = lock
        return lock


def extract_video_id(url: str) -> str:
    if re.match(r'^[a-zA-Z0-9_-]{11}$', url):
        return url

    patterns = [
        r'(?:v=)([a-zA-Z0-9_-]{11})',
        r'youtu\.be/([a-zA-Z0-9_-]{11})',
        r'/shorts/([a-zA-Z0-9_-]{11})',
        r'/embed/([a-zA-Z0-9_-]{11})',
        r'/live/([a-zA-Z0-9_-]{11})',
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)

    return url


def find_cached_file(video_id: str, type: str) -> str | None:
    if type == "audio":
        exts = ["m4a", "opus", "webm", "mp3", "ogg"]
    else:
        exts = ["mp4", "mkv", "webm"]

    for ext in exts:
        path = os.path.join(CACHE_DIR, f"{video_id}.{ext}")
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return path
    return None


def _move_to_cache(tmp_path: str, cache_path: str) -> None:
    try:
        if os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0:
            os.replace(tmp_path, cache_path)
    except Exception:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


# ── Disk-space-aware LRU eviction ────────────────────────────

def _free_bytes(path: str) -> int:
    usage = os.statvfs(path)
    return usage.f_bavail * usage.f_frsize


def _cached_files_by_lru() -> list[str]:
    """Cached media files under CACHE_DIR, oldest-accessed first.
    Uses last-access time (mtime, since FileResponse doesn't bump atime
    reliably on all filesystems) so files served more recently survive.
    Skips .tmp partial-download files — those are still in progress.
    """
    entries = []
    for fname in os.listdir(CACHE_DIR):
        if ".tmp." in fname:
            continue
        fpath = os.path.join(CACHE_DIR, fname)
        if os.path.isfile(fpath):
            entries.append((os.path.getmtime(fpath), fpath))
    entries.sort(key=lambda pair: pair[0])
    return [fpath for _, fpath in entries]


def free_up_space() -> None:
    """Delete least-recently-used cached files until free space is
    back above MIN_FREE_BYTES. Called before starting a new download."""
    try:
        if _free_bytes(CACHE_DIR) >= MIN_FREE_BYTES:
            return
        for fpath in _cached_files_by_lru():
            if _free_bytes(CACHE_DIR) >= MIN_FREE_BYTES:
                break
            try:
                os.remove(fpath)
            except Exception:
                pass
    except Exception:
        # Never let cleanup failures block a download.
        pass


def touch_cached_file(path: str) -> None:
    """Bump a cached file's mtime on access so it counts as recently
    used and survives the next eviction pass."""
    try:
        os.utime(path, None)
    except Exception:
        pass


@app.get("/")
async def home(request: Request):
    uptime = round(time.time() - START_TIME, 2)
    return JSONResponse({
        "status":  "Running...",
        "owner":   "OpusApi",
        "uptime":  f"{uptime}s",
        "message": "Welcome to OpusApi",
    })


@app.get("/stats")
async def api_stats(request: Request):
    stats = await get_stats()
    total_dl = stats.get("total_downloads", 0)
    cache_size = sum(os.path.getsize(os.path.join(CACHE_DIR, f)) for f in os.listdir(CACHE_DIR) if os.path.isfile(os.path.join(CACHE_DIR, f)))
    cache_mb = round(cache_size / (1024 * 1024), 2)
    free_mb = round(_free_bytes(CACHE_DIR) / (1024 * 1024), 2)
    return JSONResponse({
        "status":               "success",
        "total_song_downloads": total_dl,
        "total_cache_size_mb":  cache_mb,
        "free_disk_mb":         free_mb,
        "active_tokens":        len(TOKENS),
    })


@app.get("/download")
async def generate_token(request: Request, url: str, type: str = "audio"):
    video_id   = extract_video_id(url)
    opus_token = f"OpusApi{uuid.uuid4().hex[:16]}OpusBots"
    TOKENS[opus_token] = {
        "video_id": video_id,
        "type":     type,
        "expires":  time.time() + 300,
    }
    return JSONResponse({
        "status":         "success",
        "video_id":       video_id,
        "download_token": opus_token,
        "usage":          "Use token parameter in /stream endpoint",
    })


@app.get("/stream/{video_id}")
async def stream_music(
    request:          Request,
    video_id:         str,
    background_tasks: BackgroundTasks,
    type:             str = "audio",
    token:            str = None,
    x_download_token = Header(None),
):
    actual_token = token or x_download_token
    if not actual_token or actual_token not in TOKENS:
        raise HTTPException(status_code=401, detail="Invalid Token Access Denied")

    token_data = TOKENS[actual_token]
    if time.time() > token_data["expires"] or token_data["video_id"] != video_id:
        TOKENS.pop(actual_token, None)
        raise HTTPException(status_code=401, detail="Token Expired")

    # Fast path: already cached, no lock needed.
    cached = find_cached_file(video_id, type)
    if cached:
        touch_cached_file(cached)
        await add_download({"video_id": video_id})
        return FileResponse(
            cached,
            media_type="audio/mp4" if type == "audio" else "video/mp4",
        )

    # Serialize downloads per video_id so concurrent requests for the
    # same video don't race on the same temp file.
    lock = await get_video_lock(video_id)
    async with lock:
        # Re-check cache: another request may have finished downloading
        # this video while we were waiting for the lock.
        cached = find_cached_file(video_id, type)
        if cached:
            touch_cached_file(cached)
            await add_download({"video_id": video_id})
            return FileResponse(
                cached,
                media_type="audio/mp4" if type == "audio" else "video/mp4",
            )

        # Make room before downloading, so a nearly-full disk doesn't
        # fail the download partway through.
        free_up_space()

        outtmpl = os.path.join(CACHE_DIR, f"{video_id}.tmp.%(ext)s")

        if type == "audio":
            cmd = [
                "yt-dlp",
                "--cookies", COOKIES_FILE,
                "--js-runtimes", "deno",
                "--remote-components", "ejs:github",
                "--extractor-args", "youtube:player_client=tv,mweb",
                "-f", "bestaudio[ext=m4a]/bestaudio[ext=opus]/bestaudio/best",
                "-o", outtmpl,
                "--quiet",
                video_id,
            ]
        else:
            cmd = [
                "yt-dlp",
                "--cookies", COOKIES_FILE,
                "--js-runtimes", "deno",
                "--remote-components", "ejs:github",
                "--extractor-args", "youtube:player_client=tv,mweb",
                "-f", "(bestvideo[height<=?720]+bestaudio)/best",
                "-o", outtmpl,
                "--quiet",
                video_id,
            ]

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await process.communicate()

            if process.returncode != 0:
                raise HTTPException(
                    status_code=500,
                    detail=f"yt-dlp error: {stderr.decode()[:300]}",
                )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        actual_tmp = None
        for fname in os.listdir(CACHE_DIR):
            if fname.startswith(f"{video_id}.tmp.") and not fname.endswith(".tmp"):
                actual_tmp = os.path.join(CACHE_DIR, fname)
                break

        if not actual_tmp or not os.path.exists(actual_tmp):
            raise HTTPException(status_code=500, detail="Download failed — file not found")

        actual_ext  = actual_tmp.rsplit(".", 1)[-1]
        final_cache = os.path.join(CACHE_DIR, f"{video_id}.{actual_ext}")

        await add_download({"video_id": video_id})

        # Move into place before releasing the lock so any request that
        # was waiting on us sees the finished file, not a half-written one.
        _move_to_cache(actual_tmp, final_cache)

        return FileResponse(
            final_cache,
            media_type="audio/mp4" if type == "audio" else "video/mp4",
        )


if __name__ == "__main__":
    import uvicorn
    port = find_free_port(DEFAULT_PORT)
    uvicorn.run(app, host="0.0.0.0", port=port)
