import os
import json
import asyncio

DB_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(DB_DIR)
STATS_FILE = os.path.join(DB_DIR, "stats.json")
CACHE_DIR = os.path.join(BASE_DIR, "saved")

_lock = asyncio.Lock()


async def init_db():
    async with _lock:
        if not os.path.exists(STATS_FILE):
            with open(STATS_FILE, "w") as f:
                json.dump({"total_downloads": 0}, f)


async def add_download(data: dict = None):
    async with _lock:
        try:
            with open(STATS_FILE, "r") as f:
                stats = json.load(f)
        except Exception:
            stats = {"total_downloads": 0}
        stats["total_downloads"] = stats.get("total_downloads", 0) + 1
        try:
            with open(STATS_FILE, "w") as f:
                json.dump(stats, f)
        except Exception:
            pass


async def get_stats():
    total_dl = 0
    try:
        with open(STATS_FILE, "r") as f:
            data = json.load(f)
            total_dl = data.get("total_downloads", 0)
    except Exception:
        total_dl = 0

    total_size = 0
    if os.path.exists(CACHE_DIR):
        for dirpath, _, filenames in os.walk(CACHE_DIR):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    cache_size_mb = round(total_size / (1024 * 1024), 2)

    return {
        "total_downloads": total_dl,
        "total_cache_size_mb": cache_size_mb,
    }
    
