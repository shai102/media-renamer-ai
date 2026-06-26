"""海报 URL 解析与预取（PIL 缓存，无 UI 框架依赖）。

从 core/ui/manual_match.py 迁出，剔除 tkinter PhotoImage 缓存分支，
供 Qt 手动匹配（ui_qt/manual_match_qt.py）与手动搜索服务复用。
"""
import io
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

from PIL import Image

from utils.helpers import session

TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w92"
_POSTER_W = 62
_POSTER_H = 93
_POSTER_REQUEST_TIMEOUT = 8
_POSTER_POOL_WORKERS = 4
_POSTER_CACHE_MAX_ITEMS = 120

_poster_executor = ThreadPoolExecutor(
    max_workers=_POSTER_POOL_WORKERS,
    thread_name_prefix="poster",
)
_poster_lock = threading.Lock()
_poster_pil_cache = OrderedDict()
_poster_pending = set()


def _resolve_poster_url(poster_path):
    """Normalize poster_path to a downloadable absolute URL."""
    if not poster_path:
        return ""
    p = str(poster_path).strip()
    if p.startswith("http://") or p.startswith("https://"):
        return p
    if p.startswith("/"):
        return TMDB_IMAGE_BASE + p
    return ""


def _get_cached_poster_pil(url):
    """Thread-safe resized PIL cache read with LRU touch."""
    if not url:
        return None
    with _poster_lock:
        img = _poster_pil_cache.get(url)
        if img is not None:
            _poster_pil_cache.move_to_end(url)
        return img


def _cache_poster_pil(url, img):
    """Thread-safe resized PIL cache write with LRU eviction."""
    if not url or img is None:
        return
    with _poster_lock:
        _poster_pil_cache[url] = img
        _poster_pil_cache.move_to_end(url)
        while len(_poster_pil_cache) > _POSTER_CACHE_MAX_ITEMS:
            _poster_pil_cache.popitem(last=False)


def _fetch_and_resize_poster(url):
    """Download and resize poster, return PIL.Image."""
    resp = session.get(url, timeout=_POSTER_REQUEST_TIMEOUT)
    resp.raise_for_status()
    img = Image.open(io.BytesIO(resp.content)).convert("RGB")
    return img.resize((_POSTER_W, _POSTER_H), Image.LANCZOS)


def _prefetch_poster_urls(urls):
    """Prefetch candidate posters (PIL only) before the dialog opens."""
    unique_urls = []
    seen = set()
    for u in urls:
        url = str(u or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        if _get_cached_poster_pil(url) is not None:
            continue
        unique_urls.append(url)

    if not unique_urls:
        return

    def _prefetch_one(url):
        with _poster_lock:
            if url in _poster_pending:
                return
            _poster_pending.add(url)

        try:
            img = _fetch_and_resize_poster(url)
            _cache_poster_pil(url, img)
        except Exception:
            pass
        finally:
            with _poster_lock:
                _poster_pending.discard(url)

    futures = []
    for url in unique_urls:
        try:
            futures.append(_poster_executor.submit(_prefetch_one, url))
        except Exception:
            pass

    for fut in futures:
        try:
            fut.result(timeout=_POSTER_REQUEST_TIMEOUT + 2)
        except Exception:
            pass
