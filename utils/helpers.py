import atexit
import json
import logging
import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from xml.dom import minidom

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENT = "MyMediaRenamer/73.0 (Fully Customizable Edition)"
CONFIG_FILE = "renamer_config.json"
CACHE_FILE = "api_cache.json"
CACHE_EXPIRY_DAYS = 7
CACHE_FLUSH_INTERVAL_SECONDS = 8
CACHE_FLUSH_MAX_WRITES = 20

TIMEOUT_IMAGE_DOWNLOAD = (10, 30)
TIMEOUT_DB_SEARCH = (6, 20)
TIMEOUT_DB_DETAIL = (8, 25)
TIMEOUT_AI_CHAT = (10, 50)
TIMEOUT_AI_TEST = (8, 25)
TIMEOUT_OLLAMA_TAGS = (4, 12)
TIMEOUT_OLLAMA_CHAT = (8, 45)
TIMEOUT_OLLAMA_EMBED = (8, 30)

DEFAULT_TV_FORMAT = "{title} - S{s:02d}E{e:02d} - {ep_name}{ext}"
DEFAULT_MOVIE_FORMAT = "{title} ({year}){ext}"
DEFAULT_VIDEO_EXTS = ".mp4,.mkv,.avi,.rmvb,.ts,.wmv,.strm"
DEFAULT_SUB_AUDIO_EXTS = ".srt,.ass,.ssa,.vtt,.sub,.idx,.sup,.mka"
DEFAULT_LANG_TAGS = (
    "sc|tc|chs|cht|zh|zh-CN|zh-TW|jap|en|big5|gbk|utf8|default|forced|jpsc|jptc"
)

_LANG_TAG_PART = "|".join(
    re.escape(t) for t in DEFAULT_LANG_TAGS.split("|") if t.strip()
)
LANG_TAG_TOKEN_RE = re.compile(
    rf"(?i)(?:(?<=^)|(?<=[\s._\-\[\(]))(?:{_LANG_TAG_PART})(?:(?=$)|(?=[\s._\-\]\)]))"
)
INVALID_QUERY_TITLES = {
    "unknown",
    "none",
    "null",
    "untitled",
    "na",
    "nan",
    "未知",
}
INVALID_QUERY_TITLES_NORMALIZED = set(INVALID_QUERY_TITLES)

GENERIC_SEASON_TITLE_RE = re.compile(
    r"(?i)^(?:season\s*\d{1,2}|s\s*\d{1,2}|第\s*\d{1,2}\s*季)$"
)

VERSION_TAG_RE = re.compile(r"\[(NC\.Ver|SP|OVA|Extra|Special|OAD|Creditless)\]", re.I)
EPISODE_NOISE_NUMBERS = {2160, 1080, 720, 480, 265, 264, 10}

ERROR_CODE_TIMEOUT = "TIMEOUT"
ERROR_CODE_CONFIG = "CONFIG"
ERROR_CODE_HTTP = "HTTP"
ERROR_CODE_PARSE = "PARSE"
ERROR_CODE_NO_RESULT = "NO_RESULT"
ERROR_CODE_INVALID = "INVALID"
ERROR_CODE_UNKNOWN = "UNKNOWN"
ERROR_CODES = {
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_CONFIG,
    ERROR_CODE_HTTP,
    ERROR_CODE_PARSE,
    ERROR_CODE_NO_RESULT,
    ERROR_CODE_INVALID,
    ERROR_CODE_UNKNOWN,
}

_cache_file_lock = threading.Lock()
_cache_data = None
_cache_dirty = False
_cache_write_count = 0
_cache_last_flush_ts = 0.0


def format_error_message(code, message):
    code_text = str(code or "").strip().upper()
    message_text = str(message or "").strip()
    if code_text in ERROR_CODES:
        return f"{code_text}:{message_text}"
    return message_text


def parse_error_message(message):
    text = str(message or "").strip()
    if not text:
        return "", ""

    if ":" in text:
        code, detail = text.split(":", 1)
        code = code.strip().upper()
        if code in ERROR_CODES:
            return code, detail.strip()

    if "超时" in text:
        return ERROR_CODE_TIMEOUT, text
    if "未配置" in text:
        return ERROR_CODE_CONFIG, text
    if "HTTP" in text:
        return ERROR_CODE_HTTP, text
    if "解析失败" in text or "JSON" in text:
        return ERROR_CODE_PARSE, text
    if "无结果" in text or "未匹配" in text:
        return ERROR_CODE_NO_RESULT, text
    if "无效" in text:
        return ERROR_CODE_INVALID, text
    return ERROR_CODE_UNKNOWN, text


def create_retry_session(
    retries=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504]
):
    """Create a requests session with retry policy."""
    req_session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    req_session.mount("https://", adapter)
    req_session.mount("http://", adapter)
    return req_session


session = create_retry_session()

# 限制同时发起的图片下载并发数，防止 TMDB CDN 触发限速(10054)
_image_semaphore = threading.Semaphore(2)


def safe_filename(text):
    """Normalize illegal path chars and trim dangerous suffixes."""
    if not text:
        return ""
    illegal_chars = r'<>:"/\\|?*' + chr(0)
    for char in illegal_chars:
        text = text.replace(char, "_")
    text = text.strip().strip(".")
    if len(text) > 200:
        text = text[:200]
    return text


def normalize_compare_text(text):
    if not text:
        return ""
    text = str(text).lower()
    return re.sub(r"[\W_]+", "", text, flags=re.UNICODE)


def extract_year_from_release(release):
    if not release:
        return ""
    match = re.search(r"(\d{4})", str(release))
    return match.group(1) if match else ""


def format_candidate_label(candidate):
    title = candidate.get("title") or "未知"
    alt_title = candidate.get("alt_title") or ""
    if alt_title and normalize_compare_text(alt_title) == normalize_compare_text(title):
        alt_title = ""
    year = extract_year_from_release(candidate.get("release")) or "-"
    rating = candidate.get("rating")
    try:
        rating_text = (
            f"{float(rating):.1f}" if rating not in (None, "", 0, "0") else "-"
        )
    except Exception:
        rating_text = "-"
    parts = [title]
    if alt_title:
        parts.append(f"原名:{alt_title}")
    parts.append(f"年份:{year}")
    parts.append(f"评分:{rating_text}")
    parts.append(f"ID:{candidate.get('id', '-')}")
    source = candidate.get("msg")
    if source:
        parts.append(str(source))
    return " | ".join(parts)


def candidate_to_result(candidate, hit_msg):
    if not candidate:
        return "", "None", hit_msg, {}
    return (
        candidate.get("title") or "",
        str(candidate.get("id", "None")),
        hit_msg,
        candidate.get("meta") or {},
    )


def center_window(window, parent, width, height):
    parent.update_idletasks()
    window.update_idletasks()

    parent_w = parent.winfo_width()
    parent_h = parent.winfo_height()
    if parent_w <= 1 or parent_h <= 1:
        parent_w = parent.winfo_screenwidth()
        parent_h = parent.winfo_screenheight()

    # Use root coordinates for reliable placement on Windows with DPI scaling.
    parent_x = parent.winfo_rootx()
    parent_y = parent.winfo_rooty()

    x = parent_x + (parent_w // 2) - (width // 2)
    y = parent_y + (parent_h // 2) - (height // 2)
    x = max(0, x)
    y = max(0, y)
    window.geometry(f"{width}x{height}+{x}+{y}")


def clean_search_title(title):
    if not title:
        return ""
    # Keep bracket content (often contains series title), only remove bracket chars.
    text = re.sub(r"[\[\]\(\)（）]", " ", title)
    # Drop common release group tags like UHA-WINGS, KTXP, or VC-BETA.
    text = re.sub(r"(?<![a-z0-9])[A-Z0-9]{2,}(?:-[A-Z0-9]{2,})+(?![a-z0-9])", " ", text)
    text = re.sub(
        r"(?i)(?:10bit|FLAC|BluRay|1080p|720p|x264|x265|HEVC|Remastered|D3D-Raw|BDRip|Web-DL|NC\.Ver|完结合集|第.*?季|第.*?集|S\d{1,2}E\d{1,4}|EP?\s*\d{1,4})",
        "",
        text,
    )
    # Remove common language tags accidentally kept from filenames, like .cht/.chs/zh-CN.
    text = LANG_TAG_TOKEN_RE.sub(" ", text)
    text = re.sub(r"^[\W_]+|[\W_]+$", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_meaningful_query_title(title):
    text = str(title or "").strip()
    if not text:
        return False

    # Reject generic season markers that are not actual series names.
    if GENERIC_SEASON_TITLE_RE.match(text):
        return False

    key = normalize_compare_text(text)
    if not key:
        return False
    return key not in INVALID_QUERY_TITLES_NORMALIZED


def unique_keep_order(values):
    seen = set()
    out = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = normalize_compare_text(text)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def extract_episode_number(pure_name, guess_data=None, ai_data=None):
    if guess_data:
        ep = guess_data.get("episode")
        if isinstance(ep, list) and ep:
            ep = ep[0]
        if isinstance(ep, (int, float)):
            return int(ep)
        if isinstance(ep, str) and ep.strip().isdigit():
            return int(ep.strip())

    if ai_data:
        ep = ai_data.get("episode")
        if isinstance(ep, (int, float)):
            return int(ep)
        if isinstance(ep, str) and ep.strip().isdigit():
            return int(ep.strip())

    text = str(pure_name or "")
    patterns = [
        r"(?i)\bS\d{1,2}E\s*0*(\d{1,4})\b",
        r"(?i)\bEP?\s*0*(\d{1,4})\b",
        r"(?i)第\s*0*(\d{1,4})\s*[集话話]\b",
        r"(?i)[\[\(（]\s*0*(\d{1,4})(?:v\d+)?\s*[\]\)）]",
        r"(?i)-\s*0*(\d{1,4})(?:v\d+)?(?=\s*(?:$|[\[\(（]))",
    ]

    for idx, pattern in enumerate(patterns):
        match = re.search(pattern, text)
        if not match:
            continue
        try:
            num = int(match.group(1))
        except Exception:
            continue
        if idx >= 3 and num in EPISODE_NOISE_NUMBERS:
            continue
        if 0 < num <= 5000:
            return num

    return None


def derive_title_from_filename(pure_name):
    text = str(pure_name or "")
    text = text.replace("_", " ").replace(".", " ")
    text = re.sub(r"(?i)\bS\d{1,2}E\d{1,4}\b.*$", "", text)
    text = re.sub(r"(?i)\bEP?\s*\d{1,4}\b.*$", "", text)
    text = re.sub(r"(?i)第\s*\d{1,4}\s*[集话話].*$", "", text)
    text = re.sub(r"(?i)[\[\(（]\s*\d{1,4}(?:v\d+)?\s*[\]\)）]\s*$", "", text)
    return clean_search_title(text)


def build_query_titles(item, query_title, ai_data, g):
    if isinstance(item, dict):
        raw_name = item.get("old_name", "")
        item_dir = item.get("dir", "") or ""
    else:
        raw_name = getattr(item, "old_name", "") or ""
        item_dir = getattr(item, "dir", "") or ""

    pure, _ = os.path.splitext(raw_name)
    dir_title = os.path.basename(item_dir)
    guess_title = clean_search_title((g.get("title") if g else None) or "")
    candidates = [
        query_title,
        (ai_data or {}).get("title") if isinstance(ai_data, dict) else None,
        guess_title,
        derive_title_from_filename(pure),
        clean_search_title(pure),
        clean_search_title(dir_title),
    ]
    ordered = unique_keep_order(candidates)
    return [c for c in ordered if is_meaningful_query_title(c)]


def safe_str(val):
    if val is None:
        return ""
    if isinstance(val, list):
        if val:
            return str(val[0])
        return ""
    return str(val)


def safe_int(value, default=1):
    try:
        if isinstance(value, list):
            value = value[0] if value else default
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            value = value.strip()
            match = re.search(r"[-+]?\d+", value)
            return int(match.group()) if match else default
        return default
    except (ValueError, TypeError):
        return default


def load_cache():
    with _cache_file_lock:
        _ensure_cache_loaded_unlocked()
        _prune_expired_cache_entries(_cache_data)
        return dict(_cache_data)


def _load_cache_from_disk():
    if not os.path.exists(CACHE_FILE):
        return {}

    try:
        for encoding in ["utf-8", "gbk", "latin-1"]:
            try:
                with open(CACHE_FILE, "r", encoding=encoding) as f:
                    cache = json.load(f)
                break
            except UnicodeDecodeError:
                continue
        else:
            with open(CACHE_FILE, "rb") as f:
                content = f.read().decode("utf-8", errors="ignore")
            cache = json.loads(content)
        return cache if isinstance(cache, dict) else {}
    except Exception as err:
        logging.error(f"加载缓存失败: {err}")
        return {}


def _prune_expired_cache_entries(cache, now_ts=None):
    now_value = now_ts or datetime.now().timestamp()
    expired_keys = [
        key
        for key, value in list((cache or {}).items())
        if not isinstance(value, dict) or value.get("expiry", 0) < now_value
    ]
    for key in expired_keys:
        cache.pop(key, None)
    return len(expired_keys)


def _ensure_cache_loaded_unlocked():
    global _cache_data
    if _cache_data is None:
        _cache_data = _load_cache_from_disk()


def _flush_cache_to_disk_unlocked(force=False):
    global _cache_dirty, _cache_last_flush_ts, _cache_write_count
    if not _cache_dirty:
        return False

    now_ts = datetime.now().timestamp()
    should_flush = force
    if not should_flush:
        should_flush = (
            _cache_write_count >= CACHE_FLUSH_MAX_WRITES
            or now_ts - _cache_last_flush_ts >= CACHE_FLUSH_INTERVAL_SECONDS
        )
    if not should_flush:
        return False

    save_cache(_cache_data or {})
    _cache_dirty = False
    _cache_write_count = 0
    _cache_last_flush_ts = now_ts
    return True


def save_cache(cache):
    temp_file = CACHE_FILE + ".tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)

        import shutil

        shutil.move(temp_file, CACHE_FILE)
    except Exception as err:
        logging.error(f"保存缓存失败: {err}")
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except Exception:
            pass


def clear_api_cache_file():
    """Delete persistent API cache on disk in a thread-safe way."""
    global _cache_data, _cache_dirty, _cache_write_count, _cache_last_flush_ts
    with _cache_file_lock:
        try:
            _cache_data = {}
            _cache_dirty = False
            _cache_write_count = 0
            _cache_last_flush_ts = 0.0
            if os.path.exists(CACHE_FILE):
                os.remove(CACHE_FILE)
            return True
        except Exception as err:
            logging.error(f"清理API缓存文件失败: {err}")
            return False


def get_cache_key(api_name, query):
    return f"{api_name}:{str(query)}"


def cached_request(api_func, cache_key, *args, **kwargs):
    global _cache_dirty, _cache_write_count
    now_ts = datetime.now().timestamp()

    with _cache_file_lock:
        _ensure_cache_loaded_unlocked()
        expired_count = _prune_expired_cache_entries(_cache_data, now_ts)
        if expired_count > 0:
            _cache_dirty = True
            _flush_cache_to_disk_unlocked(force=False)

        cached_entry = (_cache_data or {}).get(cache_key)
        if isinstance(cached_entry, dict) and cached_entry.get("expiry", 0) >= now_ts:
            return cached_entry.get("data")

    result = api_func(*args, **kwargs)

    is_valid = True
    if result is None:
        is_valid = False
    elif isinstance(result, (list, dict, set)) and len(result) == 0:
        is_valid = False
    elif isinstance(result, str) and not result.strip():
        is_valid = False
    elif isinstance(result, tuple):
        if len(result) >= 2 and result[1] == "None":
            is_valid = False
        elif len(result) >= 3 and not result[0] and not result[1]:
            is_valid = False

    if is_valid:
        with _cache_file_lock:
            _ensure_cache_loaded_unlocked()
            _cache_data[cache_key] = {
                "data": result,
                "expiry": (
                    datetime.now() + timedelta(days=CACHE_EXPIRY_DAYS)
                ).timestamp(),
            }
            _cache_dirty = True
            _cache_write_count += 1
            _flush_cache_to_disk_unlocked(force=False)

    return result


def flush_api_cache(force=False):
    with _cache_file_lock:
        _ensure_cache_loaded_unlocked()
        return _flush_cache_to_disk_unlocked(force=force)


def save_image(path, url_part):
    if not url_part:
        return

    try:
        url = (
            url_part
            if url_part.startswith("http")
            else f"https://image.tmdb.org/t/p/original{url_part}"
        )
        if os.path.exists(path):
            return

        with _image_semaphore:
            time.sleep(0.3)  # 限速间隔，避免并发请求触发 TMDB CDN 的 10054 RST
            res = session.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=TIMEOUT_IMAGE_DOWNLOAD,
                stream=True,
            )
            if res.status_code == 200:
                with open(path, "wb") as f:
                    for chunk in res.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)
    except Exception as err:
        logging.error(f"保存图片失败 {path}: {err}")


def write_nfo(path, data, nfo_type="movie"):
    try:
        root = ET.Element(nfo_type)

        if nfo_type == "episodedetails":
            title = data.get("ep_title", "")
            if not title or title == data.get("title"):
                title = f"第 {data.get('e', 1)} 集"

            ET.SubElement(root, "title").text = str(title)
            ET.SubElement(root, "plot").text = str(data.get("ep_plot", ""))
            ET.SubElement(root, "season").text = str(data.get("s", 1))
            ET.SubElement(root, "episode").text = str(data.get("e", 1))
            ET.SubElement(root, "year").text = str(data.get("year") or "")

        elif nfo_type == "season":
            s_num = data.get("s", 1)
            ET.SubElement(root, "title").text = f"第 {s_num} 季"
            ET.SubElement(root, "sorttitle").text = f"第 {s_num} 季"
            ET.SubElement(root, "seasonnumber").text = str(s_num)
            ET.SubElement(root, "plot").text = str(data.get("overview", ""))
            ET.SubElement(root, "year").text = str(data.get("year") or "")

        else:
            ET.SubElement(root, "title").text = str(data.get("title", ""))
            overview = str(data.get("overview", ""))
            ET.SubElement(root, "plot").text = overview
            ET.SubElement(root, "outline").text = overview
            orig_title = str(data.get("original_title", ""))
            if orig_title:
                ET.SubElement(root, "originaltitle").text = orig_title
            ET.SubElement(root, "year").text = str(data.get("year") or "")
            ET.SubElement(root, "dateadded").text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        provider = str(data.get("provider") or "tmdb").strip().lower() or "tmdb"
        ET.SubElement(root, "lockdata").text = "false"
        ET.SubElement(root, "uniqueid", type=provider).text = str(data.get("id", ""))

        xml_str = ET.tostring(root, encoding="utf-8")
        dom = minidom.parseString(xml_str)
        pretty_xml = dom.toprettyxml(indent="  ")
        pretty_xml = "\n".join(
            [line for line in pretty_xml.split("\n") if line.strip()]
        )

        with open(path, "w", encoding="utf-8") as f:
            f.write(pretty_xml)
    except Exception as err:
        logging.error(f"写入NFO失败 {path}: {err}")


def _nfo_has_empty_plot(path):
    """Return True if the NFO file exists but has an empty <plot> element."""
    try:
        tree = ET.parse(path)
        plot = tree.find("plot")
        return plot is None or not (plot.text or "").strip()
    except Exception:
        return False


atexit.register(lambda: flush_api_cache(force=True))

_FOLDER_TMDBID_RE = re.compile(r"(?i)tmdb(?:id)?[-=:_](\d{1,8})")
_FOLDER_BGMID_RE = re.compile(r"(?i)bgm(?:id)?[-=:_](\d{1,8})")


def extract_db_id_from_path(path, mode):
    """从路径的目录部分提取 tmdbid 或 bgmid。

    支持格式：(tmdbid-259537) [tmdbid=259537] {tmdbid-259537} tmdb-259537 等。
    仅检测文件夹名，不检测文件名本身。
    mode: 'siliconflow_tmdb' 或 'siliconflow_bgm'
    返回: id 字符串 或 None
    """
    dir_part = os.path.dirname(str(path or ""))
    pat = _FOLDER_TMDBID_RE if mode == "siliconflow_tmdb" else _FOLDER_BGMID_RE
    m = pat.search(dir_part)
    return m.group(1) if m else None
