import os
import re

from utils.helpers import (
    ERROR_CODE_CONFIG,
    ERROR_CODE_HTTP,
    ERROR_CODE_INVALID,
    ERROR_CODE_NO_RESULT,
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    VERSION_TAG_RE,
    clean_search_title,
    derive_title_from_filename,
    normalize_compare_text,
    parse_error_message,
    safe_int,
    safe_str,
)


def extract_lang_and_ext(filename, lang_tags):
    """Extract language suffix and extension from a media name."""
    tags = str(lang_tags or "").strip()
    if not tags:
        return os.path.splitext(filename)

    tag_items = [t.strip() for t in tags.split("|") if t.strip()]
    if not tag_items:
        return os.path.splitext(filename)

    safe_tags = "|".join(re.escape(t) for t in tag_items)
    pattern = rf"(\.(?:{safe_tags}))?(\.[a-z0-9]+)$"
    try:
        regex = re.compile(pattern, re.I)
    except re.error:
        return os.path.splitext(filename)

    match = regex.search(filename)
    if match and match.group(1):
        return filename[: match.start()], match.group(1) + match.group(2)
    return os.path.splitext(filename)


def extract_explicit_season(pure_name):
    """Only parse explicit season markers to avoid treating years as seasons."""
    text = str(pure_name or "")
    patterns = [
        r"(?i)\bS\s*0*(\d{1,2})\b",
        r"(?i)\bSeason\s*0*(\d{1,2})\b",
        r"(?i)\b(\d{1,2})(?:st|nd|rd|th)\s*Season\b",
        r"第\s*0*(\d{1,2})\s*季",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        season_num = safe_int(match.group(1), 0)
        if 0 <= season_num <= 99:
            return season_num
    return None


def pick_season(pure_name, guess_data=None, fallback=1):
    """Prefer explicit season marker, then sane guessed season, then fallback."""
    explicit = extract_explicit_season(pure_name)
    if explicit is not None:
        return explicit

    guessed = safe_int((guess_data or {}).get("season"), 0)
    if 0 < guessed <= 99:
        return guessed

    fallback_num = safe_int(fallback, 1)
    if 0 <= fallback_num <= 99:
        return fallback_num
    return 1


def can_reuse_dir_ai(cached_ai, pure_name, guess_data=None):
    """Allow directory-level AI cache reuse only for clearly same title/year."""
    if not isinstance(cached_ai, dict):
        return False

    cached_title = clean_search_title(cached_ai.get("title") or "")
    cached_key = normalize_compare_text(cached_title)
    if not cached_key:
        return False

    cached_year = safe_str(cached_ai.get("year"))
    guess_year = safe_str((guess_data or {}).get("year"))
    if cached_year and guess_year and cached_year != guess_year:
        return False

    title_candidates = [
        clean_search_title((guess_data or {}).get("title") or ""),
        derive_title_from_filename(pure_name),
    ]

    for candidate in title_candidates:
        cand_key = normalize_compare_text(candidate)
        if not cand_key:
            continue
        if cand_key == cached_key:
            return True
        if len(cand_key) >= 6 and len(cached_key) >= 6:
            if cand_key in cached_key or cached_key in cand_key:
                return True

    return False


def get_version_tag(path):
    match = VERSION_TAG_RE.search(os.path.basename(path))
    return f" {match.group(0)}" if match else ""


def friendly_status_text(message):
    """Render coded errors to concise Chinese status text for UI display."""
    raw_text = str(message or "").strip()
    if not raw_text:
        return ""

    has_error_hint = (
        ":" in raw_text
        or any(
            token in raw_text
            for token in (
                "超时",
                "未配置",
                "HTTP",
                "解析",
                "JSON",
                "无结果",
                "未匹配",
                "无效",
                "失败",
                "异常",
                "错误",
            )
        )
    )
    if not has_error_hint:
        return raw_text

    code, detail = parse_error_message(message)
    if not code:
        return raw_text

    template = {
        ERROR_CODE_TIMEOUT: "请求超时，请稍后重试",
        ERROR_CODE_CONFIG: "配置缺失，请检查密钥设置",
        ERROR_CODE_HTTP: "接口请求失败，请检查网络或服务状态",
        ERROR_CODE_PARSE: "返回解析失败，请稍后重试",
        ERROR_CODE_NO_RESULT: "未找到匹配结果",
        ERROR_CODE_INVALID: "输入无效或资源不存在",
        ERROR_CODE_UNKNOWN: "处理失败，请查看日志",
    }.get(code, "处理失败，请查看日志")

    if detail and code in {ERROR_CODE_HTTP, ERROR_CODE_UNKNOWN}:
        return f"{template} ({detail[:24]})"
    return template


def build_status_text(*messages):
    raw_parts = [str(m).strip() for m in messages if str(m or "").strip()]
    if not raw_parts:
        return ""

    friendly_parts = [friendly_status_text(m) for m in raw_parts]
    merged = list(dict.fromkeys(friendly_parts))
    return " / ".join(merged)
