"""手动匹配候选搜索（按 ID / 关键词查 TMDb·BGM，无 UI 框架依赖）。

从 core/ui/manual_match.py 迁出，供 Qt 手动匹配（ui_qt/manual_match_qt.py）
与 app_qt 复用。搜索在后台线程执行，完成后经 gui.root.after(...) 把结果回送
主线程的 _show_manual_match_results 钩子（由各 UI 子类实现）。
"""
import logging
import re

import requests

from db.tmdb_api import fetch_bgm_by_id, fetch_tmdb_by_id
from core.services.poster_service import _prefetch_poster_urls, _resolve_poster_url
from utils.helpers import (
    ERROR_CODE_CONFIG,
    ERROR_CODE_HTTP,
    ERROR_CODE_NO_RESULT,
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    TIMEOUT_DB_SEARCH,
    USER_AGENT,
    clean_search_title,
    format_error_message,
    parse_error_message,
    session,
)


def _response_body_snippet(response, limit=300):
    if response is None:
        return ""
    try:
        body = response.text or ""
    except Exception:
        return ""
    compact = " ".join(str(body).split())
    if len(compact) > limit:
        return compact[:limit] + "..."
    return compact


def async_manual_match_search(gui, selected_ids, user_input, mode):
    """Search candidates for manual match by ID or keyword."""
    results = []
    search_errors = []

    def append_error(source_name, msg_text):
        code, detail = parse_error_message(msg_text)
        if not code or code == ERROR_CODE_NO_RESULT:
            return
        prefix = {
            ERROR_CODE_TIMEOUT: "请求超时",
            ERROR_CODE_CONFIG: "配置缺失",
            ERROR_CODE_HTTP: "HTTP失败",
            ERROR_CODE_PARSE: "响应解析失败",
            ERROR_CODE_UNKNOWN: "请求异常",
        }.get(code, "请求异常")
        final_text = detail or str(msg_text)
        search_errors.append(f"{source_name}{prefix}: {final_text}")

    try:
        if user_input.isdigit():
            if mode == "siliconflow_bgm":
                t, tid, msg, meta = fetch_bgm_by_id(user_input, gui.bgm_api_key.get())
                if tid != "None":
                    results = [(t, tid, msg, meta)]
                else:
                    append_error("BGM", msg)
            else:
                t, tid, msg, meta = fetch_tmdb_by_id(
                    user_input, True, gui.tmdb_api_key.get()
                )
                if tid == "None":
                    append_error("TMDb剧集", msg)
                    t, tid, msg, meta = fetch_tmdb_by_id(
                        user_input, False, gui.tmdb_api_key.get()
                    )
                    if tid == "None":
                        append_error("TMDb电影", msg)
                if tid != "None":
                    results = [(t, tid, msg, meta)]
        else:
            if mode == "siliconflow_bgm":
                query = clean_search_title(user_input)
                headers = {"User-Agent": USER_AGENT}
                if gui.bgm_api_key.get().strip():
                    headers["Authorization"] = f"Bearer {gui.bgm_api_key.get().strip()}"

                try:
                    res = session.get(
                        f"https://api.bgm.tv/search/subject/{query}?type=2",
                        headers=headers,
                        timeout=TIMEOUT_DB_SEARCH,
                    )
                    res.raise_for_status()
                    items = res.json().get("list", [])

                    for it in items[:5]:
                        title = it.get("name_cn") or it.get("name") or "未知"
                        meta = {
                            "overview": it.get("summary", ""),
                            "rating": it.get("score", 0),
                            "poster": it.get("images", {}).get("large", ""),
                            "fanart": "",
                            "release": it.get("air_date", ""),
                        }
                        results.append((title, str(it.get("id")), "搜索结果", meta))
                except requests.exceptions.Timeout:
                    append_error("BGM", format_error_message(ERROR_CODE_TIMEOUT, "请求超时"))
                except requests.exceptions.HTTPError as err:
                    snippet = _response_body_snippet(getattr(err, "response", None))
                    if snippet:
                        logging.warning(f"BGM手动搜索HTTP失败，返回内容: {snippet}")
                    append_error(
                        "BGM", format_error_message(ERROR_CODE_HTTP, f"HTTP请求失败: {err}")
                    )
                except ValueError as err:
                    snippet = _response_body_snippet(locals().get("res"))
                    if snippet:
                        logging.warning(f"BGM手动搜索解析失败，返回内容: {snippet}")
                    append_error(
                        "BGM", format_error_message(ERROR_CODE_PARSE, f"响应解析失败: {err}")
                    )
                except Exception as err:
                    logging.error(f"BGM手动搜索请求失败: {err}")
                    append_error("BGM", format_error_message(ERROR_CODE_UNKNOWN, "请求异常"))
            else:
                # Detect if query is primarily Latin/English or Chinese
                has_chinese = bool(re.search(r'[一-鿿]', user_input))
                has_latin = bool(re.search(r'[a-zA-Z]', user_input))

                # Choose language based on query content
                if has_latin and not has_chinese:
                    # English query: use en-US
                    language = "en-US"
                else:
                    # Chinese or mixed query: use zh-CN
                    language = "zh-CN"

                try:
                    res_tv = session.get(
                        "https://api.themoviedb.org/3/search/tv",
                        params={
                            "api_key": gui.tmdb_api_key.get().strip(),
                            "query": user_input,
                            "language": language,
                        },
                        timeout=TIMEOUT_DB_SEARCH,
                    )
                    res_tv.raise_for_status()
                    tv_results = res_tv.json().get("results", [])[:3]

                    for it in tv_results:
                        meta = {
                            "overview": it.get("overview", ""),
                            "rating": it.get("vote_average", 0),
                            "poster": it.get("poster_path", ""),
                            "fanart": it.get("backdrop_path", ""),
                            "release": it.get("first_air_date", ""),
                        }
                        results.append((it.get("name", "未知"), str(it.get("id")), "TMDb剧集", meta))

                    res_movie = session.get(
                        "https://api.themoviedb.org/3/search/movie",
                        params={
                            "api_key": gui.tmdb_api_key.get().strip(),
                            "query": user_input,
                            "language": language,
                        },
                        timeout=TIMEOUT_DB_SEARCH,
                    )
                    res_movie.raise_for_status()
                    movie_results = res_movie.json().get("results", [])[:2]

                    for it in movie_results:
                        meta = {
                            "overview": it.get("overview", ""),
                            "rating": it.get("vote_average", 0),
                            "poster": it.get("poster_path", ""),
                            "fanart": it.get("backdrop_path", ""),
                            "release": it.get("release_date", ""),
                        }
                        results.append((it.get("title", "未知"), str(it.get("id")), "TMDb电影", meta))
                except requests.exceptions.Timeout:
                    append_error("TMDb", format_error_message(ERROR_CODE_TIMEOUT, "请求超时"))
                except requests.exceptions.HTTPError as err:
                    snippet = _response_body_snippet(getattr(err, "response", None))
                    if snippet:
                        logging.warning(f"TMDb手动搜索HTTP失败，返回内容: {snippet}")
                    append_error(
                        "TMDb", format_error_message(ERROR_CODE_HTTP, f"HTTP请求失败: {err}")
                    )
                except ValueError as err:
                    snippet = _response_body_snippet(locals().get("res_tv") or locals().get("res_movie"))
                    if snippet:
                        logging.warning(f"TMDb手动搜索解析失败，返回内容: {snippet}")
                    append_error(
                        "TMDb", format_error_message(ERROR_CODE_PARSE, f"响应解析失败: {err}")
                    )
                except Exception as err:
                    logging.error(f"TMDb手动搜索请求失败: {err}")
                    append_error("TMDb", format_error_message(ERROR_CODE_UNKNOWN, "请求异常"))
    except Exception as err:
        logging.error(f"手动匹配搜索失败: {err}")
        append_error("手动匹配", format_error_message(ERROR_CODE_UNKNOWN, str(err)))

    poster_urls = []
    for _, _, _, meta in results:
        m = meta or {}
        poster_url = _resolve_poster_url(m.get("poster") or "")
        if poster_url:
            poster_urls.append(poster_url)

    _prefetch_poster_urls(poster_urls)

    error_msg = "；".join(dict.fromkeys(search_errors)) if search_errors else ""
    gui.root.after(0, gui._show_manual_match_results, selected_ids, results, error_msg)
