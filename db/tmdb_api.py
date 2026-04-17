import logging
import re
import threading
import time
import difflib

import requests

from utils.helpers import (
    ERROR_CODE_CONFIG,
    ERROR_CODE_HTTP,
    ERROR_CODE_INVALID,
    ERROR_CODE_NO_RESULT,
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    USER_AGENT,
    cached_request,
    candidate_to_result,
    clean_search_title,
    format_error_message,
    get_cache_key,
    session,
    TIMEOUT_DB_DETAIL,
    TIMEOUT_DB_SEARCH,
)

# ---------------------------------------------------------------------------
# TMDB API 全局限速器
# TMDB 官方速率限制约 40 请求/10 秒（即 4 req/s）。
# 用一个令牌桶：最多积累 8 个令牌，每 0.25s 补充 1 个。
# 所有向 api.themoviedb.org 发出的请求都通过 _tmdb_throttle() 限速。
# ---------------------------------------------------------------------------
_tmdb_lock = threading.Lock()
_tmdb_tokens = 8.0          # 初始令牌数（允许冷启动时短暂突发）
_tmdb_max_tokens = 8.0
_tmdb_refill_rate = 4.0     # 每秒补充令牌数（对应 TMDB 4 req/s 限制）
_tmdb_last_refill = time.monotonic()


def _tmdb_throttle():
    """消耗一个 TMDB 令牌；令牌耗尽时阻塞直到补充。"""
    global _tmdb_tokens, _tmdb_last_refill
    while True:
        with _tmdb_lock:
            now = time.monotonic()
            elapsed = now - _tmdb_last_refill
            _tmdb_tokens = min(
                _tmdb_max_tokens,
                _tmdb_tokens + elapsed * _tmdb_refill_rate,
            )
            _tmdb_last_refill = now
            if _tmdb_tokens >= 1.0:
                _tmdb_tokens -= 1.0
                return
        # 令牌不足，等待约半个补充周期后重试
        time.sleep(0.15)


def _tmdb_get(url, **kwargs):
    """对 api.themoviedb.org 所有 GET 请求的统一入口，自动限速。"""
    _tmdb_throttle()
    return session.get(url, **kwargs)


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


def fetch_bgm_by_id_raw(subject_id, api_key=""):
    headers = {"User-Agent": USER_AGENT}
    if api_key and api_key.strip():
        headers["Authorization"] = f"Bearer {api_key.strip()}"

    try:
        response = session.get(
            f"https://api.bgm.tv/v0/subjects/{subject_id}",
            headers=headers,
            timeout=TIMEOUT_DB_DETAIL,
        )
        response.raise_for_status()
        data = response.json()

        meta = {
            "overview": data.get("summary", ""),
            "rating": data.get("rating", {}).get("score", 0),
            "poster": data.get("images", {}).get("large", ""),
            "fanart": "",
            "release": data.get("date", ""),
        }

        title = data.get("name_cn") or data.get("name") or str(subject_id)
        return title, str(data.get("id")), "ID强制锁定", meta
    except requests.exceptions.Timeout:
        return (
            str(subject_id),
            "None",
            format_error_message(ERROR_CODE_TIMEOUT, "请求超时"),
            {},
        )
    except requests.exceptions.HTTPError as err:
        if err.response is not None and err.response.status_code == 404:
            msg = format_error_message(ERROR_CODE_INVALID, "ID无效")
        else:
            msg = format_error_message(ERROR_CODE_HTTP, f"HTTP请求失败: {err}")
        snippet = _response_body_snippet(getattr(err, "response", None))
        if snippet:
            logging.warning(f"BGM按ID查询HTTP失败，返回内容: {snippet}")
        return str(subject_id), "None", msg, {}
    except ValueError:
        snippet = _response_body_snippet(locals().get("response"))
        if snippet:
            logging.warning(f"BGM按ID查询解析失败，返回内容: {snippet}")
        return (
            str(subject_id),
            "None",
            format_error_message(ERROR_CODE_PARSE, "响应解析失败"),
            {},
        )
    except Exception as err:
        logging.warning(f"BGM按ID查询异常: {err}")
        return (
            str(subject_id),
            "None",
            format_error_message(ERROR_CODE_UNKNOWN, "请求异常"),
            {},
        )


def fetch_bgm_by_id(subject_id, api_key=""):
    return cached_request(
        fetch_bgm_by_id_raw, get_cache_key("bgm_id", subject_id), subject_id, api_key
    )


def fetch_bgm_candidates_raw(title, year=None, api_key=""):
    q = clean_search_title(title)
    q_norm = re.sub(r"[\W_]+", "", str(q).lower())
    headers = {"User-Agent": USER_AGENT}
    if api_key and api_key.strip():
        headers["Authorization"] = f"Bearer {api_key.strip()}"

    def _items_to_candidates(items):
        candidates = []
        seen_ids = set()
        for item in items[:8]:
            cid = str(item.get("id") or "")
            if not cid or cid in seen_ids:
                continue
            seen_ids.add(cid)
            release = item.get("air_date") or item.get("date") or ""
            rating = item.get("score", 0)
            meta = {
                "overview": item.get("summary", ""),
                "rating": rating,
                "poster": item.get("images", {}).get("large", ""),
                "fanart": "",
                "release": release,
            }
            candidates.append(
                {
                    "title": item.get("name_cn") or item.get("name") or title,
                    "alt_title": item.get("name") or "",
                    "id": cid,
                    "msg": "BGM候选",
                    "rating": rating,
                    "release": release,
                    "meta": meta,
                }
            )
        return candidates

    def _similarity_score(item):
        name_cn = item.get("name_cn") or ""
        name = item.get("name") or ""
        name_cn_norm = re.sub(r"[\W_]+", "", str(name_cn).lower())
        name_norm = re.sub(r"[\W_]+", "", str(name).lower())
        scores = []
        if name_cn_norm:
            scores.append(difflib.SequenceMatcher(None, q_norm, name_cn_norm).ratio())
        if name_norm:
            scores.append(difflib.SequenceMatcher(None, q_norm, name_norm).ratio())
        return max(scores) if scores else 0.0

    def _request_bgm(query):
        resp = session.get(
            f"https://api.bgm.tv/search/subject/{query}?type=2",
            headers=headers,
            timeout=TIMEOUT_DB_SEARCH,
        )
        resp.raise_for_status()
        return resp.json().get("list", [])

    def _year_sort_key(cand):
        if not year:
            return 1
        release = cand.get("release") or ""
        return 0 if str(release).startswith(str(year)) else 1

    try:
        queries = [q]
        q_retry = re.sub(r"(?i)HD|重制版|重製版|Remaster|Edition", "", q).strip()
        if q_retry and q_retry != q:
            queries.append(q_retry)

        for query in queries:
            results = _request_bgm(query)
            if results:
                candidates = _items_to_candidates(results)
                candidates.sort(key=_year_sort_key)
                return candidates

        # Fuzzy fallback: 拆词后重排
        token_queries = []
        for token in re.split(r"\s+", q):
            t = token.strip()
            if len(t) >= 2 and t.lower() != q.lower() and t not in token_queries:
                token_queries.append(t)

        fuzzy_pool = []
        seen = set()
        for tq in token_queries:
            for item in _request_bgm(tq):
                cid = str(item.get("id") or "")
                if not cid or cid in seen:
                    continue
                seen.add(cid)
                fuzzy_pool.append(item)

        if fuzzy_pool:
            ranked = sorted(fuzzy_pool, key=_similarity_score, reverse=True)
            top = [it for it in ranked if _similarity_score(it) >= 0.35]
            pool = top if top else ranked
            candidates = _items_to_candidates(pool)
            candidates.sort(key=_year_sort_key)
            return candidates
        return []
    except requests.exceptions.Timeout:
        return []
    except requests.exceptions.HTTPError as err:
        snippet = _response_body_snippet(getattr(err, "response", None))
        if snippet:
            logging.warning(f"BGM候选搜索HTTP失败，返回内容: {snippet}")
        return []
    except ValueError:
        snippet = _response_body_snippet(locals().get("resp"))
        if snippet:
            logging.warning(f"BGM候选搜索解析失败，返回内容: {snippet}")
        return []
    except Exception:
        return []


def fetch_bgm_candidates(title, year=None, api_key=""):
    return cached_request(
        fetch_bgm_candidates_raw,
        get_cache_key("bgm_candidates_v2", f"{title}_{year}"),
        title,
        year,
        api_key,
    )


def fetch_bgm_info_raw(title, api_key=""):
    candidates = fetch_bgm_candidates_raw(title, api_key=api_key)
    if candidates:
        return candidate_to_result(candidates[0], "BGM命中")
    return (
        title,
        "None",
        format_error_message(ERROR_CODE_NO_RESULT, "BGM无结果"),
        {},
    )


def fetch_bgm_info(title, api_key=""):
    return cached_request(
        fetch_bgm_info_raw, get_cache_key("bgm_search", title), title, api_key
    )


def fetch_bgm_episode_raw(subject_id, season, episode, api_key_bgm):
    headers = {"User-Agent": USER_AGENT}
    if api_key_bgm and api_key_bgm.strip():
        headers["Authorization"] = f"Bearer {api_key_bgm.strip()}"

    try:
        response = session.get(
            f"https://api.bgm.tv/v0/episodes?subject_id={subject_id}&type=0&limit=100",
            headers=headers,
            timeout=TIMEOUT_DB_DETAIL,
        )
        response.raise_for_status()

        for ep in response.json().get("data", []):
            if ep.get("sort") == episode:
                return ep.get("name_cn") or ep.get("name") or "", ep.get("desc", "")
    except Exception:
        pass

    return "", ""


def fetch_bgm_episode(subject_id, season, episode, api_key_bgm):
    return cached_request(
        fetch_bgm_episode_raw,
        get_cache_key("bgm_ep", f"{subject_id}_{season}_{episode}"),
        subject_id,
        season,
        episode,
        api_key_bgm,
    )


def fetch_tmdb_by_id_raw(tmdb_id, is_tv=True, api_key=""):
    if not api_key or not api_key.strip():
        return (
            str(tmdb_id),
            "None",
            format_error_message(ERROR_CODE_CONFIG, "未配置TMDb Key"),
            {},
        )

    stype = "tv" if is_tv else "movie"

    try:
        response = _tmdb_get(
            f"https://api.themoviedb.org/3/{stype}/{tmdb_id}",
            params={"api_key": api_key.strip(), "language": "zh-CN"},
            timeout=TIMEOUT_DB_DETAIL,
        )
        response.raise_for_status()
        data = response.json()

        meta = {
            "overview": data.get("overview", ""),
            "rating": data.get("vote_average", 0),
            "votes": data.get("vote_count", 0),
            "poster": data.get("poster_path", ""),
            "fanart": data.get("backdrop_path", ""),
            "release": data.get("first_air_date") or data.get("release_date") or "",
            "original_title": data.get("original_name") or data.get("original_title") or "",
            "genres": [g["name"] for g in (data.get("genres") or []) if g.get("name")],
            "studios": [
                n["name"] for n in (data.get("networks") or data.get("production_companies") or [])
                if n.get("name")
            ],
            "runtime": (data.get("episode_run_time") or [None])[0] if is_tv else data.get("runtime"),
            "status": data.get("status", ""),
        }

        # zh-CN 简介为空时补请英文版本
        if not meta["overview"]:
            try:
                resp_en = _tmdb_get(
                    f"https://api.themoviedb.org/3/{stype}/{tmdb_id}",
                    params={"api_key": api_key.strip(), "language": "en-US"},
                    timeout=TIMEOUT_DB_DETAIL,
                )
                if resp_en.status_code == 200:
                    meta["overview"] = resp_en.json().get("overview", "")
            except Exception:
                pass

        title = data.get("name") or data.get("title") or str(tmdb_id)
        return title, str(data.get("id")), "ID锁定成功", meta
    except requests.exceptions.Timeout:
        return (
            str(tmdb_id),
            "None",
            format_error_message(ERROR_CODE_TIMEOUT, "请求超时"),
            {},
        )
    except requests.exceptions.HTTPError as err:
        if err.response is not None and err.response.status_code == 404:
            msg = format_error_message(ERROR_CODE_INVALID, "ID无效")
        else:
            msg = format_error_message(ERROR_CODE_HTTP, f"HTTP请求失败: {err}")
        snippet = _response_body_snippet(getattr(err, "response", None))
        if snippet:
            logging.warning(f"TMDb按ID查询HTTP失败，返回内容: {snippet}")
        return str(tmdb_id), "None", msg, {}
    except ValueError:
        snippet = _response_body_snippet(locals().get("response"))
        if snippet:
            logging.warning(f"TMDb按ID查询解析失败，返回内容: {snippet}")
        return (
            str(tmdb_id),
            "None",
            format_error_message(ERROR_CODE_PARSE, "响应解析失败"),
            {},
        )
    except Exception as err:
        logging.warning(f"TMDb按ID查询异常: {err}")
        return (
            str(tmdb_id),
            "None",
            format_error_message(ERROR_CODE_UNKNOWN, "请求异常"),
            {},
        )


def fetch_tmdb_by_id(tmdb_id, is_tv=True, api_key=""):
    return cached_request(
        fetch_tmdb_by_id_raw,
        get_cache_key("tmdb_id", f"{tmdb_id}_{is_tv}"),
        tmdb_id,
        is_tv,
        api_key,
    )


def fetch_tmdb_candidates_raw(title, year=None, is_tv=True, api_key=""):
    if not api_key or not api_key.strip():
        return []

    q = clean_search_title(title)
    stype = "tv" if is_tv else "movie"
    q_norm = re.sub(r"[\W_]+", "", str(q).lower())

    def _items_to_candidates(items):
        candidates = []
        seen_ids = set()
        for item in items[:8]:
            cid = str(item.get("id") or "")
            if not cid or cid in seen_ids:
                continue
            seen_ids.add(cid)

            release = item.get("first_air_date") or item.get("release_date") or ""
            rating = item.get("vote_average", 0)
            meta = {
                "overview": item.get("overview", ""),
                "rating": rating,
                "poster": item.get("poster_path", ""),
                "fanart": item.get("backdrop_path", ""),
                "release": release,
                "original_title": item.get("original_name") or item.get("original_title") or "",
            }
            candidates.append(
                {
                    "title": item.get("name") or item.get("title") or title,
                    "alt_title": item.get("original_name")
                    or item.get("original_title")
                    or "",
                    "id": cid,
                    "msg": f"TMDb{'剧集' if is_tv else '电影'}候选",
                    "rating": rating,
                    "release": release,
                    "meta": meta,
                }
            )
        return candidates

    def _request_once(query, year_mode=None):
        params = {"api_key": api_key.strip(), "query": query, "language": "zh-CN"}
        if year:
            if year_mode == "year":
                params["year"] = year
            elif year_mode == "first_air_date_year":
                params["first_air_date_year"] = year
        response = _tmdb_get(
            f"https://api.themoviedb.org/3/search/{stype}",
            params=params,
            timeout=TIMEOUT_DB_SEARCH,
        )
        response.raise_for_status()
        return response.json().get("results", [])

    def _similarity_score(item):
        name = item.get("name") or item.get("title") or ""
        orig = item.get("original_name") or item.get("original_title") or ""
        name_norm = re.sub(r"[\W_]+", "", str(name).lower())
        orig_norm = re.sub(r"[\W_]+", "", str(orig).lower())
        scores = []
        if name_norm:
            scores.append(difflib.SequenceMatcher(None, q_norm, name_norm).ratio())
        if orig_norm:
            scores.append(difflib.SequenceMatcher(None, q_norm, orig_norm).ratio())
        return max(scores) if scores else 0.0

    try:
        if is_tv:
            search_plan = ["year", "first_air_date_year", None] if year else [None]
        else:
            search_plan = ["year", None] if year else [None]

        queries = [q]
        q_retry = re.sub(r"(?i)HD|重制版|重製版|Remaster|Edition", "", q).strip()
        if q_retry and q_retry != q:
            queries.append(q_retry)

        for query in queries:
            for year_mode in search_plan:
                results = _request_once(query, year_mode)
                if results:
                    return _items_to_candidates(results)

        # Fuzzy fallback: retry with split tokens and rerank by lexical similarity.
        token_queries = []
        for token in re.split(r"\s+", q):
            t = token.strip()
            if len(t) >= 4 and t.lower() != q.lower() and t not in token_queries:
                token_queries.append(t)

        fuzzy_pool = []
        seen = set()
        for tq in token_queries:
            for item in _request_once(tq, None):
                cid = str(item.get("id") or "")
                if not cid or cid in seen:
                    continue
                seen.add(cid)
                fuzzy_pool.append(item)

        if fuzzy_pool:
            ranked = sorted(fuzzy_pool, key=_similarity_score, reverse=True)
            top = [it for it in ranked if _similarity_score(it) >= 0.35]
            if top:
                return _items_to_candidates(top)
            return _items_to_candidates(ranked)
        return []
    except requests.exceptions.Timeout:
        return []
    except requests.exceptions.HTTPError as err:
        snippet = _response_body_snippet(getattr(err, "response", None))
        if snippet:
            logging.warning(f"TMDb搜索HTTP失败，返回内容: {snippet}")
        return []
    except ValueError:
        snippet = _response_body_snippet(locals().get("response"))
        if snippet:
            logging.warning(f"TMDb搜索解析失败，返回内容: {snippet}")
        return []
    except Exception as err:
        logging.error(f"TMDb搜索失败: {err}")
        return []


def fetch_tmdb_candidates(title, year=None, is_tv=True, api_key=""):
    return cached_request(
        fetch_tmdb_candidates_raw,
        get_cache_key("tmdb_candidates_v4", f"{title}_{year}_{is_tv}"),
        title,
        year,
        is_tv,
        api_key,
    )


def fetch_tmdb_info_raw(title, year=None, is_tv=True, api_key=""):
    if not api_key or not api_key.strip():
        return (
            title,
            "None",
            format_error_message(ERROR_CODE_CONFIG, "未配置TMDb Key"),
            {},
        )

    candidates = fetch_tmdb_candidates_raw(title, year, is_tv, api_key)
    if candidates:
        return candidate_to_result(candidates[0], "TMDb命中")
    return (
        title,
        "None",
        format_error_message(ERROR_CODE_NO_RESULT, "TMDb无结果"),
        {},
    )


def fetch_tmdb_info(title, year=None, is_tv=True, api_key=""):
    return cached_request(
        fetch_tmdb_info_raw,
        get_cache_key("tmdb_search_v3", f"{title}_{year}_{is_tv}"),
        title,
        year,
        is_tv,
        api_key,
    )


def fetch_tmdb_episode_meta_raw(
    tv_id, season, episode, api_key, series_title="", api_key_bgm=""
):
    if not tv_id or tv_id == "None" or not api_key.strip():
        return "", "", ""

    try:
        response = _tmdb_get(
            f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{episode}",
            params={"api_key": api_key.strip(), "language": "zh-CN"},
            timeout=TIMEOUT_DB_DETAIL,
        )
        response.raise_for_status()
        data = response.json()

        name = data.get("name")
        plot = data.get("overview")
        still = data.get("still_path", "")

        def _is_placeholder(n):
            """检测 TMDB 返回的占位集名：英文 Episode N 或中文 第N集。"""
            if not n:
                return True
            s = str(n).strip()
            return bool(
                re.fullmatch(r"(?i)episode\s*\d+", s)
                or re.fullmatch(r"第\s*\d+\s*[集話话]", s)
            )

        if _is_placeholder(name) or not (plot or "").strip():
            response_en = _tmdb_get(
                f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{episode}",
                params={"api_key": api_key.strip(), "language": "en-US"},
                timeout=TIMEOUT_DB_DETAIL,
            )
            response_en.raise_for_status()
            data_en = response_en.json()
            en_name = data_en.get("name", "")
            if _is_placeholder(name) and en_name and not _is_placeholder(en_name):
                name = en_name
            if not (plot or "").strip():
                plot = data_en.get("overview", "") or plot

        is_placeholder_name = _is_placeholder(name)

        if (
            not plot or not str(plot).strip() or not name or is_placeholder_name
        ) and series_title:
            try:
                bgm_candidates = fetch_bgm_candidates(series_title, api_key_bgm)
                if bgm_candidates:
                    bgm_subject_id = str(bgm_candidates[0].get("id", ""))
                    if bgm_subject_id:
                        bgm_ep_name, bgm_ep_plot = fetch_bgm_episode(
                            bgm_subject_id, season, episode, api_key_bgm
                        )
                        if (not name or is_placeholder_name) and bgm_ep_name:
                            name = bgm_ep_name
                        if (not plot or not str(plot).strip()) and bgm_ep_plot:
                            plot = bgm_ep_plot
            except Exception as err:
                logging.warning(f"BGM补全剧集信息失败: {err}")

        return name or "", plot or "", still or ""
    except Exception as err:
        snippet = ""
        if isinstance(err, requests.exceptions.HTTPError):
            snippet = _response_body_snippet(getattr(err, "response", None))
        else:
            snippet = _response_body_snippet(locals().get("response"))
        if snippet:
            logging.warning(f"TMDb剧集详情获取失败: {err}，返回内容: {snippet}")
        else:
            logging.warning(f"TMDb剧集详情获取失败: {err}")
        return "", "", ""


def fetch_tmdb_episode_meta(
    tv_id, season, episode, api_key, series_title="", api_key_bgm=""
):
    key = get_cache_key("tmdb_ep_v3", f"{tv_id}_{season}_{episode}_{series_title}")
    return cached_request(
        fetch_tmdb_episode_meta_raw,
        key,
        tv_id,
        season,
        episode,
        api_key,
        series_title,
        api_key_bgm,
    )


def fetch_tmdb_season_poster_raw(tv_id, season, api_key):
    if not tv_id or tv_id == "None" or not api_key.strip():
        return ""

    try:
        response = _tmdb_get(
            f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}",
            params={"api_key": api_key.strip(), "language": "zh-CN"},
            timeout=TIMEOUT_DB_DETAIL,
        )
        response.raise_for_status()
        return response.json().get("poster_path", "")
    except Exception:
        return ""


def fetch_tmdb_season_poster(tv_id, season, api_key):
    return cached_request(
        fetch_tmdb_season_poster_raw,
        get_cache_key("tmdb_season_poster", f"{tv_id}_{season}"),
        tv_id,
        season,
        api_key,
    )


def _fetch_hybrid_tmdb_id_raw(title, year, api_key_tmdb):
    """根据标题从 TMDB 搜索剧集 ID，供 hybrid 模式缓存复用。"""
    q = re.sub(r"(?i)HD|重制版|重製版|Remaster|Season.*|第.*季", "", title).strip()
    q_norm = re.sub(r"[\W_]+", "", str(q).lower())
    try:
        response = _tmdb_get(
            "https://api.themoviedb.org/3/search/tv",
            params={"api_key": api_key_tmdb.strip(), "query": q, "language": "zh-CN"},
            timeout=TIMEOUT_DB_SEARCH,
        )
        response.raise_for_status()
        results = response.json().get("results", [])

        best_item = None
        best_score = 0.0
        for item in results:
            name = item.get("name") or item.get("original_name") or ""
            name_norm = re.sub(r"[\W_]+", "", str(name).lower())
            if not name_norm or not q_norm:
                continue
            score = difflib.SequenceMatcher(None, q_norm, name_norm).ratio()
            item_year = str(item.get("first_air_date") or "")[:4]
            if year and item_year and str(year) == item_year:
                score += 0.15
            if score > best_score:
                best_score = score
                best_item = item

        if best_item and best_score >= 0.6:
            return str(best_item["id"])
    except Exception as err:
        logging.warning(f"hybrid TMDB搜索失败: {err}")
    return ""


def _fetch_hybrid_tmdb_id(title, year, api_key_tmdb):
    return cached_request(
        _fetch_hybrid_tmdb_id_raw,
        get_cache_key("hybrid_tmdb_id_v1", f"{title}_{year}"),
        title,
        year,
        api_key_tmdb,
    )


def fetch_hybrid_episode_meta_raw(
    title, subject_id, s, e, api_key_bgm, api_key_tmdb, year=None
):
    ep_n, ep_p = fetch_bgm_episode(subject_id, s, e, api_key_bgm)
    ep_s, s_p = "", ""

    if api_key_tmdb and api_key_tmdb.strip():
        try:
            tm_id = _fetch_hybrid_tmdb_id(title, year, api_key_tmdb)
            if tm_id:
                ep_s_res = _tmdb_get(
                    f"https://api.themoviedb.org/3/tv/{tm_id}/season/{s}/episode/{e}",
                    params={"api_key": api_key_tmdb.strip(), "language": "zh-CN"},
                    timeout=TIMEOUT_DB_DETAIL,
                )
                if ep_s_res.status_code == 200:
                    ep_s = ep_s_res.json().get("still_path", "")

                s_p_res = _tmdb_get(
                    f"https://api.themoviedb.org/3/tv/{tm_id}/season/{s}",
                    params={"api_key": api_key_tmdb.strip(), "language": "zh-CN"},
                    timeout=TIMEOUT_DB_DETAIL,
                )
                if s_p_res.status_code == 200:
                    s_p = s_p_res.json().get("poster_path", "")
        except Exception as err:
            snippet = ""
            if isinstance(err, requests.exceptions.HTTPError):
                snippet = _response_body_snippet(getattr(err, "response", None))
            else:
                snippet = _response_body_snippet(locals().get("response"))
            if snippet:
                logging.warning(f"混合来源补全剧集图片失败: {err}，返回内容: {snippet}")
            else:
                logging.warning(f"混合来源补全剧集图片失败: {err}")

    return ep_n, ep_p, ep_s, s_p


def fetch_hybrid_episode_meta(
    title, subject_id, s, e, api_key_bgm, api_key_tmdb, year=None
):
    return cached_request(
        fetch_hybrid_episode_meta_raw,
        get_cache_key("hybrid_ep_v1", f"{subject_id}_{s}_{e}"),
        title,
        subject_id,
        s,
        e,
        api_key_bgm,
        api_key_tmdb,
        year,
    )


def fetch_tmdb_credits_raw(tmdb_id, is_tv=True, api_key=""):
    """从 TMDB 获取演职人员信息，返回 (actors, directors) 两个列表。

    actors: [{"name": ..., "role": ..., "thumb": ...}, ...]  最多 20 人
    directors: ["导演名", ...]
    """
    if not tmdb_id or tmdb_id == "None" or not api_key.strip():
        return [], []

    stype = "tv" if is_tv else "movie"
    try:
        resp = _tmdb_get(
            f"https://api.themoviedb.org/3/{stype}/{tmdb_id}/credits",
            params={"api_key": api_key.strip(), "language": "zh-CN"},
            timeout=TIMEOUT_DB_DETAIL,
        )
        resp.raise_for_status()
        data = resp.json()

        cast = data.get("cast") or []
        crew = data.get("crew") or []

        actors = []
        for p in cast[:20]:
            name = p.get("name") or ""
            role = p.get("character") or ""
            thumb = p.get("profile_path") or ""
            if thumb:
                thumb = f"https://image.tmdb.org/t/p/w185{thumb}"
            if name:
                actors.append({"name": name, "role": role, "thumb": thumb})

        directors = [
            p.get("name") for p in crew
            if p.get("job") == "Director" and p.get("name")
        ]
        # TV 剧没有 movie Director，退而用 creator
        if is_tv and not directors:
            directors = [
                p.get("name") for p in (data.get("created_by") or [])
                if p.get("name")
            ]

        return actors, directors
    except Exception as err:
        logging.warning(f"TMDB credits 获取失败 ({tmdb_id}): {err}")
        return [], []


def fetch_tmdb_credits(tmdb_id, is_tv=True, api_key=""):
    return cached_request(
        fetch_tmdb_credits_raw,
        get_cache_key("tmdb_credits_v1", f"{tmdb_id}_{is_tv}"),
        tmdb_id,
        is_tv,
        api_key,
    )
