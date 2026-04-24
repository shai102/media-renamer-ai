import json
import logging
import os
import re

import requests

from utils.helpers import (
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    TIMEOUT_OLLAMA_CHAT,
    TIMEOUT_OLLAMA_EMBED,
    TIMEOUT_OLLAMA_TAGS,
    clean_search_title,
    extract_year_from_release,
    format_candidate_label,
    format_error_message,
    safe_str,
)


def ollama_post_json(base_url, endpoint, payload, timeout):
    """Direct local Ollama call without retry-enabled shared session."""
    normalized = str(base_url or "").strip().rstrip("/")
    if not normalized:
        raise ValueError("Ollama URL 未配置")
    return requests.post(normalized + endpoint, json=payload, timeout=timeout)


def _extract_item_old_name(item):
    if isinstance(item, dict):
        return str(item.get("old_name", "") or "")
    return str(getattr(item, "old_name", "") or "")


def extract_ollama_model_names(payload):
    """Extract installed model names from Ollama /api/tags response."""
    if not isinstance(payload, dict):
        raise ValueError("Ollama响应不是JSON对象")

    models = payload.get("models")
    if not isinstance(models, list):
        raise ValueError("Ollama响应缺少models列表")

    names = []
    for item in models:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if name and name not in names:
            names.append(name)
    return names


def list_ollama_models(base_url):
    """List installed local Ollama models from the configured server."""
    normalized = str(base_url or "").strip().rstrip("/")
    if not normalized:
        return [], "Ollama URL 未配置"

    try:
        response = requests.get(normalized + "/api/tags", timeout=TIMEOUT_OLLAMA_TAGS)
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError:
            return [], "Ollama返回非JSON响应"

        names = extract_ollama_model_names(payload)
        if not names:
            return [], "未发现本地已安装模型"
        return names, "已获取本地模型列表"
    except requests.exceptions.Timeout:
        return [], "读取本地模型超时"
    except Exception as err:
        return [], f"读取本地模型失败: {err}"


def _normalize_top_p(value, default=0.9):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    return max(0.0, min(1.0, number))


def _normalize_temperature(value, default=0.2):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    return max(0.0, min(2.0, number))


def _normalize_ollama_parse_result(data):
    title = str(data.get("title") or "").strip()

    year = data.get("year")
    try:
        year = int(year) if year not in (None, "") else None
    except (TypeError, ValueError):
        year = None

    try:
        season = int(data.get("season") or 1)
    except (TypeError, ValueError):
        season = 1
    season = max(1, season)

    try:
        episode = int(data.get("episode") or 1)
    except (TypeError, ValueError):
        episode = 1
    episode = max(1, episode)

    return {
        "title": title,
        "year": year,
        "season": season,
        "episode": episode,
    }


def parse_with_ollama(base_url, model, filename, temperature=0.2, top_p=0.9):
    """Parse media filename using local Ollama model."""
    model = str(model or "").strip()
    if not str(base_url or "").strip() or not model:
        return None, "Ollama URL 或模型未配置"

    prompt = r"""
你是动漫/影视文件名解析助手。

任务：
从文件名中提取作品标题、年份、季数、集数。

硬性规则：
1. 只输出 JSON，不要解释，不要 markdown。
2. title 必须是文件名里真实存在的作品名，不允许联想、不允许猜测其他作品。
3. 遇到番组文件名时，优先保留原标题，如 Violet_Evergarden -> Violet Evergarden。
4. 删除字幕组、分辨率、编码、语言标签、发布信息，如 KTXP、1080p、BDrip、GB、x264。
5. season 默认 1。
6. episode 必须是数字；像 [01] 这种优先识别为 episode。
7. 如果无法确定 year，填 null。
8. 如果文件名里没有明确作品名，title 设为空字符串，不要猜。

示例：
输入: [KTXP][Dungeon Meshi][01][CHS][1080P][AVC].mkv
输出: {"title": "Dungeon Meshi", "year": null, "season": 1, "episode": 1}

输入: 蜡笔小新.2024.S01E05.1080p.mkv
输出: {"title": "蜡笔小新", "year": 2024, "season": 1, "episode": 5}

输入: The.Mandalorian.S03E04.2023.WEB-DL.mkv
输出: {"title": "The Mandalorian", "year": 2023, "season": 3, "episode": 4}

输入: [UHA-WINGS][Violet Evergarden][06][CHT][1080p][MP4].mp4
输出: {"title": "Violet Evergarden", "year": null, "season": 1, "episode": 6}

返回格式：
{
  "title": "",
  "year": null,
  "season": 1,
  "episode": 1
}
"""

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": filename},
        ],
        "stream": False,
        "think": False,
        "options": {
            "temperature": _normalize_temperature(temperature),
            "top_p": _normalize_top_p(top_p),
            "num_predict": 512,
        },
        "timeout": TIMEOUT_OLLAMA_CHAT[1],
    }

    try:
        response = ollama_post_json(
            base_url, "/api/chat", payload, timeout=TIMEOUT_OLLAMA_CHAT
        )
        response.raise_for_status()
        resp = response.json()

        content = resp.get("message", {}).get("content", "").strip()
        if not content:
            return None, format_error_message(ERROR_CODE_PARSE, "Ollama返回空内容")

        content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.IGNORECASE)

        try:
            data = json.loads(content)
            if not isinstance(data, dict):
                return None, format_error_message(ERROR_CODE_PARSE, "返回内容不是 JSON 对象")
            return _normalize_ollama_parse_result(data), "Ollama解析成功"
        except json.JSONDecodeError:
            json_match = re.search(r"\{.*\}", content, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                return _normalize_ollama_parse_result(data), "Ollama解析成功"
            return None, format_error_message(ERROR_CODE_PARSE, "无法解析返回的JSON")

    except requests.exceptions.Timeout:
        return None, format_error_message(ERROR_CODE_TIMEOUT, "Ollama请求超时")
    except Exception as err:
        return None, format_error_message(ERROR_CODE_UNKNOWN, f"Ollama失败: {str(err)}")


def cosine_similarity(vec_a, vec_b):
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(float(a) * float(b) for a, b in zip(vec_a, vec_b))
    norm_a = sum(float(a) * float(a) for a in vec_a) ** 0.5
    norm_b = sum(float(b) * float(b) for b in vec_b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def build_candidate_embedding_text(candidate):
    title = candidate.get("title") or ""
    alt = candidate.get("alt_title") or ""
    year = extract_year_from_release(candidate.get("release")) or ""
    overview = (candidate.get("meta") or {}).get("overview") or ""
    return f"标题:{title}; 原名:{alt}; 年份:{year}; 简介:{overview[:120]}"


def get_embedding(base_url, embedding_model, text, cache, cache_lock, preferred_endpoint=None):
    clean_text = str(text or "").strip()
    model = str(embedding_model or "").strip()
    if not str(base_url or "").strip() or not model or not clean_text:
        return None, preferred_endpoint

    cache_key = f"{model}::{clean_text}"
    with cache_lock:
        cached = cache.get(cache_key)
    if cached:
        return cached, preferred_endpoint

    payload = {"model": model, "prompt": clean_text}
    endpoints = []
    if preferred_endpoint:
        endpoints.append(preferred_endpoint)
    for endpoint in ("/api/embed", "/api/embeddings"):
        if endpoint not in endpoints:
            endpoints.append(endpoint)

    for endpoint in endpoints:
        try:
            response = ollama_post_json(
                base_url, endpoint, payload, timeout=TIMEOUT_OLLAMA_EMBED
            )
            if response.status_code == 404:
                continue
            response.raise_for_status()
            data = response.json()

            emb = data.get("embedding")
            if not emb:
                emb_list = data.get("embeddings")
                if isinstance(emb_list, list) and emb_list:
                    emb = emb_list[0]

            if isinstance(emb, list) and emb:
                with cache_lock:
                    cache[cache_key] = emb
                return emb, endpoint
        except requests.exceptions.Timeout:
            logging.warning("Embedding请求超时")
            return None, preferred_endpoint
        except Exception as err:
            logging.error(f"Embedding请求失败({endpoint}): {err}")
    return None, preferred_endpoint


def rerank_candidates_with_embedding(item, query_title, year, is_tv, source_name, candidates, get_embedding_func):
    if not candidates:
        return candidates, None, ""

    # 用去噪后的文件名（而非原始噪声版本）丰富查询语义
    clean_fn = clean_search_title(os.path.splitext(_extract_item_old_name(item))[0])
    query_text = (
        f"标题:{query_title}; "
        f"文件:{clean_fn}; "
        f"年份:{safe_str(year)}; "
        f"类型:{'剧集' if is_tv else '电影'}"
    )
    q_emb = get_embedding_func(query_text)
    if not q_emb:
        return candidates, None, ""

    scored = []
    for candidate in candidates:
        c_emb = get_embedding_func(build_candidate_embedding_text(candidate))
        if not c_emb:
            continue
        score = cosine_similarity(q_emb, c_emb)
        scored.append((score, candidate))

    if not scored:
        return candidates, None, ""

    scored.sort(key=lambda x: x[0], reverse=True)
    scored_candidates = [c for _, c in scored]
    ranked = scored_candidates + [c for c in candidates if c not in scored_candidates]

    top_score = scored[0][0]
    second_score = scored[1][0] if len(scored) > 1 else -1.0
    rank_msg = f"Embedding重排 top={top_score:.3f}"

    if top_score >= 0.78 and (len(scored) == 1 or top_score - second_score >= 0.10):
        return ranked, scored[0][1], rank_msg

    return ranked, None, rank_msg


def pick_candidate_with_ollama(
    base_url,
    model,
    item,
    query_title,
    year,
    is_tv,
    source_name,
    candidates,
    temperature=0.2,
):
    if not str(base_url or "").strip() or not str(model or "").strip():
        return None, "未配置本地模型"

    prompt_lines = []
    for idx, candidate in enumerate(candidates, 1):
        prompt_lines.append(
            f"{idx}. 标题={candidate.get('title', '')}; 原名={candidate.get('alt_title', '')}; 年份={extract_year_from_release(candidate.get('release')) or '-'}; ID={candidate.get('id')}; 评分={candidate.get('rating', 0)}"
        )

    clean_name = os.path.splitext(_extract_item_old_name(item))[0]
    prompt = f"""你是媒体数据库匹配助手。请根据文件名、解析出的标题和年份，从候选中选出最可能匹配的一项。
如果无法确定，必须返回 pick 为 0。只允许输出 JSON，不要输出额外说明。
JSON 格式: {{"pick": 0或候选序号, "reason": "简短原因"}}
文件名: {clean_name}
解析标题: {query_title}
年份: {safe_str(year)}
类型: {"剧集" if is_tv else "电影"}
来源: {source_name}
候选列表:
{chr(10).join(prompt_lines)}"""

    payload = {
        "model": str(model).strip(),
        "messages": [
            {
                "role": "system",
                "content": "你只输出 JSON。拿不准时 pick 必须返回 0。",
            },
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "think": False,
        "options": {"temperature": _normalize_temperature(temperature)},
        "timeout": TIMEOUT_OLLAMA_CHAT[1],
    }

    try:
        response = ollama_post_json(
            base_url, "/api/chat", payload, timeout=TIMEOUT_OLLAMA_CHAT
        )
        response.raise_for_status()
        resp = response.json()
        content = resp.get("message", {}).get("content", "").strip()
        if not content:
            return None, "本地模型返回空内容"

        content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.IGNORECASE).strip()

        parsed = None
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", content, re.DOTALL)
            if match:
                parsed = json.loads(match.group())
            elif re.fullmatch(r"\d+", content):
                parsed = {"pick": int(content), "reason": "纯数字返回"}

        if not isinstance(parsed, dict):
            return None, "本地模型返回格式无效"

        pick = parsed.get("pick", parsed.get("index", parsed.get("candidate")))
        picked_id = parsed.get("id")
        reason = parsed.get("reason", "")

        if isinstance(pick, str) and pick.strip().isdigit():
            pick = int(pick.strip())

        if picked_id is not None:
            picked_id = str(picked_id).strip()
            for candidate in candidates:
                if str(candidate.get("id")) == picked_id:
                    return candidate, reason or "本地模型按 ID 选中"

        if isinstance(pick, int) and 1 <= pick <= len(candidates):
            return candidates[pick - 1], reason or "本地模型已选择候选"

        return None, reason or "本地模型无法确定"
    except requests.exceptions.Timeout:
        return None, "本地模型判定超时"
    except Exception as err:
        logging.error(f"Ollama候选判定失败: {err}")
        return None, f"本地模型判定失败: {err}"


def populate_candidate_listbox(lb, candidates):
    for candidate in candidates:
        lb.insert(0x7FFFFFFF, format_candidate_label(candidate))
