import logging
import json
import re

import requests

from utils.helpers import (
    ERROR_CODE_CONFIG,
    ERROR_CODE_HTTP,
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    TIMEOUT_AI_CHAT,
    TIMEOUT_AI_TEST,
    format_error_message,
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


def _extract_siliconflow_content(payload):
    """Validate response structure and return chat content text."""
    if not isinstance(payload, dict):
        raise ValueError("AI响应不是JSON对象")

    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError("AI响应缺少choices")

    first_choice = choices[0]
    if not isinstance(first_choice, dict):
        raise ValueError("AI响应choices结构无效")

    message = first_choice.get("message")
    if not isinstance(message, dict):
        raise ValueError("AI响应缺少message")

    content = message.get("content")
    # Qwen3 思考模式下 content 为空，回复在 reasoning_content 里
    if not isinstance(content, str) or not content.strip():
        content = message.get("reasoning_content") or ""
    if not isinstance(content, str) or not content.strip():
        raise ValueError("AI响应content为空")

    return content.strip()


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


def fetch_siliconflow_info(
    filename,
    api_key,
    api_url="https://api.siliconflow.cn/v1",
    model_name="deepseek-ai/DeepSeek-V3",
    temperature=0.2,
    top_p=0.9,
):
    """Use OpenAI-compatible API to parse title/year/season/episode from filename."""
    if not api_key or not api_key.strip():
        return None, format_error_message(ERROR_CODE_CONFIG, "未配置 AI Key")

    model = (model_name or "").strip() or "deepseek-ai/DeepSeek-V3"
    base_url = (api_url or "https://api.siliconflow.cn/v1").strip().rstrip("/")
    url = f"{base_url}/chat/completions"
    prompt = r"""
你是一个严格的动漫/影视文件名解析器。

任务：
从输入的单个文件名中提取以下字段：
- title
- year
- season
- episode

输出要求：
1. 只能输出一行 JSON。
2. 不要输出解释、注释、markdown、代码块、前后缀文本。
3. 输出字段必须且只能包含：
{"title":"","year":null,"season":1,"episode":1}

字段规则：
1. title
   - 必须来自文件名中真实存在的作品名。
   - 不允许联想、补全、翻译、猜测、改写成其他作品名。
   - 可以做最小清洗：
     - 将下划线、点号替换为空格
     - 去掉首尾空格
     - 保留原标题主体
   - 删除与作品名无关的信息，例如：
     字幕组、分辨率、编码、音频、语言、来源、发布组、校验信息、文件扩展名。
   - 如果无法明确确定作品名，返回空字符串 ""。

2. year
   - 仅当文件名中明确出现四位年份（如 2024、2023）时提取。
   - 否则返回 null。

3. season
   - 默认值为 1。
   - 若文件名中明确出现季信息，则提取对应数字。
   - 常见模式包括但不限于：
     S01、S1、Season 1、Season01、第2季、第二季、2nd Season
   - 若无明确季信息，返回 1。

4. episode
   - 必须是数字，且始终返回整数。
   - 优先识别明确集数信息。
   - 常见模式包括但不限于：
     E05、EP05、Episode 5、[05]、第05集、第5话、- 05
   - 对番组文件名，像 [01] 这种纯数字分段，优先识别为 episode。
   - 如果无法明确识别集数，则返回 1。

清洗规则：
1. 删除以下常见噪音信息：
   - 字幕组/发布组：KTXP、UHA-WINGS 等方括号组名
   - 分辨率：1080p、2160p、720p、4K
   - 编码：x264、x265、HEVC、AVC
   - 来源：WEB-DL、BDrip、BluRay、BD、DVD
   - 语言：CHS、CHT、GB、BIG5、简繁、字幕相关标签
   - 扩展名：mkv、mp4、avi
2. 不要把这些噪音拼进 title。
3. title 中的点号 "." 和下划线 "_" 可视为分隔符，必要时转为空格。

判定优先级：
1. 先识别 episode
2. 再识别 season
3. 再识别 year
4. 最后确定 title
5. title 只能取剩余文本中能明确视为作品名的部分

示例：
输入: [KTXP][Dungeon Meshi][01][CHS][1080P][AVC].mkv
输出: {"title":"Dungeon Meshi","year":null,"season":1,"episode":1}

输入: 蜡笔小新.2024.S01E05.1080p.mkv
输出: {"title":"蜡笔小新","year":2024,"season":1,"episode":5}

输入: The.Mandalorian.S03E04.2023.WEB-DL.mkv
输出: {"title":"The Mandalorian","year":2023,"season":3,"episode":4}

输入: [UHA-WINGS][Violet Evergarden][06][CHT][1080p][MP4].mp4
输出: {"title":"Violet Evergarden","year":null,"season":1,"episode":6}

输入: [SomeGroup][01][1080p].mkv
输出: {"title":"","year":null,"season":1,"episode":1}
"""

    headers = {
        "Authorization": f"Bearer {api_key.strip()}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": filename},
        ],
        "temperature": _normalize_temperature(temperature),
        "top_p": _normalize_top_p(top_p),
        "max_tokens": 500,
        "enable_thinking": False,
    }

    try:
        response = session.post(
            url, json=payload, headers=headers, timeout=TIMEOUT_AI_CHAT
        )
        response.raise_for_status()

        try:
            response_payload = response.json()
        except ValueError:
            snippet = _response_body_snippet(response)
            if snippet:
                logging.warning(f"SiliconFlow返回非JSON，返回内容: {snippet}")
            return None, format_error_message(ERROR_CODE_PARSE, "AI返回非JSON响应")

        try:
            result_text = _extract_siliconflow_content(response_payload)
        except ValueError as err:
            snippet = _response_body_snippet(response)
            if snippet:
                logging.warning(f"SiliconFlow响应结构异常: {err}，返回内容: {snippet}")
            return None, format_error_message(
                ERROR_CODE_PARSE, f"AI响应结构异常: {err}"
            )

        result_text = re.sub(
            r"^```(?:json)?\s*|\s*```$", "", result_text, flags=re.IGNORECASE
        )

        try:
            data = json.loads(result_text)
        except json.JSONDecodeError as err:
            compact = " ".join(str(result_text or "").split())
            if compact:
                logging.warning(
                    f"SiliconFlow内容JSON解析失败: {err}，内容: {compact[:300]}"
                )
            return None, format_error_message(ERROR_CODE_PARSE, "AI返回JSON解析失败")
        if not isinstance(data, dict):
            return None, format_error_message(ERROR_CODE_PARSE, "AI返回JSON不是对象")

        missing_keys = [
            k for k in ("title", "year", "season", "episode") if k not in data
        ]
        if missing_keys:
            return None, format_error_message(
                ERROR_CODE_PARSE, f"AI返回字段缺失: {', '.join(missing_keys)}"
            )

        if not isinstance(data.get("title"), str):
            return None, format_error_message(
                ERROR_CODE_PARSE, "AI返回字段类型异常: title"
            )

        year_val = data.get("year")
        if year_val is not None and not isinstance(year_val, (int, str)):
            return None, format_error_message(
                ERROR_CODE_PARSE, "AI返回字段类型异常: year"
            )

        try:
            season = int(data.get("season"))
            episode = int(data.get("episode"))
        except (TypeError, ValueError):
            return None, format_error_message(
                ERROR_CODE_PARSE, "AI返回字段类型异常: season/episode"
            )

        if isinstance(year_val, str):
            year_text = year_val.strip()
            year_val = int(year_text) if year_text.isdigit() else None

        normalized = {
            "title": data.get("title", "").strip(),
            "year": year_val,
            "season": season,
            "episode": episode,
        }

        return normalized, "AI解析成功"
    except requests.exceptions.Timeout:
        return None, format_error_message(ERROR_CODE_TIMEOUT, "AI请求超时")
    except requests.exceptions.HTTPError as err:
        snippet = _response_body_snippet(getattr(err, "response", None))
        if snippet:
            logging.warning(f"SiliconFlow请求HTTP失败: {err}，返回内容: {snippet}")
        return None, format_error_message(ERROR_CODE_HTTP, f"AI请求失败: {err}")
    except Exception as err:
        return None, format_error_message(ERROR_CODE_UNKNOWN, f"AI失败: {str(err)}")


def test_silicon_api(api_url, api_key, model_name):
    """Test OpenAI-compatible API connection. Returns (success, message)."""
    if not api_key or not api_key.strip():
        return False, "未配置 API Key"

    base_url = (api_url or "").strip().rstrip("/")
    if not base_url:
        return False, "未配置 API URL"

    url = f"{base_url}/chat/completions"
    model = (model_name or "").strip()
    if not model:
        return False, "未配置模型名称"

    headers = {
        "Authorization": f"Bearer {api_key.strip()}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": "Hi"}],
        "max_tokens": 10,
        "enable_thinking": False,
    }

    try:
        response = session.post(
            url, json=payload, headers=headers, timeout=TIMEOUT_AI_TEST
        )
        response.raise_for_status()

        try:
            result = response.json()
        except ValueError:
            snippet = _response_body_snippet(response)
            return False, f"响应非JSON: {snippet[:100] if snippet else '空响应'}"

        try:
            _extract_siliconflow_content(result)
            return True, f"连接成功! 模型: {model}"
        except ValueError as err:
            return False, f"响应结构异常: {err}"

    except requests.exceptions.Timeout:
        return False, "请求超时"
    except requests.exceptions.HTTPError as err:
        status = getattr(err.response, "status_code", "未知")
        snippet = _response_body_snippet(getattr(err, "response", None))
        detail = snippet[:100] if snippet else ""
        return False, f"HTTP错误 {status}: {detail}"
    except Exception as err:
        return False, f"连接失败: {str(err)}"
