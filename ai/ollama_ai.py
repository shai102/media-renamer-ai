import json
import re

import requests

from utils.helpers import (
    ERROR_CODE_CONFIG,
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    format_error_message,
    session,
)


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
    if not isinstance(content, str) or not content.strip():
        raise ValueError("AI响应content为空")

    return content.strip()


def fetch_siliconflow_info(filename, api_key, model_name="deepseek-ai/DeepSeek-V3"):
    """Use SiliconFlow to parse title/year/season/episode from filename."""
    if not api_key or not api_key.strip():
        return None, format_error_message(ERROR_CODE_CONFIG, "未配置 AI Key")

    model = (model_name or "").strip() or "deepseek-ai/DeepSeek-V3"

    url = "https://api.siliconflow.cn/v1/chat/completions"
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

返回格式：
{
    "title": "",
    "year": null,
    "season": 1,
    "episode": 1
}
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
        "temperature": 0.1,
        "max_tokens": 500,
    }

    try:
        response = session.post(url, json=payload, headers=headers, timeout=15)
        response.raise_for_status()

        try:
            response_payload = response.json()
        except ValueError:
            return None, format_error_message(ERROR_CODE_PARSE, "AI返回非JSON响应")

        try:
            result_text = _extract_siliconflow_content(response_payload)
        except ValueError as err:
            return None, format_error_message(ERROR_CODE_PARSE, f"AI响应结构异常: {err}")

        result_text = re.sub(
            r"^```(?:json)?\s*|\s*```$", "", result_text, flags=re.IGNORECASE
        )

        data = json.loads(result_text)
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
            return None, format_error_message(ERROR_CODE_PARSE, "AI返回字段类型异常: title")

        year_val = data.get("year")
        if year_val is not None and not isinstance(year_val, (int, str)):
            return None, format_error_message(ERROR_CODE_PARSE, "AI返回字段类型异常: year")

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
    except json.JSONDecodeError:
        return None, format_error_message(ERROR_CODE_PARSE, "AI返回JSON解析失败")
    except Exception as err:
        return None, format_error_message(ERROR_CODE_UNKNOWN, f"AI失败: {str(err)}")
