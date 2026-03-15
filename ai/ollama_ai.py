import json
import re

import requests

from utils.helpers import session


def fetch_siliconflow_info(filename, api_key, model_name="deepseek-ai/DeepSeek-V3"):
	"""Use SiliconFlow to parse title/year/season/episode from filename."""
	if not api_key or not api_key.strip():
		return None, "未配置 AI Key"

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
		response = session.post(url, json=payload, headers=headers, timeout=30)
		response.raise_for_status()

		result_text = response.json()['choices'][0]['message']['content'].strip()
		result_text = re.sub(r'^```(?:json)?\s*|\s*```$', '', result_text, flags=re.IGNORECASE)

		data = json.loads(result_text)
		return data, "AI解析成功"
	except requests.exceptions.Timeout:
		return None, "AI请求超时"
	except json.JSONDecodeError:
		return None, "AI返回JSON解析失败"
	except Exception as err:
		return None, f"AI失败: {str(err)}"
