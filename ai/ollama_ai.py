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
	prompt = """
你是影视文件名解析助手。

任务：
从文件名中提取影视元数据。

必须遵守：
1 只允许输出 JSON
2 不允许解释
3 不允许 markdown
4 title 只保留作品名
5 删除字幕组、分辨率、编码
6 season 默认 1
7 episode 必须是数字

JSON格式：

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
