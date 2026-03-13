import logging
import re

import requests

from utils.helpers import (
	USER_AGENT,
	cached_request,
	candidate_to_result,
	clean_search_title,
	get_cache_key,
	session,
)


def fetch_bgm_by_id_raw(subject_id, api_key=""):
	headers = {'User-Agent': USER_AGENT}
	if api_key and api_key.strip():
		headers['Authorization'] = f"Bearer {api_key.strip()}"

	try:
		response = session.get(f"https://api.bgm.tv/v0/subjects/{subject_id}", headers=headers, timeout=15)
		response.raise_for_status()
		data = response.json()

		meta = {
			"overview": data.get('summary', ""),
			"rating": data.get('rating', {}).get('score', 0),
			"poster": data.get('images', {}).get('large', ""),
			"fanart": "",
			"release": data.get('date', ""),
		}

		title = data.get('name_cn') or data.get('name') or str(subject_id)
		return title, str(data.get('id')), "ID强制锁定", meta
	except requests.exceptions.Timeout:
		return str(subject_id), "None", "请求超时", {}
	except Exception:
		return str(subject_id), "None", "ID无效", {}


def fetch_bgm_by_id(subject_id, api_key=""):
	return cached_request(fetch_bgm_by_id_raw, get_cache_key('bgm_id', subject_id), subject_id, api_key)


def fetch_bgm_candidates_raw(title, api_key=""):
	q = clean_search_title(title)
	headers = {'User-Agent': USER_AGENT}
	if api_key and api_key.strip():
		headers['Authorization'] = f"Bearer {api_key.strip()}"

	try:
		response = session.get(f"https://api.bgm.tv/search/subject/{q}?type=2", headers=headers, timeout=15)
		response.raise_for_status()
		data = response.json().get('list', [])

		candidates = []
		seen_ids = set()
		for item in data[:8]:
			cid = str(item.get('id') or '')
			if not cid or cid in seen_ids:
				continue
			seen_ids.add(cid)

			release = item.get('air_date') or item.get('date') or ''
			rating = item.get('score', 0)
			meta = {
				"overview": item.get('summary', ""),
				"rating": rating,
				"poster": item.get('images', {}).get('large', ""),
				"fanart": "",
				"release": release,
			}
			candidates.append({
				"title": item.get('name_cn') or item.get('name') or title,
				"alt_title": item.get('name') or '',
				"id": cid,
				"msg": "BGM候选",
				"rating": rating,
				"release": release,
				"meta": meta,
			})
		return candidates
	except requests.exceptions.Timeout:
		return []
	except Exception:
		return []


def fetch_bgm_candidates(title, api_key=""):
	return cached_request(fetch_bgm_candidates_raw, get_cache_key('bgm_candidates_v1', title), title, api_key)


def fetch_bgm_info_raw(title, api_key=""):
	candidates = fetch_bgm_candidates_raw(title, api_key)
	if candidates:
		return candidate_to_result(candidates[0], "BGM命中")
	return title, "None", "未匹配", {}


def fetch_bgm_info(title, api_key=""):
	return cached_request(fetch_bgm_info_raw, get_cache_key('bgm_search', title), title, api_key)


def fetch_bgm_episode_raw(subject_id, season, episode, api_key_bgm):
	headers = {'User-Agent': USER_AGENT}
	if api_key_bgm and api_key_bgm.strip():
		headers['Authorization'] = f"Bearer {api_key_bgm.strip()}"

	try:
		response = session.get(
			f"https://api.bgm.tv/v0/episodes?subject_id={subject_id}&type=0&limit=100",
			headers=headers,
			timeout=15,
		)
		response.raise_for_status()

		for ep in response.json().get('data', []):
			if ep.get('sort') == episode:
				return ep.get('name_cn') or ep.get('name') or "", ep.get('desc', "")
	except Exception:
		pass

	return "", ""


def fetch_bgm_episode(subject_id, season, episode, api_key_bgm):
	return cached_request(
		fetch_bgm_episode_raw,
		get_cache_key('bgm_ep', f"{subject_id}_{season}_{episode}"),
		subject_id,
		season,
		episode,
		api_key_bgm,
	)


def fetch_tmdb_by_id_raw(tmdb_id, is_tv=True, api_key=""):
	if not api_key or not api_key.strip():
		return str(tmdb_id), "None", "未配置TMDb Key", {}

	stype = "tv" if is_tv else "movie"

	try:
		response = session.get(
			f"https://api.themoviedb.org/3/{stype}/{tmdb_id}",
			params={"api_key": api_key.strip(), "language": "zh-CN"},
			timeout=15,
		)
		response.raise_for_status()
		data = response.json()

		meta = {
			"overview": data.get('overview', ""),
			"rating": data.get('vote_average', 0),
			"poster": data.get('poster_path', ""),
			"fanart": data.get('backdrop_path', ""),
			"release": data.get('first_air_date') or data.get('release_date') or "",
		}

		title = data.get('name') or data.get('title') or str(tmdb_id)
		return title, str(data.get('id')), "ID锁定成功", meta
	except requests.exceptions.Timeout:
		return str(tmdb_id), "None", "请求超时", {}
	except Exception:
		return str(tmdb_id), "None", "ID无效", {}


def fetch_tmdb_by_id(tmdb_id, is_tv=True, api_key=""):
	return cached_request(fetch_tmdb_by_id_raw, get_cache_key('tmdb_id', f"{tmdb_id}_{is_tv}"), tmdb_id, is_tv, api_key)


def fetch_tmdb_candidates_raw(title, year=None, is_tv=True, api_key=""):
	if not api_key or not api_key.strip():
		return []

	q = clean_search_title(title)
	stype = "tv" if is_tv else "movie"

	def _items_to_candidates(items):
		candidates = []
		seen_ids = set()
		for item in items[:8]:
			cid = str(item.get('id') or '')
			if not cid or cid in seen_ids:
				continue
			seen_ids.add(cid)

			release = item.get('first_air_date') or item.get('release_date') or ""
			rating = item.get('vote_average', 0)
			meta = {
				"overview": item.get('overview', ""),
				"rating": rating,
				"poster": item.get('poster_path', ""),
				"fanart": item.get('backdrop_path', ""),
				"release": release,
			}
			candidates.append({
				"title": item.get('name') or item.get('title') or title,
				"alt_title": item.get('original_name') or item.get('original_title') or '',
				"id": cid,
				"msg": f"TMDb{'剧集' if is_tv else '电影'}候选",
				"rating": rating,
				"release": release,
				"meta": meta,
			})
		return candidates

	def _request_once(query, year_mode=None):
		params = {"api_key": api_key.strip(), "query": query, "language": "zh-CN"}
		if year:
			if year_mode == "year":
				params["year"] = year
			elif year_mode == "first_air_date_year":
				params["first_air_date_year"] = year
		response = session.get(f"https://api.themoviedb.org/3/search/{stype}", params=params, timeout=15)
		response.raise_for_status()
		return response.json().get('results', [])

	try:
		if is_tv:
			search_plan = ["year", "first_air_date_year", None] if year else [None]
		else:
			search_plan = ["year", None] if year else [None]

		queries = [q]
		q_retry = re.sub(r'(?i)HD|重制版|重製版|Remaster|Edition', '', q).strip()
		if q_retry and q_retry != q:
			queries.append(q_retry)

		for query in queries:
			for year_mode in search_plan:
				results = _request_once(query, year_mode)
				if results:
					return _items_to_candidates(results)
		return []
	except requests.exceptions.Timeout:
		return []
	except Exception as err:
		logging.error(f"TMDb搜索失败: {err}")
		return []


def fetch_tmdb_candidates(title, year=None, is_tv=True, api_key=""):
	return cached_request(
		fetch_tmdb_candidates_raw,
		get_cache_key('tmdb_candidates_v3', f"{title}_{year}_{is_tv}"),
		title,
		year,
		is_tv,
		api_key,
	)


def fetch_tmdb_info_raw(title, year=None, is_tv=True, api_key=""):
	if not api_key or not api_key.strip():
		return title, "None", "未配置TMDb Key", {}

	candidates = fetch_tmdb_candidates_raw(title, year, is_tv, api_key)
	if candidates:
		return candidate_to_result(candidates[0], "TMDb命中")
	return title, "None", "TMDb无结果", {}


def fetch_tmdb_info(title, year=None, is_tv=True, api_key=""):
	return cached_request(
		fetch_tmdb_info_raw,
		get_cache_key('tmdb_search_v3', f"{title}_{year}_{is_tv}"),
		title,
		year,
		is_tv,
		api_key,
	)


def fetch_tmdb_episode_meta_raw(tv_id, season, episode, api_key, series_title="", api_key_bgm=""):
	if not tv_id or tv_id == "None" or not api_key.strip():
		return "", "", ""

	try:
		response = session.get(
			f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{episode}",
			params={"api_key": api_key.strip(), "language": "zh-CN"},
			timeout=15,
		)
		response.raise_for_status()
		data = response.json()

		name = data.get('name')
		plot = data.get('overview')
		still = data.get('still_path', "")

		if not name or name == f"Episode {episode}":
			response_en = session.get(
				f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{episode}",
				params={"api_key": api_key.strip(), "language": "en-US"},
				timeout=15,
			)
			response_en.raise_for_status()
			data_en = response_en.json()
			name = data_en.get('name', name)
			plot = plot or data_en.get('overview', "")

		is_placeholder_name = False
		if name:
			name_s = str(name).strip()
			is_placeholder_name = bool(re.fullmatch(r"(?i)episode\s*\d+", name_s))

		if (not plot or not str(plot).strip() or not name or is_placeholder_name) and series_title:
			try:
				bgm_candidates = fetch_bgm_candidates(series_title, api_key_bgm)
				if bgm_candidates:
					bgm_subject_id = str(bgm_candidates[0].get('id', ''))
					if bgm_subject_id:
						bgm_ep_name, bgm_ep_plot = fetch_bgm_episode(bgm_subject_id, season, episode, api_key_bgm)
						if (not name or is_placeholder_name) and bgm_ep_name:
							name = bgm_ep_name
						if (not plot or not str(plot).strip()) and bgm_ep_plot:
							plot = bgm_ep_plot
			except Exception:
				pass

		return name or "", plot or "", still or ""
	except Exception:
		return "", "", ""


def fetch_tmdb_episode_meta(tv_id, season, episode, api_key, series_title="", api_key_bgm=""):
	key = get_cache_key('tmdb_ep', f"{tv_id}_{season}_{episode}_{series_title}")
	return cached_request(fetch_tmdb_episode_meta_raw, key, tv_id, season, episode, api_key, series_title, api_key_bgm)


def fetch_tmdb_season_poster_raw(tv_id, season, api_key):
	if not tv_id or tv_id == "None" or not api_key.strip():
		return ""

	try:
		response = session.get(
			f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}",
			params={"api_key": api_key.strip(), "language": "zh-CN"},
			timeout=15,
		)
		response.raise_for_status()
		return response.json().get('poster_path', "")
	except Exception:
		return ""


def fetch_tmdb_season_poster(tv_id, season, api_key):
	return cached_request(
		fetch_tmdb_season_poster_raw,
		get_cache_key('tmdb_season_poster', f"{tv_id}_{season}"),
		tv_id,
		season,
		api_key,
	)


def fetch_hybrid_episode_meta(title, subject_id, s, e, api_key_bgm, api_key_tmdb):
	ep_n, ep_p = fetch_bgm_episode(subject_id, s, e, api_key_bgm)
	ep_s, s_p = "", ""

	if api_key_tmdb and api_key_tmdb.strip():
		try:
			q_tmdb = re.sub(r'(?i)HD|重制版|重製版|Remaster|Season.*|第.*季', '', title).strip()
			response = session.get(
				"https://api.themoviedb.org/3/search/tv",
				params={"api_key": api_key_tmdb.strip(), "query": q_tmdb, "language": "zh-CN"},
				timeout=10,
			)
			response.raise_for_status()
			results = response.json().get('results', [])

			if results:
				tm_id = results[0]['id']
				ep_s_res = session.get(
					f"https://api.themoviedb.org/3/tv/{tm_id}/season/{s}/episode/{e}",
					params={"api_key": api_key_tmdb.strip(), "language": "zh-CN"},
					timeout=10,
				)
				if ep_s_res.status_code == 200:
					ep_s = ep_s_res.json().get('still_path', "")

				s_p_res = session.get(
					f"https://api.themoviedb.org/3/tv/{tm_id}/season/{s}",
					params={"api_key": api_key_tmdb.strip(), "language": "zh-CN"},
					timeout=10,
				)
				if s_p_res.status_code == 200:
					s_p = s_p_res.json().get('poster_path', "")
		except Exception:
			pass

	return ep_n, ep_p, ep_s, s_p
