import os
import re
import json
import requests
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog, Toplevel, Listbox, Scrollbar
from guessit import guessit
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET
from xml.dom import minidom
import logging
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================= 配置区域 =================
USER_AGENT = "MyMediaRenamer/73.0 (Fully Customizable Edition)"
CONFIG_FILE = "renamer_config.json"
CACHE_FILE = "api_cache.json"
CACHE_EXPIRY_DAYS = 7

# 默认内置配置
DEFAULT_TV_FORMAT = "{title} - S{s:02d}E{e:02d} - {ep_name}{ext}"
DEFAULT_MOVIE_FORMAT = "{title} ({year}){ext}"
DEFAULT_VIDEO_EXTS = ".mp4,.mkv,.avi,.rmvb,.ts,.wmv,.strm"
DEFAULT_SUB_AUDIO_EXTS = ".srt,.ass,.ssa,.vtt,.sub,.idx,.sup,.mka"
DEFAULT_LANG_TAGS = "sc|tc|chs|cht|zh|zh-CN|zh-TW|jap|en|big5|gbk|utf8|default|forced|jpsc|jptc"

VERSION_TAG_RE = re.compile(r'\[(NC\.Ver|SP|OVA|Extra|Special|OAD|Creditless)\]', re.I)

# ================= 日志配置 =================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler('media_renamer.log', encoding='utf-8'), logging.StreamHandler()])
_cache_file_lock = threading.Lock()

# ================= 工具函数 =================
def create_retry_session(retries=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504]):
    """创建带重试机制的请求会话"""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# 创建全局会话
session = create_retry_session()

def safe_filename(text):
    """安全文件名处理，移除非法字符"""
    if not text:
        return ""
    # Windows和Unix/Linux中不允许的字符
    illegal_chars = r'<>:"/\\|?*' + chr(0)
    for char in illegal_chars:
        text = text.replace(char, '_')
    # 移除首尾空格和点
    text = text.strip().strip('.')
    # 限制长度
    if len(text) > 200:
        text = text[:200]
    return text

def normalize_compare_text(text):
    """规范化文本，便于候选比较"""
    if not text:
        return ""
    text = str(text).lower()
    return re.sub(r'[\W_]+', '', text, flags=re.UNICODE)

def extract_year_from_release(release):
    """从日期字符串中提取年份"""
    if not release:
        return ""
    match = re.search(r'(\d{4})', str(release))
    return match.group(1) if match else ""

def format_candidate_label(candidate):
    """格式化候选项显示文本"""
    title = candidate.get('title') or '未知'
    alt_title = candidate.get('alt_title') or ''
    if alt_title and normalize_compare_text(alt_title) == normalize_compare_text(title):
        alt_title = ''
    year = extract_year_from_release(candidate.get('release')) or '-'
    rating = candidate.get('rating')
    try:
        rating_text = f"{float(rating):.1f}" if rating not in (None, '', 0, '0') else '-'
    except Exception:
        rating_text = '-'
    parts = [title]
    if alt_title:
        parts.append(f"原名:{alt_title}")
    parts.append(f"年份:{year}")
    parts.append(f"评分:{rating_text}")
    parts.append(f"ID:{candidate.get('id', '-')}")
    source = candidate.get('msg')
    if source:
        parts.append(str(source))
    return ' | '.join(parts)

def candidate_to_result(candidate, hit_msg):
    """候选项转为统一结果元组"""
    if not candidate:
        return '', 'None', hit_msg, {}
    return (
        candidate.get('title') or '',
        str(candidate.get('id', 'None')),
        hit_msg,
        candidate.get('meta') or {}
    )

def center_window(window, parent, width, height):
    """使子窗口相对于父窗口完美居中"""
    parent.update_idletasks()
    x = parent.winfo_x() + (parent.winfo_width() // 2) - (width // 2)
    y = parent.winfo_y() + (parent.winfo_height() // 2) - (height // 2)
    x = max(0, x)  
    y = max(0, y)  
    window.geometry(f"{width}x{height}+{x}+{y}")

def clean_search_title(title):
    """增强的标题清理，移除更多干扰项"""
    if not title:
        return ""
    t = re.sub(r'\[.*?\]', '', title)                 # 移除 [发布组] 等
    t = re.sub(r'\(.*?\)', '', t)                     # 移除 (内容)
    # 移除常见编码/画质词
    t = re.sub(r'(?i)(?:10bit|FLAC|BluRay|1080p|720p|x264|x265|HEVC|Remastered|D3D-Raw|BDRip|Web-DL|NC\.Ver|完结合集|第.*?季|第.*?集)', '', t)
    t = re.sub(r'\s+', ' ', t).strip()                # 合并多余空格
    return t

def safe_str(val):
    """安全转换为字符串"""
    if val is None:
        return ""
    if isinstance(val, list):
        if val:
            return str(val[0])
        return ""
    return str(val)

def safe_int(value, default=1):
    """安全转换为整数（支持负数偏移）"""
    try:
        if isinstance(value, list):
            value = value[0] if value else default
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            value = value.strip()
            match = re.search(r'[-+]?\d+', value)
            return int(match.group()) if match else default
        return default
    except (ValueError, TypeError):
        return default

def load_cache():
    """加载缓存文件"""
    if os.path.exists(CACHE_FILE):
        try:
            # 尝试多种编码
            for encoding in ['utf-8', 'gbk', 'latin-1']:
                try:
                    with open(CACHE_FILE, 'r', encoding=encoding) as f:
                        cache = json.load(f)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                # 所有编码都失败，尝试二进制读取
                with open(CACHE_FILE, 'rb') as f:
                    content = f.read().decode('utf-8', errors='ignore')
                cache = json.loads(content)
            
            # 清理过期缓存
            now = datetime.now().timestamp()
            expired_keys = []
            for key in cache.keys():
                if cache[key].get('expiry', 0) < now:
                    expired_keys.append(key)
            
            for key in expired_keys:
                del cache[key]
                
            return cache
        except Exception as e:
            logging.error(f"加载缓存失败: {e}")
            return {}
    return {}

def save_cache(cache):
    """安全保存缓存文件"""
    temp_file = CACHE_FILE + '.tmp'
    try:
        # 使用临时文件避免写入过程中崩溃
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
        
        # 原子替换原文件（在支持原子替换的系统上）
        import shutil
        shutil.move(temp_file, CACHE_FILE)
    except Exception as e:
        logging.error(f"保存缓存失败: {e}")
        # 尝试清理临时文件
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass

def get_cache_key(api_name, query):
    """生成缓存键"""
    return f"{api_name}:{str(query)}"

def cached_request(api_func, cache_key, *args, **kwargs):
    """带缓存的请求"""
    with _cache_file_lock:
        cache = load_cache()
        if cache_key in cache:
            return cache[cache_key]['data']
    
    result = api_func(*args, **kwargs)
    
    # 检查结果有效性
    is_valid = True
    if result is None:
        is_valid = False
    elif isinstance(result, tuple):
        if len(result) >= 2 and result[1] == "None":
            is_valid = False
        elif len(result) >= 3 and not result[0] and not result[1]:
            is_valid = False

    if is_valid:
        with _cache_file_lock:
            cache = load_cache()
            cache[cache_key] = {
                'data': result, 
                'expiry': (datetime.now() + timedelta(days=CACHE_EXPIRY_DAYS)).timestamp()
            }
            save_cache(cache)
            
    return result

def fetch_siliconflow_info(filename, api_key):
    """使用SiliconFlow AI解析文件名"""
    if not api_key or not api_key.strip():
        return None, "未配置 AI Key"
    
    url = "https://api.siliconflow.cn/v1/chat/completions"
    prompt = """分析文件名提取标准元数据。要求纯JSON。规则：1. title: 标准剧名 2. year: 年份 3. season: 季数 4. episode: 集数"""
    
    headers = {
        "Authorization": f"Bearer {api_key.strip()}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "deepseek-ai/DeepSeek-V3",
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": filename}
        ],
        "temperature": 0.1,
        "max_tokens": 500
    }
    
    try:
        r = session.post(url, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        
        res_text = r.json()['choices'][0]['message']['content'].strip()
        # 清理可能的markdown代码块
        res_text = re.sub(r'^```(?:json)?\s*|\s*```$', '', res_text, flags=re.IGNORECASE)
        
        data = json.loads(res_text)
        return data, "AI解析成功"
    except requests.exceptions.Timeout:
        return None, "AI请求超时"
    except json.JSONDecodeError:
        return None, "AI返回JSON解析失败"
    except Exception as e:
        return None, f"AI失败: {str(e)}"

def fetch_bgm_by_id_raw(subject_id, api_key=""):
    """通过ID获取BGM信息"""
    headers = {'User-Agent': USER_AGENT}
    if api_key and api_key.strip():
        headers['Authorization'] = f"Bearer {api_key.strip()}"
    
    try:
        r = session.get(f"https://api.bgm.tv/v0/subjects/{subject_id}", headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        
        meta = {
            "overview": data.get('summary', ""),
            "rating": data.get('rating', {}).get('score', 0),
            "poster": data.get('images', {}).get('large', ""),
            "fanart": "",
            "release": data.get('date', "")
        }
        
        title = data.get('name_cn') or data.get('name') or str(subject_id)
        return title, str(data.get('id')), "ID强制锁定", meta
    except requests.exceptions.Timeout:
        return str(subject_id), "None", "请求超时", {}
    except Exception:
        return str(subject_id), "None", "ID无效", {}

def fetch_bgm_by_id(subject_id, api_key=""):
    """带缓存的BGM ID查询"""
    return cached_request(fetch_bgm_by_id_raw, get_cache_key('bgm_id', subject_id), subject_id, api_key)

def fetch_bgm_candidates_raw(title, api_key=""):
    """搜索 BGM 候选列表"""
    q = clean_search_title(title)
    headers = {'User-Agent': USER_AGENT}
    if api_key and api_key.strip():
        headers['Authorization'] = f"Bearer {api_key.strip()}"

    try:
        res = session.get(f"https://api.bgm.tv/search/subject/{q}?type=2", headers=headers, timeout=15)
        res.raise_for_status()
        data = res.json().get('list', [])

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
                "release": release
            }
            candidates.append({
                "title": item.get('name_cn') or item.get('name') or title,
                "alt_title": item.get('name') or '',
                "id": cid,
                "msg": "BGM候选",
                "rating": rating,
                "release": release,
                "meta": meta
            })
        return candidates
    except requests.exceptions.Timeout:
        return []
    except Exception:
        return []

def fetch_bgm_candidates(title, api_key=""):
    """带缓存的 BGM 候选搜索"""
    return cached_request(fetch_bgm_candidates_raw, get_cache_key('bgm_candidates_v1', title), title, api_key)

def fetch_bgm_info_raw(title, api_key=""):
    """搜索 BGM 信息（兼容旧调用，默认取第一个候选）"""
    candidates = fetch_bgm_candidates_raw(title, api_key)
    if candidates:
        return candidate_to_result(candidates[0], "BGM命中")
    return title, "None", "未匹配", {}

def fetch_bgm_info(title, api_key=""):
    """带缓存的 BGM 搜索"""
    return cached_request(fetch_bgm_info_raw, get_cache_key('bgm_search', title), title, api_key)

def fetch_bgm_episode_raw(subject_id, season, episode, api_key_bgm):
    """获取BGM集信息"""
    headers = {'User-Agent': USER_AGENT}
    if api_key_bgm and api_key_bgm.strip():
        headers['Authorization'] = f"Bearer {api_key_bgm.strip()}"
    
    try:
        r = session.get(f"https://api.bgm.tv/v0/episodes?subject_id={subject_id}&type=0&limit=100", headers=headers, timeout=15)
        r.raise_for_status()
        
        for ep in r.json().get('data', []):
            if ep.get('sort') == episode:
                return ep.get('name_cn') or ep.get('name') or "", ep.get('desc', "")
    except Exception:
        pass
    
    return "", ""

def fetch_bgm_episode(subject_id, season, episode, api_key_bgm):
    """带缓存的BGM集查询"""
    return cached_request(fetch_bgm_episode_raw, get_cache_key('bgm_ep', f"{subject_id}_{season}_{episode}"), subject_id, season, episode, api_key_bgm)

def fetch_tmdb_by_id_raw(tmdb_id, is_tv=True, api_key=""):
    """通过ID获取TMDb信息"""
    if not api_key or not api_key.strip():
        return str(tmdb_id), "None", "未配置TMDb Key", {}
    
    stype = "tv" if is_tv else "movie"
    
    try:
        r = session.get(f"https://api.themoviedb.org/3/{stype}/{tmdb_id}", 
                       params={"api_key": api_key.strip(), "language": "zh-CN"}, 
                       timeout=15)
        r.raise_for_status()
        data = r.json()
        
        meta = {
            "overview": data.get('overview', ""),
            "rating": data.get('vote_average', 0),
            "poster": data.get('poster_path', ""),
            "fanart": data.get('backdrop_path', ""),
            "release": data.get('first_air_date') or data.get('release_date') or ""
        }
        
        title = data.get('name') or data.get('title') or str(tmdb_id)
        return title, str(data.get('id')), "ID锁定成功", meta
    except requests.exceptions.Timeout:
        return str(tmdb_id), "None", "请求超时", {}
    except Exception:
        return str(tmdb_id), "None", "ID无效", {}

def fetch_tmdb_by_id(tmdb_id, is_tv=True, api_key=""):
    """带缓存的TMDb ID查询"""
    return cached_request(fetch_tmdb_by_id_raw, get_cache_key('tmdb_id', f"{tmdb_id}_{is_tv}"), tmdb_id, is_tv, api_key)

def fetch_tmdb_candidates_raw(title, year=None, is_tv=True, api_key=""):
    """搜索 TMDb 候选列表"""
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
                "release": release
            }
            candidates.append({
                "title": item.get('name') or item.get('title') or title,
                "alt_title": item.get('original_name') or item.get('original_title') or '',
                "id": cid,
                "msg": f"TMDb{'剧集' if is_tv else '电影'}候选",
                "rating": rating,
                "release": release,
                "meta": meta
            })
        return candidates

    def _request_once(query, year_mode=None):
        params = {"api_key": api_key.strip(), "query": query, "language": "zh-CN"}
        if year:
            if year_mode == "year":
                params["year"] = year
            elif year_mode == "first_air_date_year":
                params["first_air_date_year"] = year
        res = session.get(f"https://api.themoviedb.org/3/search/{stype}", params=params, timeout=15)
        res.raise_for_status()
        return res.json().get('results', [])

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
    except Exception as e:
        logging.error(f"TMDb搜索失败: {e}")
        return []

def fetch_tmdb_candidates(title, year=None, is_tv=True, api_key=""):
    """带缓存的 TMDb 候选搜索"""
    return cached_request(fetch_tmdb_candidates_raw, get_cache_key('tmdb_candidates_v3', f"{title}_{year}_{is_tv}"), title, year, is_tv, api_key)

def fetch_tmdb_info_raw(title, year=None, is_tv=True, api_key=""):
    """搜索 TMDb 信息（兼容旧调用，默认取第一个候选）"""
    if not api_key or not api_key.strip():
        return title, "None", "未配置TMDb Key", {}

    candidates = fetch_tmdb_candidates_raw(title, year, is_tv, api_key)
    if candidates:
        return candidate_to_result(candidates[0], "TMDb命中")
    return title, "None", "TMDb无结果", {}

def fetch_tmdb_info(title, year=None, is_tv=True, api_key=""):
    """带缓存的 TMDb 搜索"""
    return cached_request(fetch_tmdb_info_raw, get_cache_key('tmdb_search_v3', f"{title}_{year}_{is_tv}"), title, year, is_tv, api_key)

def fetch_tmdb_episode_meta_raw(tv_id, season, episode, api_key):
    """获取TMDb集元数据"""
    if not tv_id or tv_id == "None" or not api_key.strip():
        return "", "", ""
    
    try:
        # 中文元数据
        r = session.get(f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{episode}", 
                       params={"api_key": api_key.strip(), "language": "zh-CN"}, 
                       timeout=15)
        r.raise_for_status()
        data = r.json()
        
        name = data.get('name')
        plot = data.get('overview')
        still = data.get('still_path', "")
        
        # 如果中文名是默认格式，尝试英文
        if not name or name == f"Episode {episode}":
            r_en = session.get(f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{episode}", 
                             params={"api_key": api_key.strip(), "language": "en-US"}, 
                             timeout=15)
            r_en.raise_for_status()
            data_en = r_en.json()
            name = data_en.get('name', name)
            plot = plot or data_en.get('overview', "")
        
        return name or "", plot or "", still or ""
    except Exception:
        return "", "", ""

def fetch_tmdb_episode_meta(tv_id, season, episode, api_key):
    """带缓存的TMDb集查询"""
    return cached_request(fetch_tmdb_episode_meta_raw, get_cache_key('tmdb_ep', f"{tv_id}_{season}_{episode}"), tv_id, season, episode, api_key)

def fetch_tmdb_season_poster_raw(tv_id, season, api_key):
    """获取TMDb季海报"""
    if not tv_id or tv_id == "None" or not api_key.strip():
        return ""
    
    try:
        r = session.get(f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}", 
                       params={"api_key": api_key.strip(), "language": "zh-CN"}, 
                       timeout=15)
        r.raise_for_status()
        return r.json().get('poster_path', "")
    except Exception:
        return ""

def fetch_tmdb_season_poster(tv_id, season, api_key):
    """带缓存的TMDb季海报查询"""
    return cached_request(fetch_tmdb_season_poster_raw, get_cache_key('tmdb_season_poster', f"{tv_id}_{season}"), tv_id, season, api_key)

def fetch_hybrid_episode_meta(title, subject_id, s, e, api_key_bgm, api_key_tmdb):
    """混合获取集元数据（BGM + TMDb）"""
    ep_n, ep_p = fetch_bgm_episode(subject_id, s, e, api_key_bgm)
    ep_s, s_p = "", ""
    
    if api_key_tmdb and api_key_tmdb.strip():
        try:
            q_tmdb = re.sub(r'(?i)HD|重制版|重製版|Remaster|Season.*|第.*季', '', title).strip()
            res = session.get("https://api.themoviedb.org/3/search/tv", 
                            params={"api_key": api_key_tmdb.strip(), "query": q_tmdb, "language": "zh-CN"}, 
                            timeout=10)
            res.raise_for_status()
            results = res.json().get('results', [])
            
            if results:
                tm_id = results[0]['id']
                # 获取剧照
                ep_s_res = session.get(f"https://api.themoviedb.org/3/tv/{tm_id}/season/{s}/episode/{e}", 
                                     params={"api_key": api_key_tmdb.strip(), "language": "zh-CN"}, 
                                     timeout=10)
                if ep_s_res.status_code == 200:
                    ep_s = ep_s_res.json().get('still_path', "")
                
                # 获取季海报
                s_p_res = session.get(f"https://api.themoviedb.org/3/tv/{tm_id}/season/{s}", 
                                    params={"api_key": api_key_tmdb.strip(), "language": "zh-CN"}, 
                                    timeout=10)
                if s_p_res.status_code == 200:
                    s_p = s_p_res.json().get('poster_path', "")
        except Exception:
            pass
    
    return ep_n, ep_p, ep_s, s_p

def save_image(path, url_part):
    """保存图片"""
    if not url_part:
        return
    
    try:
        url = url_part if url_part.startswith("http") else f"https://image.tmdb.org/t/p/original{url_part}"
        
        # 检查文件是否已存在
        if os.path.exists(path):
            return
            
        r = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        if r.status_code == 200:
            with open(path, 'wb') as f:
                f.write(r.content)
    except Exception as e:
        logging.error(f"保存图片失败 {path}: {e}")

def write_nfo(path, data, nfo_type="movie"):
    """写入NFO文件"""
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
            ET.SubElement(root, "plot").text = str(data.get("overview", ""))
            ET.SubElement(root, "year").text = str(data.get("year") or "")
        
        ET.SubElement(root, "lockdata").text = "false"
        ET.SubElement(root, "uniqueid", type="tmdb").text = str(data.get("id", ""))
        
        # 写入文件
        xml_str = ET.tostring(root, encoding='utf-8')
        dom = minidom.parseString(xml_str)
        pretty_xml = dom.toprettyxml(indent="  ")
        
        # 移除多余的空白行
        pretty_xml = '\n'.join([line for line in pretty_xml.split('\n') if line.strip()])
        
        with open(path, "w", encoding="utf-8") as f:
            f.write(pretty_xml)
            
    except Exception as e:
        logging.error(f"写入NFO失败 {path}: {e}")

class SeasonOffsetDialog(tk.Toplevel):
    """季偏移对话框"""
    def __init__(self, parent, title_name):
        super().__init__(parent)
        self.title("高级季集映射")
        center_window(self, parent, 450, 260)
        self.result = None
        
        ttk.Label(self, text=f"已选定匹配: 【{title_name}】", font=("", 10, "bold")).pack(pady=10)
        
        f1 = ttk.Frame(self)
        f1.pack(pady=5)
        ttk.Label(f1, text="强制指定为第几季:").pack(side=tk.LEFT)
        self.s_var = tk.StringVar(value="1")
        ttk.Entry(f1, textvariable=self.s_var, width=10).pack(side=tk.LEFT, padx=5)
        
        f2 = ttk.Frame(self)
        f2.pack(pady=5)
        ttk.Label(f2, text="集数增减偏移 (可选):").pack(side=tk.LEFT)
        self.o_var = tk.StringVar(value="0")
        ttk.Entry(f2, textvariable=self.o_var, width=10).pack(side=tk.LEFT, padx=5)
        
        ttk.Label(self, text="*提示：\n1. 普通动漫直接点确定即可 (季数填1, 偏移填0)。\n2. 若选中[13]集，但在TMDB里算作第4季第1集，\n   请填 季数: 4，偏移量: -12。", foreground="gray").pack(pady=10)
        
        ttk.Button(self, text="确定应用", command=self.on_ok).pack()
        
        self.transient(parent)
        self.grab_set()
        self.wait_window(self)
    
    def on_ok(self):
        try:
            self.result = (safe_int(self.s_var.get(), 1), safe_int(self.o_var.get(), 0))
            self.destroy()
        except:
            messagebox.showerror("错误", "请输入有效的整数！")

class MediaRenamerGUI:
    """主GUI类"""
    def __init__(self, root):
        self.root = root
        self.root.title("媒体归档刮削助手 v73.0 (全自定义免打包版)")
        self.root.geometry("1300x900")
        
        self.file_list = []
        self.dir_cache = {}
        self.db_cache = {}
        self.manual_locks = {}
        self.forced_seasons = {}
        self.forced_offsets = {}
        self.db_resolution_events = {}
        self.cache_lock = threading.Lock()
        self.file_write_lock = threading.Lock()
        self.popup_lock = threading.Lock()
        
        self.config = self.load_config()
        self.target_root = tk.StringVar(value="")
        self.sf_api_key = tk.StringVar(value=self.config.get("sf_api_key", ""))
        self.bgm_api_key = tk.StringVar(value=self.config.get("bgm_api_key", ""))
        self.tmdb_api_key = tk.StringVar(value=self.config.get("tmdb_api_key", ""))
        self.tv_format = tk.StringVar(value=self.config.get("tv_format", DEFAULT_TV_FORMAT))
        self.movie_format = tk.StringVar(value=self.config.get("movie_format", DEFAULT_MOVIE_FORMAT))
        
        # 动态读取扩展名和语言标签
        self.video_exts = tk.StringVar(value=self.config.get("video_exts", DEFAULT_VIDEO_EXTS))
        self.sub_audio_exts = tk.StringVar(value=self.config.get("sub_audio_exts", DEFAULT_SUB_AUDIO_EXTS))
        self.lang_tags = tk.StringVar(value=self.config.get("lang_tags", DEFAULT_LANG_TAGS))

        # Ollama 相关配置
        self.ollama_url = tk.StringVar(value=self.config.get("ollama_url", "http://localhost:11434"))
        self.ollama_model = tk.StringVar(value=self.config.get("ollama_model", "qwen2.5:14b-instruct-q6_K"))
        self.prefer_ollama = tk.BooleanVar(value=self.config.get("prefer_ollama", False))

        self.create_widgets()

    def load_config(self):
        """加载配置"""
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"加载配置失败: {e}")
        return {}

    def save_config(self):
        """保存配置"""
        config_data = {
            "sf_api_key": self.sf_api_key.get().strip(),
            "bgm_api_key": self.bgm_api_key.get().strip(),
            "tmdb_api_key": self.tmdb_api_key.get().strip(),
            "tv_format": self.tv_format.get(),
            "movie_format": self.movie_format.get(),
            "video_exts": self.video_exts.get(),
            "sub_audio_exts": self.sub_audio_exts.get(),
            "lang_tags": self.lang_tags.get(),
            "ollama_url": self.ollama_url.get().strip(),
            "ollama_model": self.ollama_model.get().strip(),
            "prefer_ollama": self.prefer_ollama.get()
        }
        
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=4, ensure_ascii=False)
            messagebox.showinfo("成功", "所有配置与规则已保存！立即生效。")
        except Exception as e:
            messagebox.showerror("错误", f"保存失败: {e}")

    def get_media_exts(self):
        """获取媒体文件扩展名"""
        v = [e.strip().lower() for e in self.video_exts.get().split(',') if e.strip()]
        s = [e.strip().lower() for e in self.sub_audio_exts.get().split(',') if e.strip()]
        return tuple(v + s)

    def get_sub_audio_exts(self):
        """获取字幕/音频扩展名"""
        return tuple([e.strip().lower() for e in self.sub_audio_exts.get().split(',') if e.strip()])

    def extract_lang_and_ext(self, filename):
        """提取语言标签和扩展名"""
        tags = self.lang_tags.get().strip()
        if not tags:
            return os.path.splitext(filename)

        # 对用户输入的标签做转义，避免非法正则或误匹配
        tag_items = [t.strip() for t in tags.split('|') if t.strip()]
        if not tag_items:
            return os.path.splitext(filename)

        safe_tags = '|'.join(re.escape(t) for t in tag_items)
        pattern = rf'(\.(?:{safe_tags}))?(\.[a-z0-9]+)$'
        try:
            regex = re.compile(pattern, re.I)
        except re.error:
            return os.path.splitext(filename)
        match = regex.search(filename)
        
        if match and match.group(1):
            return filename[:match.start()], match.group(1) + match.group(2)
        else:
            return os.path.splitext(filename)

    def create_widgets(self):
        """创建UI组件"""
        # 根目录选择
        p_frame = ttk.LabelFrame(self.root, text=" 归档目标根目录 ", padding=5)
        p_frame.pack(fill=tk.X, padx=10, pady=5)
        ttk.Entry(p_frame, textvariable=self.target_root).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(p_frame, text="选择目录", command=lambda: self.target_root.set(filedialog.askdirectory())).pack(side=tk.LEFT, padx=5)
        
        # 顶部工具栏
        top = ttk.Frame(self.root, padding=5)
        top.pack(fill=tk.X, padx=5)
        ttk.Button(top, text="添加文件", command=self.add_files).pack(side=tk.LEFT, padx=5)
        ttk.Button(top, text="添加文件夹", command=self.add_folder).pack(side=tk.LEFT, padx=5)
        ttk.Button(top, text="设置 / API", command=self.open_settings).pack(side=tk.LEFT, padx=15)
        
        # 数据源选择
        self.source_var = tk.StringVar(value="siliconflow_tmdb")
        ttk.Radiobutton(top, text="AI + TMDb", variable=self.source_var, value="siliconflow_tmdb").pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(top, text="AI + BGM (推荐)", variable=self.source_var, value="siliconflow_bgm").pack(side=tk.LEFT)
        
        ttk.Button(top, text="清空列表(含缓存)", command=self.clear_list).pack(side=tk.RIGHT, padx=5)
        
        # 主表格
        mid = ttk.Frame(self.root, padding=10)
        mid.pack(fill=tk.BOTH, expand=True)
        cols = ("old", "title", "id", "new", "st")
        self.tree = ttk.Treeview(mid, columns=cols, show="headings", selectmode="extended")
        
        for c, h, w in zip(cols, ["原文件名", "识别标题", "匹配 ID", "新文件名 / 归档路径", "状态"], [300, 200, 80, 500, 150]):
            self.tree.heading(c, text=h)
            self.tree.column(c, width=w, anchor=tk.CENTER if c in ['id', 'st'] else tk.W)
        
        vsb = ttk.Scrollbar(mid, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=vsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.pack(fill=tk.BOTH, expand=True)
        self.tree.bind("<Button-3>", self.show_context_menu)
        
        # 底部按钮和进度条
        bot = ttk.Frame(self.root, padding=10)
        bot.pack(fill=tk.X)
        
        self.btn_pre = ttk.Button(bot, text="1. 高速识别预览", command=self.start_preview)
        self.btn_pre.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(bot, text="2. 原地重命名+刮削", command=lambda: self.start_run_logic(False)).pack(side=tk.LEFT, padx=5)
        ttk.Button(bot, text="3. 归档移动并刮削", command=lambda: self.start_run_logic(True)).pack(side=tk.LEFT, padx=5)
        
        self.pbar = ttk.Progressbar(bot, mode='determinate')
        self.pbar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=15)
        
        self.status = ttk.Label(bot, text="就绪")
        self.status.pack(side=tk.RIGHT)

    def open_settings(self):
        """打开设置窗口"""
        win = tk.Toplevel(self.root)
        win.title("高级设置与 API 配置")
        center_window(win, self.root, 650, 550)
        
        f = ttk.Frame(win, padding=20)
        f.pack(fill=tk.BOTH, expand=True)
        
        row = 0
        
        # API 配置
        ttk.Label(f, text="TMDb API Key:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.tmdb_api_key, width=45).grid(row=row, column=1, pady=5, padx=10)
        row += 1
        
        ttk.Label(f, text="BGM API Key:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.bgm_api_key, width=45).grid(row=row, column=1, pady=5, padx=10)
        row += 1
        
        ttk.Label(f, text="Silicon AI Key (备选):").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.sf_api_key, width=45).grid(row=row, column=1, pady=5, padx=10)
        row += 1
        
        # Ollama 配置
        ttk.Label(f, text="Ollama URL:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.ollama_url, width=45).grid(row=row, column=1, pady=5, padx=10)
        row += 1
        
        ttk.Label(f, text="Ollama 模型:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.ollama_model, width=45).grid(row=row, column=1, pady=5, padx=10)
        row += 1
        
        ttk.Checkbutton(f, text="优先使用本地 Ollama (失败后自动尝试 SiliconFlow)", variable=self.prefer_ollama).grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=5)
        row += 1
        
        # 格式配置
        ttk.Label(f, text="剧集 (TV) 格式:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.tv_format, width=45).grid(row=row, column=1, pady=5, padx=10)
        row += 1
        
        ttk.Label(f, text="电影 (Movie) 格式:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.movie_format, width=45).grid(row=row, column=1, pady=5, padx=10)
        row += 1
        
        # 扩展名配置
        ttk.Label(f, text="视频扩展名 (逗号分隔):").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.video_exts, width=45).grid(row=row, column=1, pady=5, padx=10)
        row += 1
        
        ttk.Label(f, text="字幕/音频扩展名 (逗号):").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.sub_audio_exts, width=45).grid(row=row, column=1, pady=5, padx=10)
        row += 1
        
        ttk.Label(f, text="语言标签 (竖线|分隔):").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.lang_tags, width=45).grid(row=row, column=1, pady=5, padx=10)
        row += 1
        
        # 保存按钮
        ttk.Button(f, text="保存并生效 (无需重启)", command=lambda: [self.save_config(), win.destroy()]).grid(row=row, column=1, sticky=tk.E, pady=15)

    def _parse_with_ollama(self, filename):
        """调用本地 Ollama 模型解析文件名"""
        url = self.ollama_url.get().strip()
        model = self.ollama_model.get().strip()
        
        if not url or not model:
            return None, "Ollama URL 或模型未配置"
        
        prompt = """分析文件名提取标准元数据。要求纯JSON。规则：1. title: 标准剧名 2. year: 年份 3. season: 季数 4. episode: 集数"""
        
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": filename}
            ],
            "stream": False,
            "options": {"temperature": 0.1},
            "timeout": 120  # Ollama 的超时设置
        }
        
        try:
            full_url = url.rstrip('/') + "/api/chat"
            r = session.post(full_url, json=payload, timeout=120)
            r.raise_for_status()
            resp = r.json()
            
            content = resp.get("message", {}).get("content", "").strip()
            if not content:
                return None, "Ollama 返回空内容"
            
            # 清理可能的 markdown 代码块
            content = re.sub(r'^```(?:json)?\s*|\s*```$', '', content, flags=re.IGNORECASE)
            
            try:
                data = json.loads(content)
                if not isinstance(data, dict):
                    return None, "返回内容不是 JSON 对象"
                return data, "Ollama解析成功"
            except json.JSONDecodeError:
                # 尝试从文本中提取 JSON
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    data = json.loads(json_match.group())
                    return data, "Ollama解析成功"
                return None, "无法解析返回的JSON"
                
        except requests.exceptions.Timeout:
            return None, "Ollama请求超时"
        except Exception as e:
            return None, f"Ollama失败: {str(e)}"

    def _can_use_ollama_for_pick(self):
        """是否可用本地模型做候选判定"""
        return bool(self.ollama_url.get().strip() and self.ollama_model.get().strip())

    def _pick_candidate_with_ollama(self, item, query_title, year, is_tv, source_name, candidates):
        """使用本地 Ollama 在多个候选中做判定"""
        if not self._can_use_ollama_for_pick():
            return None, "未配置本地模型"

        prompt_lines = []
        for idx, cand in enumerate(candidates, 1):
            prompt_lines.append(
                f"{idx}. 标题={cand.get('title', '')}; 原名={cand.get('alt_title', '')}; 年份={extract_year_from_release(cand.get('release')) or '-'}; ID={cand.get('id')}; 评分={cand.get('rating', 0)}"
            )

        prompt = f"""你是媒体数据库匹配助手。请根据文件名、解析出的标题和年份，从候选中选出最可能匹配的一项。
如果无法确定，必须返回 pick 为 0。只允许输出 JSON，不要输出额外说明。
JSON 格式: {{"pick": 0或候选序号, "reason": "简短原因"}}
文件名: {item.get('old_name', '')}
解析标题: {query_title}
年份: {safe_str(year)}
类型: {'剧集' if is_tv else '电影'}
来源: {source_name}
候选列表:
{chr(10).join(prompt_lines)}"""

        payload = {
            "model": self.ollama_model.get().strip(),
            "messages": [
                {"role": "system", "content": "你只输出 JSON。拿不准时 pick 必须返回 0。"},
                {"role": "user", "content": prompt}
            ],
            "stream": False,
            "options": {"temperature": 0.0},
            "timeout": 120
        }

        try:
            full_url = self.ollama_url.get().strip().rstrip('/') + "/api/chat"
            r = session.post(full_url, json=payload, timeout=120)
            r.raise_for_status()
            resp = r.json()
            content = resp.get("message", {}).get("content", "").strip()
            if not content:
                return None, "本地模型返回空内容"

            content = re.sub(r'^```(?:json)?\s*|\s*```$', '', content, flags=re.IGNORECASE).strip()

            parsed = None
            try:
                parsed = json.loads(content)
            except json.JSONDecodeError:
                match = re.search(r'\{.*\}', content, re.DOTALL)
                if match:
                    parsed = json.loads(match.group())
                elif re.fullmatch(r'\d+', content):
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
                for cand in candidates:
                    if str(cand.get('id')) == picked_id:
                        return cand, reason or "本地模型按 ID 选中"

            if isinstance(pick, int) and 1 <= pick <= len(candidates):
                return candidates[pick - 1], reason or "本地模型已选择候选"

            return None, reason or "本地模型无法确定"
        except requests.exceptions.Timeout:
            return None, "本地模型判定超时"
        except Exception as e:
            logging.error(f"Ollama候选判定失败: {e}")
            return None, f"本地模型判定失败: {e}"

    def _request_manual_candidate_choice(self, item, query_title, source_name, candidates):
        """在主线程弹窗，让用户手动选择候选项"""
        result_holder = {"selected": None}
        done_event = threading.Event()

        def _schedule_dialog():
            self._show_candidate_picker_dialog(item, query_title, source_name, candidates, result_holder, done_event)

        self.root.after(0, lambda: self.tree.set(item['id'], 'st', '多候选，等待手动选择'))
        with self.popup_lock:
            self.root.after(0, _schedule_dialog)
            done_event.wait()
        return result_holder.get('selected')

    def _show_candidate_picker_dialog(self, item, query_title, source_name, candidates, result_holder, done_event):
        """显示自动识别冲突的候选选择窗口"""
        prev_status = self.status.cget('text')
        self.status.config(text=f"等待手动选择: {item.get('old_name', '')}")

        select_win = Toplevel(self.root)
        select_win.title(f"手动确认 {source_name} 匹配")
        center_window(select_win, self.root, 900, 420)

        label_text = f"""文件: {item.get('old_name', '')}
识别标题: {query_title}
请在下方候选中选择正确条目："""
        ttk.Label(select_win, text=label_text, justify=tk.LEFT).pack(anchor='w', padx=10, pady=(10, 6))

        list_frame = ttk.Frame(select_win)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        lb = Listbox(list_frame, width=120, height=12)
        lb.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scroll = Scrollbar(list_frame, orient=tk.VERTICAL, command=lb.yview)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)
        lb.config(yscrollcommand=scroll.set)

        detail_var = tk.StringVar(value='')
        ttk.Label(select_win, textvariable=detail_var, justify=tk.LEFT, foreground='gray').pack(anchor='w', padx=10, pady=(0, 4))

        for candidate in candidates:
            lb.insert(tk.END, format_candidate_label(candidate))

        def update_detail(event=None):
            sel = lb.curselection()
            if not sel:
                return
            cand = candidates[sel[0]]
            overview = (cand.get('meta') or {}).get('overview') or '无简介'
            overview = re.sub(r'\s+', ' ', overview).strip()
            if len(overview) > 140:
                overview = overview[:140] + '...'
            detail_var.set(f"简介: {overview}")

        def on_confirm(event=None):
            sel = lb.curselection()
            if not sel:
                messagebox.showinfo("提示", "请先选择一项", parent=select_win)
                return
            result_holder['selected'] = candidates[sel[0]]
            if not done_event.is_set():
                done_event.set()
            select_win.destroy()

        def on_skip():
            result_holder['selected'] = None
            if not done_event.is_set():
                done_event.set()
            select_win.destroy()

        lb.bind('<<ListboxSelect>>', update_detail)
        lb.bind('<Double-Button-1>', on_confirm)
        if candidates:
            lb.selection_set(0)
            update_detail()

        btn_frame = ttk.Frame(select_win)
        btn_frame.pack(fill=tk.X, padx=10, pady=8)
        ttk.Button(btn_frame, text="确认选择", command=on_confirm).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="跳过此文件", command=on_skip).pack(side=tk.LEFT, padx=8)

        select_win.protocol("WM_DELETE_WINDOW", on_skip)
        select_win.transient(self.root)
        select_win.grab_set()
        try:
            select_win.wait_window()
        finally:
            if not done_event.is_set():
                done_event.set()
            self.status.config(text=prev_status)

    def _select_best_db_match(self, item, query_title, year, is_tv, source_name, candidates):
        """从候选列表中自动或手动选择最终匹配项"""
        if not candidates:
            return query_title, "None", f"{source_name}无结果", {}

        if len(candidates) == 1:
            return candidate_to_result(candidates[0], f"{source_name}命中")

        chosen, reason = self._pick_candidate_with_ollama(item, query_title, year, is_tv, source_name, candidates)
        if chosen:
            hit_msg = f"Ollama判定/{source_name}命中"
            if reason:
                hit_msg += f" ({reason})"
            return candidate_to_result(chosen, hit_msg)

        manual_choice = self._request_manual_candidate_choice(item, query_title, source_name, candidates)
        if manual_choice:
            return candidate_to_result(manual_choice, f"手动选择/{source_name}命中")

        return query_title, "None", "待手动确认", {}

    def _resolve_db_match(self, item, query_title, year, is_tv, mode, ai_data, g):
        """解析数据库候选，必要时调用本地模型或弹窗手动确认"""
        source_name = "TMDb" if mode == "siliconflow_tmdb" else "BGM"
        if mode == "siliconflow_tmdb":
            candidates = fetch_tmdb_candidates(query_title, year, is_tv, self.tmdb_api_key.get())
        else:
            candidates = fetch_bgm_candidates(query_title, self.bgm_api_key.get())

        if candidates:
            return self._select_best_db_match(item, query_title, year, is_tv, source_name, candidates)

        if ai_data is not None:
            guess_title = g.get('title')
            if guess_title and guess_title != query_title:
                if mode == "siliconflow_tmdb":
                    alt_candidates = fetch_tmdb_candidates(guess_title, year, is_tv, self.tmdb_api_key.get())
                else:
                    alt_candidates = fetch_bgm_candidates(guess_title, self.bgm_api_key.get())

                if alt_candidates:
                    title_hit, tid_hit, msg_hit, meta_hit = self._select_best_db_match(item, guess_title, year, is_tv, source_name, alt_candidates)
                    if tid_hit != "None":
                        msg_hit += " (备选标题)"
                    return title_hit, tid_hit, msg_hit, meta_hit

        return query_title, "None", f"{source_name}无结果", {}

    def show_context_menu(self, event):
        """显示右键菜单"""
        row = self.tree.identify_row(event.y)
        if row:
            if row not in self.tree.selection():
                self.tree.selection_set(row)
            
            sel_count = len(self.tree.selection())
            m = tk.Menu(self.root, tearoff=0)
            m.add_command(label=f"手动精准匹配并锁定 (将应用到选中的 {sel_count} 个文件)", command=self.manual_match)
            m.post(event.x_root, event.y_root)

    def manual_match(self):
        """手动匹配"""
        selected_ids = self.tree.selection()
        if not selected_ids:
            return
        
        first_row_id = selected_ids[0]
        first_idx = next((i for i, it in enumerate(self.file_list) if it["id"] == first_row_id), None)
        
        if first_idx is None:
            return
            
        item = self.file_list[first_idx]
        current_display_title = self.tree.item(first_row_id, "values")[1]
        search_initial = current_display_title if current_display_title else clean_search_title(item['old_name'])
        
        user_input = simpledialog.askstring("搜索锁定", 
                                           f"您选中了 {len(selected_ids)} 个文件。\n\n输入资料库数字ID或搜索关键词进行强制匹配:", 
                                           initialvalue=search_initial, 
                                           parent=self.root)
        
        if not user_input:
            return
            
        user_input = user_input.strip()
        mode = self.source_var.get()
        self.status.config(text="正在联网搜索，请稍候...")
        
        threading.Thread(target=self._async_manual_match_search, args=(selected_ids, user_input, mode), daemon=True).start()

    def _async_manual_match_search(self, selected_ids, user_input, mode):
        """异步搜索手动匹配"""
        results = []
        
        try:
            if user_input.isdigit():
                # ID搜索
                if mode == "siliconflow_bgm":
                    t, tid, msg, meta = fetch_bgm_by_id(user_input, self.bgm_api_key.get())
                    if tid != "None":
                        results = [(t, tid, msg, meta)]
                else:
                    # 先尝试剧集
                    t, tid, msg, meta = fetch_tmdb_by_id(user_input, True, self.tmdb_api_key.get())
                    if tid == "None":
                        # 再尝试电影
                        t, tid, msg, meta = fetch_tmdb_by_id(user_input, False, self.tmdb_api_key.get())
                    if tid != "None":
                        results = [(t, tid, msg, meta)]
            else:
                # 关键词搜索
                if mode == "siliconflow_bgm":
                    q = clean_search_title(user_input)
                    headers = {'User-Agent': USER_AGENT}
                    if self.bgm_api_key.get().strip():
                        headers['Authorization'] = f"Bearer {self.bgm_api_key.get().strip()}"
                    
                    try:
                        res = session.get(f"https://api.bgm.tv/search/subject/{q}?type=2", headers=headers, timeout=15)
                        res.raise_for_status()
                        items = res.json().get('list', [])
                        
                        for it in items[:5]:
                            title = it.get('name_cn') or it.get('name') or "未知"
                            meta = {
                                "overview": it.get('summary', ""),
                                "rating": it.get('score', 0),
                                "poster": it.get('images', {}).get('large', ""),
                                "fanart": "",
                                "release": it.get('air_date', "")
                            }
                            results.append((title, str(it.get('id')), "搜索结果", meta))
                    except Exception:
                        pass
                else:
                    # TMDb搜索
                    try:
                        # 剧集搜索
                        res_tv = session.get("https://api.themoviedb.org/3/search/tv", 
                                           params={"api_key": self.tmdb_api_key.get().strip(), 
                                                  "query": user_input, 
                                                  "language": "zh-CN"}, 
                                           timeout=15)
                        res_tv.raise_for_status()
                        tv_results = res_tv.json().get('results', [])[:3]
                        
                        for it in tv_results:
                            meta = {
                                "overview": it.get('overview', ""),
                                "rating": it.get('vote_average', 0),
                                "poster": it.get('poster_path', ""),
                                "fanart": it.get('backdrop_path', ""),
                                "release": it.get('first_air_date', "")
                            }
                            results.append((it.get('name', '未知'), str(it.get('id')), "TMDb剧集", meta))
                        
                        # 电影搜索
                        res_movie = session.get("https://api.themoviedb.org/3/search/movie", 
                                              params={"api_key": self.tmdb_api_key.get().strip(), 
                                                     "query": user_input, 
                                                     "language": "zh-CN"}, 
                                              timeout=15)
                        res_movie.raise_for_status()
                        movie_results = res_movie.json().get('results', [])[:2]
                        
                        for it in movie_results:
                            meta = {
                                "overview": it.get('overview', ""),
                                "rating": it.get('vote_average', 0),
                                "poster": it.get('poster_path', ""),
                                "fanart": it.get('backdrop_path', ""),
                                "release": it.get('release_date', "")
                            }
                            results.append((it.get('title', '未知'), str(it.get('id')), "TMDb电影", meta))
                    except Exception:
                        pass
        except Exception as e:
            logging.error(f"手动匹配搜索失败: {e}")
        
        self.root.after(0, self._show_manual_match_results, selected_ids, results)

    def _show_manual_match_results(self, selected_ids, results):
        """显示手动匹配结果"""
        self.status.config(text="就绪")
        
        if not results:
            messagebox.showinfo("无结果", "未找到匹配的条目")
            return
        
        if len(results) == 1:
            self._confirm_season_and_dispatch(selected_ids, results[0][0], results[0][1], results[0][2], results[0][3])
        else:
            select_win = Toplevel(self.root)
            select_win.title("选择匹配项")
            center_window(select_win, self.root, 650, 350)
            
            lb = Listbox(select_win, width=80, height=10)
            lb.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)
            
            scroll = Scrollbar(select_win)
            scroll.pack(side=tk.RIGHT, fill=tk.Y)
            lb.config(yscrollcommand=scroll.set)
            scroll.config(command=lb.yview)
            
            for i, (t, tid, msg, meta) in enumerate(results):
                lb.insert(tk.END, f"{t} (ID:{tid}) - {msg}")
            
            def on_select(event=None):
                sel = lb.curselection()
                if sel:
                    idx_sel = sel[0]
                    self._confirm_season_and_dispatch(selected_ids, 
                                                     results[idx_sel][0], 
                                                     results[idx_sel][1], 
                                                     results[idx_sel][2], 
                                                     results[idx_sel][3])
                    select_win.destroy()
            
            lb.bind('<Double-Button-1>', on_select)
            ttk.Button(select_win, text="确认选择", command=on_select).pack(pady=5)
            
            select_win.transient(self.root)
            select_win.grab_set()
            self.root.wait_window(select_win)

    def _confirm_season_and_dispatch(self, selected_ids, title, tid, msg, meta):
        """确认季偏移并分发任务"""
        dialog = SeasonOffsetDialog(self.root, title)
        if not dialog.result:
            return
            
        new_s, offset = dialog.result
        
        matching_indices = []
        for i, it in enumerate(self.file_list):
            if it['id'] in selected_ids:
                matching_indices.append(i)
                path_key = it['path']
                with self.cache_lock:
                    self.manual_locks[path_key] = (title, tid, msg, meta)
                    self.forced_seasons[path_key] = new_s
                    self.forced_offsets[path_key] = offset
        
        self.status.config(text="后台并发匹配中...")
        self.pbar['value'] = 0
        self.pbar.config(maximum=len(matching_indices))
        
        threading.Thread(target=self._async_batch_runner, args=(matching_indices, title, tid, msg, meta), daemon=True).start()

    def _async_batch_runner(self, indices, title, t_id, msg, meta):
        """异步批量处理"""
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = [ex.submit(self._bg_update_single_ui, idx, title, t_id, msg, meta) for idx in indices]
            for future in as_completed(futures):
                self.root.after(0, lambda: self.pbar.step(1))
        
        self.root.after(0, lambda: self.status.config(text="同步完成！"))

    def _bg_update_single_ui(self, idx, title, t_id, msg, meta):
        """后台更新单个UI项"""
        item = None
        try:
            item = self.file_list[idx]
            pure, ext = self.extract_lang_and_ext(item['old_name'])
            g = guessit(pure)
            m = item.get('metadata', {})
            path_key = item['path']
            
            # 获取强制设置
            forced_s = self.forced_seasons.get(path_key)
            s = forced_s if forced_s is not None else (g.get('season') or m.get('s', 1))
            
            raw_e = g.get('episode') or m.get('e', 1)
            if isinstance(raw_e, list):
                raw_e = raw_e[0]
            
            forced_o = self.forced_offsets.get(path_key, 0)
            e_calc = raw_e
            
            if forced_o != 0 and str(raw_e).isdigit():
                e_calc = max(1, int(raw_e) + forced_o)
            
            y = g.get('year') or m.get('year')
            media_type = m.get('type', 'episode')
            is_tv = (media_type == 'episode')
            mode = self.source_var.get()
            
            ep_n, ep_p, ep_s, s_p = "", "", "", ""
            
            if is_tv and t_id != "None" and title:
                if mode == "siliconflow_tmdb":
                    ep_n, ep_p, ep_s = fetch_tmdb_episode_meta(t_id, s, e_calc, self.tmdb_api_key.get())
                    s_p = fetch_tmdb_season_poster(t_id, s, self.tmdb_api_key.get())
                else:
                    ep_n, ep_p, ep_s, s_p = fetch_hybrid_episode_meta(title, t_id, s, e_calc, self.bgm_api_key.get(), self.tmdb_api_key.get())
            
            fallback_ep_title = g.get('episode_title') or ""
            ep_n_final = ep_n or fallback_ep_title
            
            # 安全转换
            s = safe_int(s, 1)
            e_calc = safe_int(e_calc, 1)
            s_fmt = f"{int(s):02d}"
            e_fmt = f"{int(e_calc):02d}"
            
            v_tag = self._get_version_tag(item['path'])
            
            # 安全处理标题
            safe_title = safe_filename(title)
            safe_ep_name = safe_filename(ep_n_final)
            
            if is_tv:
                new_fn = (self.tv_format.get()
                         .replace("{title}", safe_title)
                         .replace("{year}", safe_str(y))
                         .replace("{s:02d}", s_fmt)
                         .replace("{s}", s_fmt)
                         .replace("{e:02d}", e_fmt)
                         .replace("{e}", e_fmt)
                         .replace("{ep_name}", safe_ep_name)
                         .replace("{ext}", v_tag + ext))
            else:
                new_fn = (self.movie_format.get()
                         .replace("{title}", safe_title)
                         .replace("{year}", safe_str(y))
                         .replace("{ext}", v_tag + ext))
            
            # 清理格式
            new_fn = re.sub(r'\s*\(\s*\)', '', new_fn)
            new_fn = re.sub(r'\s*-\s*(?=\.)|\s*-\s*$', '', new_fn)
            new_fn = re.sub(r'\s+(?=\.)', '', new_fn).strip()
            
            item['metadata'] = {
                "id": t_id,
                "title": safe_title,
                "year": y,
                "ep_title": ep_n_final or f"第 {e_calc} 集",
                "overview": meta.get('overview', ""),
                "ep_plot": ep_p,
                "s": s,
                "e": e_calc,
                "poster": meta.get('poster'),
                "fanart": meta.get('fanart'),
                "still": ep_s,
                "s_poster": s_p,
                "type": media_type
            }
            
            item['new_name_only'] = new_fn
            
            root_d = self.target_root.get().strip()
            if root_d:
                id_tag = f"tmdbid={t_id}" if mode == "siliconflow_tmdb" else f"bgmid={t_id}"
                folder_name = safe_filename(f"{safe_title} [{id_tag}]")
                season_folder = f"Season {s}"
                
                if is_tv:
                    item['full_target'] = os.path.join(root_d, folder_name, season_folder, new_fn)
                else:
                    year_text = safe_str(y)
                    if year_text:
                        folder_name = safe_filename(f"{safe_title} ({year_text}) [{id_tag}]")
                    else:
                        folder_name = safe_filename(f"{safe_title} [{id_tag}]")
                    item['full_target'] = os.path.join(root_d, folder_name, new_fn)
            else:
                item['full_target'] = ""
            
            self.root.after(0, lambda: self.tree.item(item['id'], 
                                                    values=(item['old_name'], safe_title, t_id, item['full_target'] or new_fn, msg)))
        except Exception as e:
            logging.error(f"更新UI失败: {e}")
            err_msg = f"更新失败: {str(e)[:30]}"
            if item and item.get('id'):
                self.root.after(0, lambda id_val=item['id'], msg=err_msg: self.tree.set(id_val, "st", msg))
            else:
                self.root.after(0, lambda msg=err_msg: self.status.config(text=msg))

    def _get_version_tag(self, path):
        """获取版本标签"""
        match = VERSION_TAG_RE.search(os.path.basename(path))
        return f" {match.group(0)}" if match else ""

    def start_preview(self):
        """开始预览"""
        if not self.file_list:
            messagebox.showwarning("警告", "请先添加文件")
            return
            
        if self.prefer_ollama.get():
            if not self.ollama_url.get().strip() or not self.ollama_model.get().strip():
                messagebox.showwarning("Ollama配置不完整", 
                                      "您选择了优先使用本地Ollama，但未填写Ollama URL或模型。请先完成配置或切换回SiliconFlow。")
                return
        else:
            if not self.sf_api_key.get().strip():
                messagebox.showwarning("缺少API密钥", "请先配置SiliconFlow API Key或启用Ollama。")
                return
                
        self.btn_pre.config(state=tk.DISABLED)
        self.pbar['value'] = 0
        self.status.config(text="识别中...")
        
        threading.Thread(target=self.run_preview_pool, daemon=True).start()

    def run_preview_pool(self):
        """运行预览线程池"""
        total = len(self.file_list)
        self.root.after(0, lambda max_v=total: self.pbar.config(maximum=max_v))
        
        try:
            with ThreadPoolExecutor(max_workers=5) as ex:
                # 使用list确保所有任务完成
                list(ex.map(self.process_task, range(total)))
        except Exception as e:
            logging.error(f"预览处理失败: {e}")
            err_msg = f"处理失败: {e}"
            self.root.after(0, lambda msg=err_msg: messagebox.showerror("错误", msg))
        
        self.root.after(0, lambda: [self.btn_pre.config(state=tk.NORMAL), self.status.config(text="预览完成")])

    def process_task(self, i):
        """处理单个任务"""
        item = self.file_list[i]
        
        try:
            pure, ext = self.extract_lang_and_ext(item['old_name'])
            dir_p = item['dir']
            mode = self.source_var.get()
            g = guessit(pure)
            
            # 增强动漫集数提取
            extracted_ep = g.get('episode')
            if not extracted_ep:
                # 尝试多种格式
                ep_match = re.search(r'-\s*(\d{2,4})(?:\s+|[vV]\d|\[|\(|$|\.)', pure)
                if not ep_match:
                    ep_match = re.search(r'\[(\d{2,4})\]', pure)
                if ep_match:
                    extracted_ep = int(ep_match.group(1))
            
            # 检查目录缓存
            with self.cache_lock:
                cached_ai = self.dir_cache.get(dir_p)
            
            if cached_ai:
                t = cached_ai['title']
                y = cached_ai.get('year')
                s = g.get('season') or cached_ai.get('season') or 1
                e = extracted_ep or 1
                ai_msg = "复用"
                ai_data = cached_ai
            else:
                ai_data = None
                ai_msg = ""
                
                # AI 解析
                if self.prefer_ollama.get():
                    if self.ollama_url.get().strip() and self.ollama_model.get().strip():
                        ai_data, ai_msg = self._parse_with_ollama(pure)
                        if ai_data is None and self.sf_api_key.get().strip():
                            ai_data, ai_msg = fetch_siliconflow_info(pure, self.sf_api_key.get())
                    else:
                        if self.sf_api_key.get().strip():
                            ai_data, ai_msg = fetch_siliconflow_info(pure, self.sf_api_key.get())
                else:
                    if self.sf_api_key.get().strip():
                        ai_data, ai_msg = fetch_siliconflow_info(pure, self.sf_api_key.get())
                
                if ai_data:
                    t = ai_data.get('title', '未知')
                    y = ai_data.get('year')
                    s = g.get('season') or ai_data.get('season', 1)
                    e = extracted_ep or ai_data.get('episode', 1)
                    
                    with self.cache_lock:
                        self.dir_cache[dir_p] = ai_data
                else:
                    t = g.get('title', '未知')
                    y = g.get('year')
                    s = g.get('season', 1)
                    e = extracted_ep or 1
                    ai_msg = "猜测"
            
            # 拦截特别篇，强制归入第 0 季
            if re.search(r'(?i)(?:PROLOGUE|OVA|OAD|SP)', pure):
                s = 0
                sp_match = re.search(r'(?i)(?:SP|OVA|OAD)\s*0*(\d+)', pure)
                if sp_match:
                    e = int(sp_match.group(1))
            
            media_type = g.get('type', 'episode')
            is_tv = (media_type == 'episode')
            path_key = item['path']
            
            forced_s = self.forced_seasons.get(path_key)
            if forced_s is not None:
                s = forced_s
            
            forced_o = self.forced_offsets.get(path_key, 0)
            e_calc = e
            
            if isinstance(e, list):
                e = e[0]
                e_calc = e
            
            if forced_o != 0:
                e_calc = max(1, safe_int(e, 1) + forced_o)
            
            # 数据库查询（支持多候选时本地模型判定 / 手动弹窗）
            cache_key = f"{t}_{safe_str(y)}_{is_tv}_{mode}"

            with self.cache_lock:
                db_c = self.manual_locks.get(path_key) or self.db_cache.get(cache_key)
                pending_event = self.db_resolution_events.get(cache_key)
                is_resolver = False
                if not db_c and pending_event is None:
                    pending_event = threading.Event()
                    self.db_resolution_events[cache_key] = pending_event
                    is_resolver = True

            if not db_c:
                if is_resolver:
                    try:
                        db_c = self._resolve_db_match(item, t, y, is_tv, mode, ai_data, g)
                        with self.cache_lock:
                            self.db_cache[cache_key] = db_c
                    finally:
                        with self.cache_lock:
                            waiter = self.db_resolution_events.pop(cache_key, None)
                        if waiter:
                            waiter.set()
                else:
                    pending_event.wait()
                    with self.cache_lock:
                        db_c = self.manual_locks.get(path_key) or self.db_cache.get(cache_key)

            if not db_c:
                db_c = (t, "None", "待手动确认", {})

            std_t, tid, db_m, meta = db_c
            ep_n, ep_p, ep_s, s_p = "", "", "", ""
            
            if is_tv and tid != "None":
                if mode == "siliconflow_tmdb":
                    ep_n, ep_p, ep_s = fetch_tmdb_episode_meta(tid, s, e_calc, self.tmdb_api_key.get())
                    s_p = fetch_tmdb_season_poster(tid, s, self.tmdb_api_key.get())
                else:
                    ep_n, ep_p, ep_s, s_p = fetch_hybrid_episode_meta(std_t, tid, s, e_calc, self.bgm_api_key.get(), self.tmdb_api_key.get())
            
            fallback_ep_title = g.get('episode_title') or ""
            ep_n_final = ep_n or fallback_ep_title
            
            # 安全处理
            s = safe_int(s, 1)
            e_calc = safe_int(e_calc, 1)
            s_fmt = f"{int(s):02d}"
            e_fmt = f"{int(e_calc):02d}"
            
            v_tag = self._get_version_tag(item['path'])
            
            # 安全文件名
            safe_std_t = safe_filename(std_t)
            safe_ep_name = safe_filename(ep_n_final)
            
            if is_tv:
                new_fn = (self.tv_format.get()
                         .replace("{title}", safe_std_t)
                         .replace("{year}", safe_str(y))
                         .replace("{s:02d}", s_fmt)
                         .replace("{s}", s_fmt)
                         .replace("{e:02d}", e_fmt)
                         .replace("{e}", e_fmt)
                         .replace("{ep_name}", safe_ep_name)
                         .replace("{ext}", v_tag + ext))
            else:
                new_fn = (self.movie_format.get()
                         .replace("{title}", safe_std_t)
                         .replace("{year}", safe_str(y))
                         .replace("{ext}", v_tag + ext))
            
            # 清理格式
            new_fn = re.sub(r'\s*\(\s*\)', '', new_fn)
            new_fn = re.sub(r'\s*-\s*(?=\.)|\s*-\s*$', '', new_fn)
            new_fn = re.sub(r'\s+(?=\.)', '', new_fn).strip()
            
            item['metadata'] = {
                "id": tid,
                "title": safe_std_t,
                "year": y,
                "ep_title": ep_n_final or f"第 {e_calc} 集",
                "overview": meta.get('overview', ""),
                "ep_plot": ep_p,
                "s": s,
                "e": e_calc,
                "poster": meta.get('poster'),
                "fanart": meta.get('fanart'),
                "still": ep_s,
                "s_poster": s_p,
                "type": media_type
            }
            
            item['new_name_only'] = new_fn
            
            root_d = self.target_root.get().strip()
            if root_d:
                id_tag = f"tmdbid={tid}" if mode == "siliconflow_tmdb" else f"bgmid={tid}"
                folder_name = safe_filename(f"{safe_std_t} [{id_tag}]")
                season_folder = f"Season {s}"
                
                if is_tv:
                    item['full_target'] = os.path.join(root_d, folder_name, season_folder, new_fn)
                else:
                    year_text = safe_str(y)
                    if year_text:
                        folder_name = safe_filename(f"{safe_std_t} ({year_text}) [{id_tag}]")
                    else:
                        folder_name = safe_filename(f"{safe_std_t} [{id_tag}]")
                    item['full_target'] = os.path.join(root_d, folder_name, new_fn)
            else:
                item['full_target'] = ""
            
            self.root.after(0, lambda: self.tree.item(item['id'], 
                                                    values=(item['old_name'], safe_std_t, tid, item['full_target'] or new_fn, f"{ai_msg}/{db_m}")))
        except Exception as ex:
            logging.error(f"处理文件 {item['old_name']} 时出错: {ex}")
            err_msg = f"异常: {str(ex)[:50]}"
            self.root.after(0, lambda id_val=item['id'], old_name=item['old_name'], msg=err_msg: self.tree.item(id_val, 
                                                    values=(old_name, "错误", "None", msg, "崩溃")))
        finally:
            self.root.after(0, lambda: self.pbar.step(1))

    def start_run_logic(self, is_archive):
        """开始重命名逻辑"""
        if not self.file_list:
            return
            
        # 检查元数据
        for item in self.file_list:
            if 'metadata' not in item or item['metadata'].get('id') == "None":
                messagebox.showwarning("缺少元数据", "请先执行【高速识别预览】后再进行重命名操作。")
                return
        
        threading.Thread(target=self.run_execution, args=(is_archive,), daemon=True).start()

    def run_execution(self, is_archive):
        """执行重命名"""
        total = len(self.file_list)
        self.root.after(0, lambda max_v=total: [self.status.config(text="执行中..."), self.pbar.config(maximum=max_v), self.pbar.configure(value=0)])
        
        try:
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(self.process_one_file, item, is_archive) for item in self.file_list]
                for future in as_completed(futures):
                    self.root.after(0, lambda: self.pbar.step(1))
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"执行失败: {e}")
        except Exception as e:
            logging.error(f"执行线程池失败: {e}")
            err_msg = f"执行失败: {e}"
            self.root.after(0, lambda msg=err_msg: messagebox.showerror("错误", msg))
        
        self.root.after(0, lambda: self.status.config(text="任务全部完成"))

    def process_one_file(self, item, is_archive):
        """处理单个文件"""
        try:
            # 确定目标路径
            if is_archive and item.get('full_target'):
                target = item['full_target']
            else:
                target = os.path.join(item['dir'], item.get('new_name_only', item['old_name']))
            
            # 检查源文件是否存在
            if not os.path.exists(item['path']):
                self.root.after(0, lambda id_val=item['id']: self.tree.set(id_val, "st", "源文件不存在"))
                return
            
            # 创建目标目录
            target_dir = os.path.dirname(target)
            if target_dir:
                os.makedirs(target_dir, exist_ok=True)
            
            m = item.get('metadata', {})
            media_type = m.get('type', 'episode')
            is_tv = (media_type == 'episode')
            
            # 检查是否是字幕/音频文件
            is_sub_audio = item['old_name'].lower().endswith(self.get_sub_audio_exts())
            
            # 生成NFO文件
            if is_tv:
                if not is_sub_audio:
                    ep_nfo = os.path.splitext(target)[0] + ".nfo"
                    if not os.path.exists(ep_nfo):
                        write_nfo(ep_nfo, m, "episodedetails")
                    
                    # 保存剧照
                    if m.get('still'):
                        thumb_path = os.path.splitext(target)[0] + "-thumb.jpg"
                        if not os.path.exists(thumb_path):
                            save_image(thumb_path, m['still'])
                
                # 季相关文件
                cur_dir = target_dir
                dir_name = os.path.basename(cur_dir)
                
                # 判断是否为季文件夹
                is_season_folder = bool(re.match(r'^(Season\s*\d+|S\d+)$', dir_name, re.I))
                
                # 确定根目录
                if is_season_folder and os.path.dirname(cur_dir):
                    root_d = os.path.dirname(cur_dir)
                else:
                    root_d = cur_dir
                
                with self.file_write_lock:
                    s_num = m.get('s', 1)
                    try:
                        s_fmt = f"{int(s_num):02d}"
                    except:
                        s_fmt = str(s_num)
                    
                    # 根目录季文件
                    s_nfo_root = os.path.join(root_d, f"season{s_fmt}.nfo")
                    s_poster_root = os.path.join(root_d, f"season{s_fmt}-poster.jpg")
                    
                    if not os.path.exists(s_nfo_root):
                        write_nfo(s_nfo_root, m, "season")
                    
                    if m.get('s_poster') and not os.path.exists(s_poster_root):
                        save_image(s_poster_root, m['s_poster'])
                    
                    # 季文件夹内文件
                    if is_season_folder:
                        season_nfo_local = os.path.join(cur_dir, "season.nfo")
                        folder_jpg_local = os.path.join(cur_dir, "folder.jpg")
                        
                        if not os.path.exists(season_nfo_local):
                            write_nfo(season_nfo_local, m, "season")
                        
                        if m.get('s_poster') and not os.path.exists(folder_jpg_local):
                            save_image(folder_jpg_local, m['s_poster'])
                    
                    # 剧集级文件
                    tvshow_nfo = os.path.join(root_d, "tvshow.nfo")
                    poster_path = os.path.join(root_d, "poster.jpg")
                    
                    if not os.path.exists(tvshow_nfo):
                        write_nfo(tvshow_nfo, m, "tvshow")
                    
                    if m.get('poster') and not os.path.exists(poster_path):
                        save_image(poster_path, m['poster'])
            
            else:
                # 电影
                if not is_sub_audio:
                    movie_nfo = os.path.splitext(target)[0] + ".nfo"
                    if not os.path.exists(movie_nfo):
                        write_nfo(movie_nfo, m, "movie")
                
                # 电影海报和背景图
                poster_path = os.path.join(target_dir, "poster.jpg")
                if m.get('poster') and not os.path.exists(poster_path):
                    save_image(poster_path, m['poster'])
                
                fanart_path = os.path.join(target_dir, "fanart.jpg")
                if m.get('fanart') and not os.path.exists(fanart_path):
                    save_image(fanart_path, m['fanart'])
            
            # 重命名文件
            if item['path'] == target:
                self.root.after(0, lambda id_val=item['id']: self.tree.set(id_val, "st", "刮削完成"))
            else:
                # 检查是否只是大小写变化
                is_case_change_only = (os.path.normcase(item['path']) == os.path.normcase(target))
                
                if not is_case_change_only and os.path.exists(target):
                    self.root.after(0, lambda id_val=item['id']: self.tree.set(id_val, "st", "目标已存在"))
                else:
                    # 执行移动/重命名（兼容跨磁盘归档）
                    import shutil
                    shutil.move(item['path'], target)
                    item['path'] = target
                    self.root.after(0, lambda id_val=item['id']: self.tree.set(id_val, "st", "刮削完成"))
        
        except PermissionError as e:
            logging.error(f"权限错误 {item.get('path', '')}: {e}")
            self.root.after(0, lambda id_val=item['id']: self.tree.set(id_val, "st", f"权限错误"))
        except OSError as e:
            logging.error(f"系统错误 {item.get('path', '')}: {e}")
            err_msg = f"系统错误: {str(e)[:20]}"
            self.root.after(0, lambda id_val=item['id'], msg=err_msg: self.tree.set(id_val, "st", msg))
        except Exception as e:
            logging.error(f"处理文件失败 {item.get('path', '')}: {e}")
            err_msg = f"失败: {str(e)[:20]}"
            self.root.after(0, lambda id_val=item['id'], msg=err_msg: self.tree.set(id_val, "st", msg))

    def add_files(self):
        """添加文件"""
        files = filedialog.askopenfilenames()
        for f in files:
            self._add(f)
    
    def add_folder(self):
        """添加文件夹"""
        d = filedialog.askdirectory()
        if d:
            exts = self.get_media_exts()
            count = 0
            for root_dir, _, files in os.walk(d):
                for f in files:
                    if f.lower().endswith(exts):
                        self._add(os.path.join(root_dir, f))
                        count += 1
            
            if count > 0:
                self.status.config(text=f"已添加 {count} 个文件")
    
    def _add(self, path):
        """添加单个文件"""
        if not os.path.exists(path):
            return
            
        # 检查是否已存在
        if any(x['path'] == path for x in self.file_list):
            return
        
        _, ext = self.extract_lang_and_ext(os.path.basename(path))
        tid = self.tree.insert("", tk.END, values=(os.path.basename(path), "", "", "", "待命"))
        
        self.file_list.append({
            "id": tid,
            "path": path,
            "dir": os.path.dirname(path),
            "old_name": os.path.basename(path),
            "ext": ext,
            "metadata": {"id": "None"}
        })

    def clear_list(self):
        """清空列表"""
        for i in self.tree.get_children():
            self.tree.delete(i)
        
        self.file_list.clear()
        self.dir_cache.clear()
        self.db_cache.clear()
        self.manual_locks.clear()
        self.forced_seasons.clear()
        self.forced_offsets.clear()
        self.db_resolution_events.clear()
        
        self.status.config(text="列表已清空")

if __name__ == "__main__":
    root = tk.Tk()
    app = MediaRenamerGUI(root)
    root.mainloop()
