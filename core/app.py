import json
import logging
import os
import re
import threading
import tkinter as tk

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from guessit import guessit
from tkinter import (
    Listbox,
    Scrollbar,
    Toplevel,
    filedialog,
    messagebox,
    simpledialog,
    ttk,
)

from ai.ollama_ai import fetch_siliconflow_info
from db.tmdb_api import (
    fetch_bgm_by_id,
    fetch_bgm_candidates,
    fetch_bgm_episode,
    fetch_hybrid_episode_meta,
    fetch_tmdb_by_id,
    fetch_tmdb_candidates,
    fetch_tmdb_episode_meta,
    fetch_tmdb_season_poster,
)
from utils.helpers import (
    CONFIG_FILE,
    DEFAULT_LANG_TAGS,
    DEFAULT_MOVIE_FORMAT,
    DEFAULT_SUB_AUDIO_EXTS,
    DEFAULT_TV_FORMAT,
    DEFAULT_VIDEO_EXTS,
    USER_AGENT,
    VERSION_TAG_RE,
    build_query_titles,
    candidate_to_result,
    center_window,
    clean_search_title,
    clear_api_cache_file,
    derive_title_from_filename,
    extract_episode_number,
    extract_year_from_release,
    format_candidate_label,
    normalize_compare_text,
    safe_filename,
    safe_int,
    safe_str,
    save_image,
    session,
    write_nfo,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("media_renamer.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)


class SeasonOffsetDialog(tk.Toplevel):
    """季偏移对话框"""

    def __init__(self, parent, title_name):
        super().__init__(parent)
        self.title("高级季集映射")
        center_window(self, parent, 450, 260)
        self.result = None

        ttk.Label(
            self, text=f"已选定匹配: 【{title_name}】", font=("", 10, "bold")
        ).pack(pady=10)

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

        ttk.Label(
            self,
            text="*提示：\n1. 普通动漫直接点确定即可 (季数填1, 偏移填0)。\n2. 若选中[13]集，但在TMDB里算作第4季第1集，\n   请填 季数: 4，偏移量: -12。",
            foreground="gray",
        ).pack(pady=10)

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
        self.sf_model = tk.StringVar(
            value=self.config.get("sf_model", "deepseek-ai/DeepSeek-V3")
        )
        self.bgm_api_key = tk.StringVar(value=self.config.get("bgm_api_key", ""))
        self.tmdb_api_key = tk.StringVar(value=self.config.get("tmdb_api_key", ""))
        self.tv_format = tk.StringVar(
            value=self.config.get("tv_format", DEFAULT_TV_FORMAT)
        )
        self.movie_format = tk.StringVar(
            value=self.config.get("movie_format", DEFAULT_MOVIE_FORMAT)
        )

        # 动态读取扩展名和语言标签
        self.video_exts = tk.StringVar(
            value=self.config.get("video_exts", DEFAULT_VIDEO_EXTS)
        )
        self.sub_audio_exts = tk.StringVar(
            value=self.config.get("sub_audio_exts", DEFAULT_SUB_AUDIO_EXTS)
        )
        self.lang_tags = tk.StringVar(
            value=self.config.get("lang_tags", DEFAULT_LANG_TAGS)
        )

        # Ollama 相关配置
        self.ollama_url = tk.StringVar(
            value=self.config.get("ollama_url", "http://localhost:11434")
        )
        self.ollama_model = tk.StringVar(
            value=self.config.get("ollama_model", "qwen2.5:14b-instruct-q6_K")
        )
        self.embedding_model = tk.StringVar(
            value=self.config.get("embedding_model", "nomic-embed-text")
        )
        self.prefer_ollama = tk.BooleanVar(
            value=self.config.get("prefer_ollama", False)
        )
        self.use_embedding_rank = tk.BooleanVar(
            value=self.config.get("use_embedding_rank", True)
        )
        self.embedding_cache = {}

        self.create_widgets()
        self.apply_saved_window_geometry()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _is_geometry_in_screen(self, x, y, w, h):
        """检查窗口坐标是否仍在当前屏幕可见区域内"""
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        # 允许窗口部分在屏幕外，但至少要有一部分可见。
        return (x < sw - 80) and (y < sh - 80) and (x + w > 80) and (y + h > 80)

    def apply_saved_window_geometry(self):
        """启动时恢复上次窗口位置和大小"""
        geo = self.config.get("window_geometry", "")
        if not geo:
            return

        # 典型格式: 1300x900+200+100
        match = re.match(r"^(\d+)x(\d+)\+(-?\d+)\+(-?\d+)$", str(geo).strip())
        if not match:
            return

        w, h, x, y = map(int, match.groups())
        if w < 600 or h < 400:
            return

        if self._is_geometry_in_screen(x, y, w, h):
            self.root.geometry(f"{w}x{h}+{x}+{y}")

    def load_config(self):
        """加载配置"""
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"加载配置失败: {e}")
        return {}

    def save_config(self, show_message=True):
        """保存配置"""
        config_data = {
            "sf_api_key": self.sf_api_key.get().strip(),
            "sf_model": self.sf_model.get().strip(),
            "bgm_api_key": self.bgm_api_key.get().strip(),
            "tmdb_api_key": self.tmdb_api_key.get().strip(),
            "tv_format": self.tv_format.get(),
            "movie_format": self.movie_format.get(),
            "video_exts": self.video_exts.get(),
            "sub_audio_exts": self.sub_audio_exts.get(),
            "lang_tags": self.lang_tags.get(),
            "ollama_url": self.ollama_url.get().strip(),
            "ollama_model": self.ollama_model.get().strip(),
            "embedding_model": self.embedding_model.get().strip(),
            "prefer_ollama": self.prefer_ollama.get(),
            "use_embedding_rank": self.use_embedding_rank.get(),
            "window_geometry": self.root.winfo_geometry(),
        }

        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(config_data, f, indent=4, ensure_ascii=False)
            if show_message:
                messagebox.showinfo("成功", "所有配置与规则已保存！立即生效。")
        except Exception as e:
            if show_message:
                messagebox.showerror("错误", f"保存失败: {e}")

    def on_close(self):
        """关闭窗口时静默保存配置（含窗口位置）"""
        try:
            self.save_config(show_message=False)
        except Exception as e:
            logging.error(f"关闭时保存配置失败: {e}")
        self.root.destroy()

    def get_media_exts(self):
        """获取媒体文件扩展名"""
        v = [e.strip().lower() for e in self.video_exts.get().split(",") if e.strip()]
        s = [
            e.strip().lower() for e in self.sub_audio_exts.get().split(",") if e.strip()
        ]
        return tuple(v + s)

    def get_sub_audio_exts(self):
        """获取字幕/音频扩展名"""
        return tuple(
            [
                e.strip().lower()
                for e in self.sub_audio_exts.get().split(",")
                if e.strip()
            ]
        )

    def extract_lang_and_ext(self, filename):
        """提取语言标签和扩展名"""
        tags = self.lang_tags.get().strip()
        if not tags:
            return os.path.splitext(filename)

        # 对用户输入的标签做转义，避免非法正则或误匹配
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
        else:
            return os.path.splitext(filename)

    def _extract_explicit_season(self, pure_name):
        """仅从明确季标记中提取季号，避免把年份误判为季号。"""
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

    def _pick_season(self, pure_name, guess_data=None, fallback=1):
        """优先使用显式季标记；否则只接受合理范围内的猜测季号。"""
        explicit = self._extract_explicit_season(pure_name)
        if explicit is not None:
            return explicit

        guessed = safe_int((guess_data or {}).get("season"), 0)
        if 0 < guessed <= 99:
            return guessed

        fallback_num = safe_int(fallback, 1)
        if 0 <= fallback_num <= 99:
            return fallback_num
        return 1

    def _can_reuse_dir_ai(self, cached_ai, pure_name, guess_data=None):
        """仅在当前文件与缓存标题明显属于同一作品时复用目录级识别结果。"""
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

    def _write_sidecar_files(self, item, target_path):
        """在媒体文件已位于目标位置后写入 NFO 与图片。"""
        with self.file_write_lock:
            target_dir = os.path.dirname(target_path)
            m = item.get("metadata", {})
            media_type = m.get("type", "episode")
            is_tv = media_type == "episode"
            is_sub_audio = item["old_name"].lower().endswith(self.get_sub_audio_exts())

            if is_tv:
                if not is_sub_audio:
                    ep_nfo = os.path.splitext(target_path)[0] + ".nfo"
                    if not os.path.exists(ep_nfo):
                        write_nfo(ep_nfo, m, "episodedetails")

                    thumb_source = m.get("still") or m.get("s_poster") or m.get("poster")
                    if thumb_source:
                        thumb_path = os.path.splitext(target_path)[0] + "-thumb.jpg"
                        if not os.path.exists(thumb_path):
                            save_image(thumb_path, thumb_source)

                cur_dir = target_dir
                dir_name = os.path.basename(cur_dir)
                is_season_folder = bool(re.match(r"^(Season\s*\d+|S\d+)$", dir_name, re.I))

                if is_season_folder and os.path.dirname(cur_dir):
                    root_d = os.path.dirname(cur_dir)
                else:
                    root_d = cur_dir

                s_num = m.get("s", 1)
                try:
                    s_fmt = f"{int(s_num):02d}"
                except Exception:
                    s_fmt = str(s_num)

                s_nfo_root = os.path.join(root_d, f"season{s_fmt}.nfo")
                s_poster_root = os.path.join(root_d, f"season{s_fmt}-poster.jpg")

                if not os.path.exists(s_nfo_root):
                    write_nfo(s_nfo_root, m, "season")

                if m.get("s_poster") and not os.path.exists(s_poster_root):
                    save_image(s_poster_root, m["s_poster"])

                if is_season_folder:
                    season_nfo_local = os.path.join(cur_dir, "season.nfo")
                    folder_jpg_local = os.path.join(cur_dir, "folder.jpg")

                    if not os.path.exists(season_nfo_local):
                        write_nfo(season_nfo_local, m, "season")

                    if m.get("s_poster") and not os.path.exists(folder_jpg_local):
                        save_image(folder_jpg_local, m["s_poster"])

                tvshow_nfo = os.path.join(root_d, "tvshow.nfo")
                poster_path = os.path.join(root_d, "poster.jpg")

                if not os.path.exists(tvshow_nfo):
                    write_nfo(tvshow_nfo, m, "tvshow")

                if m.get("poster") and not os.path.exists(poster_path):
                    save_image(poster_path, m["poster"])

            else:
                if not is_sub_audio:
                    movie_nfo = os.path.splitext(target_path)[0] + ".nfo"
                    if not os.path.exists(movie_nfo):
                        write_nfo(movie_nfo, m, "movie")

                poster_path = os.path.join(target_dir, "poster.jpg")
                if m.get("poster") and not os.path.exists(poster_path):
                    save_image(poster_path, m["poster"])

                fanart_path = os.path.join(target_dir, "fanart.jpg")
                if m.get("fanart") and not os.path.exists(fanart_path):
                    save_image(fanart_path, m["fanart"])

    def create_widgets(self):
        """创建UI组件"""
        # 根目录选择
        p_frame = ttk.LabelFrame(self.root, text=" 归档目标根目录 ", padding=5)
        p_frame.pack(fill=tk.X, padx=10, pady=5)
        ttk.Entry(p_frame, textvariable=self.target_root).pack(
            side=tk.LEFT, fill=tk.X, expand=True, padx=5
        )
        ttk.Button(
            p_frame,
            text="选择目录",
            command=lambda: self.target_root.set(filedialog.askdirectory()),
        ).pack(side=tk.LEFT, padx=5)

        # 顶部工具栏
        top = ttk.Frame(self.root, padding=5)
        top.pack(fill=tk.X, padx=5)
        ttk.Button(top, text="添加文件", command=self.add_files).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(top, text="添加文件夹", command=self.add_folder).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(top, text="设置 / API", command=self.open_settings).pack(
            side=tk.LEFT, padx=15
        )

        # 数据源选择
        self.source_var = tk.StringVar(value="siliconflow_tmdb")
        ttk.Radiobutton(
            top, text="AI + TMDb", variable=self.source_var, value="siliconflow_tmdb"
        ).pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(
            top,
            text="AI + BGM (推荐)",
            variable=self.source_var,
            value="siliconflow_bgm",
        ).pack(side=tk.LEFT)

        ttk.Button(top, text="清空列表(含缓存)", command=self.clear_list).pack(
            side=tk.RIGHT, padx=5
        )

        # 主表格
        mid = ttk.Frame(self.root, padding=10)
        mid.pack(fill=tk.BOTH, expand=True)
        cols = ("old", "title", "id", "new", "st")
        self.tree = ttk.Treeview(
            mid, columns=cols, show="headings", selectmode="extended"
        )

        for c, h, w in zip(
            cols,
            ["原文件名", "识别标题", "匹配 ID", "新文件名 / 归档路径", "状态"],
            [300, 200, 80, 500, 150],
        ):
            self.tree.heading(c, text=h)
            self.tree.column(
                c, width=w, anchor=tk.CENTER if c in ["id", "st"] else tk.W
            )

        vsb = ttk.Scrollbar(mid, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=vsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.pack(fill=tk.BOTH, expand=True)
        self.tree.bind("<Button-3>", self.show_context_menu)

        # 底部按钮和进度条
        bot = ttk.Frame(self.root, padding=10)
        bot.pack(fill=tk.X)

        self.btn_pre = ttk.Button(
            bot, text="1. 高速识别预览", command=self.start_preview
        )
        self.btn_pre.pack(side=tk.LEFT, padx=5)

        ttk.Button(
            bot, text="2. 原地重命名+刮削", command=lambda: self.start_run_logic(False)
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            bot, text="3. 归档移动并刮削", command=lambda: self.start_run_logic(True)
        ).pack(side=tk.LEFT, padx=5)

        self.pbar = ttk.Progressbar(bot, mode="determinate")
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
        ttk.Entry(f, textvariable=self.tmdb_api_key, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="BGM API Key:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.bgm_api_key, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="Silicon AI Key (备选):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.sf_api_key, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="SiliconFlow 模型名:").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.sf_model, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        # Ollama 配置
        ttk.Label(f, text="Ollama URL:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.ollama_url, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="Ollama 模型:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.ollama_model, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="Embedding 模型:").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.embedding_model, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        ttk.Checkbutton(
            f,
            text="优先使用本地 Ollama (失败后自动尝试 SiliconFlow)",
            variable=self.prefer_ollama,
        ).grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=5)
        row += 1

        ttk.Checkbutton(
            f,
            text="启用 Embedding 候选重排 (提升多候选识别率)",
            variable=self.use_embedding_rank,
        ).grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=5)
        row += 1

        # 格式配置
        ttk.Label(f, text="剧集 (TV) 格式:").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.tv_format, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="电影 (Movie) 格式:").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.movie_format, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        # 扩展名配置
        ttk.Label(f, text="视频扩展名 (逗号分隔):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.video_exts, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="字幕/音频扩展名 (逗号):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.sub_audio_exts, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="语言标签 (竖线|分隔):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.lang_tags, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        # 保存按钮
        ttk.Button(
            f,
            text="保存并生效 (无需重启)",
            command=lambda: [self.save_config(), win.destroy()],
        ).grid(row=row, column=1, sticky=tk.E, pady=15)

    def _parse_with_ollama(self, filename):
        """调用本地 Ollama 模型解析文件名"""
        url = self.ollama_url.get().strip()
        model = self.ollama_model.get().strip()

        if not url or not model:
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
            "options": {"temperature": 0, "top_p": 0.9, "num_predict": 200},
            "timeout": 120,  # Ollama 的超时设置
        }

        try:
            full_url = url.rstrip("/") + "/api/chat"
            r = session.post(full_url, json=payload, timeout=120)
            r.raise_for_status()
            resp = r.json()

            content = resp.get("message", {}).get("content", "").strip()
            if not content:
                return None, "Ollama 返回空内容"

            # 清理可能的 markdown 代码块
            content = re.sub(
                r"^```(?:json)?\s*|\s*```$", "", content, flags=re.IGNORECASE
            )

            try:
                data = json.loads(content)
                if not isinstance(data, dict):
                    return None, "返回内容不是 JSON 对象"
                return data, "Ollama解析成功"
            except json.JSONDecodeError:
                # 尝试从文本中提取 JSON
                json_match = re.search(r"\{.*\}", content, re.DOTALL)
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

    def _can_use_embedding_rank(self):
        """是否可用本地 embedding 做候选重排"""
        return bool(
            self.use_embedding_rank.get()
            and self.ollama_url.get().strip()
            and self.embedding_model.get().strip()
        )

    def _cosine_similarity(self, vec_a, vec_b):
        """计算余弦相似度"""
        if not vec_a or not vec_b or len(vec_a) != len(vec_b):
            return 0.0
        dot = sum(float(a) * float(b) for a, b in zip(vec_a, vec_b))
        norm_a = sum(float(a) * float(a) for a in vec_a) ** 0.5
        norm_b = sum(float(b) * float(b) for b in vec_b) ** 0.5
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    def _get_embedding(self, text):
        """调用 Ollama embeddings 接口并缓存向量"""
        if not self._can_use_embedding_rank():
            return None

        clean_text = str(text or "").strip()
        if not clean_text:
            return None

        model = self.embedding_model.get().strip()
        cache_key = f"{model}::{clean_text}"
        with self.cache_lock:
            cached = self.embedding_cache.get(cache_key)
        if cached:
            return cached

        payload = {"model": model, "prompt": clean_text}

        try:
            full_url = self.ollama_url.get().strip().rstrip("/") + "/api/embeddings"
            r = session.post(full_url, json=payload, timeout=60)
            r.raise_for_status()
            data = r.json()
            emb = data.get("embedding")
            if isinstance(emb, list) and emb:
                with self.cache_lock:
                    self.embedding_cache[cache_key] = emb
                return emb
        except Exception as e:
            logging.error(f"Embedding请求失败: {e}")
        return None

    def _build_candidate_embedding_text(self, cand):
        """构建候选项向量语义文本"""
        title = cand.get("title") or ""
        alt = cand.get("alt_title") or ""
        year = extract_year_from_release(cand.get("release")) or ""
        source = cand.get("msg") or ""
        return f"标题:{title}; 原名:{alt}; 年份:{year}; 来源:{source}"

    def _rerank_candidates_with_embedding(
        self, item, query_title, year, is_tv, source_name, candidates
    ):
        """用 embedding 对候选重排，并在高置信时自动命中"""
        if not self._can_use_embedding_rank() or not candidates:
            return candidates, None, ""

        query_text = (
            f"文件名:{item.get('old_name', '')}; "
            f"解析标题:{query_title}; "
            f"年份:{safe_str(year)}; "
            f"类型:{'剧集' if is_tv else '电影'}; "
            f"来源:{source_name}"
        )
        q_emb = self._get_embedding(query_text)
        if not q_emb:
            return candidates, None, ""

        scored = []
        for cand in candidates:
            c_emb = self._get_embedding(self._build_candidate_embedding_text(cand))
            if not c_emb:
                continue
            score = self._cosine_similarity(q_emb, c_emb)
            scored.append((score, cand))

        if len(scored) < 1:
            return candidates, None, ""

        scored.sort(key=lambda x: x[0], reverse=True)
        ranked = [c for _, c in scored] + [
            c for c in candidates if c not in [x[1] for x in scored]
        ]

        top_score = scored[0][0]
        second_score = scored[1][0] if len(scored) > 1 else -1.0
        rank_msg = f"Embedding重排 top={top_score:.3f}"

        # 高置信阈值：相似度足够高且与第二名拉开差距时直接采用
        if top_score >= 0.78 and (len(scored) == 1 or top_score - second_score >= 0.10):
            return ranked, scored[0][1], rank_msg

        return ranked, None, rank_msg

    def _pick_candidate_with_ollama(
        self, item, query_title, year, is_tv, source_name, candidates
    ):
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
文件名: {item.get("old_name", "")}
解析标题: {query_title}
年份: {safe_str(year)}
类型: {"剧集" if is_tv else "电影"}
来源: {source_name}
候选列表:
{chr(10).join(prompt_lines)}"""

        payload = {
            "model": self.ollama_model.get().strip(),
            "messages": [
                {
                    "role": "system",
                    "content": "你只输出 JSON。拿不准时 pick 必须返回 0。",
                },
                {"role": "user", "content": prompt},
            ],
            "stream": False,
            "options": {"temperature": 0.0},
            "timeout": 120,
        }

        try:
            full_url = self.ollama_url.get().strip().rstrip("/") + "/api/chat"
            r = session.post(full_url, json=payload, timeout=120)
            r.raise_for_status()
            resp = r.json()
            content = resp.get("message", {}).get("content", "").strip()
            if not content:
                return None, "本地模型返回空内容"

            content = re.sub(
                r"^```(?:json)?\s*|\s*```$", "", content, flags=re.IGNORECASE
            ).strip()

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
                for cand in candidates:
                    if str(cand.get("id")) == picked_id:
                        return cand, reason or "本地模型按 ID 选中"

            if isinstance(pick, int) and 1 <= pick <= len(candidates):
                return candidates[pick - 1], reason or "本地模型已选择候选"

            return None, reason or "本地模型无法确定"
        except requests.exceptions.Timeout:
            return None, "本地模型判定超时"
        except Exception as e:
            logging.error(f"Ollama候选判定失败: {e}")
            return None, f"本地模型判定失败: {e}"

    def _request_manual_candidate_choice(
        self, item, query_title, source_name, candidates
    ):
        """在主线程弹窗，让用户手动选择候选项"""
        result_holder = {"selected": None}
        done_event = threading.Event()

        def _schedule_dialog():
            self._show_candidate_picker_dialog(
                item, query_title, source_name, candidates, result_holder, done_event
            )

        self.root.after(
            0, lambda: self.tree.set(item["id"], "st", "多候选，等待手动选择")
        )
        with self.popup_lock:
            self.root.after(0, _schedule_dialog)
            if not done_event.wait(timeout=120):
                logging.warning("手动候选选择等待超时，已跳过该文件")
                done_event.set()
                self.root.after(
                    0,
                    lambda: self.tree.set(item["id"], "st", "手动选择超时，已跳过"),
                )
        return result_holder.get("selected")

    def _show_candidate_picker_dialog(
        self, item, query_title, source_name, candidates, result_holder, done_event
    ):
        """显示自动识别冲突的候选选择窗口"""
        prev_status = self.status.cget("text")
        self.status.config(text=f"等待手动选择: {item.get('old_name', '')}")

        select_win = Toplevel(self.root)
        select_win.title(f"手动确认 {source_name} 匹配")
        center_window(select_win, self.root, 900, 420)
        # 避免弹窗被主窗口遮挡导致后台线程持续等待。
        select_win.attributes("-topmost", True)

        label_text = f"""文件: {item.get("old_name", "")}
识别标题: {query_title}
请在下方候选中选择正确条目："""
        ttk.Label(select_win, text=label_text, justify=tk.LEFT).pack(
            anchor="w", padx=10, pady=(10, 6)
        )

        list_frame = ttk.Frame(select_win)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        lb = Listbox(list_frame, width=120, height=12)
        lb.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scroll = Scrollbar(list_frame, orient=tk.VERTICAL, command=lb.yview)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)
        lb.config(yscrollcommand=scroll.set)

        detail_var = tk.StringVar(value="")
        ttk.Label(
            select_win, textvariable=detail_var, justify=tk.LEFT, foreground="gray"
        ).pack(anchor="w", padx=10, pady=(0, 4))

        for candidate in candidates:
            lb.insert(tk.END, format_candidate_label(candidate))

        def update_detail(event=None):
            sel = lb.curselection()
            if not sel:
                return
            cand = candidates[sel[0]]
            overview = (cand.get("meta") or {}).get("overview") or "无简介"
            overview = re.sub(r"\s+", " ", overview).strip()
            if len(overview) > 140:
                overview = overview[:140] + "..."
            detail_var.set(f"简介: {overview}")

        def on_confirm(event=None):
            sel = lb.curselection()
            if not sel:
                messagebox.showinfo("提示", "请先选择一项", parent=select_win)
                return
            result_holder["selected"] = candidates[sel[0]]
            if not done_event.is_set():
                done_event.set()
            select_win.destroy()

        def on_skip():
            result_holder["selected"] = None
            if not done_event.is_set():
                done_event.set()
            select_win.destroy()

        lb.bind("<<ListboxSelect>>", update_detail)
        lb.bind("<Double-Button-1>", on_confirm)
        if candidates:
            lb.selection_set(0)
            update_detail()

        btn_frame = ttk.Frame(select_win)
        btn_frame.pack(fill=tk.X, padx=10, pady=8)
        ttk.Button(btn_frame, text="确认选择", command=on_confirm).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="跳过此文件", command=on_skip).pack(
            side=tk.LEFT, padx=8
        )

        select_win.protocol("WM_DELETE_WINDOW", on_skip)
        select_win.transient(self.root)
        select_win.grab_set()
        try:
            select_win.wait_window()
        finally:
            if not done_event.is_set():
                done_event.set()
            self.status.config(text=prev_status)

    def _select_best_db_match(
        self, item, query_title, year, is_tv, source_name, candidates
    ):
        """从候选列表中自动或手动选择最终匹配项"""
        if not candidates:
            return query_title, "None", f"{source_name}无结果", {}

        if len(candidates) == 1:
            return candidate_to_result(candidates[0], f"{source_name}命中")

        ranked_candidates, emb_pick, emb_msg = self._rerank_candidates_with_embedding(
            item, query_title, year, is_tv, source_name, candidates
        )
        if emb_pick:
            hit_msg = f"Embedding判定/{source_name}命中"
            if emb_msg:
                hit_msg += f" ({emb_msg})"
            return candidate_to_result(emb_pick, hit_msg)

        chosen, reason = self._pick_candidate_with_ollama(
            item, query_title, year, is_tv, source_name, ranked_candidates
        )
        if chosen:
            hit_msg = f"Ollama判定/{source_name}命中"
            if emb_msg:
                hit_msg += f" ({emb_msg})"
            if reason:
                hit_msg += f" ({reason})"
            return candidate_to_result(chosen, hit_msg)

        manual_choice = self._request_manual_candidate_choice(
            item, query_title, source_name, ranked_candidates
        )
        if manual_choice:
            hit_msg = f"手动选择/{source_name}命中"
            if emb_msg:
                hit_msg += f" ({emb_msg})"
            return candidate_to_result(manual_choice, hit_msg)

        return query_title, "None", "待手动确认", {}

    def _resolve_db_match(self, item, query_title, year, is_tv, mode, ai_data, g):
        """解析数据库候选，必要时调用本地模型或弹窗手动确认"""
        source_name = "TMDb" if mode == "siliconflow_tmdb" else "BGM"
        query_titles = build_query_titles(item, query_title, ai_data, g)
        merged = []
        seen_ids = set()
        used_query = query_title

        for q in query_titles:
            if mode == "siliconflow_tmdb":
                cur = fetch_tmdb_candidates(q, year, is_tv, self.tmdb_api_key.get())
            else:
                cur = fetch_bgm_candidates(q, self.bgm_api_key.get())

            if not cur:
                continue

            if used_query == query_title:
                used_query = q

            for cand in cur:
                cid = str(cand.get("id") or "")
                if not cid or cid in seen_ids:
                    continue
                seen_ids.add(cid)
                merged.append(cand)

            # 候选足够多时提前结束，避免无效请求拖慢速度
            if len(merged) >= 10:
                break

        if merged:
            t_hit, tid_hit, msg_hit, meta_hit = self._select_best_db_match(
                item, used_query, year, is_tv, source_name, merged
            )
            if tid_hit != "None" and normalize_compare_text(
                used_query
            ) != normalize_compare_text(query_title):
                msg_hit += " (备选标题)"
            return t_hit, tid_hit, msg_hit, meta_hit

        return query_title, "None", f"{source_name}无结果", {}

    def show_context_menu(self, event):
        """显示右键菜单"""
        row = self.tree.identify_row(event.y)
        if row:
            if row not in self.tree.selection():
                self.tree.selection_set(row)

            sel_count = len(self.tree.selection())
            m = tk.Menu(self.root, tearoff=0)
            m.add_command(
                label=f"手动精准匹配并锁定 (将应用到选中的 {sel_count} 个文件)",
                command=self.manual_match,
            )
            m.post(event.x_root, event.y_root)

    def manual_match(self):
        """手动匹配"""
        selected_ids = self.tree.selection()
        if not selected_ids:
            return

        first_row_id = selected_ids[0]
        first_idx = next(
            (i for i, it in enumerate(self.file_list) if it["id"] == first_row_id), None
        )

        if first_idx is None:
            return

        item = self.file_list[first_idx]
        current_display_title = self.tree.item(first_row_id, "values")[1]
        search_initial = (
            current_display_title
            if current_display_title
            else clean_search_title(item["old_name"])
        )

        user_input = simpledialog.askstring(
            "搜索锁定",
            f"您选中了 {len(selected_ids)} 个文件。\n\n输入资料库数字ID或搜索关键词进行强制匹配:",
            initialvalue=search_initial,
            parent=self.root,
        )

        if not user_input:
            return

        user_input = user_input.strip()
        mode = self.source_var.get()
        self.status.config(text="正在联网搜索，请稍候...")

        threading.Thread(
            target=self._async_manual_match_search,
            args=(selected_ids, user_input, mode),
            daemon=True,
        ).start()

    def _async_manual_match_search(self, selected_ids, user_input, mode):
        """异步搜索手动匹配"""
        results = []

        try:
            if user_input.isdigit():
                # ID搜索
                if mode == "siliconflow_bgm":
                    t, tid, msg, meta = fetch_bgm_by_id(
                        user_input, self.bgm_api_key.get()
                    )
                    if tid != "None":
                        results = [(t, tid, msg, meta)]
                else:
                    # 先尝试剧集
                    t, tid, msg, meta = fetch_tmdb_by_id(
                        user_input, True, self.tmdb_api_key.get()
                    )
                    if tid == "None":
                        # 再尝试电影
                        t, tid, msg, meta = fetch_tmdb_by_id(
                            user_input, False, self.tmdb_api_key.get()
                        )
                    if tid != "None":
                        results = [(t, tid, msg, meta)]
            else:
                # 关键词搜索
                if mode == "siliconflow_bgm":
                    q = clean_search_title(user_input)
                    headers = {"User-Agent": USER_AGENT}
                    if self.bgm_api_key.get().strip():
                        headers["Authorization"] = (
                            f"Bearer {self.bgm_api_key.get().strip()}"
                        )

                    try:
                        res = session.get(
                            f"https://api.bgm.tv/search/subject/{q}?type=2",
                            headers=headers,
                            timeout=15,
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
                    except Exception:
                        pass
                else:
                    # TMDb搜索
                    try:
                        # 剧集搜索
                        res_tv = session.get(
                            "https://api.themoviedb.org/3/search/tv",
                            params={
                                "api_key": self.tmdb_api_key.get().strip(),
                                "query": user_input,
                                "language": "zh-CN",
                            },
                            timeout=15,
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
                            results.append(
                                (
                                    it.get("name", "未知"),
                                    str(it.get("id")),
                                    "TMDb剧集",
                                    meta,
                                )
                            )

                        # 电影搜索
                        res_movie = session.get(
                            "https://api.themoviedb.org/3/search/movie",
                            params={
                                "api_key": self.tmdb_api_key.get().strip(),
                                "query": user_input,
                                "language": "zh-CN",
                            },
                            timeout=15,
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
                            results.append(
                                (
                                    it.get("title", "未知"),
                                    str(it.get("id")),
                                    "TMDb电影",
                                    meta,
                                )
                            )
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
            self._confirm_season_and_dispatch(
                selected_ids, results[0][0], results[0][1], results[0][2], results[0][3]
            )
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
                    self._confirm_season_and_dispatch(
                        selected_ids,
                        results[idx_sel][0],
                        results[idx_sel][1],
                        results[idx_sel][2],
                        results[idx_sel][3],
                    )
                    select_win.destroy()

            lb.bind("<Double-Button-1>", on_select)
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
            if it["id"] in selected_ids:
                matching_indices.append(i)
                path_key = it["path"]
                with self.cache_lock:
                    self.manual_locks[path_key] = (title, tid, msg, meta)
                    self.forced_seasons[path_key] = new_s
                    self.forced_offsets[path_key] = offset

        self.status.config(text="后台并发匹配中...")
        self.pbar["value"] = 0
        self.pbar.config(maximum=len(matching_indices))

        threading.Thread(
            target=self._async_batch_runner,
            args=(matching_indices, title, tid, msg, meta),
            daemon=True,
        ).start()

    def _async_batch_runner(self, indices, title, t_id, msg, meta):
        """异步批量处理"""
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = [
                ex.submit(self._bg_update_single_ui, idx, title, t_id, msg, meta)
                for idx in indices
            ]
            for future in as_completed(futures):
                self.root.after(0, lambda: self.pbar.step(1))

        self.root.after(0, lambda: self.status.config(text="同步完成！"))

    def _bg_update_single_ui(self, idx, title, t_id, msg, meta):
        """后台更新单个UI项"""
        item = None
        try:
            item = self.file_list[idx]
            pure, ext = self.extract_lang_and_ext(item["old_name"])
            g = guessit(pure)
            m = item.get("metadata", {})
            path_key = item["path"]

            # 获取强制设置
            forced_s = self.forced_seasons.get(path_key)
            s = (
                forced_s
                if forced_s is not None
                else self._pick_season(pure, g, m.get("s", 1))
            )

            raw_e = g.get("episode") or m.get("e", 1)
            if isinstance(raw_e, list):
                raw_e = raw_e[0]

            forced_o = self.forced_offsets.get(path_key, 0)
            e_calc = raw_e

            if forced_o != 0 and str(raw_e).isdigit():
                e_calc = max(1, int(raw_e) + forced_o)

            y = g.get("year") or m.get("year")
            media_type = m.get("type", "episode")
            is_tv = media_type == "episode"
            mode = self.source_var.get()

            ep_n, ep_p, ep_s, s_p = "", "", "", ""

            if is_tv and t_id != "None" and title:
                if mode == "siliconflow_tmdb":
                    ep_n, ep_p, ep_s = fetch_tmdb_episode_meta(
                        t_id,
                        s,
                        e_calc,
                        self.tmdb_api_key.get(),
                        title,
                        self.bgm_api_key.get(),
                    )
                    s_p = fetch_tmdb_season_poster(t_id, s, self.tmdb_api_key.get())
                else:
                    ep_n, ep_p, ep_s, s_p = fetch_hybrid_episode_meta(
                        title,
                        t_id,
                        s,
                        e_calc,
                        self.bgm_api_key.get(),
                        self.tmdb_api_key.get(),
                    )

            fallback_ep_title = g.get("episode_title") or ""
            ep_n_final = ep_n or fallback_ep_title

            # 安全转换
            s = safe_int(s, 1)
            e_calc = safe_int(e_calc, 1)
            s_fmt = f"{int(s):02d}"
            e_fmt = f"{int(e_calc):02d}"

            v_tag = self._get_version_tag(item["path"])

            # 安全处理标题
            safe_title = safe_filename(title)
            safe_ep_name = safe_filename(ep_n_final)

            if is_tv:
                new_fn = (
                    self.tv_format.get()
                    .replace("{title}", safe_title)
                    .replace("{year}", safe_str(y))
                    .replace("{s:02d}", s_fmt)
                    .replace("{s}", s_fmt)
                    .replace("{e:02d}", e_fmt)
                    .replace("{e}", e_fmt)
                    .replace("{ep_name}", safe_ep_name)
                    .replace("{ext}", v_tag + ext)
                )
            else:
                new_fn = (
                    self.movie_format.get()
                    .replace("{title}", safe_title)
                    .replace("{year}", safe_str(y))
                    .replace("{ext}", v_tag + ext)
                )

            # 清理格式
            new_fn = re.sub(r"\s*\(\s*\)", "", new_fn)
            new_fn = re.sub(r"\s*-\s*(?=\.)|\s*-\s*$", "", new_fn)
            new_fn = re.sub(r"\s+(?=\.)", "", new_fn).strip()

            item["metadata"] = {
                "id": t_id,
                "provider": "tmdb" if mode == "siliconflow_tmdb" else "bgm",
                "title": safe_title,
                "year": y,
                "ep_title": ep_n_final or f"第 {e_calc} 集",
                "overview": meta.get("overview", ""),
                "ep_plot": ep_p,
                "s": s,
                "e": e_calc,
                "poster": meta.get("poster"),
                "fanart": meta.get("fanart"),
                "still": ep_s,
                "s_poster": s_p,
                "type": media_type,
            }

            item["new_name_only"] = new_fn

            root_d = self.target_root.get().strip()
            if root_d:
                id_tag = (
                    f"tmdbid={t_id}" if mode == "siliconflow_tmdb" else f"bgmid={t_id}"
                )
                folder_name = safe_filename(f"{safe_title} [{id_tag}]")
                season_folder = f"Season {s}"

                if is_tv:
                    item["full_target"] = os.path.join(
                        root_d, folder_name, season_folder, new_fn
                    )
                else:
                    year_text = safe_str(y)
                    if year_text:
                        folder_name = safe_filename(
                            f"{safe_title} ({year_text}) [{id_tag}]"
                        )
                    else:
                        folder_name = safe_filename(f"{safe_title} [{id_tag}]")
                    item["full_target"] = os.path.join(root_d, folder_name, new_fn)
            else:
                item["full_target"] = ""

            self.root.after(
                0,
                lambda: self.tree.item(
                    item["id"],
                    values=(
                        item["old_name"],
                        safe_title,
                        t_id,
                        item["full_target"] or new_fn,
                        msg,
                    ),
                ),
            )
        except Exception as e:
            logging.error(f"更新UI失败: {e}")
            err_msg = f"更新失败: {str(e)[:30]}"
            if item and item.get("id"):
                self.root.after(
                    0,
                    lambda id_val=item["id"], msg=err_msg: self.tree.set(
                        id_val, "st", msg
                    ),
                )
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
                messagebox.showwarning(
                    "Ollama配置不完整",
                    "您选择了优先使用本地Ollama，但未填写Ollama URL或模型。请先完成配置或切换回SiliconFlow。",
                )
                return
        else:
            if not self.sf_api_key.get().strip():
                messagebox.showwarning(
                    "缺少API密钥", "请先配置SiliconFlow API Key或启用Ollama。"
                )
                return

        self.btn_pre.config(state=tk.DISABLED)
        self.pbar["value"] = 0
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

        self.root.after(
            0,
            lambda: [
                self.btn_pre.config(state=tk.NORMAL),
                self.status.config(text="预览完成"),
            ],
        )

    def process_task(self, i):
        """处理单个任务"""
        item = self.file_list[i]

        try:
            self.root.after(0, lambda id_val=item["id"]: self.tree.set(id_val, "st", "识别中"))
            pure, ext = self.extract_lang_and_ext(item["old_name"])
            dir_p = item["dir"]
            mode = self.source_var.get()
            g = guessit(pure)

            extracted_ep = extract_episode_number(pure, g)

            # 检查目录缓存
            with self.cache_lock:
                cached_ai = self.dir_cache.get(dir_p)

            if cached_ai and self._can_reuse_dir_ai(cached_ai, pure, g):
                t = cached_ai["title"]
                y = cached_ai.get("year")
                s = self._pick_season(pure, g, cached_ai.get("season") or 1)
                e = extracted_ep or 1
                ai_msg = "复用"
                ai_data = cached_ai
            else:
                ai_data = None
                ai_msg = ""

                # AI 解析
                if self.prefer_ollama.get():
                    if (
                        self.ollama_url.get().strip()
                        and self.ollama_model.get().strip()
                    ):
                        ai_data, ai_msg = self._parse_with_ollama(pure)
                        if ai_data is None and self.sf_api_key.get().strip():
                            ai_data, ai_msg = fetch_siliconflow_info(
                                pure, self.sf_api_key.get(), self.sf_model.get()
                            )
                    else:
                        if self.sf_api_key.get().strip():
                            ai_data, ai_msg = fetch_siliconflow_info(
                                pure, self.sf_api_key.get(), self.sf_model.get()
                            )
                else:
                    if self.sf_api_key.get().strip():
                        ai_data, ai_msg = fetch_siliconflow_info(
                            pure, self.sf_api_key.get(), self.sf_model.get()
                        )

                if ai_data:
                    t = ai_data.get("title", "未知")
                    y = ai_data.get("year")
                    s = self._pick_season(pure, g, ai_data.get("season", 1))
                    e = extracted_ep or safe_int(ai_data.get("episode"), 1)

                    with self.cache_lock:
                        self.dir_cache[dir_p] = ai_data
                else:
                    t = g.get("title") or derive_title_from_filename(pure) or "未知"
                    y = g.get("year")
                    s = self._pick_season(pure, g, 1)
                    e = extracted_ep or 1
                    ai_msg = "猜测"
                    # AI unavailable/failed: still reuse stable per-directory guess to reduce repeated parsing noise.
                    if t and normalize_compare_text(t) not in ("", "未知"):
                        with self.cache_lock:
                            if dir_p not in self.dir_cache:
                                self.dir_cache[dir_p] = {
                                    "title": t,
                                    "year": y,
                                    "season": s,
                                    "episode": e,
                                }

            # 拦截特别篇，强制归入第 0 季
            if re.search(r"(?i)(?:PROLOGUE|OVA|OAD|SP)", pure):
                s = 0
                sp_match = re.search(r"(?i)(?:SP|OVA|OAD)\s*0*(\d+)", pure)
                if sp_match:
                    e = int(sp_match.group(1))

            media_type = g.get("type", "episode")
            is_tv = media_type == "episode"
            path_key = item["path"]

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
                        db_c = self._resolve_db_match(
                            item, t, y, is_tv, mode, ai_data, g
                        )
                        with self.cache_lock:
                            if db_c and len(db_c) >= 2 and db_c[1] != "None":
                                self.db_cache[cache_key] = db_c
                    finally:
                        with self.cache_lock:
                            waiter = self.db_resolution_events.pop(cache_key, None)
                        if waiter:
                            waiter.set()
                else:
                    if pending_event and not pending_event.wait(timeout=180):
                        logging.warning("等待数据库候选解析超时，已跳过缓存复用")
                    with self.cache_lock:
                        db_c = self.manual_locks.get(path_key) or self.db_cache.get(
                            cache_key
                        )

            if not db_c:
                db_c = (t, "None", "待手动确认", {})

            std_t, tid, db_m, meta = db_c
            ep_n, ep_p, ep_s, s_p = "", "", "", ""

            if is_tv and tid != "None":
                if mode == "siliconflow_tmdb":
                    ep_n, ep_p, ep_s = fetch_tmdb_episode_meta(
                        tid,
                        s,
                        e_calc,
                        self.tmdb_api_key.get(),
                        std_t,
                        self.bgm_api_key.get(),
                    )
                    s_p = fetch_tmdb_season_poster(tid, s, self.tmdb_api_key.get())
                else:
                    ep_n, ep_p, ep_s, s_p = fetch_hybrid_episode_meta(
                        std_t,
                        tid,
                        s,
                        e_calc,
                        self.bgm_api_key.get(),
                        self.tmdb_api_key.get(),
                        y,
                    )

            fallback_ep_title = g.get("episode_title") or ""
            ep_n_final = ep_n or fallback_ep_title

            # 安全处理
            s = safe_int(s, 1)
            e_calc = safe_int(e_calc, 1)
            s_fmt = f"{int(s):02d}"
            e_fmt = f"{int(e_calc):02d}"

            v_tag = self._get_version_tag(item["path"])

            # 安全文件名
            safe_std_t = safe_filename(std_t)
            safe_ep_name = safe_filename(ep_n_final)

            if is_tv:
                new_fn = (
                    self.tv_format.get()
                    .replace("{title}", safe_std_t)
                    .replace("{year}", safe_str(y))
                    .replace("{s:02d}", s_fmt)
                    .replace("{s}", s_fmt)
                    .replace("{e:02d}", e_fmt)
                    .replace("{e}", e_fmt)
                    .replace("{ep_name}", safe_ep_name)
                    .replace("{ext}", v_tag + ext)
                )
            else:
                new_fn = (
                    self.movie_format.get()
                    .replace("{title}", safe_std_t)
                    .replace("{year}", safe_str(y))
                    .replace("{ext}", v_tag + ext)
                )

            # 清理格式
            new_fn = re.sub(r"\s*\(\s*\)", "", new_fn)
            new_fn = re.sub(r"\s*-\s*(?=\.)|\s*-\s*$", "", new_fn)
            new_fn = re.sub(r"\s+(?=\.)", "", new_fn).strip()

            item["metadata"] = {
                "id": tid,
                "provider": "tmdb" if mode == "siliconflow_tmdb" else "bgm",
                "title": safe_std_t,
                "year": y,
                "ep_title": ep_n_final or f"第 {e_calc} 集",
                "overview": meta.get("overview", ""),
                "ep_plot": ep_p,
                "s": s,
                "e": e_calc,
                "poster": meta.get("poster"),
                "fanart": meta.get("fanart"),
                "still": ep_s,
                "s_poster": s_p,
                "type": media_type,
            }

            item["new_name_only"] = new_fn

            root_d = self.target_root.get().strip()
            if root_d:
                id_tag = (
                    f"tmdbid={tid}" if mode == "siliconflow_tmdb" else f"bgmid={tid}"
                )
                folder_name = safe_filename(f"{safe_std_t} [{id_tag}]")
                season_folder = f"Season {s}"

                if is_tv:
                    item["full_target"] = os.path.join(
                        root_d, folder_name, season_folder, new_fn
                    )
                else:
                    year_text = safe_str(y)
                    if year_text:
                        folder_name = safe_filename(
                            f"{safe_std_t} ({year_text}) [{id_tag}]"
                        )
                    else:
                        folder_name = safe_filename(f"{safe_std_t} [{id_tag}]")
                    item["full_target"] = os.path.join(root_d, folder_name, new_fn)
            else:
                item["full_target"] = ""

            self.root.after(
                0,
                lambda: self.tree.item(
                    item["id"],
                    values=(
                        item["old_name"],
                        safe_std_t,
                        tid,
                        item["full_target"] or new_fn,
                        f"{ai_msg}/{db_m}",
                    ),
                ),
            )
        except Exception as ex:
            logging.error(f"处理文件 {item['old_name']} 时出错: {ex}")
            err_msg = f"异常: {str(ex)[:50]}"
            self.root.after(
                0,
                lambda id_val=item["id"],
                old_name=item["old_name"],
                msg=err_msg: self.tree.item(
                    id_val, values=(old_name, "错误", "None", msg, "崩溃")
                ),
            )
        finally:
            self.root.after(0, lambda: self.pbar.step(1))

    def start_run_logic(self, is_archive):
        """开始重命名逻辑"""
        if not self.file_list:
            return

        # 检查元数据
        for item in self.file_list:
            if "metadata" not in item or item["metadata"].get("id") == "None":
                messagebox.showwarning(
                    "缺少元数据", "请先执行【高速识别预览】后再进行重命名操作。"
                )
                return

        threading.Thread(
            target=self.run_execution, args=(is_archive,), daemon=True
        ).start()

    def run_execution(self, is_archive):
        """执行重命名"""
        total = len(self.file_list)
        self.root.after(
            0,
            lambda max_v=total: [
                self.status.config(text="执行中..."),
                self.pbar.config(maximum=max_v),
                self.pbar.configure(value=0),
            ],
        )

        try:
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [
                    executor.submit(self.process_one_file, item, is_archive)
                    for item in self.file_list
                ]
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
            if is_archive and item.get("full_target"):
                target = item["full_target"]
            else:
                target = os.path.join(
                    item["dir"], item.get("new_name_only", item["old_name"])
                )

            # 检查源文件是否存在
            if not os.path.exists(item["path"]):
                self.root.after(
                    0,
                    lambda id_val=item["id"]: self.tree.set(
                        id_val, "st", "源文件不存在"
                    ),
                )
                return

            # 创建目标目录
            target_dir = os.path.dirname(target)
            if target_dir:
                os.makedirs(target_dir, exist_ok=True)

            current_path = item["path"]
            same_exact_path = current_path == target
            is_case_change_only = os.path.normcase(current_path) == os.path.normcase(
                target
            )

            if (
                not same_exact_path
                and not is_case_change_only
                and os.path.exists(target)
            ):
                self.root.after(
                    0,
                    lambda id_val=item["id"]: self.tree.set(id_val, "st", "目标已存在"),
                )
                return

            if not same_exact_path:
                import shutil

                shutil.move(current_path, target)
                item["path"] = target

            self._write_sidecar_files(item, item["path"])
            self.root.after(
                0, lambda id_val=item["id"]: self.tree.set(id_val, "st", "刮削完成")
            )

        except PermissionError as e:
            logging.error(f"权限错误 {item.get('path', '')}: {e}")
            self.root.after(
                0, lambda id_val=item["id"]: self.tree.set(id_val, "st", f"权限错误")
            )
        except OSError as e:
            logging.error(f"系统错误 {item.get('path', '')}: {e}")
            err_msg = f"系统错误: {str(e)[:20]}"
            self.root.after(
                0,
                lambda id_val=item["id"], msg=err_msg: self.tree.set(id_val, "st", msg),
            )
        except Exception as e:
            logging.error(f"处理文件失败 {item.get('path', '')}: {e}")
            err_msg = f"失败: {str(e)[:20]}"
            self.root.after(
                0,
                lambda id_val=item["id"], msg=err_msg: self.tree.set(id_val, "st", msg),
            )

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
        if any(x["path"] == path for x in self.file_list):
            return

        _, ext = self.extract_lang_and_ext(os.path.basename(path))
        tid = self.tree.insert(
            "", tk.END, values=(os.path.basename(path), "", "", "", "待命")
        )

        self.file_list.append(
            {
                "id": tid,
                "path": path,
                "dir": os.path.dirname(path),
                "old_name": os.path.basename(path),
                "ext": ext,
                "metadata": {"id": "None"},
            }
        )

    def clear_list(self):
        """清空列表"""
        for i in self.tree.get_children():
            self.tree.delete(i)

        self.file_list.clear()
        with self.cache_lock:
            # Release any pending waiters first to avoid deadlocks if clear happens during preview.
            for evt in self.db_resolution_events.values():
                try:
                    evt.set()
                except Exception:
                    pass

            self.dir_cache.clear()
            self.db_cache.clear()
            self.embedding_cache.clear()
            self.manual_locks.clear()
            self.forced_seasons.clear()
            self.forced_offsets.clear()
            self.db_resolution_events.clear()

        clear_api_cache_file()

        self.status.config(text="列表与缓存已清空")


if __name__ == "__main__":
    root = tk.Tk()
    app = MediaRenamerGUI(root)
    root.mainloop()
