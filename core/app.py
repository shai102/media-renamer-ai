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
from core.services.matcher_service import (
    get_embedding,
    list_ollama_models,
    parse_with_ollama,
    pick_candidate_with_ollama,
    rerank_candidates_with_embedding,
)
from core.services.naming_service import (
    build_status_text,
    can_reuse_dir_ai,
    extract_explicit_season,
    extract_lang_and_ext,
    friendly_status_text,
    get_version_tag,
    pick_season,
)
from core.mixins.config_mixin import ConfigMixin
from core.mixins.list_mixin import ListMixin
from core.ui.manual_match import (
    async_manual_match_search as ui_async_manual_match_search,
    confirm_season_and_dispatch as ui_confirm_season_and_dispatch,
    manual_match as ui_manual_match,
    request_manual_candidate_choice as ui_request_manual_candidate_choice,
    show_candidate_picker_dialog as ui_show_candidate_picker_dialog,
    show_context_menu as ui_show_context_menu,
    show_manual_match_results as ui_show_manual_match_results,
)
from core.ui.dialogs import SeasonOffsetDialog
from core.workers.task_runner import (
    async_batch_runner as worker_async_batch_runner,
    bg_update_single_ui as worker_bg_update_single_ui,
    process_one_file as worker_process_one_file,
    process_task as worker_process_task,
    run_execution as worker_run_execution,
    run_preview_pool as worker_run_preview_pool,
)
from utils.helpers import (
    CONFIG_FILE,
    DEFAULT_LANG_TAGS,
    DEFAULT_MOVIE_FORMAT,
    DEFAULT_SUB_AUDIO_EXTS,
    DEFAULT_TV_FORMAT,
    DEFAULT_VIDEO_EXTS,
    ERROR_CODE_CONFIG,
    ERROR_CODE_HTTP,
    ERROR_CODE_INVALID,
    ERROR_CODE_NO_RESULT,
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    USER_AGENT,
    build_query_titles,
    candidate_to_result,
    center_window,
    clean_search_title,
    clear_api_cache_file,
    derive_title_from_filename,
    extract_episode_number,
    extract_year_from_release,
    format_candidate_label,
    format_error_message,
    normalize_compare_text,
    parse_error_message,
    safe_filename,
    safe_int,
    safe_str,
    save_image,
    session,
    write_nfo,
)


class MediaRenamerGUI(ConfigMixin, ListMixin):
    """主GUI类"""

    def __init__(self, root):
        self.root = root
        self.root.title("媒体归档刮削助手 v1.4")
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
        self.sf_api_url = tk.StringVar(
            value=self.config.get("sf_api_url", "https://api.siliconflow.cn/v1")
        )
        self.sf_model = tk.StringVar(
            value=self.config.get("sf_model", "deepseek-ai/DeepSeek-V3")
        )
        self.ai_temperature = tk.StringVar(
            value=f"{self._clamp_temperature(self.config.get('ai_temperature'), 0.2):.2f}"
        )
        self.ai_top_p = tk.StringVar(
            value=f"{self._clamp_top_p(self.config.get('ai_top_p'), 0.9):.2f}"
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
            value=self.config.get("ollama_model", "")
        )
        self.embedding_model = tk.StringVar(
            value=self.config.get("embedding_model", "")
        )
        self.prefer_ollama = tk.BooleanVar(
            value=self.config.get("prefer_ollama", False)
        )
        self.use_embedding_rank = tk.BooleanVar(
            value=self.config.get("use_embedding_rank", True)
        )
        self.preview_workers = tk.StringVar(
            value=str(self._clamp_workers(self.config.get("preview_workers"), 1))
        )
        self.sync_workers = tk.StringVar(
            value=str(self._clamp_workers(self.config.get("sync_workers"), 5))
        )
        self.execution_workers = tk.StringVar(
            value=str(self._clamp_workers(self.config.get("execution_workers"), 5))
        )
        self.media_type_override = tk.StringVar(
            value=self.config.get("media_type_override", "自动判断")
        )
        self.embedding_cache = {}
        self.ollama_embed_endpoint = None
        self.ollama_model_options = []

        self.create_widgets()
        self.apply_saved_window_geometry()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

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
        return extract_lang_and_ext(filename, self.lang_tags.get())

    def _extract_explicit_season(self, pure_name):
        """仅从明确季标记中提取季号，避免把年份误判为季号。"""
        return extract_explicit_season(pure_name)

    def _pick_season(self, pure_name, guess_data=None, fallback=1):
        """优先使用显式季标记；否则只接受合理范围内的猜测季号。"""
        return pick_season(pure_name, guess_data, fallback)

    def _can_reuse_dir_ai(self, cached_ai, pure_name, guess_data=None):
        """仅在当前文件与缓存标题明显属于同一作品时复用目录级识别结果。"""
        return can_reuse_dir_ai(cached_ai, pure_name, guess_data)

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

                    thumb_source = (
                        m.get("still") or m.get("s_poster") or m.get("poster")
                    )
                    if thumb_source:
                        thumb_path = os.path.splitext(target_path)[0] + "-thumb.jpg"
                        if not os.path.exists(thumb_path):
                            save_image(thumb_path, thumb_source)

                cur_dir = target_dir
                dir_name = os.path.basename(cur_dir)
                is_season_folder = bool(
                    re.match(r"^(Season\s*\d+|S\d+)$", dir_name, re.I)
                )

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
            command=lambda: self.target_root.set(
                filedialog.askdirectory(parent=self.root)
            ),
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

        ttk.Label(top, text="类型:").pack(side=tk.LEFT, padx=(12, 4))
        ttk.Combobox(
            top,
            textvariable=self.media_type_override,
            values=("自动判断", "电影", "电视剧"),
            state="readonly",
            width=10,
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
        win.transient(self.root)
        center_window(win, self.root, 650, 650)
        win.after_idle(lambda: center_window(win, self.root, 650, 650))
        win.grab_set()
        win.focus_set()

        content_wrap = ttk.Frame(win)
        content_wrap.pack(fill=tk.BOTH, expand=True)

        canvas = tk.Canvas(content_wrap, highlightthickness=0)
        scrollbar = ttk.Scrollbar(
            content_wrap, orient=tk.VERTICAL, command=canvas.yview
        )
        canvas.configure(yscrollcommand=scrollbar.set)

        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        f = ttk.Frame(canvas, padding=20)
        canvas_window = canvas.create_window((0, 0), window=f, anchor="nw")

        def _sync_scrollregion(_event=None):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _sync_canvas_width(event):
            canvas.itemconfigure(canvas_window, width=event.width)

        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        f.bind("<Configure>", _sync_scrollregion)
        canvas.bind("<Configure>", _sync_canvas_width)
        canvas.bind(
            "<Enter>", lambda _e: canvas.bind_all("<MouseWheel>", _on_mousewheel)
        )
        canvas.bind("<Leave>", lambda _e: canvas.unbind_all("<MouseWheel>"))

        row = 0

        # API 配置
        ttk.Label(f, text="TMDb API Key:").grid(row=row, column=0, sticky=tk.W, pady=5)
        tmdb_key_entry = ttk.Entry(f, textvariable=self.tmdb_api_key, width=45, show="*")
        tmdb_key_entry.grid(row=row, column=1, pady=5, padx=10)
        tmdb_key_btn = ttk.Button(f, text="显示", width=6)
        tmdb_key_btn.config(
            command=lambda e=tmdb_key_entry, b=tmdb_key_btn: self._toggle_entry_visibility(e, b)
        )
        tmdb_key_btn.grid(row=row, column=2, sticky=tk.W, pady=5)
        row += 1

        ttk.Label(f, text="BGM API Key:").grid(row=row, column=0, sticky=tk.W, pady=5)
        bgm_key_entry = ttk.Entry(f, textvariable=self.bgm_api_key, width=45, show="*")
        bgm_key_entry.grid(row=row, column=1, pady=5, padx=10)
        bgm_key_btn = ttk.Button(f, text="显示", width=6)
        bgm_key_btn.config(
            command=lambda e=bgm_key_entry, b=bgm_key_btn: self._toggle_entry_visibility(e, b)
        )
        bgm_key_btn.grid(row=row, column=2, sticky=tk.W, pady=5)
        row += 1

        ttk.Label(f, text="Silicon AI Key (备选):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        sf_key_entry = ttk.Entry(f, textvariable=self.sf_api_key, width=45, show="*")
        sf_key_entry.grid(row=row, column=1, pady=5, padx=10)
        sf_key_btn = ttk.Button(f, text="显示", width=6)
        sf_key_btn.config(
            command=lambda e=sf_key_entry, b=sf_key_btn: self._toggle_entry_visibility(e, b)
        )
        sf_key_btn.grid(row=row, column=2, sticky=tk.W, pady=5)
        row += 1

        ttk.Label(f, text="API URL (OpenAI兼容):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.sf_api_url, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        ttk.Button(
            f,
            text="测试连接",
            command=lambda: self._test_silicon_api(sf_test_status_var),
        ).grid(row=row, column=2, sticky=tk.W, pady=5)
        row += 1

        sf_test_status_var = tk.StringVar(value="")
        ttk.Label(f, textvariable=sf_test_status_var).grid(
            row=row, column=1, sticky=tk.W, padx=10
        )
        row += 1

        ttk.Label(f, text="模型名称:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.sf_model, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="AI 温度 temperature (0-2):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.ai_temperature, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="AI top_p (0-1):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.ai_top_p, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        # Ollama 配置
        ttk.Label(f, text="Ollama URL:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.ollama_url, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        ttk.Button(
            f,
            text="刷新模型",
            command=lambda: self._refresh_ollama_model_options(
                ollama_model_combo, embedding_model_combo, ollama_status_var, True
            ),
        ).grid(row=row, column=2, sticky=tk.W, pady=5)
        row += 1

        ttk.Label(f, text="Ollama 模型:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ollama_model_combo = ttk.Combobox(
            f,
            textvariable=self.ollama_model,
            width=45,
        )
        ollama_model_combo.grid(row=row, column=1, pady=5, padx=10)
        row += 1

        ttk.Label(f, text="Embedding 模型:").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        embedding_model_combo = ttk.Combobox(
            f,
            textvariable=self.embedding_model,
            width=45,
        )
        embedding_model_combo.grid(row=row, column=1, pady=5, padx=10)
        row += 1

        ollama_status_var = tk.StringVar(value="正在读取本地模型列表...")
        ttk.Label(f, textvariable=ollama_status_var).grid(
            row=row, column=1, sticky=tk.W, padx=10
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

        ttk.Label(f, text="预览并发线程数 (1-10):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.preview_workers, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="批量同步并发线程数 (1-10):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.sync_workers, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="执行并发线程数 (1-10):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.execution_workers, width=45).grid(
            row=row, column=1, pady=5, padx=10
        )
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

        self._refresh_ollama_model_options(
            ollama_model_combo, embedding_model_combo, ollama_status_var, False
        )

    def _set_ollama_combobox_values(self, combobox, current_value, values):
        """更新下拉框候选值，同时保留当前值。"""
        items = []
        current_text = str(current_value or "").strip()
        if current_text:
            items.append(current_text)
        for value in values or []:
            clean_value = str(value or "").strip()
            if clean_value and clean_value not in items:
                items.append(clean_value)
        combobox["values"] = items
        if current_text:
            combobox.set(current_text)

    def _toggle_entry_visibility(self, entry_widget, button_widget=None):
        """在明文与掩码之间切换 Entry 显示状态。"""
        current_show = str(entry_widget.cget("show") or "")
        if current_show:
            entry_widget.config(show="")
            if button_widget is not None:
                button_widget.config(text="隐藏")
            return

        entry_widget.config(show="*")
        if button_widget is not None:
            button_widget.config(text="显示")

    def _refresh_ollama_model_options(
        self, ollama_combobox, embedding_combobox, status_var, show_message=False
    ):
        """从本地 Ollama 服务读取已安装模型，并刷新下拉框。"""
        models, message = list_ollama_models(self.ollama_url.get().strip())
        if models:
            self.ollama_model_options = models
            self._set_ollama_combobox_values(
                ollama_combobox, self.ollama_model.get(), models
            )
            self._set_ollama_combobox_values(
                embedding_combobox, self.embedding_model.get(), models
            )
            status_var.set(f"已加载 {len(models)} 个本地模型")
            return

        self._set_ollama_combobox_values(
            ollama_combobox, self.ollama_model.get(), self.ollama_model_options
        )
        self._set_ollama_combobox_values(
            embedding_combobox,
            self.embedding_model.get(),
            self.ollama_model_options,
        )
        status_var.set(message)
        if show_message:
            messagebox.showwarning("Ollama模型列表", message, parent=self.root)

    def _test_silicon_api(self, status_var):
        """测试 OpenAI 兼容 API 连接"""
        from ai.ollama_ai import test_silicon_api

        api_url = self.sf_api_url.get().strip()
        api_key = self.sf_api_key.get().strip()
        model = self.sf_model.get().strip()

        status_var.set("测试中...")
        self.root.update_idletasks()

        success, message = test_silicon_api(api_url, api_key, model)
        if success:
            status_var.set(f"✓ {message}")
        else:
            status_var.set(f"✗ {message}")

    def _parse_with_ollama(self, filename):
        """调用本地 Ollama 模型解析文件名"""
        return parse_with_ollama(
            self.ollama_url.get().strip(),
            self.ollama_model.get().strip(),
            filename,
            self._get_ai_temperature(),
            self._get_ai_top_p(),
        )

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

    def _ollama_post_json(self, endpoint, payload, timeout):
        """直接请求本地 Ollama，避免全局 session 的重试拖慢处理。"""
        from core.services.matcher_service import ollama_post_json

        return ollama_post_json(
            self.ollama_url.get().strip(), endpoint, payload, timeout
        )

    def _cosine_similarity(self, vec_a, vec_b):
        """计算余弦相似度"""
        from core.services.matcher_service import cosine_similarity

        return cosine_similarity(vec_a, vec_b)

    def _get_embedding(self, text):
        """调用 Ollama embeddings 接口并缓存向量"""
        if not self._can_use_embedding_rank():
            return None
        emb, endpoint = get_embedding(
            self.ollama_url.get().strip(),
            self.embedding_model.get().strip(),
            text,
            self.embedding_cache,
            self.cache_lock,
            self.ollama_embed_endpoint,
        )
        self.ollama_embed_endpoint = endpoint
        return emb

    def _build_candidate_embedding_text(self, cand):
        """构建候选项向量语义文本"""
        from core.services.matcher_service import build_candidate_embedding_text

        return build_candidate_embedding_text(cand)

    def _rerank_candidates_with_embedding(
        self, item, query_title, year, is_tv, source_name, candidates
    ):
        """用 embedding 对候选重排，并在高置信时自动命中"""
        if not self._can_use_embedding_rank() or not candidates:
            return candidates, None, ""
        return rerank_candidates_with_embedding(
            item,
            query_title,
            year,
            is_tv,
            source_name,
            candidates,
            self._get_embedding,
        )

    def _pick_candidate_with_ollama(
        self, item, query_title, year, is_tv, source_name, candidates
    ):
        """使用本地 Ollama 在多个候选中做判定"""
        return pick_candidate_with_ollama(
            self.ollama_url.get().strip(),
            self.ollama_model.get().strip(),
            item,
            query_title,
            year,
            is_tv,
            source_name,
            candidates,
            self._get_ai_temperature(),
        )

    def _request_manual_candidate_choice(
        self, item, query_title, source_name, candidates, recognized_title=None
    ):
        """在主线程弹窗，让用户手动选择候选项"""
        return ui_request_manual_candidate_choice(
            self,
            item,
            query_title,
            source_name,
            candidates,
            recognized_title=recognized_title,
        )

    def _show_candidate_picker_dialog(
        self, item, query_title, source_name, candidates, result_holder, done_event
    ):
        """显示自动识别冲突的候选选择窗口"""
        return ui_show_candidate_picker_dialog(
            self, item, query_title, source_name, candidates, result_holder, done_event
        )

    def _select_best_db_match(
        self,
        item,
        query_title,
        year,
        is_tv,
        source_name,
        candidates,
        recognized_title=None,
    ):
        """从候选列表中自动或手动选择最终匹配项"""
        if not candidates:
            return query_title, "None", f"{source_name}无结果", {}

        if len(candidates) == 1:
            return candidate_to_result(candidates[0], f"{source_name}命中")

        # 年份预排序：将年份匹配的候选提前，减少同名不同年作品的误匹配
        if year:
            year_str = str(year).strip()
            candidates = sorted(
                candidates,
                key=lambda c: 0 if extract_year_from_release(c.get("release") or "") == year_str else 1,
            )

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
            item,
            query_title,
            source_name,
            ranked_candidates,
            recognized_title=recognized_title,
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
                item,
                used_query,
                year,
                is_tv,
                source_name,
                merged,
                recognized_title=query_title,
            )
            if tid_hit != "None" and normalize_compare_text(
                used_query
            ) != normalize_compare_text(query_title):
                msg_hit += " (备选标题)"
            return t_hit, tid_hit, msg_hit, meta_hit

        return query_title, "None", f"{source_name}无结果", {}

    def show_context_menu(self, event):
        """显示右键菜单"""
        return ui_show_context_menu(self, event)

    def manual_match(self):
        """手动匹配"""
        return ui_manual_match(self)

    def _async_manual_match_search(self, selected_ids, user_input, mode):
        """异步搜索手动匹配"""
        return ui_async_manual_match_search(self, selected_ids, user_input, mode)

    def _show_manual_match_results(self, selected_ids, results, error_msg=""):
        """显示手动匹配结果"""
        return ui_show_manual_match_results(self, selected_ids, results, error_msg)

    def _confirm_season_and_dispatch(self, selected_ids, title, tid, msg, meta):
        """确认季偏移并分发任务"""
        return ui_confirm_season_and_dispatch(
            self, selected_ids, title, tid, msg, meta, SeasonOffsetDialog
        )

    def _async_batch_runner(self, indices, title, t_id, msg, meta):
        """异步批量处理"""
        return worker_async_batch_runner(self, indices, title, t_id, msg, meta)

    def _bg_update_single_ui(self, idx, title, t_id, msg, meta):
        """后台更新单个UI项"""
        return worker_bg_update_single_ui(self, idx, title, t_id, msg, meta)

    def _get_version_tag(self, path):
        """获取版本标签"""
        return get_version_tag(path)

    def _friendly_status_text(self, message):
        """Render coded errors to concise Chinese status text for the UI."""
        return friendly_status_text(message)

    def _build_status_text(self, *messages):
        return build_status_text(*messages)

    def _resolve_media_type(self, guess_data=None):
        """Resolve media type from UI override or parser result."""
        override = str(self.media_type_override.get() or "").strip()
        if override == "电影":
            return "movie"
        if override == "电视剧":
            return "episode"

        guessed_type = str((guess_data or {}).get("type") or "episode").strip().lower()
        if guessed_type in ("movie", "film"):
            return "movie"
        return "episode"

    def start_preview(self):
        """开始预览"""
        if not self.file_list:
            messagebox.showwarning("警告", "请先添加文件", parent=self.root)
            return

        if self.prefer_ollama.get():
            if not self.ollama_url.get().strip() or not self.ollama_model.get().strip():
                messagebox.showwarning(
                    "Ollama配置不完整",
                    "您选择了优先使用本地Ollama，但未填写Ollama URL或模型。请先完成配置或切换回SiliconFlow。",
                    parent=self.root,
                )
                return
        else:
            if not self.sf_api_key.get().strip():
                messagebox.showwarning(
                    "缺少API密钥",
                    "请先配置SiliconFlow API Key或启用Ollama。",
                    parent=self.root,
                )
                return

        self.btn_pre.config(state=tk.DISABLED)
        self.pbar["value"] = 0
        self.status.config(text="识别中...")

        threading.Thread(target=self.run_preview_pool, daemon=True).start()

    def run_preview_pool(self):
        """运行预览线程池"""
        return worker_run_preview_pool(self)

    def process_task(self, i):
        """处理单个任务"""
        return worker_process_task(self, i)

    def start_run_logic(self, is_archive):
        """开始重命名逻辑"""
        if not self.file_list:
            return

        # 检查元数据
        for item in self.file_list:
            if "metadata" not in item or item["metadata"].get("id") == "None":
                messagebox.showwarning(
                    "缺少元数据",
                    "请先执行【高速识别预览】后再进行重命名操作。",
                    parent=self.root,
                )
                return

        threading.Thread(
            target=self.run_execution, args=(is_archive,), daemon=True
        ).start()

    def run_execution(self, is_archive):
        """执行重命名"""
        return worker_run_execution(self, is_archive)

    def process_one_file(self, item, is_archive):
        """处理单个文件"""
        return worker_process_one_file(self, item, is_archive)


if __name__ == "__main__":
    root = tk.Tk()
    app = MediaRenamerGUI(root)
    root.mainloop()
