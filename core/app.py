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
from core.models.media_item import MediaItem
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
    process_one_file_scrape as worker_process_one_file_scrape,
    process_task as worker_process_task,
    run_execution as worker_run_execution,
    run_preview_pool as worker_run_preview_pool,
    run_scrape_execution as worker_run_scrape_execution,
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
    _nfo_has_empty_plot,
)


class MediaRenamerGUI(ConfigMixin, ListMixin):
    """主GUI类"""

    def __init__(self, root):
        self.root = root
        self.root.title("媒体归档刮削助手 v2.0")
        self.root.geometry("1300x900")

        self.file_list: list[MediaItem] = []
        self.item_by_id: dict[str, MediaItem] = {}
        self.dir_cache = {}
        self.db_cache = {}
        self.manual_locks = {}
        self.forced_seasons = {}
        self.forced_offsets = {}
        self.db_resolution_events = {}
        self.cache_lock = threading.Lock()
        self.file_write_lock = threading.Lock()
        self.popup_lock = threading.Lock()
        self.preview_skip_all_event = threading.Event()
        self.preview_skip_dirs: set = set()
        self.view_mode = tk.StringVar(value="group")
        self.expanded_groups: set[str] = set()
        self._item_seq = 0
        self.action_scope_item_ids: list[str] = []

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
        self.ai_mode = tk.StringVar(value=self.config.get("ai_mode", "assist"))
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
        self.strip_keywords_var = tk.StringVar(
            value=self._normalize_strip_keywords_text(
                self.config.get("strip_keywords", [])
            )
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
        """在媒体文件已位于目标位置后写入 NFO 与图片。

        锁内仅做 NFO 写入（毫秒级），图片下载在锁外并发执行，
        避免 file_write_lock 把多线程刮削串行化。
        """
        target_dir = os.path.dirname(target_path)
        m = item.metadata or {}
        media_type = m.get("type", "episode")
        is_tv = media_type == "episode"
        is_sub_audio = item.old_name.lower().endswith(self.get_sub_audio_exts())

        image_tasks = []  # [(local_path, url), ...]

        with self.file_write_lock:
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
                            image_tasks.append((thumb_path, thumb_source))

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
                    image_tasks.append((s_poster_root, m["s_poster"]))

                if is_season_folder:
                    season_nfo_local = os.path.join(cur_dir, "season.nfo")
                    folder_jpg_local = os.path.join(cur_dir, "folder.jpg")

                    if not os.path.exists(season_nfo_local):
                        write_nfo(season_nfo_local, m, "season")

                    if m.get("s_poster") and not os.path.exists(folder_jpg_local):
                        image_tasks.append((folder_jpg_local, m["s_poster"]))

                tvshow_nfo = os.path.join(root_d, "tvshow.nfo")
                poster_path = os.path.join(root_d, "poster.jpg")

                if not os.path.exists(tvshow_nfo) or _nfo_has_empty_plot(tvshow_nfo):
                    write_nfo(tvshow_nfo, m, "tvshow")

                if m.get("poster") and not os.path.exists(poster_path):
                    image_tasks.append((poster_path, m["poster"]))

            else:
                if not is_sub_audio:
                    movie_nfo = os.path.splitext(target_path)[0] + ".nfo"
                    if not os.path.exists(movie_nfo):
                        write_nfo(movie_nfo, m, "movie")

                poster_path = os.path.join(target_dir, "poster.jpg")
                if m.get("poster") and not os.path.exists(poster_path):
                    image_tasks.append((poster_path, m["poster"]))

                fanart_path = os.path.join(target_dir, "fanart.jpg")
                if m.get("fanart") and not os.path.exists(fanart_path):
                    image_tasks.append((fanart_path, m["fanart"]))

        # 锁外并发下载图片，不阻塞其他线程的 NFO 写入
        for img_path, img_url in image_tasks:
            save_image(img_path, img_url)

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
        cols = ("title", "id", "new", "st")
        self.tree = ttk.Treeview(
            mid, columns=cols, show="tree headings", selectmode="extended"
        )

        self.tree.heading("#0", text="添加路径 / Season / 原文件名")
        self.tree.column("#0", width=320, anchor=tk.W, stretch=True)

        for c, h, w in zip(
            cols,
            ["识别标题", "匹配 ID", "新文件名 / 归档路径", "状态"],
            [220, 90, 560, 180],
        ):
            self.tree.heading(c, text=h)
            self.tree.column(
                c, width=w, anchor=tk.CENTER if c in ["id", "st"] else tk.W
            )

        vsb = ttk.Scrollbar(mid, orient=tk.VERTICAL, command=self.tree.yview)
        hsb = ttk.Scrollbar(mid, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.tree.pack(fill=tk.BOTH, expand=True)
        self.tree.bind("<Button-3>", self.show_context_menu)
        self.tree.bind("<Control-a>", self.select_all_files)
        self.tree.bind("<Control-A>", self.select_all_files)
        self.tree.bind("<<TreeviewSelect>>", self.update_details_panel)
        self.tree.bind("<<TreeviewOpen>>", self.on_treeview_open)
        self.tree.bind("<<TreeviewClose>>", self.on_treeview_close)

        detail_frame = ttk.LabelFrame(self.root, text=" 当前选中详情 ", padding=8)
        detail_frame.pack(fill=tk.X, padx=10, pady=(0, 5))
        self.detail_left_var = tk.StringVar(value="")
        self.detail_right_var = tk.StringVar(value="")

        detail_body = ttk.Frame(detail_frame)
        detail_body.pack(fill=tk.X, expand=True)
        detail_body.columnconfigure(0, weight=1)
        detail_body.columnconfigure(2, weight=1)

        left_panel = ttk.Frame(detail_body)
        left_panel.grid(row=0, column=0, sticky="nsew", padx=(0, 10))

        separator = ttk.Separator(detail_body, orient=tk.VERTICAL)
        separator.grid(row=0, column=1, sticky="ns")

        right_panel = ttk.Frame(detail_body)
        right_panel.grid(row=0, column=2, sticky="nsew", padx=(10, 0))

        self.detail_left_label = ttk.Label(
            left_panel,
            textvariable=self.detail_left_var,
            justify=tk.LEFT,
            anchor="nw",
        )
        self.detail_left_label.pack(fill=tk.X, expand=True)

        self.detail_right_label = ttk.Label(
            right_panel,
            textvariable=self.detail_right_var,
            justify=tk.LEFT,
            anchor="nw",
        )
        self.detail_right_label.pack(fill=tk.X, expand=True)

        detail_body.bind("<Configure>", self._on_detail_body_resize)

        # 底部按钮和进度条
        bot = ttk.Frame(self.root, padding=10)
        bot.pack(fill=tk.X)

        self.btn_pre = ttk.Button(
            bot, text="1. 高速识别预览", command=self.start_preview
        )
        self.btn_pre.pack(side=tk.LEFT, padx=5)

        ttk.Button(
            bot, text="2. 原地重命名", command=lambda: self.start_run_logic("rename")
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            bot, text="3. 归档移动", command=lambda: self.start_run_logic("archive")
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            bot, text="4. 原地整理", command=lambda: self.start_run_logic("organize")
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            bot, text="5. 刮削", command=self.start_scrape_logic
        ).pack(side=tk.LEFT, padx=5)

        self.pbar = ttk.Progressbar(bot, mode="determinate")
        self.pbar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=15)

        self.status = ttk.Label(bot, text="就绪")
        self.status.pack(side=tk.RIGHT)
        self.refresh_tree_view(preserve_selection=False)
        self._set_details_content("当前没有选中任何分组或文件。", "")

    def _new_item_id(self):
        """Return a stable Treeview/file identifier."""
        self._item_seq += 1
        return f"file::{self._item_seq}"

    def _normalize_strip_keywords_text(self, value):
        """Normalize strip-keyword config values to one display string."""
        if isinstance(value, (list, tuple, set)):
            items = [str(v).strip() for v in value if str(v).strip()]
            return " | ".join(items)
        return str(value or "").strip()

    def _get_strip_keywords(self):
        """Return normalized strip keywords as an ordered list."""
        raw = str(self.strip_keywords_var.get() or "")
        parts = re.split(r"[\r\n,|]+", raw)
        seen = set()
        items = []
        for part in parts:
            text = part.strip()
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            items.append(text)
        return items

    def _has_ai_backend_configured(self):
        """Return whether at least one usable AI backend is configured."""
        if self.prefer_ollama.get():
            if self.ollama_url.get().strip() and self.ollama_model.get().strip():
                return True
            return bool(self.sf_api_key.get().strip())
        return bool(self.sf_api_key.get().strip())

    def _build_library_target_path(self, item, root_d):
        """Build a Kodi/Jellyfin-style organized target path under root_d."""
        root_dir = str(root_d or "").strip()
        if not root_dir:
            return os.path.join(item.dir, item.new_name_only or item.old_name)

        metadata = item.metadata or {}
        media_id = str(metadata.get("id") or "None").strip()
        if media_id == "None":
            return os.path.join(item.dir, item.new_name_only or item.old_name)

        title = safe_filename(
            str(metadata.get("title") or item.display_title or item.old_name).strip()
        )
        year_text = safe_str(metadata.get("year"))
        provider = str(metadata.get("provider") or "tmdb").strip().lower()
        id_tag = f"bgmid={media_id}" if provider == "bgm" else f"tmdbid={media_id}"
        filename = item.new_name_only or item.old_name

        if str(metadata.get("type") or "episode").strip().lower() == "episode":
            season_num = safe_int(metadata.get("s"), 1)
            folder_name = safe_filename(f"{title} [{id_tag}]")
            return os.path.join(root_dir, folder_name, f"Season {season_num}", filename)

        if year_text:
            folder_name = safe_filename(f"{title} ({year_text}) [{id_tag}]")
        else:
            folder_name = safe_filename(f"{title} [{id_tag}]")
        return os.path.join(root_dir, folder_name, filename)

    def _build_target_for_mode(self, item, run_mode):
        """Resolve the effective filesystem target for one run mode."""
        if run_mode == "archive" and item.full_target:
            return item.full_target
        if run_mode == "organize":
            source_root = item.organize_root or item.source_path or item.dir
            return self._build_library_target_path(item, source_root)
        return os.path.join(item.dir, item.new_name_only or item.old_name)

    def get_item_by_id(self, item_id):
        """Look up one media item by its stable identifier."""
        return self.item_by_id.get(item_id)

    def is_source_row(self, row_id):
        """Return whether a row id belongs to a source-path node."""
        return str(row_id or "").startswith("source::")

    def is_season_row(self, row_id):
        """Return whether a row id belongs to a season subgroup node."""
        return str(row_id or "").startswith("season::")

    def is_group_row(self, row_id):
        """Return whether a row id belongs to any non-file grouping node."""
        return self.is_source_row(row_id) or self.is_season_row(row_id)

    def source_path_from_row_id(self, row_id):
        """Extract source path from a group row identifier."""
        if self.is_source_row(row_id):
            return str(row_id).split("source::", 1)[1]
        if self.is_season_row(row_id):
            payload = str(row_id).split("season::", 1)[1]
            return payload.split("||", 1)[0]
        return ""

    def season_key_from_row_id(self, row_id):
        """Extract season subgroup key from a season row identifier."""
        if not self.is_season_row(row_id):
            return ""
        payload = str(row_id).split("season::", 1)[1]
        parts = payload.split("||", 1)
        return parts[1] if len(parts) == 2 else ""

    def _source_row_id(self, source_path):
        """Build the Treeview id for a top-level source path."""
        return f"source::{source_path}"

    def _season_row_id(self, source_path, season_key):
        """Build the Treeview id for a season subgroup."""
        return f"season::{source_path}||{season_key}"

    def _group_items(self, group_path):
        """Return all files that belong to one grouped source path."""
        return [item for item in self.file_list if item.source_path == group_path]

    def _season_group_label(self, item):
        """Return the second-level subgroup label under one source path."""
        source_path = item.source_path or item.dir
        rel_dir = ""
        try:
            rel_dir = os.path.relpath(item.dir, source_path)
        except Exception:
            rel_dir = ""

        if not rel_dir or rel_dir in (".", ""):
            return "根目录文件"

        first_part = str(rel_dir).split(os.sep, 1)[0].strip()
        return first_part or "根目录文件"

    def _season_groups_for_source(self, source_path):
        """Return ordered season subgroups and their items for one source path."""
        groups = {}
        order = []
        for item in self._group_items(source_path):
            label = self._season_group_label(item)
            if label not in groups:
                groups[label] = []
                order.append(label)
            groups[label].append(item)
        return [(label, groups[label]) for label in order]

    def _use_flat_source_layout(self, source_path):
        """Return whether one source path should show files directly under root."""
        season_groups = self._season_groups_for_source(source_path)
        return len(season_groups) == 1 and season_groups[0][0] == "根目录文件"

    def get_selected_file_ids(self):
        """Return selected file row ids only."""
        return [row_id for row_id in self.tree.selection() if self.get_item_by_id(row_id)]

    def _collect_file_descendants(self, row_id):
        """Collect all file-node descendants under one tree row."""
        if not row_id or not self.tree.exists(row_id):
            return []

        item = self.get_item_by_id(row_id)
        if item:
            return [item.id]

        result = []
        for child_id in self.tree.get_children(row_id):
            result.extend(self._collect_file_descendants(child_id))
        return result

    def _selection_scope_row_for_ctrl_a(self):
        """Resolve which row should define Ctrl+A scope in grouped view."""
        focus_row = self.tree.focus()
        if focus_row and self.tree.exists(focus_row):
            return focus_row

        selection = self.tree.selection()
        return selection[0] if selection else ""

    def _resolve_current_action_scope(self):
        """Resolve the file subset targeted by the bottom action buttons."""
        if not self.file_list:
            return [], [], "全部文件"

        scope_row = self._selection_scope_row_for_ctrl_a()
        if not scope_row or not self.tree.exists(scope_row):
            indices = list(range(len(self.file_list)))
            return indices, list(self.file_list), "全部文件"

        if self.get_item_by_id(scope_row):
            scope_row = self.tree.parent(scope_row) or scope_row

        file_ids = self._collect_file_descendants(scope_row)
        if not file_ids:
            indices = list(range(len(self.file_list)))
            return indices, list(self.file_list), "全部文件"

        id_to_index = {item.id: idx for idx, item in enumerate(self.file_list)}
        items = []
        indices = []
        for file_id in file_ids:
            item = self.get_item_by_id(file_id)
            idx = id_to_index.get(file_id)
            if item is None or idx is None:
                continue
            items.append(item)
            indices.append(idx)

        scope_label = self.tree.item(scope_row, "text") or "当前分组"
        return indices, items, scope_label

    def _item_values(self, item):
        """Build the visible column values for one file row."""
        return (
            item.display_title,
            item.display_match_id,
            item.display_target,
            item.status_text,
        )

    def refresh_tree_view(self, preserve_selection=True):
        """Rebuild the grouped tree view."""
        selected_ids = set(self.get_selected_file_ids()) if preserve_selection else set()
        focused = self.tree.focus() if preserve_selection else ""

        for row_id in self.tree.get_children():
            self.tree.delete(row_id)

        self.tree.heading("#0", text="添加路径 / Season / 原文件名")
        group_order = []
        seen = set()
        selected_sources = set()
        selected_seasons = set()
        for item_id in selected_ids:
            selected_item = self.get_item_by_id(item_id)
            if not selected_item:
                continue
            source_path = selected_item.source_path or selected_item.dir
            season_key = self._season_group_label(selected_item)
            selected_sources.add(self._source_row_id(source_path))
            selected_seasons.add(self._season_row_id(source_path, season_key))
        for item in self.file_list:
            group_path = item.source_path or item.dir
            if group_path not in seen:
                seen.add(group_path)
                group_order.append(group_path)

        for group_path in group_order:
            group_iid = self._source_row_id(group_path)
            self.tree.insert(
                "",
                tk.END,
                iid=group_iid,
                text=group_path,
                open=(group_iid in self.expanded_groups or group_iid in selected_sources),
                values=("", "", "", ""),
            )
            if self._use_flat_source_layout(group_path):
                for item in self._group_items(group_path):
                    self.tree.insert(
                        group_iid,
                        tk.END,
                        iid=item.id,
                        text=item.old_name,
                        values=self._item_values(item),
                    )
                continue
            for season_label, items in self._season_groups_for_source(group_path):
                season_iid = self._season_row_id(group_path, season_label)
                self.tree.insert(
                    group_iid,
                    tk.END,
                    iid=season_iid,
                    text=season_label,
                    open=(
                        season_iid in self.expanded_groups
                        or season_iid in selected_seasons
                    ),
                    values=("", "", "", ""),
                )
                for item in items:
                    self.tree.insert(
                        season_iid,
                        tk.END,
                        iid=item.id,
                        text=item.old_name,
                        values=self._item_values(item),
                    )

        existing_selected = [item_id for item_id in selected_ids if self.tree.exists(item_id)]
        if existing_selected:
            self.tree.selection_set(existing_selected)
            self.tree.focus(existing_selected[0])
        elif focused and self.tree.exists(focused):
            self.tree.focus(focused)

        self.update_details_panel()

    def refresh_item_row(self, item_id):
        """Refresh one file row in the active tree if it is visible."""
        item = self.get_item_by_id(item_id)
        if not item or not self.tree.exists(item.id):
            self.update_details_panel()
            return

        self.tree.item(item.id, text=item.old_name, values=self._item_values(item))
        self.update_details_panel()

    def update_item_display(
        self,
        item_or_id,
        *,
        old_name=None,
        title=None,
        match_id=None,
        target=None,
        status=None,
    ):
        """Update cached UI text for one item and refresh the visible row."""
        item = (
            item_or_id
            if isinstance(item_or_id, MediaItem)
            else self.get_item_by_id(item_or_id)
        )
        if not item:
            return

        if old_name is not None:
            item.old_name = old_name
        if title is not None:
            item.display_title = str(title)
        if match_id is not None:
            item.display_match_id = str(match_id)
        if target is not None:
            item.display_target = str(target)
        if status is not None:
            item.status_text = str(status)

        self.refresh_item_row(item.id)

    def _on_detail_body_resize(self, event):
        """Keep left/right detail blocks wrapped to the available width."""
        column_width = max(220, int((event.width - 32) / 2))
        self.detail_left_label.configure(wraplength=column_width)
        self.detail_right_label.configure(wraplength=column_width)

    def _set_details_text(self, text):
        """Backward-compatible single-column detail update helper."""
        self._set_details_content(text, "")

    def _set_details_content(self, left_text, right_text):
        """Render wrapped details text inside the lower two-column panel."""
        self.detail_left_var.set((left_text or "").strip())
        self.detail_right_var.set((right_text or "").strip())

    def _build_group_details(self, group_path):
        """Build the wrapped details blocks for one grouped path."""
        items = self._group_items(group_path)
        if not items:
            return (
                f"添加路径:\n{group_path}\n\n该分组当前没有文件。",
                "",
            )

        recognized = sum(1 for item in items if item.metadata.get("id") != "None")
        pending = sum(1 for item in items if item.status_text in ("待命", "识别中"))
        done = sum(
            1
            for item in items
            if item.status_text
            in ("重命名完成", "归档完成", "原地整理完成", "原地整理+刮削完成", "刮削完成")
        )

        left_text = (
            f"添加路径:\n{group_path}\n\n"
            f"文件数量: {len(items)}\n"
            f"已识别: {recognized}\n"
            f"进行中/待命: {pending}\n"
            f"已完成: {done}"
        )
        if self._use_flat_source_layout(group_path):
            sample_lines = [item.old_name for item in items[:12]]
            if len(items) > 12:
                sample_lines.append(f"... 还有 {len(items) - 12} 个文件")
            right_text = "当前目录文件:\n" + "\n".join(sample_lines)
        else:
            season_lines = []
            for season_label, season_items in self._season_groups_for_source(group_path):
                season_lines.append(f"{season_label}: {len(season_items)} 个文件")
            right_text = "Season 分组:\n" + ("\n".join(season_lines) if season_lines else "(无)")
        return left_text, right_text

    def _build_season_group_details(self, source_path, season_key):
        """Build the wrapped details blocks for one season subgroup."""
        items = [
            item
            for item in self._group_items(source_path)
            if self._season_group_label(item) == season_key
        ]
        if not items:
            return (
                f"添加路径:\n{source_path}\n\nSeason 分组:\n{season_key}\n\n该分组当前没有文件。",
                "",
            )

        recognized = sum(1 for item in items if item.metadata.get("id") != "None")
        pending = sum(1 for item in items if item.status_text in ("待命", "识别中"))
        done = sum(
            1
            for item in items
            if item.status_text
            in ("重命名完成", "归档完成", "原地整理完成", "原地整理+刮削完成", "刮削完成")
        )
        sample_lines = [item.old_name for item in items[:12]]
        if len(items) > 12:
            sample_lines.append(f"... 还有 {len(items) - 12} 个文件")

        left_text = (
            f"添加路径:\n{source_path}\n\n"
            f"Season 分组:\n{season_key}\n\n"
            f"文件数量: {len(items)}\n"
            f"已识别: {recognized}\n"
            f"进行中/待命: {pending}\n"
            f"已完成: {done}"
        )
        right_text = "当前 Season 文件:\n" + "\n".join(sample_lines)
        return left_text, right_text

    def _build_item_details(self, item):
        """Build the wrapped details blocks for one selected file."""
        title = item.display_title or "(未识别)"
        match_id = item.display_match_id or "(无)"
        target = item.display_target or "(尚未生成)"
        status = item.status_text or "待命"
        source_path = item.source_path or item.dir
        full_path = item.path or ""
        left_text = (
            f"原文件名:\n{item.old_name}\n\n"
            f"原始完整路径:\n{full_path}\n\n"
            f"所属添加路径:\n{source_path}"
        )
        right_text = (
            f"识别标题:\n{title}\n\n"
            f"识别来源:\n{item.parse_source or '(未记录)'}\n\n"
            f"匹配 ID:\n{match_id}\n\n"
            f"新文件名 / 归档路径:\n{target}\n\n"
            f"状态:\n{status}"
        )
        return left_text, right_text

    def update_details_panel(self, _event=None):
        """Refresh the lower details panel based on current selection."""
        selection = self.tree.selection()
        if not selection:
            self._set_details_content("当前没有选中任何分组或文件。", "")
            return

        row_id = selection[0]
        if self.is_source_row(row_id):
            self._set_details_content(
                *self._build_group_details(self.source_path_from_row_id(row_id))
            )
            return

        if self.is_season_row(row_id):
            self._set_details_content(
                *self._build_season_group_details(
                    self.source_path_from_row_id(row_id),
                    self.season_key_from_row_id(row_id),
                )
            )
            return

        item = self.get_item_by_id(row_id)
        if not item:
            self._set_details_content("当前选中项已失效，请重新选择。", "")
            return

        self._set_details_content(*self._build_item_details(item))

    def on_treeview_open(self, _event=None):
        """Persist group expanded state while using grouped view."""
        row_id = self.tree.focus()
        if self.is_group_row(row_id):
            self.expanded_groups.add(row_id)

    def on_treeview_close(self, _event=None):
        """Persist group collapsed state while using grouped view."""
        row_id = self.tree.focus()
        if self.is_group_row(row_id):
            self.expanded_groups.discard(row_id)

    def toggle_group_row(self, row_id):
        """Toggle one grouped path row and persist its open state."""
        if not self.is_group_row(row_id) or not self.tree.exists(row_id):
            return

        new_state = not bool(self.tree.item(row_id, "open"))
        self.tree.item(row_id, open=new_state)
        if new_state:
            self.expanded_groups.add(row_id)
        else:
            self.expanded_groups.discard(row_id)

    def open_settings(self):
        """打开设置窗口"""
        win = tk.Toplevel(self.root)
        win.title("高级设置与 API 配置")
        win.transient(self.root)
        center_window(win, self.root, 860, 760)
        win.after_idle(lambda: center_window(win, self.root, 860, 760))
        win.minsize(760, 620)
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
        f.columnconfigure(0, weight=0, minsize=210)
        f.columnconfigure(1, weight=1)
        f.columnconfigure(2, weight=0, minsize=120)
        wrap_labels = []

        def _sync_scrollregion(_event=None):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _sync_canvas_width(event):
            canvas.itemconfigure(canvas_window, width=event.width)

        def _wrap_label(label, minimum=260):
            wrap_labels.append((label, minimum))
            return label

        def _update_wrap_labels(_event=None):
            label_col_width = max(180, f.grid_columnconfigure(0).get("minsize", 210))
            action_col_width = max(110, f.grid_columnconfigure(2).get("minsize", 120))
            available = max(minimum for _label, minimum in wrap_labels) if wrap_labels else 260
            current = max(available, f.winfo_width() - label_col_width - action_col_width - 80)
            for label, minimum in wrap_labels:
                label.configure(wraplength=max(minimum, current))

        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        f.bind("<Configure>", _sync_scrollregion)
        f.bind("<Configure>", _update_wrap_labels, add="+")
        canvas.bind("<Configure>", _sync_canvas_width)
        canvas.bind(
            "<Enter>", lambda _e: canvas.bind_all("<MouseWheel>", _on_mousewheel)
        )
        canvas.bind("<Leave>", lambda _e: canvas.unbind_all("<MouseWheel>"))

        row = 0

        # API 配置
        ttk.Label(f, text="TMDb API Key:").grid(row=row, column=0, sticky=tk.W, pady=5)
        tmdb_key_entry = ttk.Entry(f, textvariable=self.tmdb_api_key, show="*")
        tmdb_key_entry.grid(row=row, column=1, sticky="ew", pady=5, padx=10)
        tmdb_key_btn = ttk.Button(f, text="显示", width=6)
        tmdb_key_btn.config(
            command=lambda e=tmdb_key_entry, b=tmdb_key_btn: self._toggle_entry_visibility(e, b)
        )
        tmdb_key_btn.grid(row=row, column=2, sticky="w", pady=5)
        row += 1

        ttk.Label(f, text="BGM API Key:").grid(row=row, column=0, sticky=tk.W, pady=5)
        bgm_key_entry = ttk.Entry(f, textvariable=self.bgm_api_key, show="*")
        bgm_key_entry.grid(row=row, column=1, sticky="ew", pady=5, padx=10)
        bgm_key_btn = ttk.Button(f, text="显示", width=6)
        bgm_key_btn.config(
            command=lambda e=bgm_key_entry, b=bgm_key_btn: self._toggle_entry_visibility(e, b)
        )
        bgm_key_btn.grid(row=row, column=2, sticky="w", pady=5)
        row += 1

        ttk.Label(f, text="Silicon AI Key (备选):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        sf_key_entry = ttk.Entry(f, textvariable=self.sf_api_key, show="*")
        sf_key_entry.grid(row=row, column=1, sticky="ew", pady=5, padx=10)
        sf_key_btn = ttk.Button(f, text="显示", width=6)
        sf_key_btn.config(
            command=lambda e=sf_key_entry, b=sf_key_btn: self._toggle_entry_visibility(e, b)
        )
        sf_key_btn.grid(row=row, column=2, sticky="w", pady=5)
        row += 1

        ttk.Label(f, text="API URL (OpenAI兼容):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.sf_api_url).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        ttk.Button(
            f,
            text="测试连接",
            command=lambda: self._test_silicon_api(sf_test_status_var),
        ).grid(row=row, column=2, sticky="w", pady=5)
        row += 1

        sf_test_status_var = tk.StringVar(value="")
        _wrap_label(
            ttk.Label(f, textvariable=sf_test_status_var, justify=tk.LEFT)
        ).grid(
            row=row, column=1, columnspan=2, sticky="ew", padx=10
        )
        row += 1

        ttk.Label(f, text="模型名称:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.sf_model).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="AI 温度 temperature (0-2):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.ai_temperature).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="AI top_p (0-1):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.ai_top_p).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="AI 识别模式:").grid(row=row, column=0, sticky=tk.W, pady=5)
        mode_wrap = ttk.Frame(f)
        mode_wrap.grid(row=row, column=1, columnspan=2, sticky="w", pady=5, padx=10)
        ttk.Radiobutton(
            mode_wrap, text="禁用", variable=self.ai_mode, value="disabled"
        ).pack(side=tk.LEFT)
        ttk.Radiobutton(
            mode_wrap, text="辅助识别", variable=self.ai_mode, value="assist"
        ).pack(side=tk.LEFT, padx=(12, 0))
        ttk.Radiobutton(
            mode_wrap, text="强制使用", variable=self.ai_mode, value="force"
        ).pack(side=tk.LEFT, padx=(12, 0))
        row += 1

        _wrap_label(
            ttk.Label(
                f,
                text="禁用：只用文件名猜测；辅助识别：先 guessit，搜不到再让 AI 重提标题；强制使用：只走 AI。",
                justify=tk.LEFT,
            ),
            minimum=320,
        ).grid(row=row, column=1, columnspan=2, sticky="ew", pady=(0, 5), padx=10)
        row += 1

        ttk.Label(f, text="剔除关键词:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.strip_keywords_var).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        _wrap_label(
            ttk.Label(f, text="多个关键词可用 | 或逗号分隔", justify=tk.LEFT),
            minimum=120,
        ).grid(row=row, column=2, sticky="w", pady=5)
        row += 1

        # Ollama 配置
        ttk.Label(f, text="Ollama URL:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ttk.Entry(f, textvariable=self.ollama_url).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        ttk.Button(
            f,
            text="刷新模型",
            command=lambda: self._refresh_ollama_model_options(
                ollama_model_combo, embedding_model_combo, ollama_status_var, True
            ),
        ).grid(row=row, column=2, sticky="w", pady=5)
        row += 1

        ttk.Label(f, text="Ollama 模型:").grid(row=row, column=0, sticky=tk.W, pady=5)
        ollama_model_combo = ttk.Combobox(
            f,
            textvariable=self.ollama_model,
        )
        ollama_model_combo.grid(row=row, column=1, sticky="ew", pady=5, padx=10)
        row += 1

        ttk.Label(f, text="Embedding 模型:").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        embedding_model_combo = ttk.Combobox(
            f,
            textvariable=self.embedding_model,
        )
        embedding_model_combo.grid(row=row, column=1, sticky="ew", pady=5, padx=10)
        row += 1

        ollama_status_var = tk.StringVar(value="正在读取本地模型列表...")
        _wrap_label(
            ttk.Label(f, textvariable=ollama_status_var, justify=tk.LEFT)
        ).grid(
            row=row, column=1, columnspan=2, sticky="ew", padx=10
        )
        row += 1

        ttk.Checkbutton(
            f,
            text="优先使用本地 Ollama (失败后自动尝试 SiliconFlow)",
            variable=self.prefer_ollama,
        ).grid(row=row, column=0, columnspan=3, sticky=tk.W, pady=5)
        row += 1

        ttk.Checkbutton(
            f,
            text="启用 Embedding 候选重排 (提升多候选识别率)",
            variable=self.use_embedding_rank,
        ).grid(row=row, column=0, columnspan=3, sticky=tk.W, pady=5)
        row += 1

        ttk.Label(f, text="预览并发线程数 (1-10):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.preview_workers).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="批量同步并发线程数 (1-10):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.sync_workers).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="执行并发线程数 (1-10):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.execution_workers).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        row += 1

        # 格式配置
        ttk.Label(f, text="剧集 (TV) 格式:").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.tv_format).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="电影 (Movie) 格式:").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.movie_format).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        row += 1

        # 扩展名配置
        ttk.Label(f, text="视频扩展名 (逗号分隔):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.video_exts).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="字幕/音频扩展名 (逗号):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.sub_audio_exts).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        row += 1

        ttk.Label(f, text="语言标签 (竖线|分隔):").grid(
            row=row, column=0, sticky=tk.W, pady=5
        )
        ttk.Entry(f, textvariable=self.lang_tags).grid(
            row=row, column=1, sticky="ew", pady=5, padx=10
        )
        row += 1

        # 保存按钮
        action_bar = ttk.Frame(f)
        action_bar.grid(row=row, column=0, columnspan=3, sticky="ew", pady=15)
        action_bar.columnconfigure(0, weight=1)
        ttk.Button(
            action_bar,
            text="保存并生效 (无需重启)",
            command=lambda: [self.save_config(), win.destroy()],
        ).grid(row=0, column=1, sticky="e")

        self._refresh_ollama_model_options(
            ollama_model_combo, embedding_model_combo, ollama_status_var, False
        )
        win.after_idle(_update_wrap_labels)

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

        # 精确/高置信标题匹配：无需 embedding/Ollama 直接命中
        import difflib as _difflib
        import re as _re
        _q_norm = _re.sub(r"[\W_]+", "", str(query_title or "").lower())
        if _q_norm:
            _exact = None
            _scores = []
            for _c in candidates:
                _ct = _re.sub(r"[\W_]+", "", str(_c.get("title") or "").lower())
                _ca = _re.sub(r"[\W_]+", "", str(_c.get("alt_title") or "").lower())
                _s = max(
                    _difflib.SequenceMatcher(None, _q_norm, _ct).ratio() if _ct else 0.0,
                    _difflib.SequenceMatcher(None, _q_norm, _ca).ratio() if _ca else 0.0,
                )
                _scores.append((_s, _c))
                if _ct == _q_norm or _ca == _q_norm:
                    _exact = _c
                    break
            if _exact is None and _scores:
                _scores.sort(key=lambda x: x[0], reverse=True)
                _top_s, _top_c = _scores[0]
                _second_s = _scores[1][0] if len(_scores) > 1 else 0.0
                if _top_s >= 0.90 and (_top_s - _second_s) >= 0.20:
                    _exact = _top_c
            if _exact is not None:
                return candidate_to_result(_exact, f"标题匹配/{source_name}命中")

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
        _first_hit = False

        for q in query_titles:
            if mode == "siliconflow_tmdb":
                cur = fetch_tmdb_candidates(q, year, is_tv, self.tmdb_api_key.get())
            else:
                cur = fetch_bgm_candidates(q, year, self.bgm_api_key.get())

            if not cur:
                continue

            if not _first_hit:
                used_query = q
                _first_hit = True

            for cand in cur:
                cid = str(cand.get("id") or "")
                if not cid or cid in seen_ids:
                    continue
                seen_ids.add(cid)
                merged.append(cand)

            # 候选足够多时提前结束，避免无效请求拖慢速度
            if len(merged) >= 10:
                break

        bgm_fallback = False
        if not merged and mode == "siliconflow_tmdb":
            for q in query_titles:
                cur = fetch_bgm_candidates(q, year, self.bgm_api_key.get())
                if not cur:
                    continue

                if not _first_hit:
                    used_query = q
                    _first_hit = True

                for cand in cur:
                    cid = str(cand.get("id") or "")
                    if not cid or cid in seen_ids:
                        continue
                    seen_ids.add(cid)
                    merged.append(cand)

                if len(merged) >= 10:
                    break

            if merged:
                bgm_fallback = True
                source_name = "BGM(回退)"

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
            if bgm_fallback and tid_hit != "None":
                meta_hit["_provider"] = "bgm"
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

        scope_indices, scope_items, scope_label = self._resolve_current_action_scope()
        if not scope_items:
            messagebox.showwarning("警告", "当前作用域内没有可处理的文件", parent=self.root)
            return

        ai_mode = str(self.ai_mode.get() or "assist").strip().lower()
        if ai_mode == "force" and not self._has_ai_backend_configured():
            messagebox.showwarning(
                "AI配置不完整",
                "当前选择了强制使用AI，但尚未配置可用的 Ollama 或 OpenAI 兼容接口。",
                parent=self.root,
            )
            return

        self.action_scope_item_ids = [item.id for item in scope_items]
        self.btn_pre.config(state=tk.DISABLED)
        self.pbar["value"] = 0
        self.status.config(text=f"识别中: {scope_label}")
        self.preview_skip_all_event.clear()
        self.preview_skip_dirs.clear()

        threading.Thread(target=self.run_preview_pool, daemon=True).start()

    def run_preview_pool(self):
        """运行预览线程池"""
        return worker_run_preview_pool(self)

    def process_task(self, i):
        """处理单个任务"""
        return worker_process_task(self, i)

    def start_run_logic(self, run_mode):
        """开始重命名逻辑"""
        if not self.file_list:
            return

        _scope_indices, scope_items, scope_label = self._resolve_current_action_scope()
        if not scope_items:
            messagebox.showwarning("警告", "当前作用域内没有可处理的文件", parent=self.root)
            return

        unprocessed = sum(
            1 for item in scope_items if item.metadata.get("id") == "None"
        )
        if unprocessed:
            op_map = {
                "rename": "重命名",
                "archive": "归档",
                "organize": "原地整理",
            }
            op = op_map.get(run_mode, "处理")
            ok = messagebox.askokcancel(
                "部分文件未预览",
                f"有 {unprocessed} 个文件尚未完成识别预览，将被跳过，其余已识别的文件正常{op}。\n\n是否继续？",
                parent=self.root,
            )
            if not ok:
                return

        self.action_scope_item_ids = [item.id for item in scope_items]
        self.status.config(text=f"准备处理: {scope_label}")
        threading.Thread(
            target=self.run_execution, args=(run_mode,), daemon=True
        ).start()

    def start_scrape_logic(self):
        """开始独立刮削"""
        if not self.file_list:
            return

        _scope_indices, scope_items, scope_label = self._resolve_current_action_scope()
        if not scope_items:
            messagebox.showwarning("警告", "当前作用域内没有可处理的文件", parent=self.root)
            return

        unprocessed = sum(
            1 for item in scope_items if item.metadata.get("id") == "None"
        )
        if unprocessed:
            ok = messagebox.askokcancel(
                "部分文件未预览",
                f"有 {unprocessed} 个文件尚未完成识别预览，将被跳过刮削，其余已识别的文件正常刮削。\n\n是否继续？",
                parent=self.root,
            )
            if not ok:
                return

        self.action_scope_item_ids = [item.id for item in scope_items]
        self.status.config(text=f"准备刮削: {scope_label}")
        threading.Thread(
            target=self.run_scrape_execution, daemon=True
        ).start()

    def run_execution(self, run_mode):
        """执行重命名"""
        return worker_run_execution(self, run_mode)

    def run_scrape_execution(self):
        """执行独立刮削"""
        return worker_run_scrape_execution(self)

    def process_one_file(self, item, run_mode):
        """处理单个文件"""
        return worker_process_one_file(self, item, run_mode)

    def process_one_file_scrape(self, item):
        """单独刮削单个文件"""
        return worker_process_one_file_scrape(self, item)


if __name__ == "__main__":
    root = tk.Tk()
    app = MediaRenamerGUI(root)
    root.mainloop()

