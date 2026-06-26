"""PySide6 版主窗口。

继承 MediaRenamerGUI 复用全部业务方法（识别/匹配/执行/刮削），仅重写 UI 构建与
视图更新相关方法，底层控件走 PySide6 + ui_qt 适配层，使 worker 代码零改动。

阶段 2：设置对话框、手动匹配弹窗、季偏移、完整右键菜单均已接入 PySide6。
"""

from __future__ import annotations

import os
import threading

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QIcon, QKeySequence, QPixmap, QShortcut
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QSplitter,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
    QFrame,
    QButtonGroup,
    QHeaderView,
    QGraphicsDropShadowEffect,
)

from core.renamer_core import MediaRenamerCore
from ui_qt.adapters import (
    ButtonAdapter,
    ProgressAdapter,
    RootAdapter,
    StatusAdapter,
    TreeAdapter,
    VarAdapter,
    DISABLED,
)
from ui_qt.theme import apply_theme, is_system_dark
from ui_qt.widgets import NoWheelComboBox


class MediaRenamerGUIQt(MediaRenamerCore):
    """PySide6 主窗口，与 MediaRenamerGUI 接口兼容。"""

    def __init__(self, qmain_window: QMainWindow):
        # 不调用 super().__init__（它会建 tkinter 控件），自行初始化状态。
        self._qt_root = qmain_window
        self.root = RootAdapter(qmain_window)
        self.bootstrap_style = None

        self.file_list = []
        self.item_by_id = {}
        self.dir_cache = {}
        self.db_cache = {}
        self.manual_locks = {}
        self.forced_seasons = {}
        self.forced_offsets = {}
        self.dir_parse_events = {}
        self.db_resolution_events = {}
        self.cache_lock = threading.Lock()
        self.file_write_lock = threading.Lock()
        self.popup_lock = threading.Lock()
        self.preview_skip_all_event = threading.Event()
        self.preview_skip_dirs = set()
        self.expanded_groups = set()
        self._item_seq = 0
        self.action_scope_item_ids = []
        self.remote_ai_cooldown_until = 0.0
        self.ai_retry_inflight = set()

        self.config = self.load_config()

        # 配置变量全部用 VarAdapter（worker 通过 .get() 读）
        self.target_root = VarAdapter("")
        self.sf_api_key = VarAdapter(self.config.get("sf_api_key", ""))
        self.sf_api_url = VarAdapter(self.config.get("sf_api_url", "https://api.siliconflow.cn/v1"))
        self.sf_model = VarAdapter(self.config.get("sf_model", "deepseek-ai/DeepSeek-V3"))
        self.ai_temperature = VarAdapter(f"{self._clamp_temperature(self.config.get('ai_temperature'), 0.2):.2f}")
        self.ai_top_p = VarAdapter(f"{self._clamp_top_p(self.config.get('ai_top_p'), 0.9):.2f}")
        self.ai_mode = VarAdapter(self.config.get("ai_mode", "assist"))
        self.bgm_api_key = VarAdapter(self.config.get("bgm_api_key", ""))
        self.tmdb_api_key = VarAdapter(self.config.get("tmdb_api_key", ""))
        self.tv_format = VarAdapter(self.config.get("tv_format", self._default_tv_format()))
        self.movie_format = VarAdapter(self.config.get("movie_format", self._default_movie_format()))
        self.preserve_media_suffix = VarAdapter(self.config.get("preserve_media_suffix", False))
        self.video_exts = VarAdapter(self.config.get("video_exts", self._default_video_exts()))
        self.sub_audio_exts = VarAdapter(self.config.get("sub_audio_exts", self._default_sub_audio_exts()))
        self.lang_tags = VarAdapter(self.config.get("lang_tags", self._default_lang_tags()))
        self.strip_keywords_var = VarAdapter(
            self._normalize_strip_keywords_text(self.config.get("strip_keywords", []))
        )
        self.ollama_url = VarAdapter(self.config.get("ollama_url", "http://localhost:11434"))
        self.ollama_model = VarAdapter(self.config.get("ollama_model", ""))
        self.embedding_model = VarAdapter(self.config.get("embedding_model", ""))
        self.prefer_ollama = VarAdapter(self.config.get("prefer_ollama", False))
        self.use_embedding_rank = VarAdapter(self.config.get("use_embedding_rank", True))
        self.preview_workers = VarAdapter(str(self._clamp_workers(self.config.get("preview_workers"), 1)))
        self.sync_workers = VarAdapter(str(self._clamp_workers(self.config.get("sync_workers"), 5)))
        self.execution_workers = VarAdapter(str(self._clamp_workers(self.config.get("execution_workers"), 5)))
        self.media_type_override = VarAdapter(self.config.get("media_type_override", "自动判断"))
        self.source_var = VarAdapter("siliconflow_tmdb")
        self.view_mode = VarAdapter("group")
        self.detail_left_var = VarAdapter("")
        self.detail_right_var = VarAdapter("")
        self.embedding_cache = {}
        self.ollama_embed_endpoint = None
        self.ollama_model_options = []
        self.colors = {}

        # 主题
        self._dark = is_system_dark()
        apply_theme(self._dark)
        self._apply_app_icon()
        self._build_ui()
        self.apply_saved_window_geometry()
        qmain_window.closeEvent = self._on_close_event
        qmain_window.setAcceptDrops(True)
        qmain_window.dragEnterEvent = self._on_drag_enter
        qmain_window.dropEvent = self._on_drop

    # ---- 默认值辅助（避免循环 import helpers 常量） ----
    def _default_tv_format(self):
        from utils.helpers import DEFAULT_TV_FORMAT
        return DEFAULT_TV_FORMAT

    def _default_movie_format(self):
        from utils.helpers import DEFAULT_MOVIE_FORMAT
        return DEFAULT_MOVIE_FORMAT

    def _default_video_exts(self):
        from utils.helpers import DEFAULT_VIDEO_EXTS
        return DEFAULT_VIDEO_EXTS

    def _default_sub_audio_exts(self):
        from utils.helpers import DEFAULT_SUB_AUDIO_EXTS
        return DEFAULT_SUB_AUDIO_EXTS

    def _default_lang_tags(self):
        from utils.helpers import DEFAULT_LANG_TAGS
        return DEFAULT_LANG_TAGS

    # ---- 样式与图标 ----
    def _apply_cosmo_styles(self):
        # tkinter 样式方法，Qt 版用 QSS，空实现
        self.colors = {"bg": "#ffffff", "text": "#1f2937", "primary": "#2563eb"}

    def _apply_app_icon(self):
        from utils.helpers import resource_path

        icon_path = resource_path("assets", "app_icon.ico")
        if os.path.exists(icon_path):
            try:
                self._qt_root.setWindowIcon(QIcon(icon_path))
            except Exception:
                pass

    # ---- UI 构建 ----
    def _build_ui(self):
        win = self._qt_root
        win.setWindowTitle("媒体归档刮削助手 v3.5")
        win.resize(1300, 900)

        central = QWidget()
        win.setCentralWidget(central)
        outer = QHBoxLayout(central)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        # 左侧边栏
        sidebar = self._build_sidebar()
        outer.addWidget(sidebar, 0)

        # 主区（垂直：工具栏 + splitter(tree+详情) + 底部操作栏）
        main_area = QWidget()
        main_layout = QVBoxLayout(main_area)
        main_layout.setContentsMargins(12, 12, 12, 12)
        main_layout.setSpacing(8)

        toolbar = self._build_toolbar()
        main_layout.addWidget(toolbar)

        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(self._build_tree_area())
        splitter.addWidget(self._build_detail_area())
        splitter.setStretchFactor(0, 4)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([620, 180])
        main_layout.addWidget(splitter, 1)

        main_layout.addWidget(self._build_bottom_bar())
        outer.addWidget(main_area, 1)

        # 快捷键
        QShortcut(QKeySequence("Ctrl+A"), win, activated=self.select_all_files)

    def _build_sidebar(self) -> QWidget:
        side = QFrame()
        side.setObjectName("Sidebar")
        side.setFixedWidth(210)
        layout = QVBoxLayout(side)
        layout.setContentsMargins(12, 16, 12, 12)
        layout.setSpacing(14)

        brand = QWidget()
        brand_row = QHBoxLayout(brand)
        brand_row.setContentsMargins(0, 0, 0, 2)
        brand_row.setSpacing(10)
        logo = QLabel()
        from utils.helpers import resource_path
        logo_path = resource_path("assets", "app_icon.png")
        if os.path.exists(logo_path):
            logo.setPixmap(
                QPixmap(logo_path).scaled(
                    32, 32, Qt.KeepAspectRatio, Qt.SmoothTransformation
                )
            )
        brand_row.addWidget(logo)
        title = QLabel("媒体归档\n刮削助手")
        title.setObjectName("BrandTitle")
        title.setWordWrap(True)
        brand_row.addWidget(title, 1)
        layout.addWidget(brand)

        # 数据源
        src_group = QButtonGroup(side)
        r1 = QRadioButton("AI + TMDb")
        r1.setChecked(True)
        r2 = QRadioButton("AI + BGM (推荐)")
        src_group.addButton(r1)
        src_group.addButton(r2)
        src_label = QLabel("数据源")
        src_label.setObjectName("Subtle")
        layout.addWidget(src_label)
        src_wrap = QWidget()
        src_lay = QVBoxLayout(src_wrap)
        src_lay.setContentsMargins(0, 0, 0, 0)
        src_lay.setSpacing(6)
        src_lay.addWidget(r1)
        src_lay.addWidget(r2)
        layout.addWidget(src_wrap)

        def _on_src(btn):
            self.source_var.set("siliconflow_tmdb" if btn is r1 else "siliconflow_bgm")

        r1.toggled.connect(lambda checked: checked and _on_src(r1))
        r2.toggled.connect(lambda checked: checked and _on_src(r2))

        # 媒体类型
        type_label = QLabel("媒体类型")
        type_label.setObjectName("Subtle")
        layout.addWidget(type_label)
        type_combo = NoWheelComboBox()
        type_combo.addItems(["自动判断", "电影", "电视剧"])
        saved_type = str(self.media_type_override.get() or "自动判断")
        idx = type_combo.findText(saved_type)
        if idx >= 0:
            type_combo.setCurrentIndex(idx)
        type_combo.currentTextChanged.connect(self.media_type_override.set)
        layout.addWidget(type_combo)

        # 归档根目录
        root_label = QLabel("归档根目录")
        root_label.setObjectName("Subtle")
        layout.addWidget(root_label)
        root_row = QHBoxLayout()
        root_entry = QLineEdit()
        root_entry.textChanged.connect(self.target_root.set)
        root_row.addWidget(root_entry, 1)
        root_btn = QPushButton("选择")
        root_btn.clicked.connect(lambda: self._pick_directory(root_entry))
        root_row.addWidget(root_btn)
        layout.addLayout(root_row)

        layout.addStretch(1)

        # 主题切换
        self._theme_btn = QPushButton("☀ 浅色模式" if self._dark else "🌙 深色模式")
        self._theme_btn.setObjectName("ghost")
        self._theme_btn.clicked.connect(self._toggle_theme)
        layout.addWidget(self._theme_btn)

        # 底部按钮
        btn_settings = QPushButton("设置 / API")
        btn_settings.clicked.connect(self.open_settings)
        layout.addWidget(btn_settings)

        btn_clear = QPushButton("清空列表")
        btn_clear.setObjectName("danger")
        btn_clear.clicked.connect(self.clear_list)
        layout.addWidget(btn_clear)

        self._soft_shadow(side, blur=24, alpha=28, dx=2, dy=0)
        return side

    def _soft_shadow(self, widget, blur=18, alpha=36, dx=0, dy=3):
        """给无滚动容器加柔和阴影提升层次（不用于 QTreeWidget 等滚动控件，避免渲染坑）。"""
        try:
            eff = QGraphicsDropShadowEffect(widget)
            eff.setBlurRadius(blur)
            eff.setColor(QColor(15, 23, 42, alpha))
            eff.setOffset(dx, dy)
            widget.setGraphicsEffect(eff)
        except Exception:
            pass

    def _toggle_theme(self):
        self._dark = not self._dark
        apply_theme(self._dark)
        if getattr(self, "tree", None) is not None:
            self.tree._dark = self._dark
        self._theme_btn.setText("☀ 浅色模式" if self._dark else "🌙 深色模式")
        self.refresh_tree_view()

    def _on_drag_enter(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def _on_drop(self, event):
        paths = [u.toLocalFile() for u in event.mimeData().urls() if u.toLocalFile()]
        if not paths:
            event.ignore()
            return
        self._add_dropped_paths(paths)
        event.acceptProposedAction()

    def _add_dropped_paths(self, paths):
        exts = self.get_media_exts()
        count = 0
        for p in paths:
            if os.path.isdir(p):
                for root_dir, _, files in os.walk(p):
                    for fn in files:
                        if fn.lower().endswith(exts):
                            if self._add(
                                os.path.join(root_dir, fn),
                                source_path=p,
                                organize_root=self._default_organize_root(p),
                                refresh=False,
                            ):
                                count += 1
            elif os.path.isfile(p):
                d = os.path.dirname(p)
                if self._add(p, source_path=d, organize_root=d, refresh=False):
                    count += 1
        if count:
            self.refresh_tree_view()
            self.status.config(text=f"已拖入 {count} 个文件")

    def _pick_directory(self, entry: QLineEdit):
        folder = QFileDialog.getExistingDirectory(self._qt_root, "选择目录")
        if folder:
            entry.setText(folder)

    def _build_toolbar(self) -> QWidget:
        bar = QWidget()
        lay = QHBoxLayout(bar)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(8)
        b_add_files = QPushButton("添加文件")
        b_add_files.setObjectName("primary")
        b_add_files.clicked.connect(self.add_files)
        b_add_folder = QPushButton("添加文件夹")
        b_add_folder.setObjectName("primary")
        b_add_folder.clicked.connect(self.add_folder)
        lay.addWidget(b_add_files)
        lay.addWidget(b_add_folder)
        lay.addStretch(1)
        return bar

    def _build_tree_area(self) -> QWidget:
        wrap = QWidget()
        lay = QVBoxLayout(wrap)
        lay.setContentsMargins(0, 0, 0, 0)
        tree = QTreeWidget()
        tree.setHeaderLabels([
            "添加路径 / Season / 原文件名",
            "识别标题",
            "匹配 ID",
            "新文件名 / 归档路径",
            "状态",
        ])
        header = tree.header()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(QHeaderView.Interactive)
        tree.setColumnWidth(0, 320)
        tree.setColumnWidth(1, 220)
        tree.setColumnWidth(2, 90)
        tree.setColumnWidth(3, 420)
        tree.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        tree.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        tree.setAlternatingRowColors(True)
        tree.setSelectionMode(QTreeWidget.ExtendedSelection)
        # 兼容对象
        self.tree = TreeAdapter(tree, dark=self._dark)
        self._qtree = tree

        tree.itemSelectionChanged.connect(self.update_details_panel)
        tree.itemExpanded.connect(lambda it: self._on_expanded(it))
        tree.itemCollapsed.connect(lambda it: self._on_collapsed(it))
        tree.setContextMenuPolicy(Qt.CustomContextMenu)
        tree.customContextMenuRequested.connect(self._on_context_menu_pos)

        lay.addWidget(tree)
        return wrap

    def _on_expanded(self, it: QTreeWidgetItem):
        iid = self._iid_from_item(it)
        if iid and self.is_group_row(iid):
            self.expanded_groups.add(iid)

    def _on_collapsed(self, it: QTreeWidgetItem):
        iid = self._iid_from_item(it)
        if iid and self.is_group_row(iid):
            self.expanded_groups.discard(iid)

    def _iid_from_item(self, it: QTreeWidgetItem) -> str:
        for iid, mapped in self.tree._item_map.items():
            if mapped is it:
                return iid
        return ""

    def _on_context_menu_pos(self, pos):
        from ui_qt.manual_match_qt import show_context_menu
        global_pos = self._qtree.viewport().mapToGlobal(pos)
        show_context_menu(self, global_pos)

    def _build_detail_area(self) -> QWidget:
        wrap = QFrame()
        wrap.setObjectName("DetailCard")
        lay = QHBoxLayout(wrap)
        lay.setContentsMargins(16, 12, 16, 12)
        lay.setSpacing(16)
        self.detail_left_label = QLabel("")
        self.detail_left_label.setWordWrap(True)
        self.detail_left_label.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        self.detail_left_label.setObjectName("DetailBody")
        self.detail_right_label = QLabel("")
        self.detail_right_label.setWordWrap(True)
        self.detail_right_label.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        self.detail_right_label.setObjectName("DetailBody")
        sep = QFrame()
        sep.setObjectName("DetailSep")
        sep.setFrameShape(QFrame.VLine)
        sep.setFixedWidth(1)
        lay.addWidget(self.detail_left_label, 1)
        lay.addWidget(sep)
        lay.addWidget(self.detail_right_label, 1)
        self._set_details_content("当前没有选中任何分组或文件。", "")
        return wrap

    def _build_bottom_bar(self) -> QWidget:
        bar = QWidget()
        lay = QHBoxLayout(bar)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(8)

        def _action_btn(text, obj, slot):
            b = QPushButton(text)
            b.setObjectName(obj)
            b.clicked.connect(slot)
            return b

        self.btn_pre = ButtonAdapter(_action_btn("1. 高速识别预览", "primary", self.start_preview))
        lay.addWidget(self.btn_pre._btn)
        lay.addWidget(_action_btn("2. 原地重命名", "secondary", lambda: self.start_run_logic("rename")))
        lay.addWidget(_action_btn("3. 归档移动", "info", lambda: self.start_run_logic("archive")))
        lay.addWidget(_action_btn("4. 原地整理", "success", lambda: self.start_run_logic("organize")))
        lay.addWidget(_action_btn("5. 刮削", "warning", self.start_scrape_logic))

        self._qbar = QProgressBar()
        self._qbar.setTextVisible(True)
        self._qbar.setFormat("%v / %m")
        self.pbar = ProgressAdapter(self._qbar)
        lay.addWidget(self._qbar, 1)

        self._qstatus = QLabel("就绪")
        self._qstatus.setObjectName("Subtle")
        self.status = StatusAdapter(self._qstatus)
        lay.addWidget(self._qstatus)
        return bar

    # ---- 视图更新（覆盖 tkinter 版） ----
    def refresh_tree_view(self, preserve_selection=True):
        selected_ids = set(self.get_selected_file_ids()) if preserve_selection else set()
        focused = self.tree.focus() if preserve_selection else ""

        self.tree.clear()
        self.expanded_groups  # noqa

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

        END = "end"
        for group_path in group_order:
            group_iid = self._source_row_id(group_path)
            self.tree.insert(
                "", END, iid=group_iid, text=group_path,
                open=True,
                tags=("source",), values=("", "", "", ""),
            )
            self.expanded_groups.add(group_iid)
            if self._use_flat_source_layout(group_path):
                for item in self._group_items(group_path):
                    self.tree.insert(
                        group_iid, END, iid=item.id, text=item.old_name,
                        tags=("file",), values=self._item_values(item),
                    )
                continue
            for season_label, items in self._season_groups_for_source(group_path):
                season_iid = self._season_row_id(group_path, season_label)
                self.tree.insert(
                    group_iid, END, iid=season_iid, text=season_label,
                    open=True,
                    tags=("season",), values=("", "", "", ""),
                )
                self.expanded_groups.add(season_iid)
                for item in items:
                    self.tree.insert(
                        season_iid, END, iid=item.id, text=item.old_name,
                        tags=("file",), values=self._item_values(item),
                    )

        existing_selected = [item_id for item_id in selected_ids if self.tree.exists(item_id)]
        if existing_selected:
            self.tree.selection_set(existing_selected)
            self.tree.focus(existing_selected[0])
        elif focused and self.tree.exists(focused):
            self.tree.focus(focused)
        elif self.file_list:
            self._select_default_tree_row()
        self.update_details_panel()

    def _select_default_tree_row(self):
        """无选中项时默认选中第一个分组，便于详情区展示汇总。"""
        if not self.file_list:
            return
        group_path = self.file_list[0].source_path or self.file_list[0].dir
        row_id = self._source_row_id(group_path)
        if self.tree.exists(row_id):
            self.tree.selection_set(row_id)
            self.tree.focus(row_id)

    def refresh_item_row(self, item_id):
        item = self.get_item_by_id(item_id)
        if not item or not self.tree.exists(item.id):
            return
        self.tree.item(item.id, text=item.old_name, values=self._item_values(item))
        if self.tree.selection():
            self.update_details_panel()

    def _build_overview_details(self):
        total = len(self.file_list)
        recognized = sum(1 for it in self.file_list if it.metadata.get("id") != "None")
        pending = sum(
            1 for it in self.file_list
            if it.status_text in ("待命", "识别中", "") or it.metadata.get("id") == "None"
        )
        left = (
            f"当前列表共 {total} 个文件。\n\n"
            f"已识别: {recognized}\n"
            f"待处理/未识别: {pending}\n\n"
            "提示：点击树中的分组或文件行可查看详细识别结果。"
        )
        groups = []
        seen = set()
        for it in self.file_list:
            gp = it.source_path or it.dir
            if gp not in seen:
                seen.add(gp)
                groups.append(gp)
        if groups:
            preview = "\n".join(f"• {gp}" for gp in groups[:6])
            if len(groups) > 6:
                preview += f"\n• ... 共 {len(groups)} 个分组"
            right = f"添加路径:\n{preview}"
        else:
            right = ""
        return left, right

    def _on_preview_finished(self):
        self._reset_progress_bar()
        self._select_default_tree_row()
        self.update_details_panel()

    def _reset_progress_bar(self):
        super()._reset_progress_bar()
        self._qbar.setFormat("")

    def _set_details_content(self, left_text, right_text):
        self.detail_left_var.set((left_text or "").strip())
        self.detail_right_var.set((right_text or "").strip())
        self.detail_left_label.setText((left_text or "").strip())
        self.detail_right_label.setText((right_text or "").strip())

    def update_details_panel(self, _event=None):
        selection = self.tree.selection()
        if not selection:
            if self.file_list:
                self._set_details_content(*self._build_overview_details())
            else:
                self._set_details_content("当前没有选中任何分组或文件。", "")
            return
        row_id = selection[0]
        if self.is_source_row(row_id):
            self._set_details_content(*self._build_group_details(self.source_path_from_row_id(row_id)))
            return
        if self.is_season_row(row_id):
            self._set_details_content(
                *self._build_season_group_details(
                    self.source_path_from_row_id(row_id), self.season_key_from_row_id(row_id)
                )
            )
            return
        item = self.get_item_by_id(row_id)
        if not item:
            self._set_details_content("当前选中项已失效，请重新选择。", "")
            return
        self._set_details_content(*self._build_item_details(item))

    def on_treeview_open(self, _event=None):
        pass

    def on_treeview_close(self, _event=None):
        pass

    def toggle_group_row(self, row_id):
        if not self.is_group_row(row_id) or not self.tree.exists(row_id):
            return
        new_state = not bool(self.tree.item(row_id, "open"))
        self.tree.item(row_id, open=new_state)
        if new_state:
            self.expanded_groups.add(row_id)
        else:
            self.expanded_groups.discard(row_id)

    # ---- 启动按钮（messagebox 用 QMessageBox） ----
    def _warn(self, title, msg):
        QMessageBox.warning(self._qt_root, title, msg)

    def _show_error(self, title, msg):
        QMessageBox.critical(self._qt_root, title, msg)

    def _ask_ok(self, title, msg) -> bool:
        return QMessageBox.Ok == QMessageBox.question(
            self._qt_root, title, msg,
            QMessageBox.Ok | QMessageBox.Cancel, QMessageBox.Ok,
        )

    def start_preview(self):
        if not self.file_list:
            self._warn("警告", "请先添加文件")
            return
        scope_indices, scope_items, scope_label = self._resolve_current_action_scope()
        if not scope_items:
            self._warn("警告", "当前作用域内没有可处理的文件")
            return
        ai_mode = str(self.ai_mode.get() or "assist").strip().lower()
        if ai_mode == "force" and not self._has_ai_backend_configured():
            self._warn("AI配置不完整", "当前选择了强制使用AI，但尚未配置可用的 Ollama 或 OpenAI 兼容接口。")
            return
        self.action_scope_item_ids = [item.id for item in scope_items]
        self.btn_pre.config(state=DISABLED)
        self.pbar["value"] = 0
        self.status.config(text=f"识别中: {scope_label}")
        self.preview_skip_all_event.clear()
        self.preview_skip_dirs.clear()
        threading.Thread(target=self.run_preview_pool, daemon=True).start()

    def start_run_logic(self, run_mode):
        if not self.file_list:
            return
        _si, scope_items, scope_label = self._resolve_current_action_scope()
        if not scope_items:
            self._warn("警告", "当前作用域内没有可处理的文件")
            return
        unprocessed = sum(1 for item in scope_items if item.metadata.get("id") == "None")
        if unprocessed:
            op_map = {"rename": "重命名", "archive": "归档", "organize": "原地整理"}
            op = op_map.get(run_mode, "处理")
            if not self._ask_ok("部分文件未预览",
                                f"有 {unprocessed} 个文件尚未完成识别预览，将被跳过，其余已识别的文件正常{op}。\n\n是否继续？"):
                return
        self.action_scope_item_ids = [item.id for item in scope_items]
        self.status.config(text=f"准备处理: {scope_label}")
        threading.Thread(target=self.run_execution, args=(run_mode,), daemon=True).start()

    def start_scrape_logic(self):
        if not self.file_list:
            return
        _si, scope_items, scope_label = self._resolve_current_action_scope()
        if not scope_items:
            self._warn("警告", "当前作用域内没有可处理的文件")
            return
        unprocessed = sum(1 for item in scope_items if item.metadata.get("id") == "None")
        if unprocessed:
            if not self._ask_ok("部分文件未预览",
                                f"有 {unprocessed} 个文件尚未完成识别预览，将被跳过刮削，其余已识别的文件正常刮削。\n\n是否继续？"):
                return
        self.action_scope_item_ids = [item.id for item in scope_items]
        self.status.config(text=f"准备刮削: {scope_label}")
        threading.Thread(target=self.run_scrape_execution, daemon=True).start()

    # ---- 设置 / 手动匹配 / 模板预览（PySide6） ----
    def open_settings(self):
        from ui_qt.settings_dialog import SettingsDialog
        dlg = SettingsDialog(self)
        dlg.exec()

    def _request_manual_candidate_choice(
        self, item, query_title, source_name, candidates, recognized_title=None
    ):
        from ui_qt.manual_match_qt import request_manual_candidate_choice
        return request_manual_candidate_choice(
            self, item, query_title, source_name, candidates, recognized_title
        )

    def manual_match(self):
        from ui_qt.manual_match_qt import manual_match as qt_manual_match
        return qt_manual_match(self)

    def _async_manual_match_search(self, selected_ids, user_input, mode):
        from core.services.manual_search_service import async_manual_match_search
        return async_manual_match_search(self, selected_ids, user_input, mode)

    def _show_manual_match_results(self, selected_ids, results, error_msg=""):
        from ui_qt.manual_match_qt import show_manual_match_results
        return show_manual_match_results(self, selected_ids, results, error_msg)

    def _confirm_season_and_dispatch(self, selected_ids, title, tid, msg, meta):
        from ui_qt.manual_match_qt import confirm_season_and_dispatch
        return confirm_season_and_dispatch(self, selected_ids, title, tid, msg, meta)

    def show_context_menu(self, event):
        # tkinter 版接口保留；Qt 走 _on_context_menu_pos
        pass

    def _show_filename_template_preview(self, template, is_tv=True, parent=None):
        template_text = str(template or "").strip()
        if not template_text:
            QMessageBox.information(
                self._qt_root, "模板预览", "请先输入文件名模板。"
            )
            return

        sample = {
            "title": "正年" if is_tv else "流媒体示例电影",
            "year": "2024",
            "season": "01",
            "episode": "01",
            "ep_name": "无法城市" if is_tv else "",
            "ext": ".strm" if is_tv else ".mkv",
            "source_filename": (
                "正年.S01E01.2160p.TVING.WEB-DL.H265.AAC-ZeroTV.strm"
                if is_tv
                else "流媒体示例电影.2024.2160p.TVING.WEB-DL.H265.AAC-ZeroTV.mkv"
            ),
            "pure_name": (
                "正年.S01E01.2160p.TVING.WEB-DL.H265.AAC-ZeroTV"
                if is_tv
                else "流媒体示例电影.2024.2160p.TVING.WEB-DL.H265.AAC-ZeroTV"
            ),
        }
        try:
            preview_name, media_suffix = self._render_media_filename(
                template_text,
                title=sample["title"],
                year=sample["year"],
                season=sample["season"],
                episode=sample["episode"],
                ep_name=sample["ep_name"],
                ext=sample["ext"],
                source_filename=sample["source_filename"],
                pure_name=sample["pure_name"],
                source_provider="tmdb",
                media_id="119495" if is_tv else "939243",
                is_tv=is_tv,
            )
        except Exception as err:
            QMessageBox.critical(
                self._qt_root, "模板预览失败", f"当前模板无法渲染预览：\n{err}"
            )
            return

        media_suffix_text = media_suffix or "未启用 / 未提取"
        preview_lines = [
            f"模板类型：{'剧集 (TV)' if is_tv else '电影 (Movie)'}",
            f"当前模板：{template_text}",
            "",
            f"预览结果：{preview_name}",
            "",
            "示例变量：",
            f"title = {sample['title']}",
            f"year = {sample['year']}",
            f"season = {sample['season']}",
            f"episode = {sample['episode']}",
            f"ep_name = {sample['ep_name'] or '(空)'}",
            f"ext = {sample['ext']}",
            f"media_suffix = {media_suffix_text}",
            (
                "保留媒体信息后缀 = 开启"
                if self.preserve_media_suffix.get()
                else "保留媒体信息后缀 = 关闭"
            ),
        ]
        QMessageBox.information(self._qt_root, "模板预览", "\n".join(preview_lines))

    # ---- ConfigMixin 覆盖：messagebox / geometry 走 Qt ----
    def save_config(self, show_message=True):
        from utils.helpers import CONFIG_FILE
        import json
        preview_workers = self._clamp_workers(self.preview_workers.get(), 1)
        sync_workers = self._clamp_workers(self.sync_workers.get(), 5)
        execution_workers = self._clamp_workers(self.execution_workers.get(), 5)
        self.preview_workers.set(str(preview_workers))
        self.sync_workers.set(str(sync_workers))
        self.execution_workers.set(str(execution_workers))
        ai_temperature = self._clamp_temperature(self.ai_temperature.get(), 0.2)
        self.ai_temperature.set(f"{ai_temperature:.2f}")
        ai_top_p = self._clamp_top_p(self.ai_top_p.get(), 0.9)
        self.ai_top_p.set(f"{ai_top_p:.2f}")
        config_data = {
            "sf_api_key": self.sf_api_key.get().strip(),
            "sf_api_url": self.sf_api_url.get().strip(),
            "sf_model": self.sf_model.get().strip(),
            "ai_temperature": ai_temperature,
            "ai_top_p": ai_top_p,
            "ai_mode": self.ai_mode.get().strip() or "assist",
            "bgm_api_key": self.bgm_api_key.get().strip(),
            "tmdb_api_key": self.tmdb_api_key.get().strip(),
            "tv_format": self.tv_format.get(),
            "movie_format": self.movie_format.get(),
            "preserve_media_suffix": self.preserve_media_suffix.get(),
            "video_exts": self.video_exts.get(),
            "sub_audio_exts": self.sub_audio_exts.get(),
            "lang_tags": self.lang_tags.get(),
            "strip_keywords": self._get_strip_keywords(),
            "ollama_url": self.ollama_url.get().strip(),
            "ollama_model": self.ollama_model.get().strip(),
            "embedding_model": self.embedding_model.get().strip(),
            "prefer_ollama": self.prefer_ollama.get(),
            "use_embedding_rank": self.use_embedding_rank.get(),
            "preview_workers": preview_workers,
            "sync_workers": sync_workers,
            "execution_workers": execution_workers,
            "media_type_override": self.media_type_override.get(),
            "window_geometry": self.root.winfo_geometry(),
            "settings_window_geometry": self.config.get("settings_window_geometry", ""),
        }
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(config_data, f, indent=4, ensure_ascii=False)
            if show_message:
                QMessageBox.information(self._qt_root, "成功", "所有配置与规则已保存！立即生效。")
        except Exception as err:
            if show_message:
                QMessageBox.critical(self._qt_root, "错误", f"保存失败: {err}")

    def on_close(self):
        try:
            self.save_config(show_message=False)
        except Exception:
            pass
        self.root.destroy()

    def _on_close_event(self, event):
        self.on_close()
        event.accept()

    # add_files/add_folder 用 QFileDialog（覆盖 list_mixin 的 tkinter filedialog）
    def add_files(self):
        files, _ = QFileDialog.getOpenFileNames(self._qt_root, "添加文件", "")
        import os
        count = 0
        for file_path in files:
            source_dir = os.path.dirname(file_path)
            if self._add(file_path, source_path=source_dir, organize_root=source_dir, refresh=False):
                count += 1
        if count:
            self.refresh_tree_view()
            self.status.config(text=f"已添加 {count} 个文件")

    def add_folder(self):
        folder = QFileDialog.getExistingDirectory(self._qt_root, "添加文件夹", "")
        if not folder:
            return
        import os
        exts = self.get_media_exts()
        count = 0
        for root_dir, _, files in os.walk(folder):
            for file_name in files:
                if file_name.lower().endswith(exts):
                    if self._add(
                        os.path.join(root_dir, file_name),
                        source_path=folder,
                        organize_root=self._default_organize_root(folder),
                        refresh=False,
                    ):
                        count += 1
        if count > 0:
            self.refresh_tree_view()
            self.status.config(text=f"已添加 {count} 个文件")
