"""高级设置与 API 配置对话框（PySide6）。"""

from __future__ import annotations

import threading

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QRadioButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
    QButtonGroup,
)

from core.services.matcher_service import list_ollama_models
from ui_qt.widgets import NoWheelComboBox


class SettingsDialog(QDialog):
    def __init__(self, gui):
        super().__init__(gui._qt_root)
        self._gui = gui
        self.setWindowTitle("高级设置与 API 配置")
        self.setMinimumSize(760, 620)
        self.resize(860, 760)

        saved = gui.get_saved_geometry("settings_window_geometry", min_width=760, min_height=620)
        if saved:
            parsed = gui._parse_geometry(saved)
            if parsed:
                w, h, x, y = parsed
                self.setGeometry(x, y, w, h)

        outer = QVBoxLayout(self)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.NoFrame)
        form_host = QWidget()
        form_host.setObjectName("FormHost")
        self._form = QFormLayout(form_host)
        self._form.setLabelAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self._form.setFormAlignment(Qt.AlignTop)
        self._form.setHorizontalSpacing(12)
        self._form.setVerticalSpacing(8)
        scroll.setWidget(form_host)
        outer.addWidget(scroll, 1)

        self._sf_test_label = QLabel("")
        self._sf_test_label.setWordWrap(True)
        self._ollama_status_label = QLabel("")
        self._ollama_status_label.setWordWrap(True)

        self._ollama_model_combo = NoWheelComboBox()
        self._embedding_model_combo = NoWheelComboBox()
        for combo in (self._ollama_model_combo, self._embedding_model_combo):
            combo.setEditable(True)
            combo.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
            combo.setMinimumWidth(320)
            combo.setSizeAdjustPolicy(
                QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon
            )
            combo.setMinimumContentsLength(24)

        self._build_fields()
        self._seed_ollama_combos()
        self._refresh_ollama_models(show_message=False)

        bar = QHBoxLayout()
        bar.addStretch(1)
        btn_save = QPushButton("保存并生效 (无需重启)")
        btn_save.setObjectName("success")
        btn_save.clicked.connect(self._save_and_close)
        bar.addWidget(btn_save)
        outer.addLayout(bar)

        self.finished.connect(self._remember_geometry)

    def _remember_geometry(self, *_args):
        try:
            g = self.geometry()
            geo = f"{g.width()}x{g.height()}+{g.x()}+{g.y()}"
            self._gui.remember_window_geometry(
                "settings_window_geometry",
                geo,
                min_width=760,
                min_height=620,
            )
        except Exception:
            pass

    def _bind_entry(self, var, password=False) -> QLineEdit:
        e = QLineEdit(str(var.get() or ""))
        if password:
            e.setEchoMode(QLineEdit.Password)
        e.textChanged.connect(var.set)
        return e

    def _add_password_row(self, label: str, var):
        row = QHBoxLayout()
        entry = self._bind_entry(var, password=True)
        btn = QPushButton("显示")
        btn.setFixedWidth(56)

        def toggle():
            if entry.echoMode() == QLineEdit.Password:
                entry.setEchoMode(QLineEdit.Normal)
                btn.setText("隐藏")
            else:
                entry.setEchoMode(QLineEdit.Password)
                btn.setText("显示")

        btn.clicked.connect(toggle)
        row.addWidget(entry, 1)
        row.addWidget(btn)
        wrap = QWidget()
        wrap.setLayout(row)
        self._form.addRow(label, wrap)
        return entry

    def _build_fields(self):
        g = self._gui
        self._add_password_row("TMDb API Key:", g.tmdb_api_key)
        self._add_password_row("BGM API Key:", g.bgm_api_key)
        self._add_password_row("Silicon AI Key (备选):", g.sf_api_key)

        sf_url_row = QHBoxLayout()
        sf_url = self._bind_entry(g.sf_api_url)
        btn_test = QPushButton("测试连接")
        btn_test.setObjectName("info")
        btn_test.clicked.connect(self._test_silicon)
        sf_url_row.addWidget(sf_url, 1)
        sf_url_row.addWidget(btn_test)
        w = QWidget()
        w.setLayout(sf_url_row)
        self._form.addRow("API URL (OpenAI兼容):", w)
        self._form.addRow("", self._sf_test_label)

        self._form.addRow("模型名称:", self._bind_entry(g.sf_model))
        self._form.addRow("AI 温度 temperature (0-2):", self._bind_entry(g.ai_temperature))
        self._form.addRow("AI top_p (0-1):", self._bind_entry(g.ai_top_p))

        mode_wrap = QWidget()
        mode_wrap.setObjectName("AiModeRow")
        mode_lay = QHBoxLayout(mode_wrap)
        mode_lay.setContentsMargins(0, 0, 0, 0)
        mode_lay.setSpacing(20)
        grp = QButtonGroup(mode_wrap)
        modes = [("disabled", "禁用"), ("assist", "辅助识别"), ("force", "强制使用")]
        current = str(g.ai_mode.get() or "assist")
        for val, text in modes:
            rb = QRadioButton(text)
            rb.setChecked(current == val)
            rb.toggled.connect(lambda checked, v=val: checked and g.ai_mode.set(v))
            grp.addButton(rb)
            mode_lay.addWidget(rb)
        self._form.addRow("AI 识别模式:", mode_wrap)
        hint = QLabel(
            "禁用：只用文件名猜测；辅助识别：标准命名优先用 guessit，番组/弱标题/季集不清晰时自动拉起 AI；"
            "强制使用：只走 AI。"
        )
        hint.setWordWrap(True)
        hint.setObjectName("Subtle")
        self._form.addRow("", hint)

        strip_row = QHBoxLayout()
        strip_row.addWidget(self._bind_entry(g.strip_keywords_var), 1)
        strip_row.addWidget(QLabel("多个关键词可用 | 或逗号分隔"))
        sw = QWidget()
        sw.setLayout(strip_row)
        self._form.addRow("剔除关键词:", sw)

        cb_suffix = QCheckBox("保留媒体信息后缀（如 2160p.TVING.WEB-DL.H.265.AAC-ColorTV）")
        cb_suffix.setChecked(bool(g.preserve_media_suffix.get()))
        cb_suffix.toggled.connect(lambda v: g.preserve_media_suffix.set(v))
        self._form.addRow("", cb_suffix)

        ollama_row = QHBoxLayout()
        ollama_row.addWidget(self._bind_entry(g.ollama_url), 1)
        btn_refresh = QPushButton("刷新模型")
        btn_refresh.clicked.connect(lambda: self._refresh_ollama_models(show_message=True))
        ollama_row.addWidget(btn_refresh)
        ow = QWidget()
        ow.setLayout(ollama_row)
        self._form.addRow("Ollama URL:", ow)

        self._ollama_model_combo.setCurrentText(str(g.ollama_model.get() or ""))
        self._ollama_model_combo.currentTextChanged.connect(g.ollama_model.set)
        self._form.addRow("Ollama 模型:", self._ollama_model_combo)

        self._embedding_model_combo.setCurrentText(str(g.embedding_model.get() or ""))
        self._embedding_model_combo.currentTextChanged.connect(g.embedding_model.set)
        self._form.addRow("Embedding 模型:", self._embedding_model_combo)
        self._form.addRow("", self._ollama_status_label)

        cb_ollama = QCheckBox("优先使用本地 Ollama (失败后自动尝试 SiliconFlow)")
        cb_ollama.setChecked(bool(g.prefer_ollama.get()))
        cb_ollama.toggled.connect(lambda v: g.prefer_ollama.set(v))
        self._form.addRow("", cb_ollama)

        cb_emb = QCheckBox("启用 Embedding 候选重排 (提升多候选识别率)")
        cb_emb.setChecked(bool(g.use_embedding_rank.get()))
        cb_emb.toggled.connect(lambda v: g.use_embedding_rank.set(v))
        self._form.addRow("", cb_emb)

        self._form.addRow("预览并发线程数 (1-10):", self._bind_entry(g.preview_workers))
        self._form.addRow("批量同步并发线程数 (1-10):", self._bind_entry(g.sync_workers))
        self._form.addRow("执行并发线程数 (1-10):", self._bind_entry(g.execution_workers))

        tv_row = QHBoxLayout()
        tv_entry = self._bind_entry(g.tv_format)
        btn_tv = QPushButton("预览")
        btn_tv.clicked.connect(lambda: g._show_filename_template_preview(g.tv_format.get(), is_tv=True))
        tv_row.addWidget(tv_entry, 1)
        tv_row.addWidget(btn_tv)
        tw = QWidget()
        tw.setLayout(tv_row)
        self._form.addRow("剧集 (TV) 格式:", tw)

        mv_row = QHBoxLayout()
        mv_entry = self._bind_entry(g.movie_format)
        btn_mv = QPushButton("预览")
        btn_mv.clicked.connect(lambda: g._show_filename_template_preview(g.movie_format.get(), is_tv=False))
        mv_row.addWidget(mv_entry, 1)
        mv_row.addWidget(btn_mv)
        mw = QWidget()
        mw.setLayout(mv_row)
        self._form.addRow("电影 (Movie) 格式:", mw)

        tpl_hint = QLabel(
            "支持两种写法：旧占位符 {title} / {s:02d} / {ep_name}；高级写法 Jinja {{ title }}。"
            "另外可用 {media_suffix} / {{ media_suffix }}。"
        )
        tpl_hint.setWordWrap(True)
        tpl_hint.setObjectName("Subtle")
        self._form.addRow("", tpl_hint)

        self._form.addRow("视频扩展名 (逗号分隔):", self._bind_entry(g.video_exts))
        self._form.addRow("字幕/音频扩展名 (逗号):", self._bind_entry(g.sub_audio_exts))
        self._form.addRow("语言标签 (竖线|分隔):", self._bind_entry(g.lang_tags))

    def _seed_ollama_combos(self):
        """打开对话框时先用已保存的模型名填充，避免等待 Ollama 响应前下拉框为空。"""
        g = self._gui
        saved = [
            str(g.ollama_model.get() or "").strip(),
            str(g.embedding_model.get() or "").strip(),
        ]
        seed = list(g.ollama_model_options or [])
        for name in saved:
            if name and name not in seed:
                seed.insert(0, name)
        if not seed:
            seed = [n for n in saved if n]
        self._set_combo_items(self._ollama_model_combo, g.ollama_model.get(), seed)
        self._set_combo_items(self._embedding_model_combo, g.embedding_model.get(), seed)

    def _set_combo_items(self, combo: QComboBox, current: str, values: list):
        items = []
        cur = str(current or "").strip()
        if cur:
            items.append(cur)
        for v in values or []:
            t = str(v or "").strip()
            if t and t not in items:
                items.append(t)
        combo.blockSignals(True)
        combo.clear()
        combo.addItems(items)
        if cur:
            combo.setCurrentText(cur)
        combo.blockSignals(False)

    def _apply_ollama_models(self, models, message, show_message=False):
        if models:
            self._gui.ollama_model_options = models
            self._set_combo_items(
                self._ollama_model_combo,
                self._gui.ollama_model.get(),
                models,
            )
            self._set_combo_items(
                self._embedding_model_combo,
                self._gui.embedding_model.get(),
                models,
            )
            self._ollama_status_label.setText(f"已加载 {len(models)} 个本地模型")
        else:
            self._seed_ollama_combos()
            self._ollama_status_label.setText(message or "未能读取本地模型列表")
            if show_message and message:
                QMessageBox.warning(self, "Ollama模型列表", message)

    def _refresh_ollama_models(self, show_message=False):
        self._ollama_status_label.setText("正在读取本地模型列表...")
        url = self._gui.ollama_url.get().strip()

        def worker():
            models, message = list_ollama_models(url)
            self._gui.root.after(
                0,
                lambda m=models, msg=message, sm=show_message: self._apply_ollama_models(
                    m, msg, show_message=sm
                ),
            )

        threading.Thread(target=worker, daemon=True).start()

    def _test_silicon(self):
        from ai.ollama_ai import test_silicon_api

        self._sf_test_label.setText("测试中...")
        api_url = self._gui.sf_api_url.get().strip()
        api_key = self._gui.sf_api_key.get().strip()
        model = self._gui.sf_model.get().strip()

        def worker():
            success, message = test_silicon_api(api_url, api_key, model)
            text = f"✓ {message}" if success else f"✗ {message}"
            self._gui.root.after(0, lambda: self._sf_test_label.setText(text))

        threading.Thread(target=worker, daemon=True).start()

    def _save_and_close(self):
        self._remember_geometry()
        self._gui.save_config(show_message=True)
        self.accept()
