"""PySide6 主题系统：深/浅色跟随系统 + 手动切换，语义色按钮 QSS。

色板参考原型图：亮色白底浅灰侧栏、深色 #1e1f22/#252627。语义按钮：
primary(蓝)/secondary(灰)/info(青)/success(绿)/warning(橙)/danger(红)。

控件指示器（Radio / CheckBox / ComboBox 箭头）一律用纯 QSS 绘制，不用 SVG
data URI——PyInstaller 单文件 EXE 下 Qt 样式表无法加载 inline 图片。
"""

from __future__ import annotations

from PySide6.QtGui import QPalette, QColor
from PySide6.QtWidgets import QApplication


# 语义色（不随主题变）
SEMANTIC_COLORS = {
    "primary": "#2563eb",
    "primary_hover": "#1d4ed8",
    "secondary": "#6b7280",
    "secondary_hover": "#4b5563",
    "info": "#0d9488",
    "info_hover": "#0f766e",
    "success": "#16a34a",
    "success_hover": "#15803d",
    "warning": "#ea580c",
    "warning_hover": "#c2410c",
    "danger": "#dc2626",
    "danger_hover": "#b91c1c",
}

LIGHT = {
    "window": "#ffffff",
    "sidebar": "#f3f4f6",
    "pane": "#ffffff",
    "border": "#e5e7eb",
    "card_border": "#d7e3f4",
    "text": "#1f2937",
    "subtle": "#6b7280",
    "source_bg": "#eef5ff",
    "season_bg": "#f7fbff",
    "file_bg": "#ffffff",
    "select_bg": "#d9eafb",
    "select_fg": "#16324f",
    "input_bg": "#ffffff",
    "hover": "#f3f4f6",
}

DARK = {
    "window": "#252627",
    "sidebar": "#1e1f22",
    "pane": "#252627",
    "border": "#3a3b3c",
    "card_border": "#3a3b3c",
    "text": "#e5e7eb",
    "subtle": "#9ca3af",
    "source_bg": "#2a2d33",
    "season_bg": "#27282c",
    "file_bg": "#252627",
    "select_bg": "#2f4a6b",
    "select_fg": "#e5e7eb",
    "input_bg": "#2a2b2e",
    "hover": "#333437",
}


def is_system_dark() -> bool:
    app = QApplication.instance()
    if app is None:
        return False
    try:
        scheme = app.styleHints().colorScheme()
        return scheme == Qt.ColorScheme.Dark
    except Exception:
        return False


from PySide6.QtCore import Qt  # noqa: E402


def current_palette(dark: bool) -> dict:
    return dict(DARK if dark else LIGHT)


def theme_colors(dark: bool) -> tuple[dict, dict]:
    """返回 (palette, semantic) 供对话框等动态取色。"""
    return current_palette(dark), SEMANTIC_COLORS


def build_qss(dark: bool) -> str:
    c = current_palette(dark)
    s = SEMANTIC_COLORS
    return f"""
    QWidget {{
        background: {c['window']};
        color: {c['text']};
        font-family: "Microsoft YaHei UI", "Segoe UI", sans-serif;
        font-size: 10pt;
    }}
    QMainWindow, QDialog {{
        background: {c['window']};
    }}
    QScrollArea {{
        background: transparent;
        border: none;
    }}
    QWidget#FormHost, QWidget#AiModeRow {{
        background: transparent;
    }}
    QFrame#Sidebar {{
        background: {c['sidebar']};
        border-right: 1px solid {c['border']};
    }}
    QGroupBox {{
        background: {c['pane']};
        border: 1px solid {c['border']};
        border-radius: 8px;
        margin-top: 10px;
        padding: 8px;
        font-weight: 600;
    }}
    QGroupBox::title {{
        subcontrol-origin: margin;
        left: 10px;
        padding: 0 4px;
        color: {c['text']};
    }}
    QPushButton {{
        background: {c['pane']};
        border: 1px solid {c['border']};
        border-radius: 6px;
        padding: 6px 14px;
        color: {c['text']};
    }}
    QPushButton:hover {{
        background: {c['hover']};
        border-color: {c['subtle']};
    }}
    QPushButton:disabled {{
        color: {c['subtle']};
        background: {c['sidebar']};
    }}
    QPushButton#primary {{ background: {s['primary']}; border: none; color: white; }}
    QPushButton#primary:hover {{ background: {s['primary_hover']}; }}
    QPushButton#primary:disabled {{ background: {c['border']}; color: {c['subtle']}; }}
    QPushButton#secondary {{ background: {s['secondary']}; border: none; color: white; }}
    QPushButton#secondary:hover {{ background: {s['secondary_hover']}; }}
    QPushButton#info {{ background: {s['info']}; border: none; color: white; }}
    QPushButton#info:hover {{ background: {s['info_hover']}; }}
    QPushButton#success {{ background: {s['success']}; border: none; color: white; }}
    QPushButton#success:hover {{ background: {s['success_hover']}; }}
    QPushButton#warning {{ background: {s['warning']}; border: none; color: white; }}
    QPushButton#warning:hover {{ background: {s['warning_hover']}; }}
    QPushButton#danger {{ background: {s['danger']}; border: none; color: white; }}
    QPushButton#danger:hover {{ background: {s['danger_hover']}; }}
    QLineEdit, QComboBox, QSpinBox, QTextEdit {{
        background: {c['input_bg']};
        border: 1px solid {c['border']};
        border-radius: 6px;
        padding: 5px 8px;
        color: {c['text']};
        selection-background-color: {c['select_bg']};
    }}
    QLineEdit:focus, QComboBox:focus, QSpinBox:focus {{
        border-color: {s['primary']};
    }}
    QComboBox QLineEdit {{
        background: transparent;
        border: none;
        padding: 0 4px;
        margin: 0;
    }}
    QComboBox {{
        padding-right: 28px;
    }}
    QComboBox::drop-down {{
        subcontrol-origin: padding;
        subcontrol-position: center right;
        width: 28px;
        border: none;
        border-left: 1px solid {c['border']};
        border-top-right-radius: 5px;
        border-bottom-right-radius: 5px;
        background: transparent;
    }}
    QComboBox::drop-down:hover {{
        background: {c['hover']};
    }}
    QComboBox::down-arrow {{
        image: none;
        width: 0;
        height: 0;
        border-left: 5px solid transparent;
        border-right: 5px solid transparent;
        border-top: 6px solid {c['subtle']};
    }}
    QComboBox::down-arrow:on {{
        margin-top: 1px;
    }}
    QRadioButton {{
        spacing: 8px;
        color: {c['text']};
        background: transparent;
        padding: 2px 0;
    }}
    QRadioButton::indicator {{
        width: 18px;
        height: 18px;
    }}
    QRadioButton::indicator:unchecked {{
        border-radius: 9px;
        border: 2px solid {c['subtle']};
        background: {c['input_bg']};
    }}
    QRadioButton::indicator:checked {{
        border-radius: 9px;
        border: 2px solid {s['primary']};
        background: qradialgradient(
            cx:0.5, cy:0.5, radius:0.5, fx:0.5, fy:0.5,
            stop:0 {s['primary']}, stop:0.42 {s['primary']},
            stop:0.43 {c['input_bg']}, stop:1 {c['input_bg']}
        );
    }}
    QCheckBox {{
        spacing: 8px;
        color: {c['text']};
        background: transparent;
        padding: 2px 0;
    }}
    QCheckBox::indicator {{
        width: 18px;
        height: 18px;
        border-radius: 4px;
    }}
    QCheckBox::indicator:unchecked {{
        border: 2px solid {c['subtle']};
        background: {c['input_bg']};
    }}
    QCheckBox::indicator:checked {{
        border: 2px solid {s['primary']};
        background: {s['primary']};
    }}
    QSpinBox::up-button, QSpinBox::down-button {{
        subcontrol-origin: border;
        width: 18px;
        border-left: 1px solid {c['border']};
        background: transparent;
    }}
    QSpinBox::up-button:hover, QSpinBox::down-button:hover {{
        background: {c['hover']};
    }}
    QSpinBox::up-arrow {{
        image: none;
        width: 0;
        height: 0;
        border-left: 4px solid transparent;
        border-right: 4px solid transparent;
        border-bottom: 5px solid {c['subtle']};
    }}
    QSpinBox::down-arrow {{
        image: none;
        width: 0;
        height: 0;
        border-left: 4px solid transparent;
        border-right: 4px solid transparent;
        border-top: 5px solid {c['subtle']};
    }}
    QComboBox QAbstractItemView {{
        background: {c['input_bg']};
        border: 1px solid {c['border']};
        selection-background-color: {c['select_bg']};
        color: {c['text']};
    }}
    QTreeWidget {{
        background: {c['pane']};
        border: 1px solid {c['border']};
        border-radius: 8px;
        alternate-background-color: {c['season_bg']};
        selection-background-color: {c['select_bg']};
        selection-color: {c['select_fg']};
        outline: 0;
    }}
    QTreeWidget::item {{ padding: 4px 6px; border: none; }}
    QHeaderView::section {{
        background: {c['sidebar']};
        color: {c['text']};
        padding: 6px 8px;
        border: none;
        border-right: 1px solid {c['border']};
        border-bottom: 1px solid {c['border']};
        font-weight: 600;
    }}
    QHeaderView::section:last {{
        border-right: none;
    }}
    QSplitter::handle {{
        background: {c['border']};
    }}
    QSplitter::handle:vertical {{
        height: 5px;
        margin: 2px 0;
    }}
    QSplitter::handle:horizontal {{
        width: 5px;
        margin: 0 2px;
    }}
    QProgressBar {{
        background: {c['sidebar']};
        border: 1px solid {c['border']};
        border-radius: 6px;
        text-align: center;
        height: 18px;
        color: {c['text']};
    }}
    QProgressBar::chunk {{
        background: {s['primary']};
        border-radius: 5px;
    }}
    QLabel#Subtle {{ color: {c['subtle']}; }}
    QLabel#DetailTitle {{ font-weight: 600; color: {c['text']}; }}
    QLabel#DetailBody {{ color: {c['text']}; background: transparent; }}
    QFrame#CandidateCard {{
        background: {c['pane']};
        border: 1px solid {c['border']};
        border-radius: 6px;
    }}
    QFrame#DetailSep {{
        background: {c['border']};
        border: none;
        max-width: 1px;
    }}
    QFrame#CardAccent {{
        border: none;
        min-width: 5px;
        max-width: 5px;
    }}
    QScrollBar:vertical {{
        background: transparent; width: 10px; margin: 0;
    }}
    QScrollBar::handle:vertical {{
        background: {c['border']}; border-radius: 5px; min-height: 24px;
    }}
    QScrollBar::add-line, QScrollBar::sub-line {{ height: 0; }}
    QScrollBar:horizontal {{
        background: transparent; height: 10px; margin: 0;
    }}
    QScrollBar::handle:horizontal {{
        background: {c['border']}; border-radius: 5px; min-width: 24px;
    }}
    QMenu {{
        background: {c['input_bg']};
        border: 1px solid {c['border']};
        border-radius: 6px;
        padding: 4px;
        color: {c['text']};
    }}
    QMenu::item {{
        padding: 6px 18px; border-radius: 4px;
    }}
    QMenu::item:selected {{ background: {c['select_bg']}; color: {c['select_fg']}; }}
    QMenu::separator {{
        height: 1px; background: {c['border']}; margin: 4px 8px;
    }}
    QToolTip {{
        background: {c['input_bg']}; color: {c['text']};
        border: 1px solid {c['border']}; border-radius: 4px; padding: 4px;
    }}
    """


def apply_theme(dark: bool) -> None:
    app = QApplication.instance()
    if app is None:
        return
    qss = build_qss(dark)
    app.setStyleSheet(qss)
    # 基础调色板（控件未覆盖 QSS 的部分用）
    pal = QPalette()
    c = current_palette(dark)
    pal.setColor(QPalette.Window, QColor(c["window"]))
    pal.setColor(QPalette.WindowText, QColor(c["text"]))
    pal.setColor(QPalette.Base, QColor(c["input_bg"]))
    pal.setColor(QPalette.Text, QColor(c["text"]))
    pal.setColor(QPalette.Button, QColor(c["pane"]))
    pal.setColor(QPalette.ButtonText, QColor(c["text"]))
    pal.setColor(QPalette.Highlight, QColor(c["select_bg"]))
    pal.setColor(QPalette.HighlightedText, QColor(c["select_fg"]))
    app.setPalette(pal)
