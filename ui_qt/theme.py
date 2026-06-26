"""PySide6 主题系统：深/浅色跟随系统 + 手动切换，语义色按钮 QSS。

色板：亮色白底 + 柔和蓝灰侧栏；暗色带蓝调炭灰 #1b1d21/#232529。语义按钮：
primary(蓝)/secondary(灰)/info(青)/success(绿)/warning(橙)/danger(红)。

控件指示器（Radio / CheckBox / ComboBox 箭头）一律用纯 QSS 绘制，不用 SVG
data URI——PyInstaller 单文件 EXE 下 Qt 样式表无法加载 inline 图片。
"""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QPalette, QColor
from PySide6.QtWidgets import QApplication


# 语义色（不随主题变）
SEMANTIC_COLORS = {
    "primary": "#2563eb",
    "primary_hover": "#1d4ed8",
    "primary_pressed": "#1e40af",
    "secondary": "#64748b",
    "secondary_hover": "#475569",
    "secondary_pressed": "#334155",
    "info": "#0d9488",
    "info_hover": "#0f766e",
    "info_pressed": "#115e59",
    "success": "#16a34a",
    "success_hover": "#15803d",
    "success_pressed": "#166534",
    "warning": "#ea580c",
    "warning_hover": "#c2410c",
    "warning_pressed": "#9a3412",
    "danger": "#dc2626",
    "danger_hover": "#b91c1c",
    "danger_pressed": "#991b1b",
}

# 状态语义前景色（列表状态列着色用）
STATUS_COLORS = {
    "success": "#16a34a",
    "warning": "#d97706",
    "danger": "#dc2626",
    "info": "#2563eb",
    "muted": "#94a3b8",
}
STATUS_COLORS_DARK = {
    "success": "#4ade80",
    "warning": "#fbbf24",
    "danger": "#f87171",
    "info": "#60a5fa",
    "muted": "#94a3b8",
}

LIGHT = {
    "window": "#ffffff",
    "sidebar": "#f4f6fb",
    "pane": "#ffffff",
    "border": "#e6e9f0",
    "card_border": "#e2e8f0",
    "text": "#1e293b",
    "subtle": "#64748b",
    "source_bg": "#eef4ff",
    "season_bg": "#f6f9ff",
    "file_bg": "#ffffff",
    "select_bg": "#dbeafe",
    "select_fg": "#15396b",
    "input_bg": "#ffffff",
    "hover": "#eef2f7",
    "hover_row": "#f5f8ff",
}

DARK = {
    "window": "#232529",
    "sidebar": "#1b1d21",
    "pane": "#232529",
    "border": "#34373d",
    "card_border": "#34373d",
    "text": "#e6e8ec",
    "subtle": "#9aa3af",
    "source_bg": "#2a2e36",
    "season_bg": "#262930",
    "file_bg": "#232529",
    "select_bg": "#2c4a73",
    "select_fg": "#e8f0fb",
    "input_bg": "#2a2c31",
    "hover": "#30333a",
    "hover_row": "#2a2d34",
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


def current_palette(dark: bool) -> dict:
    return dict(DARK if dark else LIGHT)


def theme_colors(dark: bool) -> tuple[dict, dict]:
    """返回 (palette, semantic) 供对话框等动态取色。"""
    return current_palette(dark), SEMANTIC_COLORS


def status_colors(dark: bool) -> dict:
    """返回状态语义前景色映射。"""
    return dict(STATUS_COLORS_DARK if dark else STATUS_COLORS)


def build_qss(dark: bool) -> str:
    c = current_palette(dark)
    s = SEMANTIC_COLORS
    return f"""
    QWidget {{
        background: {c['window']};
        color: {c['text']};
        font-family: "Microsoft YaHei UI", "Segoe UI", "PingFang SC", sans-serif;
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
        border: none;
        border-right: 1px solid {c['border']};
    }}
    QLabel#BrandTitle {{
        font-size: 13pt;
        font-weight: 700;
        color: {c['text']};
        background: transparent;
    }}
    QGroupBox {{
        background: {c['pane']};
        border: 1px solid {c['card_border']};
        border-radius: 10px;
        margin-top: 12px;
        padding: 12px 10px 10px 10px;
        font-size: 10.5pt;
        font-weight: 600;
    }}
    QGroupBox::title {{
        subcontrol-origin: margin;
        subcontrol-position: top left;
        left: 12px;
        top: 2px;
        padding: 0 6px;
        color: {c['text']};
    }}
    QPushButton {{
        background: {c['pane']};
        border: 1px solid {c['border']};
        border-radius: 8px;
        padding: 7px 16px;
        font-weight: 500;
        color: {c['text']};
    }}
    QPushButton:hover {{
        background: {c['hover']};
        border-color: {c['subtle']};
    }}
    QPushButton:pressed {{
        background: {c['border']};
    }}
    QPushButton:disabled {{
        color: {c['subtle']};
        background: {c['sidebar']};
        border-color: {c['border']};
    }}
    QPushButton#primary   {{ background: {s['primary']};   border: none; color: #ffffff; font-weight: 600; }}
    QPushButton#primary:hover   {{ background: {s['primary_hover']}; }}
    QPushButton#primary:pressed {{ background: {s['primary_pressed']}; }}
    QPushButton#primary:disabled {{ background: {c['border']}; color: {c['subtle']}; }}
    QPushButton#secondary {{ background: {s['secondary']}; border: none; color: #ffffff; font-weight: 600; }}
    QPushButton#secondary:hover   {{ background: {s['secondary_hover']}; }}
    QPushButton#secondary:pressed {{ background: {s['secondary_pressed']}; }}
    QPushButton#info      {{ background: {s['info']};      border: none; color: #ffffff; font-weight: 600; }}
    QPushButton#info:hover   {{ background: {s['info_hover']}; }}
    QPushButton#info:pressed {{ background: {s['info_pressed']}; }}
    QPushButton#success   {{ background: {s['success']};   border: none; color: #ffffff; font-weight: 600; }}
    QPushButton#success:hover   {{ background: {s['success_hover']}; }}
    QPushButton#success:pressed {{ background: {s['success_pressed']}; }}
    QPushButton#warning   {{ background: {s['warning']};   border: none; color: #ffffff; font-weight: 600; }}
    QPushButton#warning:hover   {{ background: {s['warning_hover']}; }}
    QPushButton#warning:pressed {{ background: {s['warning_pressed']}; }}
    QPushButton#danger    {{ background: {s['danger']};    border: none; color: #ffffff; font-weight: 600; }}
    QPushButton#danger:hover   {{ background: {s['danger_hover']}; }}
    QPushButton#danger:pressed {{ background: {s['danger_pressed']}; }}
    QPushButton#ghost {{
        background: transparent;
        border: 1px solid {c['border']};
        padding: 6px 10px;
    }}
    QPushButton#ghost:hover {{ background: {c['hover']}; }}
    QLineEdit, QComboBox, QSpinBox, QTextEdit {{
        background: {c['input_bg']};
        border: 1px solid {c['border']};
        border-radius: 8px;
        padding: 6px 10px;
        color: {c['text']};
        selection-background-color: {c['select_bg']};
        selection-color: {c['select_fg']};
    }}
    QLineEdit:hover, QComboBox:hover, QSpinBox:hover {{
        border-color: {c['subtle']};
    }}
    QLineEdit:focus, QComboBox:focus, QSpinBox:focus, QTextEdit:focus {{
        border: 2px solid {s['primary']};
        padding: 5px 9px;
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
        width: 26px;
        border: none;
        background: transparent;
    }}
    QComboBox::drop-down:hover {{
        background: {c['hover']};
        border-top-right-radius: 7px;
        border-bottom-right-radius: 7px;
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
        padding: 3px 0;
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
    QRadioButton::indicator:unchecked:hover {{
        border-color: {s['primary']};
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
        padding: 3px 0;
    }}
    QCheckBox::indicator {{
        width: 18px;
        height: 18px;
        border-radius: 5px;
    }}
    QCheckBox::indicator:unchecked {{
        border: 2px solid {c['subtle']};
        background: {c['input_bg']};
    }}
    QCheckBox::indicator:unchecked:hover {{
        border-color: {s['primary']};
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
        image: none; width: 0; height: 0;
        border-left: 4px solid transparent;
        border-right: 4px solid transparent;
        border-bottom: 5px solid {c['subtle']};
    }}
    QSpinBox::down-arrow {{
        image: none; width: 0; height: 0;
        border-left: 4px solid transparent;
        border-right: 4px solid transparent;
        border-top: 5px solid {c['subtle']};
    }}
    QComboBox QAbstractItemView {{
        background: {c['input_bg']};
        border: 1px solid {c['border']};
        border-radius: 8px;
        padding: 4px;
        selection-background-color: {c['select_bg']};
        selection-color: {c['select_fg']};
        color: {c['text']};
        outline: 0;
    }}
    QTreeWidget {{
        background: {c['pane']};
        border: 1px solid {c['card_border']};
        border-radius: 10px;
        alternate-background-color: {c['season_bg']};
        selection-background-color: {c['select_bg']};
        selection-color: {c['select_fg']};
        outline: 0;
        padding: 2px;
    }}
    QTreeWidget::item {{
        padding: 7px 8px;
        border: none;
        border-radius: 5px;
    }}
    QTreeWidget::item:hover {{
        background: {c['hover_row']};
    }}
    QTreeWidget::item:selected {{
        background: {c['select_bg']};
        color: {c['select_fg']};
    }}
    QHeaderView::section {{
        background: {c['sidebar']};
        color: {c['subtle']};
        padding: 8px 8px;
        border: none;
        border-bottom: 1px solid {c['border']};
        border-right: 1px solid {c['border']};
        font-weight: 600;
    }}
    QHeaderView::section:last {{ border-right: none; }}
    QSplitter::handle {{ background: transparent; }}
    QSplitter::handle:vertical {{ height: 8px; margin: 2px 0; }}
    QSplitter::handle:horizontal {{ width: 8px; margin: 0 2px; }}
    QProgressBar {{
        background: {c['sidebar']};
        border: 1px solid {c['border']};
        border-radius: 9px;
        text-align: center;
        height: 20px;
        color: {c['text']};
        font-size: 9pt;
    }}
    QProgressBar::chunk {{
        background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
            stop:0 {s['primary']}, stop:1 {s['info']});
        border-radius: 8px;
    }}
    QLabel#Subtle {{ color: {c['subtle']}; font-size: 9pt; }}
    QLabel#DetailTitle {{ font-size: 11pt; font-weight: 700; color: {c['text']}; }}
    QLabel#DetailBody {{ color: {c['text']}; background: transparent; }}
    QFrame#CandidateCard {{
        background: {c['pane']};
        border: 1px solid {c['card_border']};
        border-radius: 8px;
    }}
    QFrame#DetailCard {{
        background: {c['pane']};
        border: 1px solid {c['card_border']};
        border-radius: 10px;
    }}
    QFrame#CandidateCard:hover {{
        border-color: {s['primary']};
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
        border-radius: 2px;
    }}
    QScrollBar:vertical {{
        background: transparent; width: 12px; margin: 2px;
    }}
    QScrollBar::handle:vertical {{
        background: {c['border']}; border-radius: 4px; min-height: 28px;
    }}
    QScrollBar::handle:vertical:hover {{ background: {c['subtle']}; }}
    QScrollBar::add-line, QScrollBar::sub-line {{ height: 0; width: 0; }}
    QScrollBar:horizontal {{
        background: transparent; height: 12px; margin: 2px;
    }}
    QScrollBar::handle:horizontal {{
        background: {c['border']}; border-radius: 4px; min-width: 28px;
    }}
    QScrollBar::handle:horizontal:hover {{ background: {c['subtle']}; }}
    QScrollBar::add-page, QScrollBar::sub-page {{ background: transparent; }}
    QMenu {{
        background: {c['input_bg']};
        border: 1px solid {c['border']};
        border-radius: 8px;
        padding: 6px;
        color: {c['text']};
    }}
    QMenu::item {{ padding: 7px 20px; border-radius: 5px; }}
    QMenu::item:selected {{ background: {c['select_bg']}; color: {c['select_fg']}; }}
    QMenu::separator {{ height: 1px; background: {c['border']}; margin: 5px 8px; }}
    QToolTip {{
        background: {c['input_bg']}; color: {c['text']};
        border: 1px solid {c['border']}; border-radius: 6px; padding: 5px 8px;
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
