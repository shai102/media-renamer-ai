"""季偏移对话框（PySide6）。"""

from __future__ import annotations

from PySide6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)

from utils.helpers import safe_int


class SeasonOffsetDialog(QDialog):
    """与 tk SeasonOffsetDialog 契约一致：构造阻塞，result = (season, offset) 或 None。"""

    def __init__(self, parent, title_name: str):
        super().__init__(parent)
        self.setWindowTitle("高级季集映射")
        self.setMinimumSize(450, 320)
        self.result = None

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(f"已选定匹配: 【{title_name}】"))
        layout.addWidget(QLabel("", objectName="DetailTitle"))

        row1 = QHBoxLayout()
        row1.addWidget(QLabel("强制指定为第几季:"))
        self._season_edit = QLineEdit("1")
        self._season_edit.setFixedWidth(80)
        row1.addWidget(self._season_edit)
        row1.addStretch(1)
        layout.addLayout(row1)

        row2 = QHBoxLayout()
        row2.addWidget(QLabel("集数增减偏移 (可选):"))
        self._offset_edit = QLineEdit("0")
        self._offset_edit.setFixedWidth(80)
        row2.addWidget(self._offset_edit)
        row2.addStretch(1)
        layout.addLayout(row2)

        hint = QLabel(
            "*提示：\n"
            "1. 普通动漫直接点确定即可 (季数填1, 偏移填0)。\n"
            "2. 若选中[13]集，但在TMDB里算作第4季第1集，\n"
            "   请填 季数: 4，偏移量: -12。"
        )
        hint.setWordWrap(True)
        hint.setObjectName("Subtle")
        layout.addWidget(hint)

        btn_ok = QPushButton("确定应用")
        btn_ok.setObjectName("success")
        btn_ok.clicked.connect(self._on_ok)
        layout.addWidget(btn_ok)

        self.setModal(True)

    def _on_ok(self):
        try:
            self.result = (
                safe_int(self._season_edit.text(), 1),
                safe_int(self._offset_edit.text(), 0),
            )
            self.accept()
        except ValueError:
            QMessageBox.warning(self, "错误", "请输入有效的整数！")

    @classmethod
    def run(cls, parent, title_name: str):
        dlg = cls(parent, title_name)
        dlg.exec()
        return dlg.result
