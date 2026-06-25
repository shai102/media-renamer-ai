"""可复用的 Qt 控件。"""

from __future__ import annotations

from PySide6.QtWidgets import QComboBox, QSpinBox, QDoubleSpinBox


class NoWheelComboBox(QComboBox):
    """未展开下拉列表时忽略滚轮，避免滚动设置页时误改选项。"""

    def wheelEvent(self, event):
        view = self.view()
        if view is not None and view.isVisible():
            super().wheelEvent(event)
        else:
            event.ignore()


class NoWheelSpinBox(QSpinBox):
    """未聚焦时忽略滚轮，避免滚动表单时误改数值。"""

    def wheelEvent(self, event):
        if self.hasFocus():
            super().wheelEvent(event)
        else:
            event.ignore()


class NoWheelDoubleSpinBox(QDoubleSpinBox):
    """未聚焦时忽略滚轮。"""

    def wheelEvent(self, event):
        if self.hasFocus():
            super().wheelEvent(event)
        else:
            event.ignore()
