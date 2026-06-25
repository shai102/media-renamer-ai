"""tkinter 兼容适配层。

业务层（core/workers/task_runner.py、execution_runner.py、core/ui/manual_match.py、
core/mixins/list_mixin.py）按 tkinter API 访问 gui 上的控件与变量。本模块提供与
tkinter 行为等价但底层走 PySide6 的适配对象，使 worker 代码零改动即可运行。
"""

from __future__ import annotations

import threading
from typing import Any, Callable, Iterable

from PySide6.QtCore import QObject, Qt, QThread, QTimer, Signal
from PySide6.QtGui import QBrush, QColor
from PySide6.QtWidgets import QApplication, QProgressBar, QPushButton, QTreeWidget, QTreeWidgetItem

from ui_qt.theme import theme_colors


class _MainThreadBridge(QObject):
    """把任意 callable 投递到 Qt 主线程执行（worker 线程安全）。"""

    call = Signal(object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.call.connect(self._run)

    def _run(self, fn):
        try:
            fn()
        except Exception:
            import logging
            logging.exception("主线程回调异常")


# tkinter 常量值（worker 用 tk.DISABLED / tk.NORMAL 传给 ButtonAdapter）
DISABLED = "disabled"
NORMAL = "normal"


class VarAdapter:
    """替代 tk.StringVar / tk.BooleanVar / tk.IntVar。

    只需 .get() / .set()，以及可被 trace 的最小支持。worker 仅调用 get()。
    """

    def __init__(self, value: Any = ""):
        self._value = value
        self._lock = threading.Lock()
        self._traces: list[Callable[..., None]] = []

    def get(self) -> Any:
        with self._lock:
            return self._value

    def set(self, value: Any) -> None:
        with self._lock:
            self._value = value
        for trace in list(self._traces):
            try:
                trace()
            except Exception:
                pass

    def trace_add(self, mode: str, callback: Callable[..., None]) -> None:
        self._traces.append(callback)

    def __bool__(self) -> bool:
        return bool(self.get())


class RootAdapter:
    """替代 tk.Tk / ttkbootstrap.Window，作为 dialog parent 与线程调度入口。

    实际持有 QMainWindow，提供 after / wait_window / destroy / winfo_geometry 等
    tkinter 语义方法供 worker 调用。
    """

    def __init__(self, qmainwindow):
        self._win = qmainwindow
        self._main_thread_bridge = _MainThreadBridge(qmainwindow)

    @property
    def tk(self):  # 兼容个别 cget('tk') 之类查询，返回 None 即可
        return None

    def _schedule_on_main_thread(self, ms: int, callback: Callable, *args, **kwargs) -> None:
        def _invoke():
            try:
                if args or kwargs:
                    callback(*args, **kwargs)
                else:
                    callback()
            except Exception:
                import logging
                logging.exception("root.after 回调异常")

        QTimer.singleShot(max(0, int(ms)), _invoke)

    def after(self, ms: int, callback: Callable, *args, **kwargs) -> None:
        # worker 用 after(0, fn) 切回主线程；必须从主线程创建 QTimer
        def _dispatch():
            self._schedule_on_main_thread(ms, callback, *args, **kwargs)

        app = QApplication.instance()
        main_thread = app.thread() if app is not None else self._win.thread()
        if QThread.currentThread() is main_thread:
            _dispatch()
        else:
            self._main_thread_bridge.call.emit(_dispatch)

    def after_cancel(self, timer_id: Any) -> None:
        # 最小实现：PySide6 QTimer.singleShot 不返回可取消 id，此处忽略
        return None

    def wait_window(self, window) -> None:
        # 阻塞等待窗口销毁。window 应为 QDialog 或 QWidget。
        # manual_match 在 worker 线程调用 wait_window，但 worker 线程不应阻塞 Qt 事件循环。
        # 实际 manual_match_qt 改用 QDialog.exec() 在主线程阻塞，故此处仅作占位。
        return None

    def destroy(self) -> None:
        try:
            self._win.close()
        except Exception:
            pass

    def winfo_geometry(self) -> str:
        g = self._win.geometry()
        return f"{g.width()}x{g.height()}+{g.x()}+{g.y()}"

    def winfo_width(self) -> int:
        return self._win.width()

    def winfo_height(self) -> int:
        return self._win.height()

    def winfo_x(self) -> int:
        return self._win.x()

    def winfo_y(self) -> int:
        return self._win.y()

    def winfo_screenwidth(self) -> int:
        scr = self._win.screen()
        return scr.size().width()

    def winfo_screenheight(self) -> int:
        scr = self._win.screen()
        return scr.size().height()

    def geometry(self, geo: str | None = None) -> str:
        if geo is None:
            g = self._win.geometry()
            return f"{g.width()}x{g.height()}+{g.x()}+{g.y()}"
        # 解析 "WxH+X+Y"
        import re
        m = re.match(r"^(\d+)x(\d+)\+(-?\d+)\+(-?\d+)$", str(geo).strip())
        if m:
            w, h, x, y = map(int, m.groups())
            self._win.setGeometry(x, y, w, h)
        return geo

    def update_idletasks(self) -> None:
        return None

    def update(self) -> None:
        return None

    def __getattr__(self, name: str):
        # 兜底：未实现的方法转发给 QMainWindow，避免 AttributeError
        return getattr(self._win, name)


class TreeAdapter:
    """替代 ttk.Treeview，底层走 QTreeWidget。

    仅实现 worker / manual_match / list_mixin 实际调用的方法子集。
    """

    def __init__(self, qtreewidget: QTreeWidget, *, dark: bool = False):
        self._tree = qtreewidget
        self._dark = dark
        # iid -> QTreeWidgetItem
        self._item_map: dict[str, QTreeWidgetItem] = {}
        # 父子关系: iid -> parent_iid（"" 表示根）
        self._parent_map: dict[str, str] = {}

    # ---- 查询 ----
    def exists(self, iid: str) -> bool:
        return iid in self._item_map

    def get_children(self, parent: str = "") -> tuple:
        qparent = self._tree.invisibleRootItem() if parent == "" else self._item_map.get(parent)
        if qparent is None:
            return ()
        return tuple(
            iid
            for iid, item in self._item_map.items()
            if self._parent_map.get(iid, "") == parent and item is not None
        )

    def parent(self, iid: str) -> str:
        return self._parent_map.get(iid, "")

    def identify_row(self, y: int) -> str:
        # y 相对 tree 视口；QTreeWidget.itemAt 返回 QTreeWidgetItem
        item = self._tree.itemAt(0, y)
        if item is None:
            return ""
        for iid, it in self._item_map.items():
            if it is item:
                return iid
        return ""

    def focus(self, iid: str | None = None) -> str:
        if iid is None:
            cur = self._tree.currentItem()
            for iid_, it in self._item_map.items():
                if it is cur:
                    return iid_
            return ""
        item = self._item_map.get(iid)
        if item is not None:
            self._tree.setCurrentItem(item)
        return iid

    def selection(self) -> tuple:
        return tuple(
            iid
            for iid, it in self._item_map.items()
            if it is not None and it.isSelected()
        )

    def selection_set(self, iids) -> None:
        # 先清空
        for it in self._item_map.values():
            if it is not None:
                it.setSelected(False)
        if isinstance(iids, str):
            iids = [iids]
        for iid in iids:
            it = self._item_map.get(iid)
            if it is not None:
                it.setSelected(True)

    def item(self, iid: str, key: str | None = None, **kwargs):
        it = self._item_map.get(iid)
        if it is None:
            return "" if key else None
        if key is not None:
            # 读取
            if key == "text":
                return it.text(0)
            if key == "values":
                return (it.text(1), it.text(2), it.text(3), it.text(4))
            if key == "open":
                return it.isExpanded()
            if key == "tags":
                return it.data(0, Qt.UserRole) or ()
            return ""
        # 写入 kwargs
        if "text" in kwargs:
            it.setText(0, str(kwargs["text"] or ""))
        if "values" in kwargs:
            vals = kwargs["values"]
            if vals is None:
                vals = ("", "", "", "")
            for col, val in enumerate(vals, start=1):
                it.setText(col, str(val if val is not None else ""))
        if "open" in kwargs:
            it.setExpanded(bool(kwargs["open"]))
        if "tags" in kwargs:
            it.setData(0, Qt.UserRole, kwargs["tags"] or ())
            self._apply_tag_style(it, kwargs["tags"] or ())
        return None

    def _apply_tag_style(self, it: QTreeWidgetItem, tags) -> None:
        c, _ = theme_colors(self._dark)
        tag_set = set(tags or ())
        bold = any(t in ("source", "season") for t in tag_set)
        font = it.font(0)
        font.setBold(bold)
        for col in range(max(it.columnCount(), 5)):
            it.setFont(col, font)
        if "source" in tag_set:
            bg = c["source_bg"]
        elif "season" in tag_set:
            bg = c["season_bg"]
        elif "file" in tag_set:
            bg = c["file_bg"]
        else:
            bg = None
        if bg:
            brush = QBrush(QColor(bg))
            for col in range(max(it.columnCount(), 5)):
                it.setBackground(col, brush)

    # ---- 增删 ----
    def insert(self, parent, index, iid=None, text="", values=None, open=False, tags=()):
        qparent = self._tree.invisibleRootItem() if parent in ("", None) else self._item_map.get(parent)
        if qparent is None:
            qparent = self._tree.invisibleRootItem()
        new_item = QTreeWidgetItem()
        new_item.setText(0, str(text or ""))
        if values:
            for col, val in enumerate(values, start=1):
                new_item.setText(col, str(val if val is not None else ""))
        new_item.setExpanded(bool(open))
        new_item.setData(0, Qt.UserRole, tags or ())
        self._apply_tag_style(new_item, tags or ())
        if index in ("end", None):
            qparent.addChild(new_item)
        else:
            qparent.insertChild(int(index), new_item)
        if iid:
            self._item_map[iid] = new_item
            self._parent_map[iid] = parent if parent not in ("", None) else ""
        return iid

    def delete(self, iid) -> None:
        if isinstance(iid, (list, tuple)):
            for one in iid:
                self.delete(one)
            return
        it = self._item_map.pop(iid, None)
        self._parent_map.pop(iid, None)
        if it is None:
            return
        qparent = it.parent() or self._tree.invisibleRootItem()
        idx = qparent.indexOfChild(it)
        if idx >= 0:
            qparent.takeChild(idx)

    def clear(self) -> None:
        self._tree.clear()
        self._item_map.clear()
        self._parent_map.clear()

    def see(self, iid: str) -> None:
        it = self._item_map.get(iid)
        if it is not None:
            self._tree.scrollToItem(it)

    def bbox(self, iid: str):
        return None

    def __getattr__(self, name: str):
        return getattr(self._tree, name)


class ProgressAdapter:
    """替代 ttk.Progressbar，底层走 QProgressBar。"""

    def __init__(self, qbar: QProgressBar):
        self._bar = qbar

    def config(self, **kwargs):
        if "maximum" in kwargs:
            self._bar.setMaximum(int(kwargs["maximum"]))
        if "value" in kwargs:
            self._bar.setValue(int(kwargs["value"]))

    configure = config

    def step(self, n: int = 1) -> None:
        self._bar.setValue(self._bar.value() + int(n))

    def __setitem__(self, key: str, value):
        if key == "value":
            self._bar.setValue(int(value))
        elif key == "maximum":
            self._bar.setMaximum(int(value))

    def __getitem__(self, key: str):
        if key == "value":
            return self._bar.value()
        if key == "maximum":
            return self._bar.maximum()
        return None


class StatusAdapter:
    """替代 ttk.Label 的 status 控件。

    worker 用 .config(text=...) 写、.cget('text') 读。
    """

    def __init__(self, qlabel):
        self._label = qlabel

    def config(self, text=None, **kwargs):
        if text is not None:
            self._label.setText(str(text))

    configure = config

    def cget(self, key: str):
        if key == "text":
            return self._label.text()
        return ""

    def __setitem__(self, key: str, value):
        if key == "text":
            self._label.setText(str(value))

    def __getitem__(self, key: str):
        if key == "text":
            return self._label.text()
        return None


class ButtonAdapter:
    """替代 ttk.Button（btn_pre），支持 .config(state=tk.DISABLED/NORMAL)。"""

    def __init__(self, qbutton: QPushButton):
        self._btn = qbutton

    def config(self, state=None, text=None, **kwargs):
        if state is not None:
            self._btn.setEnabled(state != DISABLED)
        if text is not None:
            self._btn.setText(str(text))

    configure = config

    def cget(self, key: str):
        if key == "state":
            return DISABLED if not self._btn.isEnabled() else NORMAL
        if key == "text":
            return self._btn.text()
        return ""

    def __setitem__(self, key: str, value):
        if key == "state":
            self._btn.setEnabled(value != DISABLED)
        elif key == "text":
            self._btn.setText(str(value))

    def __getitem__(self, key: str):
        return self.cget(key)
