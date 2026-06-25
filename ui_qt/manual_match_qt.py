"""手动匹配与候选选择（PySide6）。"""

from __future__ import annotations

import io
import logging
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

import requests
from PIL import Image
from PySide6.QtCore import Qt
from PySide6.QtGui import QImage, QPixmap
from PySide6.QtWidgets import (
    QDialog,
    QFrame,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from core.services.matcher_service import auto_pick_candidate_by_score
from core.ui.manual_match import (
    _prefetch_poster_urls,
    _resolve_poster_url,
)
from ui_qt.season_offset_dialog import SeasonOffsetDialog
from ui_qt.theme import theme_colors
from utils.helpers import clean_search_title, session

_POSTER_W = 62
_POSTER_H = 93
_POSTER_REQUEST_TIMEOUT = 8

_poster_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="poster_qt")
_poster_lock = threading.Lock()
_poster_pixmap_cache: OrderedDict[str, QPixmap] = OrderedDict()
_poster_pil_cache: OrderedDict[str, Image.Image] = OrderedDict()
_poster_pending: set[str] = set()
_POSTER_CACHE_MAX = 120


def _get_cached_pixmap(url: str) -> QPixmap | None:
    if not url:
        return None
    with _poster_lock:
        pm = _poster_pixmap_cache.get(url)
        if pm is not None:
            _poster_pixmap_cache.move_to_end(url)
        return pm


def _cache_pixmap(url: str, pm: QPixmap) -> None:
    if not url or pm is None or pm.isNull():
        return
    with _poster_lock:
        _poster_pixmap_cache[url] = pm
        _poster_pixmap_cache.move_to_end(url)
        while len(_poster_pixmap_cache) > _POSTER_CACHE_MAX:
            _poster_pixmap_cache.popitem(last=False)


def _get_cached_pil(url: str) -> Image.Image | None:
    if not url:
        return None
    with _poster_lock:
        img = _poster_pil_cache.get(url)
        if img is not None:
            _poster_pil_cache.move_to_end(url)
        return img


def _cache_pil(url: str, img: Image.Image) -> None:
    if not url or img is None:
        return
    with _poster_lock:
        _poster_pil_cache[url] = img
        _poster_pil_cache.move_to_end(url)
        while len(_poster_pil_cache) > _POSTER_CACHE_MAX:
            _poster_pil_cache.popitem(last=False)


def _fetch_and_resize_poster(url: str) -> Image.Image:
    resp = session.get(url, timeout=_POSTER_REQUEST_TIMEOUT)
    resp.raise_for_status()
    img = Image.open(io.BytesIO(resp.content)).convert("RGB")
    return img.resize((_POSTER_W, _POSTER_H), Image.LANCZOS)


def _pil_to_pixmap(img: Image.Image) -> QPixmap:
    data = img.tobytes("raw", "RGB")
    qimg = QImage(data, img.width, img.height, img.width * 3, QImage.Format.Format_RGB888)
    return QPixmap.fromImage(qimg)


def _load_poster_async(url: str, label: QLabel, gui, keep_refs: list) -> None:
    if not url:
        return

    cached = _get_cached_pixmap(url)
    if cached is not None:
        keep_refs.append(cached)
        label.setPixmap(cached)
        return

    cached_pil = _get_cached_pil(url)
    if cached_pil is not None:
        pm = _pil_to_pixmap(cached_pil)
        _cache_pixmap(url, pm)
        keep_refs.append(pm)
        label.setPixmap(pm)
        return

    with _poster_lock:
        if url in _poster_pending:
            return
        _poster_pending.add(url)

    def _clear():
        with _poster_lock:
            _poster_pending.discard(url)

    def _apply(img: Image.Image):
        try:
            _cache_pil(url, img)
            pm = _pil_to_pixmap(img)
            _cache_pixmap(url, pm)
            keep_refs.append(pm)
            label.setPixmap(pm)
        except Exception:
            pass
        finally:
            _clear()

    def _worker():
        try:
            img = _fetch_and_resize_poster(url)
        except Exception:
            _clear()
            return
        gui.root.after(0, lambda im=img: _apply(im))

    try:
        _poster_executor.submit(_worker)
    except Exception:
        _clear()


class CandidatePickerDialog(QDialog):
    """候选卡片选择对话框。"""

    def __init__(
        self,
        gui,
        title: str,
        header_text: str,
        items: list[dict],
        *,
        show_skip_folder=False,
        on_skip_folder=None,
        on_close_skip_all=None,
    ):
        super().__init__(gui._qt_root)
        self._gui = gui
        self._items = items
        self._selected_idx = -1
        self._result = None
        self._closed = False
        self._image_refs: list = []
        self._palette, self._semantic = theme_colors(getattr(gui, "_dark", False))

        self.setWindowTitle(title)
        self.setMinimumSize(720, 440)
        self.resize(760, 520)
        self.setModal(True)

        layout = QVBoxLayout(self)
        hdr = QLabel(header_text)
        hdr.setWordWrap(True)
        layout.addWidget(hdr)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        inner = QWidget()
        inner_lay = QVBoxLayout(inner)
        inner_lay.setContentsMargins(4, 4, 4, 4)
        inner_lay.setSpacing(6)

        self._accent_bars: list[QFrame] = []
        for idx, item in enumerate(items):
            inner_lay.addWidget(self._build_card(idx, item))
        inner_lay.addStretch(1)
        scroll.setWidget(inner)
        layout.addWidget(scroll, 1)

        if items:
            self._highlight(0)

        btn_row = QHBoxLayout()
        btn_ok = QPushButton("确认选择")
        btn_ok.setObjectName("success")
        btn_ok.clicked.connect(self._on_confirm)
        btn_row.addWidget(btn_ok)
        if show_skip_folder and on_skip_folder:
            btn_skip = QPushButton("跳过此文件夹")
            btn_skip.clicked.connect(on_skip_folder)
            btn_row.addWidget(btn_skip)
        elif not show_skip_folder:
            btn_cancel = QPushButton("取消")
            btn_cancel.clicked.connect(self.reject)
            btn_row.addWidget(btn_cancel)
        btn_row.addStretch(1)
        layout.addLayout(btn_row)

        self._on_close_skip_all = on_close_skip_all
        self._on_skip_folder = on_skip_folder if show_skip_folder else None

    def closeEvent(self, event):
        if self.result() != QDialog.DialogCode.Accepted and self._on_close_skip_all:
            self._on_close_skip_all()
        super().closeEvent(event)

    def keyPressEvent(self, event):
        if event.key() == Qt.Key.Key_Escape and self._on_skip_folder:
            self._on_skip_folder()
            event.accept()
            return
        super().keyPressEvent(event)

    def _build_card(self, idx: int, item: dict) -> QWidget:
        title = item.get("title") or "未知"
        release = item.get("release") or ""
        year = release[:4] if release and len(release) >= 4 else "-"
        tmdb_id = item.get("id") or "-"
        source = item.get("msg") or ""
        meta = item.get("meta") or {}
        overview = " ".join((meta.get("overview") or "").strip().split())
        if len(overview) > 100:
            overview = overview[:100] + "..."
        poster_url = _resolve_poster_url(meta.get("poster") or "")

        card = QFrame()
        card.setObjectName("CandidateCard")
        card.setFrameShape(QFrame.NoFrame)
        row = QHBoxLayout(card)
        row.setContentsMargins(8, 6, 8, 6)

        accent = QFrame()
        accent.setObjectName("CardAccent")
        accent.setFixedWidth(5)
        accent.setStyleSheet(f"background:{self._palette['border']};")
        self._accent_bars.append(accent)
        row.addWidget(accent)

        poster = QLabel()
        poster.setFixedSize(_POSTER_W, _POSTER_H)
        poster.setStyleSheet(f"background:{self._palette['sidebar']};")
        if poster_url:
            _load_poster_async(poster_url, poster, self._gui, self._image_refs)
        row.addWidget(poster)

        text_col = QVBoxLayout()
        id_tag = f"TMDB ID: {tmdb_id}" if "BGM" not in source else f"BGM ID: {tmdb_id}"
        c = self._palette
        s = self._semantic
        head = QLabel(
            f"<b>{title}</b>  <span style='color:{c['subtle']}'>{year}</span>  "
            f"<span style='color:{s['warning']}'>{id_tag}</span>"
        )
        head.setTextFormat(Qt.RichText)
        text_col.addWidget(head)
        if overview:
            ov = QLabel(overview)
            ov.setWordWrap(True)
            ov.setObjectName("Subtle")
            text_col.addWidget(ov)
        text_col.addStretch(1)
        row.addLayout(text_col, 1)

        btn = QPushButton("选择")
        btn.setFixedWidth(64)
        btn.clicked.connect(lambda _=False, i=idx: self._highlight(i))
        row.addWidget(btn)

        def click_card(_event=None, i=idx):
            self._highlight(i)

        card.mousePressEvent = lambda e, i=idx: click_card(i)
        poster.mousePressEvent = lambda e, i=idx: click_card(i)
        return card

    def _highlight(self, idx: int):
        self._selected_idx = idx
        idle = self._palette["border"]
        active = self._semantic["primary"]
        for i, bar in enumerate(self._accent_bars):
            bar.setStyleSheet(f"background:{active if i == idx else idle};")

    def _on_confirm(self):
        if self._selected_idx < 0 or self._selected_idx >= len(self._items):
            QMessageBox.information(self, "提示", '请先点击某条目的"选择"按钮')
            return
        self._result = self._items[self._selected_idx]
        self.accept()

    def selected_item(self):
        return self._result

    @property
    def selected_index(self) -> int:
        return self._selected_idx


def show_candidate_picker_dialog(
    gui,
    item,
    query_title,
    source_name,
    candidates,
    result_holder,
    done_event,
    recognized_title=None,
):
    prev_status = gui.status.cget("text")
    gui.status.config(text=f"等待手动选择: {item.old_name}")

    recognized_text = str(recognized_title or query_title or "").strip()
    searched_text = str(query_title or "").strip()
    if recognized_text and searched_text and recognized_text != searched_text:
        title_block = f"识别标题: {recognized_text}\n搜索标题: {searched_text}"
    else:
        title_block = f"识别标题: {recognized_text or searched_text}"

    header = (
        f"文件: {item.old_name}\n"
        f"{title_block}\n"
        "请在下方候选中选择正确条目："
    )

    items = []
    for cand in candidates:
        meta = (cand or {}).get("meta") or {}
        items.append(
            {
                "title": cand.get("title"),
                "id": cand.get("id"),
                "msg": cand.get("msg") or source_name,
                "release": meta.get("release", ""),
                "meta": meta,
            }
        )

    dlg_holder: list[CandidatePickerDialog] = []

    def on_skip_folder():
        skip_dir = item.dir
        gui.preview_skip_dirs.add(skip_dir)
        for other in gui.file_list:
            if other.dir == skip_dir and other.id != item.id:
                gui.root.after(
                    0, lambda o=other: gui.update_item_display(o, status="已跳过")
                )
        result_holder["selected"] = None
        done_event.set()
        gui.status.config(text=prev_status)
        if dlg_holder:
            dlg_holder[0].accept()

    def on_close_skip_all():
        gui.preview_skip_all_event.set()
        gui.root.after(0, lambda: gui.status.config(text="已终止本轮剩余识别"))
        result_holder["selected"] = None
        done_event.set()
        gui.status.config(text=prev_status)

    dlg = CandidatePickerDialog(
        gui,
        f"手动确认 {source_name} 匹配",
        header,
        items,
        show_skip_folder=True,
        on_skip_folder=on_skip_folder,
        on_close_skip_all=on_close_skip_all,
    )
    dlg_holder.append(dlg)

    def _finalize():
        if done_event.is_set() and result_holder.get("selected") is not None:
            gui.status.config(text=prev_status)
            return
        if dlg.result() == QDialog.Accepted and dlg.selected_item() is not None:
            result_holder["selected"] = dlg.selected_item()
        done_event.set()
        gui.status.config(text=prev_status)

    dlg.exec()
    if not done_event.is_set():
        _finalize()


def request_manual_candidate_choice(
    gui,
    item,
    query_title,
    source_name,
    candidates,
    recognized_title=None,
):
    if gui.preview_skip_all_event.is_set():
        return None

    item_dir = item.dir if hasattr(item, "dir") else (
        item.get("dir") if isinstance(item, dict) else None
    )
    if item_dir and item_dir in gui.preview_skip_dirs:
        return None

    auto_query = recognized_title or query_title
    auto_pick, auto_reason = auto_pick_candidate_by_score(
        auto_query, None, source_name, candidates
    )
    if auto_pick:
        logging.info(
            "候选弹窗前自动判定命中: title=%s source=%s id=%s reason=%s",
            auto_query,
            source_name,
            auto_pick.get("id"),
            auto_reason,
        )
        gui.root.after(
            0,
            lambda reason=auto_reason: gui.update_item_display(
                item, status=f"自动评分判定/{source_name}命中 ({reason})"
            ),
        )
        return auto_pick

    result_holder = {"selected": None}
    done_event = threading.Event()

    def _schedule_dialog():
        show_candidate_picker_dialog(
            gui,
            item,
            query_title,
            source_name,
            candidates,
            result_holder,
            done_event,
            recognized_title=recognized_title,
        )

    poster_urls = []
    for cand in candidates:
        meta = (cand or {}).get("meta") or {}
        url = _resolve_poster_url(meta.get("poster") or "")
        if url:
            poster_urls.append(url)

    gui.root.after(0, lambda: gui.update_item_display(item, status="海报加载中..."))
    _prefetch_poster_urls(poster_urls)
    gui.root.after(
        0, lambda: gui.update_item_display(item, status="多候选，等待手动选择")
    )

    with gui.popup_lock:
        if gui.preview_skip_all_event.is_set():
            return None
        if item_dir and item_dir in gui.preview_skip_dirs:
            return None
        gui.root.after(0, _schedule_dialog)
        if not done_event.wait(timeout=120):
            logging.warning("手动候选选择等待超时，已跳过该文件")
            done_event.set()
            gui.root.after(
                0,
                lambda: gui.update_item_display(item, status="手动选择超时，已跳过"),
            )
    return result_holder.get("selected")


def manual_match(gui):
    selected_ids = gui.get_selected_file_ids()
    if not selected_ids:
        return

    first_row_id = selected_ids[0]
    first_idx = next(
        (i for i, it in enumerate(gui.file_list) if it.id == first_row_id), None
    )
    if first_idx is None:
        return

    item = gui.file_list[first_idx]
    current_display_title = gui.tree.item(first_row_id, "values")[0]
    search_initial = (
        current_display_title if current_display_title else clean_search_title(item.old_name)
    )

    user_input, ok = QInputDialog.getText(
        gui._qt_root,
        "搜索锁定",
        f"您选中了 {len(selected_ids)} 个文件。\n\n"
        "输入资料库数字ID或搜索关键词进行强制匹配:",
        text=search_initial,
    )
    if not ok or not user_input.strip():
        return

    gui.status.config(text="正在联网搜索，请稍候...")
    threading.Thread(
        target=gui._async_manual_match_search,
        args=(selected_ids, user_input.strip(), gui.source_var.get()),
        daemon=True,
    ).start()


def show_manual_match_results(gui, selected_ids, results, error_msg=""):
    gui.status.config(text="就绪")

    if not results:
        if error_msg:
            QMessageBox.critical(gui._qt_root, "搜索失败", error_msg)
        else:
            QMessageBox.information(gui._qt_root, "无结果", "未找到匹配的条目")
        return

    if len(results) == 1:
        gui._confirm_season_and_dispatch(
            selected_ids, results[0][0], results[0][1], results[0][2], results[0][3]
        )
        return

    items = []
    for title, tid, msg, meta in results:
        release = (meta or {}).get("release", "")
        items.append(
            {
                "title": title,
                "id": tid,
                "msg": msg,
                "release": release,
                "meta": meta or {},
            }
        )

    dlg = CandidatePickerDialog(gui, "选择匹配项", "请选择正确的匹配条目：", items)
    if dlg.exec() != QDialog.Accepted or dlg.selected_index < 0:
        return

    idx = dlg.selected_index
    gui._confirm_season_and_dispatch(
        selected_ids,
        results[idx][0],
        results[idx][1],
        results[idx][2],
        results[idx][3],
    )


def confirm_season_and_dispatch(gui, selected_ids, title, tid, msg, meta):
    result = SeasonOffsetDialog.run(gui._qt_root, title)
    if not result:
        return

    new_s, offset = result
    matching_indices = []
    for i, it in enumerate(gui.file_list):
        if it.id in selected_ids:
            matching_indices.append(i)
            path_key = it.path
            with gui.cache_lock:
                gui.manual_locks[path_key] = (title, tid, msg, meta)
                gui.forced_seasons[path_key] = new_s
                gui.forced_offsets[path_key] = offset

    gui.status.config(text="后台并发匹配中...")
    gui.pbar["value"] = 0
    gui.pbar.config(maximum=len(matching_indices))

    threading.Thread(
        target=gui._async_batch_runner,
        args=(matching_indices, title, tid, msg, meta),
        daemon=True,
    ).start()


def show_context_menu(gui, global_pos):
    """完整右键菜单（global_pos 为全局坐标 QPoint）。"""
    from PySide6.QtWidgets import QMenu

    item = gui._qtree.itemAt(gui._qtree.viewport().mapFromGlobal(global_pos))
    if item is None:
        return
    iid = gui._iid_from_item(item)
    if not iid:
        return
    if iid not in gui.tree.selection():
        gui.tree.selection_set(iid)
    gui.tree.focus(iid)

    menu = QMenu(gui._qt_root)
    if gui.is_source_row(iid):
        menu.addAction("高速识别预览该分组", gui.start_preview)
        menu.addSeparator()
        is_open = bool(gui.tree.item(iid, "open"))
        menu.addAction(
            "折叠该分组" if is_open else "展开该分组",
            lambda rid=iid: gui.toggle_group_row(rid),
        )
        menu.addSeparator()
        menu.addAction(
            "从列表删除该分组",
            lambda rid=iid: gui.remove_group_by_row_id(rid),
        )
    elif gui.is_season_row(iid):
        menu.addAction("高速识别预览该 Season", gui.start_preview)
        menu.addSeparator()
        is_open = bool(gui.tree.item(iid, "open"))
        menu.addAction(
            "折叠该 Season" if is_open else "展开该 Season",
            lambda rid=iid: gui.toggle_group_row(rid),
        )
        menu.addSeparator()
        menu.addAction(
            "从列表删除该 Season",
            lambda rid=iid: gui.remove_season_group_by_row_id(rid),
        )
    else:
        sel_count = len(gui.get_selected_file_ids())
        menu.addAction(
            f"手动精准匹配并锁定(将应用到选中的 {sel_count} 个文件)",
            gui.manual_match,
        )
        menu.addSeparator()
        menu.addAction(
            "从列表删除该文件",
            lambda rid=iid: gui.remove_file_by_row_id(rid),
        )
    menu.exec(global_pos)
