import io
import logging
import threading
import tkinter as tk

import requests
from tkinter import Listbox, Scrollbar, Toplevel, messagebox, simpledialog, ttk
from PIL import Image, ImageTk

from db.tmdb_api import (
    fetch_bgm_by_id,
    fetch_tmdb_by_id,
)
from utils.helpers import (
    ERROR_CODE_CONFIG,
    ERROR_CODE_HTTP,
    ERROR_CODE_NO_RESULT,
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    USER_AGENT,
    center_window,
    clean_search_title,
    format_candidate_label,
    format_error_message,
    parse_error_message,
    session,
)

TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w92"
_POSTER_W = 62
_POSTER_H = 93


def _resolve_poster_url(poster_path):
    """将 poster_path 字段转为可下载的完整 URL。"""
    if not poster_path:
        return ""
    p = str(poster_path).strip()
    if p.startswith("http://") or p.startswith("https://"):
        return p
    if p.startswith("/"):
        return TMDB_IMAGE_BASE + p
    return ""


def _load_poster_async(url, label, image_cache):
    """在后台线程下载海报图片，下载完成后在主线程更新 Label。"""
    if not url:
        return

    def _worker():
        try:
            resp = session.get(url, timeout=10)
            resp.raise_for_status()
            img = Image.open(io.BytesIO(resp.content)).convert("RGB")
            img = img.resize((_POSTER_W, _POSTER_H), Image.LANCZOS)
            photo = ImageTk.PhotoImage(img)
            image_cache.append(photo)  # 防止 GC 回收
            label.after(0, lambda: label.config(image=photo, text=""))
        except Exception:
            pass  # 网络失败、解码失败时保留占位灰块

    threading.Thread(target=_worker, daemon=True).start()


def _bind_scroll_recursive(widget, handler):
    """递归将 MouseWheel 事件绑定到 widget 及其所有子控件。"""
    widget.bind("<MouseWheel>", handler)
    for child in widget.winfo_children():
        _bind_scroll_recursive(child, handler)


def _build_scrollable_cards(parent, items, on_select_cb):
    """
    构建可滚动的卡片列表。

    items: list of dict，每项需包含：
        title, release, id, msg, meta (含 overview、poster)
    on_select_cb(idx): 用户点击某卡片"选择"按钮时的回调
    """
    image_cache = []
    accent_bars = []   # 每张卡片的左侧蓝色选中标记条

    # 用 Pillow 生成固定像素尺寸的灰色占位图，避免 tk.Label 字符单位问题
    _placeholder = ImageTk.PhotoImage(
        Image.new("RGB", (_POSTER_W, _POSTER_H), "#cccccc")
    )
    image_cache.append(_placeholder)

    outer = ttk.Frame(parent)
    outer.pack(fill=tk.BOTH, expand=True)

    canvas = tk.Canvas(outer, highlightthickness=0)
    vbar = ttk.Scrollbar(outer, orient=tk.VERTICAL, command=canvas.yview)
    canvas.configure(yscrollcommand=vbar.set)

    vbar.pack(side=tk.RIGHT, fill=tk.Y)
    canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    inner = ttk.Frame(canvas)
    canvas_win = canvas.create_window((0, 0), window=inner, anchor="nw")

    inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
    canvas.bind("<Configure>", lambda e: canvas.itemconfig(canvas_win, width=e.width))

    def _scroll(event):
        canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _highlight(idx):
        for i, bar in enumerate(accent_bars):
            bar.configure(bg="#2563eb" if i == idx else "#d0d0d0")
        on_select_cb(idx)

    for idx, item in enumerate(items):
        title = item.get("title") or "未知"
        release = item.get("release") or ""
        year = release[:4] if release and len(release) >= 4 else "-"
        tmdb_id = item.get("id") or "-"
        source = item.get("msg") or ""
        meta = item.get("meta") or {}
        overview = (meta.get("overview") or "").strip()
        overview = " ".join(overview.split())
        if len(overview) > 100:
            overview = overview[:100] + "..."
        poster_url = _resolve_poster_url(meta.get("poster") or "")

        card = ttk.Frame(inner, relief="groove", borderwidth=1, padding=4)
        card.pack(fill=tk.X, padx=6, pady=3)

        # 左侧选中高亮竖条（未选中=灰色，选中=蓝色）
        accent = tk.Frame(card, width=5, bg="#d0d0d0")
        accent.grid(row=0, column=0, rowspan=3, sticky="ns", padx=(0, 6))
        accent_bars.append(accent)

        # 海报图（直接用 image 模式，像素级精确尺寸）
        poster_lbl = tk.Label(card, image=_placeholder, borderwidth=0, cursor="hand2")
        poster_lbl.grid(row=0, column=1, rowspan=3, padx=(0, 8), pady=2, sticky="ns")
        if poster_url:
            _load_poster_async(poster_url, poster_lbl, image_cache)

        # 中部文字区
        id_tag = f"TMDB ID: {tmdb_id}" if "BGM" not in source else f"BGM ID: {tmdb_id}"
        header_frame = ttk.Frame(card)
        header_frame.grid(row=0, column=2, sticky="w")
        ttk.Label(
            header_frame, text=title, font=("", 10, "bold"), anchor="w"
        ).pack(side=tk.LEFT)
        ttk.Label(
            header_frame, text=f"  {year}", foreground="#888888", font=("", 9)
        ).pack(side=tk.LEFT)
        ttk.Label(
            header_frame,
            text=f"  {id_tag}",
            foreground="#e08020",
            font=("", 9),
        ).pack(side=tk.LEFT)

        ttk.Label(
            card, text=overview, wraplength=420, justify=tk.LEFT,
            foreground="#555555", font=("", 9),
        ).grid(row=1, column=2, sticky="w", pady=(0, 2))

        # 右侧选择按钮
        def _make_cb(i):
            def _cb():
                _highlight(i)
            return _cb

        btn = ttk.Button(card, text="选择", command=_make_cb(idx), width=6)
        btn.grid(row=0, column=3, rowspan=2, padx=(8, 2), sticky="e")

        card.columnconfigure(2, weight=1)

        # 点击卡片任意位置也触发高亮
        def _make_click_cb(i):
            def _cb(event=None):
                _highlight(i)
            return _cb

        for w in (card, poster_lbl, accent):
            w.bind("<Button-1>", _make_click_cb(idx))

    # 所有卡片构建完毕后，递归绑定滚轮到全部子控件
    _bind_scroll_recursive(outer, _scroll)

    # 将 image_cache 挂到 outer widget 上，防止 PhotoImage 被 Python GC 提前回收
    outer._image_cache = image_cache

    return outer


def _response_body_snippet(response, limit=300):
    if response is None:
        return ""
    try:
        body = response.text or ""
    except Exception:
        return ""
    compact = " ".join(str(body).split())
    if len(compact) > limit:
        return compact[:limit] + "..."
    return compact


def request_manual_candidate_choice(gui, item, query_title, source_name, candidates):
    """Schedule manual picker on main thread and wait from worker thread."""
    result_holder = {"selected": None}
    done_event = threading.Event()

    def _schedule_dialog():
        show_candidate_picker_dialog(
            gui, item, query_title, source_name, candidates, result_holder, done_event
        )

    gui.root.after(0, lambda: gui.tree.set(item["id"], "st", "多候选，等待手动选择"))
    with gui.popup_lock:
        gui.root.after(0, _schedule_dialog)
        if not done_event.wait(timeout=120):
            logging.warning("手动候选选择等待超时，已跳过该文件")
            done_event.set()
            gui.root.after(
                0,
                lambda: gui.tree.set(item["id"], "st", "手动选择超时，已跳过"),
            )
    return result_holder.get("selected")


def show_candidate_picker_dialog(
    gui, item, query_title, source_name, candidates, result_holder, done_event
):
    """Show candidate picker dialog for ambiguous DB matches."""
    prev_status = gui.status.cget("text")
    gui.status.config(text=f"等待手动选择: {item.get('old_name', '')}")

    select_win = Toplevel(gui.root)
    select_win.title(f"手动确认 {source_name} 匹配")
    select_win.transient(gui.root)
    center_window(select_win, gui.root, 760, 520)
    select_win.after_idle(lambda: center_window(select_win, gui.root, 760, 520))
    select_win.attributes("-topmost", True)

    label_text = (
        f"文件: {item.get('old_name', '')}\n"
        f"识别标题: {query_title}\n"
        "请在下方候选中选择正确条目："
    )
    ttk.Label(select_win, text=label_text, justify=tk.LEFT).pack(
        anchor="w", padx=10, pady=(10, 4)
    )

    card_area = ttk.Frame(select_win)
    card_area.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

    selected_holder = {"idx": -1}

    def on_card_select(idx):
        selected_holder["idx"] = idx

    _build_scrollable_cards(card_area, candidates, on_card_select)

    def on_confirm():
        idx = selected_holder["idx"]
        if idx < 0 or idx >= len(candidates):
            messagebox.showinfo("提示", '请先点击某条目的"选择"按钮', parent=select_win)
            return
        result_holder["selected"] = candidates[idx]
        if not done_event.is_set():
            done_event.set()
        select_win.destroy()

    def on_skip():
        result_holder["selected"] = None
        if not done_event.is_set():
            done_event.set()
        select_win.destroy()

    btn_frame = ttk.Frame(select_win)
    btn_frame.pack(fill=tk.X, padx=10, pady=8)
    ttk.Button(btn_frame, text="确认选择", command=on_confirm).pack(side=tk.LEFT)
    ttk.Button(btn_frame, text="跳过此文件", command=on_skip).pack(
        side=tk.LEFT, padx=8
    )

    select_win.protocol("WM_DELETE_WINDOW", on_skip)
    select_win.grab_set()
    try:
        select_win.wait_window()
    finally:
        if not done_event.is_set():
            done_event.set()
        gui.status.config(text=prev_status)


def show_context_menu(gui, event):
    """Show right-click menu for manual lock matching."""
    row = gui.tree.identify_row(event.y)
    if row:
        if row not in gui.tree.selection():
            gui.tree.selection_set(row)

        sel_count = len(gui.tree.selection())
        menu = tk.Menu(gui.root, tearoff=0)
        menu.add_command(
            label=f"手动精准匹配并锁定 (将应用到选中的 {sel_count} 个文件)",
            command=gui.manual_match,
        )
        menu.post(event.x_root, event.y_root)


def manual_match(gui):
    """Entry point for manual match workflow."""
    selected_ids = gui.tree.selection()
    if not selected_ids:
        return

    first_row_id = selected_ids[0]
    first_idx = next(
        (i for i, it in enumerate(gui.file_list) if it["id"] == first_row_id), None
    )

    if first_idx is None:
        return

    item = gui.file_list[first_idx]
    current_display_title = gui.tree.item(first_row_id, "values")[1]
    search_initial = (
        current_display_title
        if current_display_title
        else clean_search_title(item["old_name"])
    )

    user_input = simpledialog.askstring(
        "搜索锁定",
        f"您选中了 {len(selected_ids)} 个文件。\n\n输入资料库数字ID或搜索关键词进行强制匹配:",
        initialvalue=search_initial,
        parent=gui.root,
    )

    if not user_input:
        return

    user_input = user_input.strip()
    mode = gui.source_var.get()
    gui.status.config(text="正在联网搜索，请稍候...")

    threading.Thread(
        target=gui._async_manual_match_search,
        args=(selected_ids, user_input, mode),
        daemon=True,
    ).start()


def async_manual_match_search(gui, selected_ids, user_input, mode):
    """Search candidates for manual match by ID or keyword."""
    results = []
    search_errors = []

    def append_error(source_name, msg_text):
        code, detail = parse_error_message(msg_text)
        if not code or code == ERROR_CODE_NO_RESULT:
            return
        prefix = {
            ERROR_CODE_TIMEOUT: "请求超时",
            ERROR_CODE_CONFIG: "配置缺失",
            ERROR_CODE_HTTP: "HTTP失败",
            ERROR_CODE_PARSE: "响应解析失败",
            ERROR_CODE_UNKNOWN: "请求异常",
        }.get(code, "请求异常")
        final_text = detail or str(msg_text)
        search_errors.append(f"{source_name}{prefix}: {final_text}")

    try:
        if user_input.isdigit():
            if mode == "siliconflow_bgm":
                t, tid, msg, meta = fetch_bgm_by_id(user_input, gui.bgm_api_key.get())
                if tid != "None":
                    results = [(t, tid, msg, meta)]
                else:
                    append_error("BGM", msg)
            else:
                t, tid, msg, meta = fetch_tmdb_by_id(
                    user_input, True, gui.tmdb_api_key.get()
                )
                if tid == "None":
                    append_error("TMDb剧集", msg)
                    t, tid, msg, meta = fetch_tmdb_by_id(
                        user_input, False, gui.tmdb_api_key.get()
                    )
                    if tid == "None":
                        append_error("TMDb电影", msg)
                if tid != "None":
                    results = [(t, tid, msg, meta)]
        else:
            if mode == "siliconflow_bgm":
                query = clean_search_title(user_input)
                headers = {"User-Agent": USER_AGENT}
                if gui.bgm_api_key.get().strip():
                    headers["Authorization"] = f"Bearer {gui.bgm_api_key.get().strip()}"

                try:
                    res = session.get(
                        f"https://api.bgm.tv/search/subject/{query}?type=2",
                        headers=headers,
                        timeout=60,
                    )
                    res.raise_for_status()
                    items = res.json().get("list", [])

                    for it in items[:5]:
                        title = it.get("name_cn") or it.get("name") or "未知"
                        meta = {
                            "overview": it.get("summary", ""),
                            "rating": it.get("score", 0),
                            "poster": it.get("images", {}).get("large", ""),
                            "fanart": "",
                            "release": it.get("air_date", ""),
                        }
                        results.append((title, str(it.get("id")), "搜索结果", meta))
                except requests.exceptions.Timeout:
                    append_error("BGM", format_error_message(ERROR_CODE_TIMEOUT, "请求超时"))
                except requests.exceptions.HTTPError as err:
                    snippet = _response_body_snippet(getattr(err, "response", None))
                    if snippet:
                        logging.warning(f"BGM手动搜索HTTP失败，返回内容: {snippet}")
                    append_error(
                        "BGM", format_error_message(ERROR_CODE_HTTP, f"HTTP请求失败: {err}")
                    )
                except ValueError as err:
                    snippet = _response_body_snippet(locals().get("res"))
                    if snippet:
                        logging.warning(f"BGM手动搜索解析失败，返回内容: {snippet}")
                    append_error(
                        "BGM", format_error_message(ERROR_CODE_PARSE, f"响应解析失败: {err}")
                    )
                except Exception as err:
                    logging.error(f"BGM手动搜索请求失败: {err}")
                    append_error("BGM", format_error_message(ERROR_CODE_UNKNOWN, "请求异常"))
            else:
                try:
                    res_tv = session.get(
                        "https://api.themoviedb.org/3/search/tv",
                        params={
                            "api_key": gui.tmdb_api_key.get().strip(),
                            "query": user_input,
                            "language": "zh-CN",
                        },
                        timeout=60,
                    )
                    res_tv.raise_for_status()
                    tv_results = res_tv.json().get("results", [])[:3]

                    for it in tv_results:
                        meta = {
                            "overview": it.get("overview", ""),
                            "rating": it.get("vote_average", 0),
                            "poster": it.get("poster_path", ""),
                            "fanart": it.get("backdrop_path", ""),
                            "release": it.get("first_air_date", ""),
                        }
                        results.append((it.get("name", "未知"), str(it.get("id")), "TMDb剧集", meta))

                    res_movie = session.get(
                        "https://api.themoviedb.org/3/search/movie",
                        params={
                            "api_key": gui.tmdb_api_key.get().strip(),
                            "query": user_input,
                            "language": "zh-CN",
                        },
                        timeout=60,
                    )
                    res_movie.raise_for_status()
                    movie_results = res_movie.json().get("results", [])[:2]

                    for it in movie_results:
                        meta = {
                            "overview": it.get("overview", ""),
                            "rating": it.get("vote_average", 0),
                            "poster": it.get("poster_path", ""),
                            "fanart": it.get("backdrop_path", ""),
                            "release": it.get("release_date", ""),
                        }
                        results.append((it.get("title", "未知"), str(it.get("id")), "TMDb电影", meta))
                except requests.exceptions.Timeout:
                    append_error("TMDb", format_error_message(ERROR_CODE_TIMEOUT, "请求超时"))
                except requests.exceptions.HTTPError as err:
                    snippet = _response_body_snippet(getattr(err, "response", None))
                    if snippet:
                        logging.warning(f"TMDb手动搜索HTTP失败，返回内容: {snippet}")
                    append_error(
                        "TMDb", format_error_message(ERROR_CODE_HTTP, f"HTTP请求失败: {err}")
                    )
                except ValueError as err:
                    snippet = _response_body_snippet(locals().get("res_tv") or locals().get("res_movie"))
                    if snippet:
                        logging.warning(f"TMDb手动搜索解析失败，返回内容: {snippet}")
                    append_error(
                        "TMDb", format_error_message(ERROR_CODE_PARSE, f"响应解析失败: {err}")
                    )
                except Exception as err:
                    logging.error(f"TMDb手动搜索请求失败: {err}")
                    append_error("TMDb", format_error_message(ERROR_CODE_UNKNOWN, "请求异常"))
    except Exception as err:
        logging.error(f"手动匹配搜索失败: {err}")
        append_error("手动匹配", format_error_message(ERROR_CODE_UNKNOWN, str(err)))

    error_msg = "；".join(dict.fromkeys(search_errors)) if search_errors else ""
    gui.root.after(0, gui._show_manual_match_results, selected_ids, results, error_msg)


def show_manual_match_results(gui, selected_ids, results, error_msg=""):
    """Present manual match search results and continue with selected entry."""
    gui.status.config(text="就绪")

    if not results:
        if error_msg:
            messagebox.showerror("搜索失败", error_msg, parent=gui.root)
        else:
            messagebox.showinfo("无结果", "未找到匹配的条目", parent=gui.root)
        return

    if len(results) == 1:
        gui._confirm_season_and_dispatch(
            selected_ids, results[0][0], results[0][1], results[0][2], results[0][3]
        )
        return

    # 将 (title, tid, msg, meta) tuple 列表转成 dict 供卡片组件使用
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

    select_win = Toplevel(gui.root)
    select_win.title("选择匹配项")
    select_win.transient(gui.root)
    center_window(select_win, gui.root, 720, 480)
    select_win.after_idle(lambda: center_window(select_win, gui.root, 720, 480))

    selected_holder = {"idx": -1}

    def on_card_select(idx):
        selected_holder["idx"] = idx

    card_area = ttk.Frame(select_win)
    card_area.pack(fill=tk.BOTH, expand=True, padx=8, pady=(8, 4))
    _build_scrollable_cards(card_area, items, on_card_select)

    def on_confirm():
        idx = selected_holder["idx"]
        if idx < 0 or idx >= len(results):
            messagebox.showinfo("提示", '请先点击某条目的"选择"按钮', parent=select_win)
            return
        gui._confirm_season_and_dispatch(
            selected_ids,
            results[idx][0],
            results[idx][1],
            results[idx][2],
            results[idx][3],
        )
        select_win.destroy()

    btn_frame = ttk.Frame(select_win)
    btn_frame.pack(fill=tk.X, padx=10, pady=8)
    ttk.Button(btn_frame, text="确认选择", command=on_confirm).pack(side=tk.LEFT)
    ttk.Button(btn_frame, text="取消", command=select_win.destroy).pack(
        side=tk.LEFT, padx=8
    )

    select_win.grab_set()
    gui.root.wait_window(select_win)


def confirm_season_and_dispatch(gui, selected_ids, title, tid, msg, meta, dialog_cls):
    """Collect season/offset override and fan out background updates."""
    dialog = dialog_cls(gui.root, title)
    if not dialog.result:
        return

    new_s, offset = dialog.result

    matching_indices = []
    for i, it in enumerate(gui.file_list):
        if it["id"] in selected_ids:
            matching_indices.append(i)
            path_key = it["path"]
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
