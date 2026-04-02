import io
import logging
import threading
import tkinter as tk
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

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
_POSTER_REQUEST_TIMEOUT = 8
_POSTER_POOL_WORKERS = 4
_POSTER_CACHE_MAX_ITEMS = 120

_poster_executor = ThreadPoolExecutor(
    max_workers=_POSTER_POOL_WORKERS,
    thread_name_prefix="poster",
)
_poster_lock = threading.Lock()
_poster_photo_cache = OrderedDict()
_poster_pil_cache = OrderedDict()
_poster_pending = set()


def _resolve_poster_url(poster_path):
    """灏?poster_path 瀛楁杞负鍙笅杞界殑瀹屾暣 URL銆"""
    if not poster_path:
        return ""
    p = str(poster_path).strip()
    if p.startswith("http://") or p.startswith("https://"):
        return p
    if p.startswith("/"):
        return TMDB_IMAGE_BASE + p
    return ""


def _get_cached_poster_photo(url):
    """绾跨▼瀹夊叏璇诲彇娴锋姤缂撳瓨锛屽懡涓椂鍒锋柊 LRU 椤哄簭銆"""
    if not url:
        return None
    with _poster_lock:
        photo = _poster_photo_cache.get(url)
        if photo is not None:
            _poster_photo_cache.move_to_end(url)
        return photo


def _cache_poster_photo(url, photo):
    """绾跨▼瀹夊叏鍐欏叆娴锋姤缂撳瓨骞舵墽琛?LRU 娣樻卑銆"""
    if not url or photo is None:
        return
    with _poster_lock:
        _poster_photo_cache[url] = photo
        _poster_photo_cache.move_to_end(url)
        while len(_poster_photo_cache) > _POSTER_CACHE_MAX_ITEMS:
            _poster_photo_cache.popitem(last=False)


def _get_cached_poster_pil(url):
    """绾跨▼瀹夊叏璇诲彇宸茬缉鏀剧殑 PIL 娴锋姤缂撳瓨锛屽懡涓椂鍒锋柊 LRU 椤哄簭銆"""
    if not url:
        return None
    with _poster_lock:
        img = _poster_pil_cache.get(url)
        if img is not None:
            _poster_pil_cache.move_to_end(url)
        return img


def _cache_poster_pil(url, img):
    """绾跨▼瀹夊叏鍐欏叆宸茬缉鏀剧殑 PIL 娴锋姤缂撳瓨骞舵墽琛?LRU 娣樻卑銆"""
    if not url or img is None:
        return
    with _poster_lock:
        _poster_pil_cache[url] = img
        _poster_pil_cache.move_to_end(url)
        while len(_poster_pil_cache) > _POSTER_CACHE_MAX_ITEMS:
            _poster_pil_cache.popitem(last=False)


def _fetch_and_resize_poster(url):
    """涓嬭浇骞剁缉鏀炬捣鎶ワ紝杩斿洖 PIL.Image銆"""
    resp = session.get(url, timeout=_POSTER_REQUEST_TIMEOUT)
    resp.raise_for_status()
    img = Image.open(io.BytesIO(resp.content)).convert("RGB")
    return img.resize((_POSTER_W, _POSTER_H), Image.LANCZOS)


def _set_label_from_cache(url, label, image_cache):
    """浼樺厛浠庣紦瀛樿缃捣鎶ワ細PhotoImage 缂撳瓨 > PIL 缂撳瓨銆"""
    cached_photo = _get_cached_poster_photo(url)
    if cached_photo is not None:
        try:
            if label.winfo_exists():
                image_cache.append(cached_photo)  # 闃叉 GC 鍥炴敹
                label.config(image=cached_photo, text="")
                return True
        except Exception:
            return False

    cached_img = _get_cached_poster_pil(url)
    if cached_img is not None:
        try:
            if label.winfo_exists():
                photo = ImageTk.PhotoImage(cached_img)
                _cache_poster_photo(url, photo)
                image_cache.append(photo)  # 闃叉 GC 鍥炴敹
                label.config(image=photo, text="")
                return True
        except Exception:
            return False

    return False


def _prefetch_poster_urls(urls):
    """鍦ㄥ悗鍙伴鍔犺浇鍊欓€夋捣鎶ワ紝纭繚寮圭獥鍑虹幇鏃跺浘鐗囧凡灏辩华銆"""
    unique_urls = []
    seen = set()
    for u in urls:
        url = str(u or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        if _get_cached_poster_photo(url) is not None:
            continue
        if _get_cached_poster_pil(url) is not None:
            continue
        unique_urls.append(url)

    if not unique_urls:
        return

    def _prefetch_one(url):
        with _poster_lock:
            if url in _poster_pending:
                return
            _poster_pending.add(url)

        try:
            img = _fetch_and_resize_poster(url)
            _cache_poster_pil(url, img)
        except Exception:
            pass
        finally:
            with _poster_lock:
                _poster_pending.discard(url)

    futures = []
    for url in unique_urls:
        try:
            futures.append(_poster_executor.submit(_prefetch_one, url))
        except Exception:
            pass

    for fut in futures:
        try:
            fut.result(timeout=_POSTER_REQUEST_TIMEOUT + 2)
        except Exception:
            pass


def _load_poster_async(url, label, image_cache):
    """鍦ㄥ悗鍙扮嚎绋嬩笅杞芥捣鎶ワ紝涓荤嚎绋嬪垱寤?PhotoImage 骞舵洿鏂?Label銆"""
    if not url:
        return
    try:
        if not label.winfo_exists():
            return
    except Exception:
        return

    if _set_label_from_cache(url, label, image_cache):
        return

    with _poster_lock:
        if url in _poster_pending:
            return
        _poster_pending.add(url)

    def _clear_pending():
        with _poster_lock:
            _poster_pending.discard(url)

    def _apply_on_main_thread(img):
        try:
            if not label.winfo_exists():
                return
            _cache_poster_pil(url, img)
            photo = ImageTk.PhotoImage(img)
            _cache_poster_photo(url, photo)
            image_cache.append(photo)  # 闃叉 GC 鍥炴敹
            label.config(image=photo, text="")
        except Exception:
            pass
        finally:
            _clear_pending()

    def _worker():
        try:
            img = _fetch_and_resize_poster(url)
        except Exception:
            _clear_pending()
            return

        try:
            if label.winfo_exists():
                label.after(0, lambda im=img: _apply_on_main_thread(im))
            else:
                _clear_pending()
        except Exception:
            _clear_pending()

    try:
        _poster_executor.submit(_worker)
    except Exception:
        _clear_pending()


def _bind_scroll_recursive(widget, handler):
    """閫掑綊灏?MouseWheel 浜嬩欢缁戝畾鍒?widget 鍙婂叾鎵€鏈夊瓙鎺т欢銆"""
    widget.bind("<MouseWheel>", handler)
    for child in widget.winfo_children():
        _bind_scroll_recursive(child, handler)


def _build_scrollable_cards(parent, items, on_select_cb):
    """
    鏋勫缓鍙粴鍔ㄧ殑鍗＄墖鍒楄〃銆?

    items: list of dict锛屾瘡椤归渶鍖呭惈锛?
        title, release, id, msg, meta (鍚?overview銆乸oster)
    on_select_cb(idx): 鐢ㄦ埛鐐瑰嚮鏌愬崱鐗?閫夋嫨"鎸夐挳鏃剁殑鍥炶皟
    """
    image_cache = []
    accent_bars = []   # 姣忓紶鍗＄墖鐨勫乏渚ц摑鑹查€変腑鏍囪鏉?

    # 鐢?Pillow 鐢熸垚鍥哄畾鍍忕礌灏哄鐨勭伆鑹插崰浣嶅浘锛岄伩鍏?tk.Label 瀛楃鍗曚綅闂
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
        title = item.get("title") or "鏈煡"
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

        # 宸︿晶閫変腑楂樹寒绔栨潯锛堟湭閫変腑=鐏拌壊锛岄€変腑=钃濊壊锛?
        accent = tk.Frame(card, width=5, bg="#d0d0d0")
        accent.grid(row=0, column=0, rowspan=3, sticky="ns", padx=(0, 6))
        accent_bars.append(accent)

        # 娴锋姤鍥撅紙鐩存帴鐢?image 妯″紡锛屽儚绱犵骇绮剧‘灏哄锛?
        poster_lbl = tk.Label(card, image=_placeholder, borderwidth=0, cursor="hand2")
        poster_lbl.grid(row=0, column=1, rowspan=3, padx=(0, 8), pady=2, sticky="ns")
        if poster_url:
            _load_poster_async(poster_url, poster_lbl, image_cache)

        # 涓儴鏂囧瓧鍖?
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

        # 鍙充晶閫夋嫨鎸夐挳
        def _make_cb(i):
            def _cb():
                _highlight(i)
            return _cb

        btn = ttk.Button(card, text="选择", command=_make_cb(idx), width=6)
        btn.grid(row=0, column=3, rowspan=2, padx=(8, 2), sticky="e")

        card.columnconfigure(2, weight=1)

        # 鐐瑰嚮鍗＄墖浠绘剰浣嶇疆涔熻Е鍙戦珮浜?
        def _make_click_cb(i):
            def _cb(event=None):
                _highlight(i)
            return _cb

        for w in (card, poster_lbl, accent):
            w.bind("<Button-1>", _make_click_cb(idx))

    # 鎵€鏈夊崱鐗囨瀯寤哄畬姣曞悗锛岄€掑綊缁戝畾婊氳疆鍒板叏閮ㄥ瓙鎺т欢
    _bind_scroll_recursive(outer, _scroll)

    # 灏?image_cache 鎸傚埌 outer widget 涓婏紝闃叉 PhotoImage 琚?Python GC 鎻愬墠鍥炴敹
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


def request_manual_candidate_choice(
    gui,
    item,
    query_title,
    source_name,
    candidates,
    recognized_title=None,
):
    """Schedule manual picker on main thread and wait from worker thread."""
    if gui.preview_skip_all_event.is_set():
        return None

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
        poster_url = _resolve_poster_url(meta.get("poster") or "")
        if poster_url:
            poster_urls.append(poster_url)

    gui.root.after(0, lambda: gui.tree.set(item["id"], "st", "海报加载中..."))
    _prefetch_poster_urls(poster_urls)

    gui.root.after(0, lambda: gui.tree.set(item["id"], "st", "多候选，等待手动选择"))
    with gui.popup_lock:
        if gui.preview_skip_all_event.is_set():
            return None
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
    gui,
    item,
    query_title,
    source_name,
    candidates,
    result_holder,
    done_event,
    recognized_title=None,
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

    recognized_text = str(recognized_title or query_title or "").strip()
    searched_text = str(query_title or "").strip()

    if recognized_text and searched_text and recognized_text != searched_text:
        title_block = f"识别标题: {recognized_text}\n搜索标题: {searched_text}"
    else:
        title_block = f"识别标题: {recognized_text or searched_text}"

    label_text = (
        f"文件: {item.get('old_name', '')}\n"
        f"{title_block}\n"
        "请在下方候选中选择正确条目："
    )
    ttk.Label(select_win, text=label_text, justify=tk.LEFT).pack(
        anchor="w", padx=10, pady=(10, 4)
    )

    card_area = ttk.Frame(select_win)
    card_area.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

    selected_holder = {"idx": -1}
    dialog_closed = {"done": False}

    def on_card_select(idx):
        selected_holder["idx"] = idx

    _build_scrollable_cards(card_area, candidates, on_card_select)

    def _finalize(selected):
        if dialog_closed["done"]:
            return
        dialog_closed["done"] = True
        result_holder["selected"] = selected
        if not done_event.is_set():
            done_event.set()
        gui.status.config(text=prev_status)

    def _close_with_result(selected):
        if dialog_closed["done"]:
            return
        result_holder["selected"] = selected
        try:
            if select_win.winfo_exists():
                try:
                    select_win.grab_release()
                except Exception:
                    pass
                select_win.destroy()
        finally:
            _finalize(result_holder.get("selected"))

    def on_confirm():
        idx = selected_holder["idx"]
        if idx < 0 or idx >= len(candidates):
            messagebox.showinfo("提示", '请先点击某条目的"选择"按钮', parent=select_win)
            return
        _close_with_result(candidates[idx])

    def on_skip():
        _close_with_result(None)

    def on_close_skip_all():
        gui.preview_skip_all_event.set()
        gui.root.after(0, lambda: gui.status.config(text="已终止本轮剩余识别"))
        _close_with_result(None)

    def on_destroy(event):
        if event.widget is select_win:
            _finalize(result_holder.get("selected"))

    btn_frame = ttk.Frame(select_win)
    btn_frame.pack(fill=tk.X, padx=10, pady=8)
    ttk.Button(btn_frame, text="确认选择", command=on_confirm).pack(side=tk.LEFT)
    ttk.Button(btn_frame, text="跳过此文件", command=on_skip).pack(
        side=tk.LEFT, padx=8
    )

    select_win.bind("<Destroy>", on_destroy)
    select_win.bind("<Escape>", lambda _e: on_skip())
    select_win.protocol("WM_DELETE_WINDOW", on_close_skip_all)
    select_win.grab_set()
    select_win.wait_window()
    _finalize(result_holder.get("selected"))


def show_context_menu(gui, event):
    """Show right-click menu for manual lock matching."""
    row = gui.tree.identify_row(event.y)
    if row:
        if row not in gui.tree.selection():
            gui.tree.selection_set(row)

        sel_count = len(gui.tree.selection())
        menu = tk.Menu(gui.root, tearoff=0)
        menu.add_command(
            label=f"手动精准匹配并锁定(将应用到选中的 {sel_count} 个文件)",
            command=gui.manual_match,
        )
        menu.add_separator()
        menu.add_command(
            label="从列表删除该文件",
            command=lambda row_id=row: gui.remove_file_by_row_id(row_id),
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
        "鎼滅储閿佸畾",
        f"鎮ㄩ€変腑浜?{len(selected_ids)} 涓枃浠躲€俓n\n杈撳叆璧勬枡搴撴暟瀛桰D鎴栨悳绱㈠叧閿瘝杩涜寮哄埗鍖归厤:",
        initialvalue=search_initial,
        parent=gui.root,
    )

    if not user_input:
        return

    user_input = user_input.strip()
    mode = gui.source_var.get()
    gui.status.config(text="姝ｅ湪鑱旂綉鎼滅储锛岃绋嶅€?..")

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
            ERROR_CODE_TIMEOUT: "璇锋眰瓒呮椂",
            ERROR_CODE_CONFIG: "閰嶇疆缂哄け",
            ERROR_CODE_HTTP: "HTTP澶辫触",
            ERROR_CODE_PARSE: "鍝嶅簲瑙ｆ瀽澶辫触",
            ERROR_CODE_UNKNOWN: "璇锋眰寮傚父",
        }.get(code, "璇锋眰寮傚父")
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
                    append_error("TMDb鍓ч泦", msg)
                    t, tid, msg, meta = fetch_tmdb_by_id(
                        user_input, False, gui.tmdb_api_key.get()
                    )
                    if tid == "None":
                        append_error("TMDb鐢靛奖", msg)
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
                        title = it.get("name_cn") or it.get("name") or "鏈煡"
                        meta = {
                            "overview": it.get("summary", ""),
                            "rating": it.get("score", 0),
                            "poster": it.get("images", {}).get("large", ""),
                            "fanart": "",
                            "release": it.get("air_date", ""),
                        }
                        results.append((title, str(it.get("id")), "鎼滅储缁撴灉", meta))
                except requests.exceptions.Timeout:
                    append_error("BGM", format_error_message(ERROR_CODE_TIMEOUT, "璇锋眰瓒呮椂"))
                except requests.exceptions.HTTPError as err:
                    snippet = _response_body_snippet(getattr(err, "response", None))
                    if snippet:
                        logging.warning(f"BGM鎵嬪姩鎼滅储HTTP澶辫触锛岃繑鍥炲唴瀹? {snippet}")
                    append_error(
                        "BGM", format_error_message(ERROR_CODE_HTTP, f"HTTP璇锋眰澶辫触: {err}")
                    )
                except ValueError as err:
                    snippet = _response_body_snippet(locals().get("res"))
                    if snippet:
                        logging.warning(f"BGM鎵嬪姩鎼滅储瑙ｆ瀽澶辫触锛岃繑鍥炲唴瀹? {snippet}")
                    append_error(
                        "BGM", format_error_message(ERROR_CODE_PARSE, f"鍝嶅簲瑙ｆ瀽澶辫触: {err}")
                    )
                except Exception as err:
                    logging.error(f"BGM鎵嬪姩鎼滅储璇锋眰澶辫触: {err}")
                    append_error("BGM", format_error_message(ERROR_CODE_UNKNOWN, "璇锋眰寮傚父"))
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
                        results.append((it.get("name", "鏈煡"), str(it.get("id")), "TMDb鍓ч泦", meta))

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
                        results.append((it.get("title", "鏈煡"), str(it.get("id")), "TMDb鐢靛奖", meta))
                except requests.exceptions.Timeout:
                    append_error("TMDb", format_error_message(ERROR_CODE_TIMEOUT, "璇锋眰瓒呮椂"))
                except requests.exceptions.HTTPError as err:
                    snippet = _response_body_snippet(getattr(err, "response", None))
                    if snippet:
                        logging.warning(f"TMDb鎵嬪姩鎼滅储HTTP澶辫触锛岃繑鍥炲唴瀹? {snippet}")
                    append_error(
                        "TMDb", format_error_message(ERROR_CODE_HTTP, f"HTTP璇锋眰澶辫触: {err}")
                    )
                except ValueError as err:
                    snippet = _response_body_snippet(locals().get("res_tv") or locals().get("res_movie"))
                    if snippet:
                        logging.warning(f"TMDb鎵嬪姩鎼滅储瑙ｆ瀽澶辫触锛岃繑鍥炲唴瀹? {snippet}")
                    append_error(
                        "TMDb", format_error_message(ERROR_CODE_PARSE, f"鍝嶅簲瑙ｆ瀽澶辫触: {err}")
                    )
                except Exception as err:
                    logging.error(f"TMDb鎵嬪姩鎼滅储璇锋眰澶辫触: {err}")
                    append_error("TMDb", format_error_message(ERROR_CODE_UNKNOWN, "璇锋眰寮傚父"))
    except Exception as err:
        logging.error(f"鎵嬪姩鍖归厤鎼滅储澶辫触: {err}")
        append_error("鎵嬪姩鍖归厤", format_error_message(ERROR_CODE_UNKNOWN, str(err)))

    poster_urls = []
    for _, _, _, meta in results:
        m = meta or {}
        poster_url = _resolve_poster_url(m.get("poster") or "")
        if poster_url:
            poster_urls.append(poster_url)

    _prefetch_poster_urls(poster_urls)

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

    # 灏?(title, tid, msg, meta) tuple 鍒楄〃杞垚 dict 渚涘崱鐗囩粍浠朵娇鐢?
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

    gui.status.config(text="鍚庡彴骞跺彂鍖归厤涓?..")
    gui.pbar["value"] = 0
    gui.pbar.config(maximum=len(matching_indices))

    threading.Thread(
        target=gui._async_batch_runner,
        args=(matching_indices, title, tid, msg, meta),
        daemon=True,
    ).start()


