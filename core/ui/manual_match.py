import logging
import threading
import tkinter as tk

import requests
from tkinter import Listbox, Scrollbar, Toplevel, messagebox, simpledialog, ttk

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
    center_window(select_win, gui.root, 900, 420)
    select_win.attributes("-topmost", True)

    label_text = f"""文件: {item.get("old_name", "")}
识别标题: {query_title}
请在下方候选中选择正确条目："""
    ttk.Label(select_win, text=label_text, justify=tk.LEFT).pack(
        anchor="w", padx=10, pady=(10, 6)
    )

    list_frame = ttk.Frame(select_win)
    list_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

    lb = Listbox(list_frame, width=120, height=12)
    lb.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    scroll = Scrollbar(list_frame, orient=tk.VERTICAL, command=lb.yview)
    scroll.pack(side=tk.RIGHT, fill=tk.Y)
    lb.config(yscrollcommand=scroll.set)

    detail_var = tk.StringVar(value="")
    ttk.Label(
        select_win, textvariable=detail_var, justify=tk.LEFT, foreground="gray"
    ).pack(anchor="w", padx=10, pady=(0, 4))

    for candidate in candidates:
        lb.insert(tk.END, format_candidate_label(candidate))

    def update_detail(event=None):
        sel = lb.curselection()
        if not sel:
            return
        cand = candidates[sel[0]]
        overview = (cand.get("meta") or {}).get("overview") or "无简介"
        overview = " ".join(str(overview).split())
        if len(overview) > 140:
            overview = overview[:140] + "..."
        detail_var.set(f"简介: {overview}")

    def on_confirm(event=None):
        sel = lb.curselection()
        if not sel:
            messagebox.showinfo("提示", "请先选择一项", parent=select_win)
            return
        result_holder["selected"] = candidates[sel[0]]
        if not done_event.is_set():
            done_event.set()
        select_win.destroy()

    def on_skip():
        result_holder["selected"] = None
        if not done_event.is_set():
            done_event.set()
        select_win.destroy()

    lb.bind("<<ListboxSelect>>", update_detail)
    lb.bind("<Double-Button-1>", on_confirm)
    if candidates:
        lb.selection_set(0)
        update_detail()

    btn_frame = ttk.Frame(select_win)
    btn_frame.pack(fill=tk.X, padx=10, pady=8)
    ttk.Button(btn_frame, text="确认选择", command=on_confirm).pack(side=tk.LEFT)
    ttk.Button(btn_frame, text="跳过此文件", command=on_skip).pack(
        side=tk.LEFT, padx=8
    )

    select_win.protocol("WM_DELETE_WINDOW", on_skip)
    select_win.transient(gui.root)
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
                        timeout=15,
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
                    append_error(
                        "BGM", format_error_message(ERROR_CODE_HTTP, f"HTTP请求失败: {err}")
                    )
                except ValueError as err:
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
                        timeout=15,
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
                        timeout=15,
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
                    append_error(
                        "TMDb", format_error_message(ERROR_CODE_HTTP, f"HTTP请求失败: {err}")
                    )
                except ValueError as err:
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
            messagebox.showerror("搜索失败", error_msg)
        else:
            messagebox.showinfo("无结果", "未找到匹配的条目")
        return

    if len(results) == 1:
        gui._confirm_season_and_dispatch(
            selected_ids, results[0][0], results[0][1], results[0][2], results[0][3]
        )
        return

    select_win = Toplevel(gui.root)
    select_win.title("选择匹配项")
    center_window(select_win, gui.root, 650, 350)

    lb = Listbox(select_win, width=80, height=10)
    lb.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)

    scroll = Scrollbar(select_win)
    scroll.pack(side=tk.RIGHT, fill=tk.Y)
    lb.config(yscrollcommand=scroll.set)
    scroll.config(command=lb.yview)

    for title, tid, msg, _meta in results:
        lb.insert(tk.END, f"{title} (ID:{tid}) - {msg}")

    def on_select(event=None):
        sel = lb.curselection()
        if sel:
            idx_sel = sel[0]
            gui._confirm_season_and_dispatch(
                selected_ids,
                results[idx_sel][0],
                results[idx_sel][1],
                results[idx_sel][2],
                results[idx_sel][3],
            )
            select_win.destroy()

    lb.bind("<Double-Button-1>", on_select)
    ttk.Button(select_win, text="确认选择", command=on_select).pack(pady=5)

    select_win.transient(gui.root)
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
