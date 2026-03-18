import logging
import os
import re
import tkinter as tk

from concurrent.futures import ThreadPoolExecutor, as_completed
from tkinter import messagebox

from guessit import guessit

from ai.ollama_ai import fetch_siliconflow_info
from db.tmdb_api import (
    fetch_hybrid_episode_meta,
    fetch_tmdb_episode_meta,
    fetch_tmdb_season_poster,
)
from utils.helpers import (
    ERROR_CODE_UNKNOWN,
    derive_title_from_filename,
    extract_episode_number,
    format_error_message,
    normalize_compare_text,
    safe_filename,
    safe_int,
    safe_str,
)


def async_batch_runner(gui, indices, title, t_id, msg, meta):
    """Run background sync updates for selected files."""
    with ThreadPoolExecutor(max_workers=gui._get_sync_workers()) as executor:
        futures = [
            executor.submit(gui._bg_update_single_ui, idx, title, t_id, msg, meta)
            for idx in indices
        ]
        for _future in as_completed(futures):
            gui.root.after(0, lambda: gui.pbar.step(1))

    gui.root.after(0, lambda: gui.status.config(text="同步完成！"))


def bg_update_single_ui(gui, idx, title, t_id, msg, meta):
    """Update single row metadata and naming in background sync flow."""
    item = None
    try:
        item = gui.file_list[idx]
        pure, ext = gui.extract_lang_and_ext(item["old_name"])
        g = guessit(pure)
        m = item.get("metadata", {})
        path_key = item["path"]

        forced_s = gui.forced_seasons.get(path_key)
        s = (
            forced_s
            if forced_s is not None
            else gui._pick_season(pure, g, m.get("s", 1))
        )

        raw_e = g.get("episode") or m.get("e", 1)
        if isinstance(raw_e, list):
            raw_e = raw_e[0]

        forced_o = gui.forced_offsets.get(path_key, 0)
        e_calc = raw_e
        if forced_o != 0 and str(raw_e).isdigit():
            e_calc = max(1, int(raw_e) + forced_o)

        y = g.get("year") or m.get("year")
        media_type = gui._resolve_media_type({"type": m.get("type", "episode")})
        is_tv = media_type == "episode"
        mode = gui.source_var.get()

        ep_n, ep_p, ep_s, s_p = "", "", "", ""
        if is_tv and t_id != "None" and title:
            if mode == "siliconflow_tmdb":
                ep_n, ep_p, ep_s = fetch_tmdb_episode_meta(
                    t_id,
                    s,
                    e_calc,
                    gui.tmdb_api_key.get(),
                    title,
                    gui.bgm_api_key.get(),
                )
                s_p = fetch_tmdb_season_poster(t_id, s, gui.tmdb_api_key.get())
            else:
                ep_n, ep_p, ep_s, s_p = fetch_hybrid_episode_meta(
                    title,
                    t_id,
                    s,
                    e_calc,
                    gui.bgm_api_key.get(),
                    gui.tmdb_api_key.get(),
                )

        fallback_ep_title = g.get("episode_title") or ""
        ep_n_final = ep_n or fallback_ep_title

        s = safe_int(s, 1)
        e_calc = safe_int(e_calc, 1)
        s_fmt = f"{int(s):02d}"
        e_fmt = f"{int(e_calc):02d}"

        v_tag = gui._get_version_tag(item["path"])
        safe_title = safe_filename(title)
        safe_ep_name = safe_filename(ep_n_final)

        if is_tv:
            new_fn = (
                gui.tv_format.get()
                .replace("{title}", safe_title)
                .replace("{year}", safe_str(y))
                .replace("{s:02d}", s_fmt)
                .replace("{s}", s_fmt)
                .replace("{e:02d}", e_fmt)
                .replace("{e}", e_fmt)
                .replace("{ep_name}", safe_ep_name)
                .replace("{ext}", v_tag + ext)
            )
        else:
            new_fn = (
                gui.movie_format.get()
                .replace("{title}", safe_title)
                .replace("{year}", safe_str(y))
                .replace("{ext}", v_tag + ext)
            )

        new_fn = re.sub(r"\s*\(\s*\)", "", new_fn)
        new_fn = re.sub(r"\s*-\s*(?=\.)|\s*-\s*$", "", new_fn)
        new_fn = re.sub(r"\s+(?=\.)", "", new_fn).strip()

        item["metadata"] = {
            "id": t_id,
            "provider": "tmdb" if mode == "siliconflow_tmdb" else "bgm",
            "title": safe_title,
            "year": y,
            "ep_title": ep_n_final or f"第 {e_calc} 集",
            "overview": meta.get("overview", ""),
            "ep_plot": ep_p,
            "s": s,
            "e": e_calc,
            "poster": meta.get("poster"),
            "fanart": meta.get("fanart"),
            "still": ep_s,
            "s_poster": s_p,
            "type": media_type,
        }
        item["new_name_only"] = new_fn

        root_d = gui.target_root.get().strip()
        if root_d:
            id_tag = f"tmdbid={t_id}" if mode == "siliconflow_tmdb" else f"bgmid={t_id}"
            folder_name = safe_filename(f"{safe_title} [{id_tag}]")
            season_folder = f"Season {s}"
            if is_tv:
                item["full_target"] = os.path.join(
                    root_d, folder_name, season_folder, new_fn
                )
            else:
                year_text = safe_str(y)
                if year_text:
                    folder_name = safe_filename(
                        f"{safe_title} ({year_text}) [{id_tag}]"
                    )
                else:
                    folder_name = safe_filename(f"{safe_title} [{id_tag}]")
                item["full_target"] = os.path.join(root_d, folder_name, new_fn)
        else:
            item["full_target"] = ""

        gui.root.after(
            0,
            lambda: gui.tree.item(
                item["id"],
                values=(
                    item["old_name"],
                    safe_title,
                    t_id,
                    item["full_target"] or new_fn,
                    msg,
                ),
            ),
        )
    except Exception as err:
        logging.error(f"更新UI失败: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"更新失败: {str(err)[:30]}")
        if item and item.get("id"):
            gui.root.after(
                0,
                lambda id_val=item["id"], msg=err_msg: gui.tree.set(
                    id_val, "st", gui._friendly_status_text(msg)
                ),
            )
        else:
            gui.root.after(
                0,
                lambda msg=err_msg: gui.status.config(
                    text=gui._friendly_status_text(msg)
                ),
            )


def run_preview_pool(gui):
    """Run preview recognition tasks with configured worker count."""
    total = len(gui.file_list)
    gui.root.after(0, lambda max_v=total: gui.pbar.config(maximum=max_v))

    try:
        with ThreadPoolExecutor(max_workers=gui._get_preview_workers()) as executor:
            list(executor.map(gui.process_task, range(total)))
    except Exception as err:
        logging.error(f"预览处理失败: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"处理失败: {str(err)[:30]}")
        gui.root.after(
            0,
            lambda msg=err_msg: messagebox.showerror("错误", msg, parent=gui.root),
        )

    gui.root.after(
        0,
        lambda: [
            gui.btn_pre.config(state=tk.NORMAL),
            gui.status.config(text="预览完成"),
        ],
    )


def process_task(gui, i):
    """Process a single preview task."""
    item = gui.file_list[i]

    try:
        gui.root.after(
            0, lambda id_val=item["id"]: gui.tree.set(id_val, "st", "识别中")
        )
        pure, ext = gui.extract_lang_and_ext(item["old_name"])
        dir_p = item["dir"]
        mode = gui.source_var.get()
        g = guessit(pure)

        extracted_ep = extract_episode_number(pure, g)

        with gui.cache_lock:
            cached_ai = gui.dir_cache.get(dir_p)

        if cached_ai and gui._can_reuse_dir_ai(cached_ai, pure, g):
            t = cached_ai["title"]
            y = cached_ai.get("year")
            s = gui._pick_season(pure, g, cached_ai.get("season") or 1)
            e = extracted_ep or 1
            ai_msg = "复用"
            ai_data = cached_ai
        else:
            ai_data = None
            ai_msg = ""

            if gui.prefer_ollama.get():
                if gui.ollama_url.get().strip() and gui.ollama_model.get().strip():
                    ai_data, ai_msg = gui._parse_with_ollama(pure)
                    if ai_data is None and gui.sf_api_key.get().strip():
                        ai_data, ai_msg = fetch_siliconflow_info(
                            pure,
                            gui.sf_api_key.get(),
                            gui.sf_api_url.get(),
                            gui.sf_model.get(),
                            gui._get_ai_temperature(),
                            gui._get_ai_top_p(),
                        )
                elif gui.sf_api_key.get().strip():
                    ai_data, ai_msg = fetch_siliconflow_info(
                        pure,
                        gui.sf_api_key.get(),
                        gui.sf_api_url.get(),
                        gui.sf_model.get(),
                        gui._get_ai_temperature(),
                        gui._get_ai_top_p(),
                    )
            elif gui.sf_api_key.get().strip():
                ai_data, ai_msg = fetch_siliconflow_info(
                    pure,
                    gui.sf_api_key.get(),
                    gui.sf_api_url.get(),
                    gui.sf_model.get(),
                    gui._get_ai_temperature(),
                    gui._get_ai_top_p(),
                )

            if ai_data:
                t = ai_data.get("title", "未知")
                y = ai_data.get("year")
                s = gui._pick_season(pure, g, ai_data.get("season", 1))
                e = extracted_ep or safe_int(ai_data.get("episode"), 1)
                with gui.cache_lock:
                    gui.dir_cache[dir_p] = ai_data
            else:
                t = g.get("title") or derive_title_from_filename(pure) or "未知"
                y = g.get("year")
                s = gui._pick_season(pure, g, 1)
                e = extracted_ep or 1
                ai_msg = "猜测"
                if t and normalize_compare_text(t) not in ("", "未知"):
                    with gui.cache_lock:
                        if dir_p not in gui.dir_cache:
                            gui.dir_cache[dir_p] = {
                                "title": t,
                                "year": y,
                                "season": s,
                                "episode": e,
                            }

        if re.search(r"(?i)(?:PROLOGUE|OVA|OAD|SP\b|SPECIAL|NC\.VER|EXTRA)", pure):
            s = 0
            sp_match = re.search(
                r"(?i)(?:SP|OVA|OAD|SPECIAL|EXTRA)\s*(?:BD)?\s*0*(\d+)", pure
            )
            if sp_match:
                e = int(sp_match.group(1))
            elif re.search(r"(?i)PROLOGUE", pure):
                e = 0

        media_type = gui._resolve_media_type(g)
        is_tv = media_type == "episode"
        path_key = item["path"]

        forced_s = gui.forced_seasons.get(path_key)
        if forced_s is not None:
            s = forced_s

        forced_o = gui.forced_offsets.get(path_key, 0)
        e_calc = e

        if isinstance(e, list):
            e = e[0]
            e_calc = e

        if forced_o != 0:
            e_calc = max(1, safe_int(e, 1) + forced_o)

        cache_key = f"{t}_{safe_str(y)}_{is_tv}_{mode}"

        with gui.cache_lock:
            db_c = gui.manual_locks.get(path_key) or gui.db_cache.get(cache_key)
            pending_event = gui.db_resolution_events.get(cache_key)
            is_resolver = False
            if not db_c and pending_event is None:
                import threading

                pending_event = threading.Event()
                gui.db_resolution_events[cache_key] = pending_event
                is_resolver = True

        if not db_c:
            if is_resolver:
                try:
                    db_c = gui._resolve_db_match(item, t, y, is_tv, mode, ai_data, g)
                    with gui.cache_lock:
                        if db_c and len(db_c) >= 2 and db_c[1] != "None":
                            gui.db_cache[cache_key] = db_c
                finally:
                    with gui.cache_lock:
                        waiter = gui.db_resolution_events.pop(cache_key, None)
                    if waiter:
                        waiter.set()
            else:
                if pending_event and not pending_event.wait(timeout=240):
                    logging.warning("等待数据库候选解析超时，已跳过缓存复用")
                with gui.cache_lock:
                    db_c = gui.manual_locks.get(path_key) or gui.db_cache.get(cache_key)

        if not db_c:
            db_c = (t, "None", "待手动确认", {})

        std_t, tid, db_m, meta = db_c
        ep_n, ep_p, ep_s, s_p = "", "", "", ""

        if is_tv and tid != "None":
            if mode == "siliconflow_tmdb":
                ep_n, ep_p, ep_s = fetch_tmdb_episode_meta(
                    tid,
                    s,
                    e_calc,
                    gui.tmdb_api_key.get(),
                    std_t,
                    gui.bgm_api_key.get(),
                )
                s_p = fetch_tmdb_season_poster(tid, s, gui.tmdb_api_key.get())
            else:
                ep_n, ep_p, ep_s, s_p = fetch_hybrid_episode_meta(
                    std_t,
                    tid,
                    s,
                    e_calc,
                    gui.bgm_api_key.get(),
                    gui.tmdb_api_key.get(),
                    y,
                )

        fallback_ep_title = g.get("episode_title") or ""
        ep_n_final = ep_n or fallback_ep_title

        s = safe_int(s, 1)
        e_calc = safe_int(e_calc, 1)
        s_fmt = f"{int(s):02d}"
        e_fmt = f"{int(e_calc):02d}"

        v_tag = gui._get_version_tag(item["path"])

        safe_std_t = safe_filename(std_t)
        safe_ep_name = safe_filename(ep_n_final)

        if is_tv:
            new_fn = (
                gui.tv_format.get()
                .replace("{title}", safe_std_t)
                .replace("{year}", safe_str(y))
                .replace("{s:02d}", s_fmt)
                .replace("{s}", s_fmt)
                .replace("{e:02d}", e_fmt)
                .replace("{e}", e_fmt)
                .replace("{ep_name}", safe_ep_name)
                .replace("{ext}", v_tag + ext)
            )
        else:
            new_fn = (
                gui.movie_format.get()
                .replace("{title}", safe_std_t)
                .replace("{year}", safe_str(y))
                .replace("{ext}", v_tag + ext)
            )

        new_fn = re.sub(r"\s*\(\s*\)", "", new_fn)
        new_fn = re.sub(r"\s*-\s*(?=\.)|\s*-\s*$", "", new_fn)
        new_fn = re.sub(r"\s+(?=\.)", "", new_fn).strip()

        item["metadata"] = {
            "id": tid,
            "provider": "tmdb" if mode == "siliconflow_tmdb" else "bgm",
            "title": safe_std_t,
            "year": y,
            "ep_title": ep_n_final or f"第 {e_calc} 集",
            "overview": meta.get("overview", ""),
            "ep_plot": ep_p,
            "s": s,
            "e": e_calc,
            "poster": meta.get("poster"),
            "fanart": meta.get("fanart"),
            "still": ep_s,
            "s_poster": s_p,
            "type": media_type,
        }

        item["new_name_only"] = new_fn

        root_d = gui.target_root.get().strip()
        if root_d:
            id_tag = f"tmdbid={tid}" if mode == "siliconflow_tmdb" else f"bgmid={tid}"
            folder_name = safe_filename(f"{safe_std_t} [{id_tag}]")
            season_folder = f"Season {s}"

            if is_tv:
                item["full_target"] = os.path.join(
                    root_d, folder_name, season_folder, new_fn
                )
            else:
                year_text = safe_str(y)
                if year_text:
                    folder_name = safe_filename(
                        f"{safe_std_t} ({year_text}) [{id_tag}]"
                    )
                else:
                    folder_name = safe_filename(f"{safe_std_t} [{id_tag}]")
                item["full_target"] = os.path.join(root_d, folder_name, new_fn)
        else:
            item["full_target"] = ""

        gui.root.after(
            0,
            lambda: gui.tree.item(
                item["id"],
                values=(
                    item["old_name"],
                    safe_std_t,
                    tid,
                    item["full_target"] or new_fn,
                    gui._build_status_text(ai_msg, db_m),
                ),
            ),
        )
    except Exception as ex:
        logging.error(f"处理文件 {item['old_name']} 时出错: {ex}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"异常: {str(ex)[:50]}")
        gui.root.after(
            0,
            lambda id_val=item["id"],
            old_name=item["old_name"],
            msg=err_msg: gui.tree.item(
                id_val,
                values=(
                    old_name,
                    "错误",
                    "None",
                    gui._friendly_status_text(msg),
                    "崩溃",
                ),
            ),
        )
    finally:
        gui.root.after(0, lambda: gui.pbar.step(1))


def run_execution(gui, is_archive):
    """Run rename/archive execution with background worker pool."""
    total = len(gui.file_list)
    gui.root.after(
        0,
        lambda max_v=total: [
            gui.status.config(text="执行中..."),
            gui.pbar.config(maximum=max_v),
            gui.pbar.configure(value=0),
        ],
    )

    try:
        with ThreadPoolExecutor(max_workers=gui._get_execution_workers()) as executor:
            futures = [
                executor.submit(gui.process_one_file, item, is_archive)
                for item in gui.file_list
            ]
            for future in as_completed(futures):
                gui.root.after(0, lambda: gui.pbar.step(1))
                try:
                    future.result()
                except Exception as err:
                    logging.error(f"执行失败: {err}")
    except Exception as err:
        logging.error(f"执行线程池失败: {err}")
        err_msg = f"执行失败: {err}"
        gui.root.after(
            0,
            lambda msg=err_msg: messagebox.showerror("错误", msg, parent=gui.root),
        )

    gui.root.after(0, lambda: gui.status.config(text="任务全部完成"))


def process_one_file(gui, item, is_archive):
    """Process single file move/rename and sidecar writing."""
    try:
        if is_archive and item.get("full_target"):
            target = item["full_target"]
        else:
            target = os.path.join(
                item["dir"], item.get("new_name_only", item["old_name"])
            )

        if not os.path.exists(item["path"]):
            gui.root.after(
                0, lambda id_val=item["id"]: gui.tree.set(id_val, "st", "源文件不存在")
            )
            return

        target_dir = os.path.dirname(target)
        if target_dir:
            os.makedirs(target_dir, exist_ok=True)

        current_path = item["path"]
        same_exact_path = current_path == target
        is_case_change_only = os.path.normcase(current_path) == os.path.normcase(target)

        if not same_exact_path and not is_case_change_only and os.path.exists(target):
            gui.root.after(
                0, lambda id_val=item["id"]: gui.tree.set(id_val, "st", "目标已存在")
            )
            return

        if not same_exact_path:
            import shutil

            shutil.move(current_path, target)
            item["path"] = target

        gui._write_sidecar_files(item, item["path"])
        gui.root.after(
            0, lambda id_val=item["id"]: gui.tree.set(id_val, "st", "刮削完成")
        )

    except PermissionError as err:
        logging.error(f"权限错误 {item.get('path', '')}: {err}")
        gui.root.after(
            0, lambda id_val=item["id"]: gui.tree.set(id_val, "st", "权限错误")
        )
    except OSError as err:
        logging.error(f"系统错误 {item.get('path', '')}: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"系统错误: {str(err)[:20]}")
        gui.root.after(
            0,
            lambda id_val=item["id"], msg=err_msg: gui.tree.set(
                id_val, "st", gui._friendly_status_text(msg)
            ),
        )
    except Exception as err:
        logging.error(f"处理文件失败 {item.get('path', '')}: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"失败: {str(err)[:20]}")
        gui.root.after(
            0,
            lambda id_val=item["id"], msg=err_msg: gui.tree.set(
                id_val, "st", gui._friendly_status_text(msg)
            ),
        )
