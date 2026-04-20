import logging
import os
import re
import tkinter as tk

from concurrent.futures import ThreadPoolExecutor, as_completed
from tkinter import messagebox

from guessit import guessit

from ai.ollama_ai import fetch_siliconflow_info
from db.tmdb_api import (
    fetch_bgm_by_id,
    fetch_hybrid_episode_meta,
    fetch_tmdb_by_id,
    fetch_tmdb_credits,
    fetch_tmdb_episode_meta,
    fetch_tmdb_season_poster,
)
from core.workers.execution_runner import (
    process_one_file as execution_process_one_file,
    process_one_file_scrape as execution_process_one_file_scrape,
    run_execution as execution_run_execution,
    run_scrape_execution as execution_run_scrape_execution,
)
from utils.helpers import (
    ERROR_CODE_UNKNOWN,
    derive_title_from_filename,
    extract_db_id_from_path,
    extract_episode_number,
    format_error_message,
    normalize_compare_text,
    safe_filename,
    safe_int,
    safe_str,
)


SPECIAL_TAG_RE = re.compile(
    r"(?i)(?<![A-Z0-9])(?:PROLOGUE|OVA|OAD|SP|SPECIAL|NC\.VER|EXTRA)(?![A-Z0-9])"
)
SPECIAL_EPISODE_RE = re.compile(
    r"(?i)(?<![A-Z0-9])(?:SP|OVA|OAD|SPECIAL|EXTRA)(?![A-Z0-9])\s*(?:BD)?\s*0*(\d+)"
)
PROLOGUE_RE = re.compile(r"(?i)(?<![A-Z0-9])PROLOGUE(?![A-Z0-9])")


def extract_season_from_dir(dir_path):
    """Extract a season number from folder names like Season 2 / S02."""
    current = str(dir_path or "")
    for _ in range(3):
        folder_name = os.path.basename(current)
        match = re.search(r"(?i)^(?:season\s*|s)\s*(\d{1,2})$", folder_name)
        if match:
            try:
                return int(match.group(1))
            except Exception:
                return None
        parent = os.path.dirname(current)
        if not parent or parent == current:
            break
        current = parent
    return None


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
        # 搜索路径返回的 meta 可能缺少 genres/runtime/status，用 detail 接口补全
        mode = gui.source_var.get()
        if mode == "siliconflow_tmdb" and t_id and t_id != "None" and not meta.get("genres"):
            _, _, _, detail_meta = fetch_tmdb_by_id(t_id, True, gui.tmdb_api_key.get())
            if not detail_meta:
                _, _, _, detail_meta = fetch_tmdb_by_id(t_id, False, gui.tmdb_api_key.get())
            if detail_meta:
                meta = {**detail_meta, **{k: v for k, v in meta.items() if v}}
        item = gui.file_list[idx]
        pure, ext = gui.extract_lang_and_ext(item.old_name)
        g = guessit(pure)
        m = item.metadata or {}
        path_key = item.path

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

        v_tag = gui._get_version_tag(item.path)
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

        actors, directors = [], []
        if mode == "siliconflow_tmdb" and t_id and t_id != "None":
            actors, directors = fetch_tmdb_credits(
                t_id, is_tv=is_tv, api_key=gui.tmdb_api_key.get()
            )

        item.metadata = {
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
            "actors": actors,
            "directors": directors,
            "genres": meta.get("genres") or [],
            "studios": meta.get("studios") or [],
            "runtime": meta.get("runtime"),
            "status": meta.get("status", ""),
            "rating": meta.get("rating", 0),
            "votes": meta.get("votes", 0),
            "release": meta.get("release", ""),
            "original_title": meta.get("original_title", ""),
        }
        item.new_name_only = new_fn

        root_d = gui.target_root.get().strip()
        if root_d:
            id_tag = f"tmdbid={t_id}" if mode == "siliconflow_tmdb" else f"bgmid={t_id}"
            folder_name = safe_filename(f"{safe_title} [{id_tag}]")
            season_folder = f"Season {s}"
            if is_tv:
                item.full_target = os.path.join(
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
                item.full_target = os.path.join(root_d, folder_name, new_fn)
        else:
            item.full_target = ""

        gui.root.after(
            0,
            lambda: gui.update_item_display(
                item,
                title=safe_title,
                match_id=t_id,
                target=item.full_target or new_fn,
                status=msg,
            ),
        )
    except Exception as err:
        logging.error(f"更新UI失败: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"更新失败: {str(err)[:30]}")
        if item and item.id:
            gui.root.after(
                0,
                lambda msg=err_msg: gui.update_item_display(
                    item, status=gui._friendly_status_text(msg)
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
    active_ids = set(gui.action_scope_item_ids or [item.id for item in gui.file_list])
    indices = [i for i, item in enumerate(gui.file_list) if item.id in active_ids]
    total = len(indices)
    gui.root.after(0, lambda max_v=total: gui.pbar.config(maximum=max_v))

    try:
        with ThreadPoolExecutor(max_workers=gui._get_preview_workers()) as executor:
            list(executor.map(gui.process_task, indices))
    except Exception as err:
        logging.error(f"预览处理失败: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"处理失败: {str(err)[:30]}")
        gui.root.after(
            0,
            lambda msg=err_msg: messagebox.showerror("错误", msg, parent=gui.root),
        )

    def _finish_preview_ui():
        gui.btn_pre.config(state=tk.NORMAL)
        if gui.preview_skip_all_event.is_set():
            gui.status.config(text="已终止本轮剩余识别")
        else:
            gui.status.config(text="预览完成")

    gui.root.after(0, _finish_preview_ui)


def process_task(gui, i):
    """Process a single preview task."""
    item = gui.file_list[i]

    try:
        if gui.preview_skip_all_event.is_set() or item.dir in gui.preview_skip_dirs:
            gui.root.after(0, lambda: gui.update_item_display(item, status="已跳过"))
            return

        gui.root.after(0, lambda: gui.update_item_display(item, status="识别中"))

        if gui.preview_skip_all_event.is_set() or item.dir in gui.preview_skip_dirs:
            gui.root.after(0, lambda: gui.update_item_display(item, status="已跳过"))
            return

        pure, ext = gui.extract_lang_and_ext(item.old_name)
        dir_p = item.dir
        mode = gui.source_var.get()

        strip_kw = []
        if hasattr(gui, "_get_strip_keywords"):
            strip_kw = gui._get_strip_keywords()
        elif getattr(gui, "strip_keywords", None):
            strip_kw = list(getattr(gui, "strip_keywords", None) or [])

        pure_for_parse = pure
        if strip_kw:
            for keyword in strip_kw:
                if keyword:
                    pure_for_parse = re.sub(
                        re.escape(keyword), " ", pure_for_parse, flags=re.IGNORECASE
                    )
            pure_for_parse = re.sub(r"\s+", " ", pure_for_parse).strip()

        g = guessit(pure_for_parse)

        extracted_ep = extract_episode_number(pure, g)

        ai_mode_obj = getattr(gui, "ai_mode", None)
        ai_mode_val = str(ai_mode_obj.get() if ai_mode_obj else "assist").strip().lower()

        with gui.cache_lock:
            cached_ai = gui.dir_cache.get(dir_p)

        parse_source = "guessit"
        cached_parse_source = str((cached_ai or {}).get("parse_source") or "guessit")
        can_reuse_cached_parse = bool(cached_ai) and gui._can_reuse_dir_ai(
            cached_ai, pure, g
        )
        if can_reuse_cached_parse:
            if ai_mode_val == "force":
                can_reuse_cached_parse = cached_parse_source == "ai"
            elif ai_mode_val == "disabled":
                can_reuse_cached_parse = cached_parse_source != "ai"

        if can_reuse_cached_parse:
            t = cached_ai["title"]
            y = cached_ai.get("year")
            s = gui._pick_season(pure, g, cached_ai.get("season") or 1)
            e = extracted_ep or 1
            ai_msg = "复用"
            ai_data = cached_ai
            parse_source = cached_ai.get("parse_source", "guessit")
        else:
            ai_data = None
            ai_msg = ""

            if ai_mode_val == "force":
                if gui.prefer_ollama.get():
                    if gui.ollama_url.get().strip() and gui.ollama_model.get().strip():
                        ai_data, ai_msg = gui._parse_with_ollama(pure_for_parse)
                        if ai_data is None and gui.sf_api_key.get().strip():
                            ai_data, ai_msg = fetch_siliconflow_info(
                                pure_for_parse,
                                gui.sf_api_key.get(),
                                gui.sf_api_url.get(),
                                gui.sf_model.get(),
                                gui._get_ai_temperature(),
                                gui._get_ai_top_p(),
                            )
                    elif gui.sf_api_key.get().strip():
                        ai_data, ai_msg = fetch_siliconflow_info(
                            pure_for_parse,
                            gui.sf_api_key.get(),
                            gui.sf_api_url.get(),
                            gui.sf_model.get(),
                            gui._get_ai_temperature(),
                            gui._get_ai_top_p(),
                        )
                elif gui.sf_api_key.get().strip():
                    ai_data, ai_msg = fetch_siliconflow_info(
                        pure_for_parse,
                        gui.sf_api_key.get(),
                        gui.sf_api_url.get(),
                        gui.sf_model.get(),
                        gui._get_ai_temperature(),
                        gui._get_ai_top_p(),
                    )

                if ai_data:
                    t = ai_data.get("title", "未知")
                    y = ai_data.get("year")
                    ai_season = safe_int(ai_data.get("season"), 1)
                    if ai_season < 1:
                        ai_season = 1
                    s = gui._pick_season(pure, g, ai_season)
                    e = extracted_ep or safe_int(ai_data.get("episode"), 1)
                    parse_source = "ai"
                    with gui.cache_lock:
                        ai_data["parse_source"] = "ai"
                        gui.dir_cache[dir_p] = ai_data
                else:
                    item.metadata = {"id": "None", "parse_source": "ai"}
                    item.new_name_only = ""
                    item.full_target = ""
                    item.parse_source = "ai"
                    gui.root.after(
                        0,
                        lambda: gui.update_item_display(
                            item,
                            title="待手动",
                            match_id="None",
                            target="(AI 强制模式未识别成功)",
                            status="待手动确认",
                        ),
                    )
                    return
            else:
                t = g.get("title") or derive_title_from_filename(pure) or "未知"
                y = g.get("year")
                if not y:
                    year_dir = dir_p
                    for _ in range(3):
                        folder_name = os.path.basename(year_dir)
                        year_match = re.search(r"\b((?:19|20)\d{2})\b", folder_name)
                        if year_match:
                            y = int(year_match.group(1))
                            break
                        parent_dir = os.path.dirname(year_dir)
                        if not parent_dir or parent_dir == year_dir:
                            break
                        year_dir = parent_dir
                dir_season = extract_season_from_dir(dir_p)
                s = gui._pick_season(
                    pure, g, dir_season if dir_season is not None else 1
                )
                e = extracted_ep or 1
                ai_msg = "猜测"
                parse_source = "guessit"
                if t and normalize_compare_text(t) not in ("", "未知"):
                    with gui.cache_lock:
                        if dir_p not in gui.dir_cache:
                            gui.dir_cache[dir_p] = {
                                "title": t,
                                "year": y,
                                "season": s,
                                "episode": e,
                                "parse_source": "guessit",
                            }

        if SPECIAL_TAG_RE.search(pure):
            # 若文件名已有显式 S\d+E\d+ 标记（如 S01E01），尊重该标记，
            # 不强制覆盖为 Season 0，避免把 OVA 系列误归入特别篇。
            explicit_s_in_name = gui._extract_explicit_season(pure)
            if explicit_s_in_name is None:
                s = 0
                sp_match = SPECIAL_EPISODE_RE.search(pure)
                if sp_match:
                    e = int(sp_match.group(1))
                elif PROLOGUE_RE.search(pure):
                    e = 0

        media_type = gui._resolve_media_type(g)
        is_tv = media_type == "episode"
        path_key = item.path

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

        folder_id_for_cache = extract_db_id_from_path(item.path, mode) or ""
        cache_key = f"{t}_{safe_str(y)}_{is_tv}_{mode}_{folder_id_for_cache}"

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
                    folder_id = extract_db_id_from_path(item.path, mode)
                    if folder_id:
                        if mode == "siliconflow_tmdb":
                            _ft, _fid, _fm, _fmeta = fetch_tmdb_by_id(
                                folder_id, is_tv, gui.tmdb_api_key.get()
                            )
                            if _fid == "None":
                                _ft, _fid, _fm, _fmeta = fetch_tmdb_by_id(
                                    folder_id, not is_tv, gui.tmdb_api_key.get()
                                )
                        else:
                            _ft, _fid, _fm, _fmeta = fetch_bgm_by_id(
                                folder_id, gui.bgm_api_key.get()
                            )
                        if _fid != "None":
                            db_c = (_ft, _fid, "文件夹ID锁定", _fmeta)
                    if not db_c:
                        db_c = gui._resolve_db_match(item, t, y, is_tv, mode, ai_data, g)

                    if ai_mode_val == "assist" and (
                        not db_c or (len(db_c) >= 2 and db_c[1] == "None")
                    ):
                        retry_ai = None
                        if gui.prefer_ollama.get():
                            if gui.ollama_url.get().strip() and gui.ollama_model.get().strip():
                                retry_ai, _ = gui._parse_with_ollama(pure_for_parse)
                                if retry_ai is None and gui.sf_api_key.get().strip():
                                    retry_ai, _ = fetch_siliconflow_info(
                                        pure_for_parse,
                                        gui.sf_api_key.get(),
                                        gui.sf_api_url.get(),
                                        gui.sf_model.get(),
                                        gui._get_ai_temperature(),
                                        gui._get_ai_top_p(),
                                    )
                            elif gui.sf_api_key.get().strip():
                                retry_ai, _ = fetch_siliconflow_info(
                                    pure_for_parse,
                                    gui.sf_api_key.get(),
                                    gui.sf_api_url.get(),
                                    gui.sf_model.get(),
                                    gui._get_ai_temperature(),
                                    gui._get_ai_top_p(),
                                )
                        elif gui.sf_api_key.get().strip():
                            retry_ai, _ = fetch_siliconflow_info(
                                pure_for_parse,
                                gui.sf_api_key.get(),
                                gui.sf_api_url.get(),
                                gui.sf_model.get(),
                                gui._get_ai_temperature(),
                                gui._get_ai_top_p(),
                            )

                        if retry_ai:
                            ai_title = retry_ai.get("title", "")
                            ai_year = retry_ai.get("year")
                            if ai_title and normalize_compare_text(ai_title) != normalize_compare_text(t):
                                t = ai_title
                                y = ai_year
                                ai_data = retry_ai
                                ai_msg = "AI辅助"
                                parse_source = "ai"
                                with gui.cache_lock:
                                    retry_ai["parse_source"] = "ai"
                                    gui.dir_cache[dir_p] = retry_ai
                                db_retry = gui._resolve_db_match(
                                    item, t, y, is_tv, mode, ai_data, g
                                )
                                if db_retry and len(db_retry) >= 2 and db_retry[1] != "None":
                                    db_c = db_retry

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

        is_bgm_fallback = meta.get("_provider") == "bgm"
        effective_tmdb = mode == "siliconflow_tmdb" and not is_bgm_fallback

        # 搜索路径返回的 meta 缺少 genres/runtime/status/studios，用 detail 接口补全
        if effective_tmdb and tid and tid != "None" and not meta.get("genres"):
            _, _, _, detail_meta = fetch_tmdb_by_id(tid, is_tv, gui.tmdb_api_key.get())
            if detail_meta:
                meta = {**detail_meta, **{k: v for k, v in meta.items() if v}}

        ep_n, ep_p, ep_s, s_p = "", "", "", ""

        if is_tv and tid != "None":
            if effective_tmdb:
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

        v_tag = gui._get_version_tag(item.path)

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

        actors, directors = [], []
        if effective_tmdb and tid and tid != "None":
            actors, directors = fetch_tmdb_credits(
                tid, is_tv=is_tv, api_key=gui.tmdb_api_key.get()
            )

        item.metadata = {
            "id": tid,
            "provider": "tmdb" if effective_tmdb else "bgm",
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
            "actors": actors,
            "directors": directors,
            "genres": meta.get("genres") or [],
            "studios": meta.get("studios") or [],
            "runtime": meta.get("runtime"),
            "status": meta.get("status", ""),
            "rating": meta.get("rating", 0),
            "votes": meta.get("votes", 0),
            "release": meta.get("release", ""),
            "original_title": meta.get("original_title", ""),
            "parse_source": parse_source,
        }
        item.parse_source = parse_source

        item.new_name_only = new_fn

        root_d = gui.target_root.get().strip()
        if root_d:
            id_tag = f"tmdbid={tid}" if effective_tmdb else f"bgmid={tid}"
            folder_name = safe_filename(f"{safe_std_t} [{id_tag}]")
            season_folder = f"Season {s}"

            if is_tv:
                item.full_target = os.path.join(
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
                item.full_target = os.path.join(root_d, folder_name, new_fn)
        else:
            item.full_target = ""

        gui.root.after(
            0,
            lambda: gui.update_item_display(
                item,
                title=safe_std_t,
                match_id=tid,
                target=item.full_target or new_fn,
                status=gui._build_status_text(ai_msg, db_m),
            ),
        )
    except Exception as ex:
        logging.error(f"处理文件 {item.old_name} 时出错: {ex}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"异常: {str(ex)[:50]}")
        gui.root.after(
            0,
            lambda msg=err_msg: gui.update_item_display(
                item,
                title="错误",
                match_id="None",
                target=gui._friendly_status_text(msg),
                status="崩溃",
            ),
        )
    finally:
        gui.root.after(0, lambda: gui.pbar.step(1))


def run_execution(gui, run_mode):
    """Run rename/archive execution with background worker pool."""
    return execution_run_execution(gui, run_mode)


def process_one_file(gui, item, run_mode):
    """Process single file move/rename and sidecar writing."""
    return execution_process_one_file(gui, item, run_mode)


def run_scrape_execution(gui):
    """Run scrape-only execution with background worker pool."""
    return execution_run_scrape_execution(gui)


def process_one_file_scrape(gui, item):
    """Process single file scrape-only (write NFO and download images)."""
    return execution_process_one_file_scrape(gui, item)

