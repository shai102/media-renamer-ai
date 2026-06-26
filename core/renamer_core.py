import os
import re


from db.tmdb_api import (
    fetch_bgm_candidates,
    fetch_tmdb_candidates,
)
from core.services.matcher_service import (
    auto_pick_candidate_by_score,
    get_embedding,
    parse_with_ollama,
    pick_candidate_with_ollama,
    pick_candidate_with_openai_compatible,
    rerank_candidates_with_embedding,
)
from core.services.template_service import (
    build_filename_context,
    render_filename_template,
)
from core.services.naming_service import (
    build_status_text,
    can_reuse_dir_ai,
    extract_explicit_season,
    extract_lang_and_ext,
    extract_media_suffix,
    friendly_status_text,
    get_version_tag,
    pick_season,
)
from core.mixins.config_mixin import ConfigMixin
from core.mixins.list_mixin import ListMixin
from core.models.media_item import MediaItem
from core.workers.task_runner import (
    async_batch_runner as worker_async_batch_runner,
    bg_update_single_ui as worker_bg_update_single_ui,
    process_one_file as worker_process_one_file,
    process_one_file_scrape as worker_process_one_file_scrape,
    process_task as worker_process_task,
    run_execution as worker_run_execution,
    run_preview_pool as worker_run_preview_pool,
    run_scrape_execution as worker_run_scrape_execution,
)
from utils.helpers import (
    build_db_query_plan,
    candidate_looks_like_extra_title,
    candidate_looks_like_movie_version,
    candidate_looks_like_unrequested_subtitle_arc,
    candidate_looks_like_unrequested_variant,
    candidate_to_result,
    derive_title_from_filename,
    extract_year_from_release,
    normalize_compare_text,
    safe_filename,
    safe_int,
    safe_str,
    save_image,
    text_mentions_extra_title,
    write_nfo,
    years_within_tolerance,
    _nfo_has_empty_plot,
)


def _has_cjk(text):
    return bool(re.search(r"[\u4e00-\u9fff\u3040-\u30ff]", str(text or "")))


def _is_substantial_query_norm(norm):
    """判断规范化后的查询标题是否足够长以信任直搜命中。

    拉丁标题需 >=6 字符避免误命中；CJK 标题天然短（如 咒术回战=4字），
    否则永远无法命中，故 CJK 放宽到 >=2 字符。
    """
    if not norm:
        return False
    if _has_cjk(norm):
        return len(norm) >= 2
    return len(norm) >= 6


class MediaRenamerCore(ConfigMixin, ListMixin):
    """无 UI 的业务逻辑基类（识别/匹配/命名/执行/刮削/配置），零 tkinter。

    UI 钩子（_show_error / update_item_display / _request_manual_candidate_choice /
    start_preview / refresh_item_row 等）由子类（ui_qt 的 MediaRenamerGUIQt）实现。
    """

    def get_media_exts(self):
        """获取媒体文件扩展名"""
        v = [e.strip().lower() for e in self.video_exts.get().split(",") if e.strip()]
        s = [
            e.strip().lower() for e in self.sub_audio_exts.get().split(",") if e.strip()
        ]
        return tuple(v + s)

    def get_sub_audio_exts(self):
        """获取字幕/音频扩展名"""
        return tuple(
            [
                e.strip().lower()
                for e in self.sub_audio_exts.get().split(",")
                if e.strip()
            ]
        )

    def extract_lang_and_ext(self, filename):
        """提取语言标签和扩展名"""
        return extract_lang_and_ext(filename, self.lang_tags.get())

    def _extract_explicit_season(self, pure_name):
        """仅从明确季标记中提取季号，避免把年份误判为季号。"""
        return extract_explicit_season(pure_name)

    def _pick_season(self, pure_name, guess_data=None, fallback=1):
        """优先使用显式季标记；否则只接受合理范围内的猜测季号。"""
        return pick_season(pure_name, guess_data, fallback)

    def _can_reuse_dir_ai(self, cached_ai, pure_name, guess_data=None):
        """仅在当前文件与缓存标题明显属于同一作品时复用目录级识别结果。"""
        return can_reuse_dir_ai(cached_ai, pure_name, guess_data)

    def _write_sidecar_files(self, item, target_path):
        """在媒体文件已位于目标位置后写入 NFO 与图片。

        锁内仅做 NFO 写入（毫秒级），图片下载在锁外并发执行，
        避免 file_write_lock 把多线程刮削串行化。
        """
        target_dir = os.path.dirname(target_path)
        m = item.metadata or {}
        media_type = m.get("type", "episode")
        is_tv = media_type == "episode"
        is_sub_audio = item.old_name.lower().endswith(self.get_sub_audio_exts())

        image_tasks = []  # [(local_path, url), ...]

        with self.file_write_lock:
            if is_tv:
                if not is_sub_audio:
                    ep_nfo = os.path.splitext(target_path)[0] + ".nfo"
                    if not os.path.exists(ep_nfo):
                        write_nfo(ep_nfo, m, "episodedetails")

                    thumb_source = (
                        m.get("still") or m.get("s_poster") or m.get("poster")
                    )
                    if thumb_source:
                        thumb_path = os.path.splitext(target_path)[0] + "-thumb.jpg"
                        if not os.path.exists(thumb_path):
                            image_tasks.append((thumb_path, thumb_source))

                cur_dir = target_dir
                dir_name = os.path.basename(cur_dir)
                is_season_folder = bool(
                    re.match(r"^(Season\s*\d+|S\d+)$", dir_name, re.I)
                )

                if is_season_folder and os.path.dirname(cur_dir):
                    root_d = os.path.dirname(cur_dir)
                else:
                    root_d = cur_dir

                s_num = m.get("s", 1)
                try:
                    s_fmt = f"{int(s_num):02d}"
                except Exception:
                    s_fmt = str(s_num)

                s_nfo_root = os.path.join(root_d, f"season{s_fmt}.nfo")
                s_poster_root = os.path.join(root_d, f"season{s_fmt}-poster.jpg")

                if not os.path.exists(s_nfo_root):
                    write_nfo(s_nfo_root, m, "season")

                if m.get("s_poster") and not os.path.exists(s_poster_root):
                    image_tasks.append((s_poster_root, m["s_poster"]))

                if is_season_folder:
                    season_nfo_local = os.path.join(cur_dir, "season.nfo")
                    folder_jpg_local = os.path.join(cur_dir, "folder.jpg")

                    if not os.path.exists(season_nfo_local):
                        write_nfo(season_nfo_local, m, "season")

                    if m.get("s_poster") and not os.path.exists(folder_jpg_local):
                        image_tasks.append((folder_jpg_local, m["s_poster"]))

                tvshow_nfo = os.path.join(root_d, "tvshow.nfo")
                poster_path = os.path.join(root_d, "poster.jpg")

                if not os.path.exists(tvshow_nfo) or _nfo_has_empty_plot(tvshow_nfo):
                    write_nfo(tvshow_nfo, m, "tvshow")

                if m.get("poster") and not os.path.exists(poster_path):
                    image_tasks.append((poster_path, m["poster"]))

            else:
                if not is_sub_audio:
                    movie_nfo = os.path.splitext(target_path)[0] + ".nfo"
                    if not os.path.exists(movie_nfo):
                        write_nfo(movie_nfo, m, "movie")

                poster_path = os.path.join(target_dir, "poster.jpg")
                if m.get("poster") and not os.path.exists(poster_path):
                    image_tasks.append((poster_path, m["poster"]))

                fanart_path = os.path.join(target_dir, "fanart.jpg")
                if m.get("fanart") and not os.path.exists(fanart_path):
                    image_tasks.append((fanart_path, m["fanart"]))

        # 锁外并发下载图片，不阻塞其他线程的 NFO 写入
        for img_path, img_url in image_tasks:
            save_image(img_path, img_url)

    def _new_item_id(self):
        """Return a stable Treeview/file identifier."""
        self._item_seq += 1
        return f"file::{self._item_seq}"

    def _normalize_strip_keywords_text(self, value):
        """Normalize strip-keyword config values to one display string."""
        if isinstance(value, (list, tuple, set)):
            items = [str(v).strip() for v in value if str(v).strip()]
            return " | ".join(items)
        return str(value or "").strip()

    def _get_strip_keywords(self):
        """Return normalized strip keywords as an ordered list."""
        raw = str(self.strip_keywords_var.get() or "")
        parts = re.split(r"[\r\n,|]+", raw)
        seen = set()
        items = []
        for part in parts:
            text = part.strip()
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            items.append(text)
        return items

    def _extract_media_suffix(self, filename, pure_name=None):
        """Extract quality/source suffix from an original filename."""
        return extract_media_suffix(filename, pure_name)

    def _render_media_filename(
        self,
        template,
        *,
        title="",
        year="",
        season="",
        episode="",
        ep_name="",
        ext="",
        source_filename="",
        pure_name="",
        parse_source="",
        source_provider="",
        media_id="",
        is_tv=True,
    ):
        """Render a filename and optionally preserve original media suffix."""
        media_suffix = ""
        if self.preserve_media_suffix.get():
            media_suffix = safe_filename(
                self._extract_media_suffix(source_filename, pure_name)
            )
        context = build_filename_context(
            title=title,
            year=year,
            season=season,
            episode=episode,
            ep_name=ep_name,
            ext=ext,
            media_suffix=media_suffix,
            parse_source=parse_source,
            source_provider=source_provider,
            media_id=media_id,
            is_tv=is_tv,
        )
        new_name = render_filename_template(
            template,
            context,
            preserve_media_suffix=self.preserve_media_suffix.get(),
        )
        return new_name, media_suffix

    def _has_ai_backend_configured(self):
        """Return whether at least one usable AI backend is configured."""
        if self.prefer_ollama.get():
            if self.ollama_url.get().strip() and self.ollama_model.get().strip():
                return True
            return bool(self.sf_api_key.get().strip())
        return bool(self.sf_api_key.get().strip())

    def _build_library_target_path(self, item, root_d):
        """Build a Kodi/Jellyfin-style organized target path under root_d."""
        root_dir = str(root_d or "").strip()
        if not root_dir:
            return os.path.join(item.dir, item.new_name_only or item.old_name)

        metadata = item.metadata or {}
        media_id = str(metadata.get("id") or "None").strip()
        if media_id == "None":
            return os.path.join(item.dir, item.new_name_only or item.old_name)

        title = safe_filename(
            str(metadata.get("title") or item.display_title or item.old_name).strip()
        )
        year_text = safe_str(metadata.get("year"))
        provider = str(metadata.get("provider") or "tmdb").strip().lower()
        id_tag = f"bgmid={media_id}" if provider == "bgm" else f"tmdbid={media_id}"
        filename = item.new_name_only or item.old_name

        if str(metadata.get("type") or "episode").strip().lower() == "episode":
            season_num = safe_int(metadata.get("s"), 1)
            folder_name = safe_filename(f"{title} [{id_tag}]")
            return os.path.join(root_dir, folder_name, f"Season {season_num}", filename)

        if year_text:
            folder_name = safe_filename(f"{title} ({year_text}) [{id_tag}]")
        else:
            folder_name = safe_filename(f"{title} [{id_tag}]")
        return os.path.join(root_dir, folder_name, filename)

    def _build_target_for_mode(self, item, run_mode):
        """Resolve the effective filesystem target for one run mode."""
        if run_mode == "archive" and item.full_target:
            return item.full_target
        if run_mode == "organize":
            source_root = item.organize_root or item.source_path or item.dir
            return self._build_library_target_path(item, source_root)
        return os.path.join(item.dir, item.new_name_only or item.old_name)

    def get_item_by_id(self, item_id):
        """Look up one media item by its stable identifier."""
        return self.item_by_id.get(item_id)

    def is_source_row(self, row_id):
        """Return whether a row id belongs to a source-path node."""
        return str(row_id or "").startswith("source::")

    def is_season_row(self, row_id):
        """Return whether a row id belongs to a season subgroup node."""
        return str(row_id or "").startswith("season::")

    def is_group_row(self, row_id):
        """Return whether a row id belongs to any non-file grouping node."""
        return self.is_source_row(row_id) or self.is_season_row(row_id)

    def source_path_from_row_id(self, row_id):
        """Extract source path from a group row identifier."""
        if self.is_source_row(row_id):
            return str(row_id).split("source::", 1)[1]
        if self.is_season_row(row_id):
            payload = str(row_id).split("season::", 1)[1]
            return payload.split("||", 1)[0]
        return ""

    def season_key_from_row_id(self, row_id):
        """Extract season subgroup key from a season row identifier."""
        if not self.is_season_row(row_id):
            return ""
        payload = str(row_id).split("season::", 1)[1]
        parts = payload.split("||", 1)
        return parts[1] if len(parts) == 2 else ""

    def _source_row_id(self, source_path):
        """Build the Treeview id for a top-level source path."""
        return f"source::{source_path}"

    def _season_row_id(self, source_path, season_key):
        """Build the Treeview id for a season subgroup."""
        return f"season::{source_path}||{season_key}"

    def _group_items(self, group_path):
        """Return all files that belong to one grouped source path."""
        return [item for item in self.file_list if item.source_path == group_path]

    def _season_group_label(self, item):
        """Return the second-level subgroup label under one source path."""
        source_path = item.source_path or item.dir
        rel_dir = ""
        try:
            rel_dir = os.path.relpath(item.dir, source_path)
        except Exception:
            rel_dir = ""

        if not rel_dir or rel_dir in (".", ""):
            return "根目录文件"

        first_part = str(rel_dir).split(os.sep, 1)[0].strip()
        return first_part or "根目录文件"

    def _season_groups_for_source(self, source_path):
        """Return ordered season subgroups and their items for one source path."""
        groups = {}
        order = []
        for item in self._group_items(source_path):
            label = self._season_group_label(item)
            if label not in groups:
                groups[label] = []
                order.append(label)
            groups[label].append(item)
        return [(label, groups[label]) for label in order]

    def _use_flat_source_layout(self, source_path):
        """Return whether one source path should show files directly under root."""
        season_groups = self._season_groups_for_source(source_path)
        return len(season_groups) == 1 and season_groups[0][0] == "根目录文件"

    def get_selected_file_ids(self):
        """Return selected file row ids only."""
        return [row_id for row_id in self.tree.selection() if self.get_item_by_id(row_id)]

    def _collect_file_descendants(self, row_id):
        """Collect all file-node descendants under one tree row."""
        if not row_id or not self.tree.exists(row_id):
            return []

        item = self.get_item_by_id(row_id)
        if item:
            return [item.id]

        result = []
        for child_id in self.tree.get_children(row_id):
            result.extend(self._collect_file_descendants(child_id))
        return result

    def _selection_scope_row_for_ctrl_a(self):
        """Resolve which row should define Ctrl+A scope in grouped view."""
        focus_row = self.tree.focus()
        if focus_row and self.tree.exists(focus_row):
            return focus_row

        selection = self.tree.selection()
        return selection[0] if selection else ""

    def _resolve_current_action_scope(self):
        """Resolve the file subset targeted by the bottom action buttons."""
        if not self.file_list:
            return [], [], "全部文件"

        scope_row = self._selection_scope_row_for_ctrl_a()
        if not scope_row or not self.tree.exists(scope_row):
            indices = list(range(len(self.file_list)))
            return indices, list(self.file_list), "全部文件"

        if self.get_item_by_id(scope_row):
            scope_row = self.tree.parent(scope_row) or scope_row

        file_ids = self._collect_file_descendants(scope_row)
        if not file_ids:
            indices = list(range(len(self.file_list)))
            return indices, list(self.file_list), "全部文件"

        id_to_index = {item.id: idx for idx, item in enumerate(self.file_list)}
        items = []
        indices = []
        for file_id in file_ids:
            item = self.get_item_by_id(file_id)
            idx = id_to_index.get(file_id)
            if item is None or idx is None:
                continue
            items.append(item)
            indices.append(idx)

        scope_label = self.tree.item(scope_row, "text") or "当前分组"
        return indices, items, scope_label

    def _item_values(self, item):
        """Build the visible column values for one file row."""
        return (
            item.display_title,
            item.display_match_id,
            item.display_target,
            item.status_text,
        )

    def refresh_item_row(self, item_id):
        """Refresh one file row in the active tree if it is visible."""
        item = self.get_item_by_id(item_id)
        if not item or not self.tree.exists(item.id):
            self.update_details_panel()
            return

        self.tree.item(item.id, text=item.old_name, values=self._item_values(item))
        self.update_details_panel()

    def update_item_display(
        self,
        item_or_id,
        *,
        old_name=None,
        title=None,
        match_id=None,
        target=None,
        status=None,
    ):
        """Update cached UI text for one item and refresh the visible row."""
        item = (
            item_or_id
            if isinstance(item_or_id, MediaItem)
            else self.get_item_by_id(item_or_id)
        )
        if not item:
            return

        if old_name is not None:
            item.old_name = old_name
        if title is not None:
            item.display_title = str(title)
        if match_id is not None:
            item.display_match_id = str(match_id)
        if target is not None:
            item.display_target = str(target)
        if status is not None:
            item.status_text = str(status)

        self.refresh_item_row(item.id)

    def _set_details_content(self, left_text, right_text):
        """Render wrapped details text inside the lower two-column panel."""
        self.detail_left_var.set((left_text or "").strip())
        self.detail_right_var.set((right_text or "").strip())

    def _build_group_details(self, group_path):
        """Build the wrapped details blocks for one grouped path."""
        items = self._group_items(group_path)
        if not items:
            return (
                f"添加路径:\n{group_path}\n\n该分组当前没有文件。",
                "",
            )

        recognized = sum(1 for item in items if item.metadata.get("id") != "None")
        pending = sum(1 for item in items if item.status_text in ("待命", "识别中"))
        done = sum(
            1
            for item in items
            if item.status_text
            in ("重命名完成", "归档完成", "原地整理完成", "原地整理+刮削完成", "刮削完成")
        )

        left_text = (
            f"添加路径:\n{group_path}\n\n"
            f"文件数量: {len(items)}\n"
            f"已识别: {recognized}\n"
            f"进行中/待命: {pending}\n"
            f"已完成: {done}"
        )
        if self._use_flat_source_layout(group_path):
            sample_lines = [item.old_name for item in items[:12]]
            if len(items) > 12:
                sample_lines.append(f"... 还有 {len(items) - 12} 个文件")
            right_text = "当前目录文件:\n" + "\n".join(sample_lines)
        else:
            season_lines = []
            for season_label, season_items in self._season_groups_for_source(group_path):
                season_lines.append(f"{season_label}: {len(season_items)} 个文件")
            right_text = "Season 分组:\n" + ("\n".join(season_lines) if season_lines else "(无)")
        return left_text, right_text

    def _build_season_group_details(self, source_path, season_key):
        """Build the wrapped details blocks for one season subgroup."""
        items = [
            item
            for item in self._group_items(source_path)
            if self._season_group_label(item) == season_key
        ]
        if not items:
            return (
                f"添加路径:\n{source_path}\n\nSeason 分组:\n{season_key}\n\n该分组当前没有文件。",
                "",
            )

        recognized = sum(1 for item in items if item.metadata.get("id") != "None")
        pending = sum(1 for item in items if item.status_text in ("待命", "识别中"))
        done = sum(
            1
            for item in items
            if item.status_text
            in ("重命名完成", "归档完成", "原地整理完成", "原地整理+刮削完成", "刮削完成")
        )
        sample_lines = [item.old_name for item in items[:12]]
        if len(items) > 12:
            sample_lines.append(f"... 还有 {len(items) - 12} 个文件")

        left_text = (
            f"添加路径:\n{source_path}\n\n"
            f"Season 分组:\n{season_key}\n\n"
            f"文件数量: {len(items)}\n"
            f"已识别: {recognized}\n"
            f"进行中/待命: {pending}\n"
            f"已完成: {done}"
        )
        right_text = "当前 Season 文件:\n" + "\n".join(sample_lines)
        return left_text, right_text

    def _build_item_details(self, item):
        """Build the wrapped details blocks for one selected file."""
        title = item.display_title or "(未识别)"
        match_id = item.display_match_id or "(无)"
        target = item.display_target or "(尚未生成)"
        status = item.status_text or "待命"
        source_path = item.source_path or item.dir
        full_path = item.path or ""
        left_text = (
            f"原文件名:\n{item.old_name}\n\n"
            f"原始完整路径:\n{full_path}\n\n"
            f"所属添加路径:\n{source_path}"
        )
        right_text = (
            f"识别标题:\n{title}\n\n"
            f"识别来源:\n{item.parse_source or '(未记录)'}\n\n"
            f"匹配 ID:\n{match_id}\n\n"
            f"新文件名 / 归档路径:\n{target}\n\n"
            f"状态:\n{status}"
        )
        return left_text, right_text

    def update_details_panel(self, _event=None):
        """Refresh the lower details panel based on current selection."""
        selection = self.tree.selection()
        if not selection:
            self._set_details_content("当前没有选中任何分组或文件。", "")
            return

        row_id = selection[0]
        if self.is_source_row(row_id):
            self._set_details_content(
                *self._build_group_details(self.source_path_from_row_id(row_id))
            )
            return

        if self.is_season_row(row_id):
            self._set_details_content(
                *self._build_season_group_details(
                    self.source_path_from_row_id(row_id),
                    self.season_key_from_row_id(row_id),
                )
            )
            return

        item = self.get_item_by_id(row_id)
        if not item:
            self._set_details_content("当前选中项已失效，请重新选择。", "")
            return

        self._set_details_content(*self._build_item_details(item))

    def on_treeview_open(self, _event=None):
        """Persist group expanded state while using grouped view."""
        row_id = self.tree.focus()
        if self.is_group_row(row_id):
            self.expanded_groups.add(row_id)

    def on_treeview_close(self, _event=None):
        """Persist group collapsed state while using grouped view."""
        row_id = self.tree.focus()
        if self.is_group_row(row_id):
            self.expanded_groups.discard(row_id)

    def toggle_group_row(self, row_id):
        """Toggle one grouped path row and persist its open state."""
        if not self.is_group_row(row_id) or not self.tree.exists(row_id):
            return

        new_state = not bool(self.tree.item(row_id, "open"))
        self.tree.item(row_id, open=new_state)
        if new_state:
            self.expanded_groups.add(row_id)
        else:
            self.expanded_groups.discard(row_id)

    def _parse_with_ollama(self, filename):
        """调用本地 Ollama 模型解析文件名"""
        return parse_with_ollama(
            self.ollama_url.get().strip(),
            self.ollama_model.get().strip(),
            filename,
            self._get_ai_temperature(),
            self._get_ai_top_p(),
        )

    def _can_use_ollama_for_pick(self):
        """是否可用本地模型做候选判定"""
        return bool(self.ollama_url.get().strip() and self.ollama_model.get().strip())

    def _can_use_embedding_rank(self):
        """是否可用本地 embedding 做候选重排"""
        return bool(
            self.use_embedding_rank.get()
            and self.prefer_ollama.get()
            and self.ollama_url.get().strip()
            and self.embedding_model.get().strip()
        )

    def _ollama_post_json(self, endpoint, payload, timeout):
        """直接请求本地 Ollama，避免全局 session 的重试拖慢处理。"""
        from core.services.matcher_service import ollama_post_json

        return ollama_post_json(
            self.ollama_url.get().strip(), endpoint, payload, timeout
        )

    def _cosine_similarity(self, vec_a, vec_b):
        """计算余弦相似度"""
        from core.services.matcher_service import cosine_similarity

        return cosine_similarity(vec_a, vec_b)

    def _get_embedding(self, text):
        """调用 Ollama embeddings 接口并缓存向量"""
        if not self._can_use_embedding_rank():
            return None
        emb, endpoint = get_embedding(
            self.ollama_url.get().strip(),
            self.embedding_model.get().strip(),
            text,
            self.embedding_cache,
            self.cache_lock,
            self.ollama_embed_endpoint,
        )
        self.ollama_embed_endpoint = endpoint
        return emb

    def _build_candidate_embedding_text(self, cand):
        """构建候选项向量语义文本"""
        from core.services.matcher_service import build_candidate_embedding_text

        return build_candidate_embedding_text(cand)

    def _rerank_candidates_with_embedding(
        self, item, query_title, year, is_tv, source_name, candidates
    ):
        """用 embedding 对候选重排，并在高置信时自动命中"""
        if not self._can_use_embedding_rank() or not candidates:
            return candidates, None, ""
        return rerank_candidates_with_embedding(
            item,
            query_title,
            year,
            is_tv,
            source_name,
            candidates,
            self._get_embedding,
        )

    def _pick_candidate_with_ollama(
        self, item, query_title, year, is_tv, source_name, candidates
    ):
        """使用本地 Ollama 在多个候选中做判定"""
        return pick_candidate_with_ollama(
            self.ollama_url.get().strip(),
            self.ollama_model.get().strip(),
            item,
            query_title,
            year,
            is_tv,
            source_name,
            candidates,
            self._get_ai_temperature(),
        )

    def _can_use_online_ai_for_pick(self):
        """Return whether online AI should judge DB candidates."""
        return bool(
            not self.prefer_ollama.get()
            and self.sf_api_key.get().strip()
            and self.sf_api_url.get().strip()
            and self.sf_model.get().strip()
        )

    def _pick_candidate_with_online_ai(
        self, item, query_title, year, is_tv, source_name, candidates
    ):
        """使用在线 OpenAI 兼容接口在多个候选中做判定"""
        return pick_candidate_with_openai_compatible(
            self.sf_api_url.get().strip(),
            self.sf_api_key.get().strip(),
            self.sf_model.get().strip(),
            item,
            query_title,
            year,
            is_tv,
            source_name,
            candidates,
            self._get_ai_temperature(),
            self._get_ai_top_p(),
        )

    def _auto_pick_candidate_by_score(self, query_title, year, source_name, candidates):
        """通用候选评分判定，供 GUI 和自动化流程共用。"""
        return auto_pick_candidate_by_score(query_title, year, source_name, candidates)

    def _pick_strong_tmdb_direct_hit(self, query_titles, year, candidates):
        """Trust a direct TMDb rank-1 hit before semantic rerank/model override."""
        requested_year = safe_str(year).strip()
        preferred_norms = []
        seen_norms = set()
        for raw in query_titles or []:
            text = str(raw or "").strip()
            norm = normalize_compare_text(text)
            if not _is_substantial_query_norm(norm) or norm in seen_norms:
                continue
            seen_norms.add(norm)
            preferred_norms.append(norm)

        if not preferred_norms:
            return None, ""

        grouped = {}
        for candidate in candidates or []:
            meta = candidate.get("meta") or {}
            search_query = str(meta.get("search_query") or "").strip()
            search_norm = normalize_compare_text(search_query)
            if not search_norm:
                continue
            grouped.setdefault(search_norm, []).append(candidate)

        for norm in preferred_norms:
            hits = grouped.get(norm) or []
            if not hits:
                continue

            hits = sorted(
                hits,
                key=lambda cand: (
                    safe_int((cand.get("meta") or {}).get("search_rank"), 999),
                    -float(cand.get("rating") or 0),
                ),
            )
            top = hits[0]
            top_meta = top.get("meta") or {}
            top_rank = safe_int(top_meta.get("search_rank"), 999)
            top_year = extract_year_from_release(top.get("release") or "")
            year_ok = years_within_tolerance(requested_year, top_year)
            if top_rank == 1 and year_ok:
                return top, str(top_meta.get("search_query") or "")

        return None, ""

    def _select_best_db_match(
        self,
        item,
        query_title,
        year,
        is_tv,
        source_name,
        candidates,
        recognized_title=None,
    ):
        """从候选列表中自动或手动选择最终匹配项"""
        if not candidates:
            return query_title, "None", f"{source_name}无结果", {}
        rank_pick_allowed = True
        raw_name = ""
        if isinstance(item, dict):
            raw_name = str(item.get("old_name") or "")
        else:
            raw_name = str(getattr(item, "old_name", "") or "")
        if source_name.startswith("TMDb") and getattr(item, "old_name", None):
            derived_query = derive_title_from_filename(raw_name)
            if (
                derived_query
                and normalize_compare_text(derived_query)
                != normalize_compare_text(query_title)
            ):
                rank_pick_allowed = False

        if source_name.startswith("TMDb") and not text_mentions_extra_title(
            f"{raw_name} {query_title}"
        ):
            regular_candidates = [
                c for c in candidates if not candidate_looks_like_extra_title(c)
            ]
            if regular_candidates:
                candidates = regular_candidates
            elif candidates:
                return (
                    query_title,
                    "None",
                    "TMDb候选疑似总集篇或特别篇，需手动确认",
                    {},
                )

        if source_name.startswith("TMDb"):
            source_text = f"{raw_name} {query_title}"
            regular_candidates = [
                c
                for c in candidates
                if not candidate_looks_like_unrequested_variant(c, source_text)
            ]
            if regular_candidates:
                candidates = regular_candidates
            elif candidates:
                return (
                    query_title,
                    "None",
                    "TMDb候选疑似外传或衍生剧，需手动确认",
                    {},
                )

        if is_tv and source_name.startswith("TMDb"):
            source_text = f"{raw_name} {query_title}"
            tv_candidates = [
                c
                for c in candidates
                if not candidate_looks_like_movie_version(c)
                and not candidate_looks_like_unrequested_subtitle_arc(
                    c, query_title, source_text
                )
            ]
            if tv_candidates:
                candidates = tv_candidates

        if len(candidates) == 1 and (
            not source_name.startswith("TMDb") or rank_pick_allowed
        ):
            return candidate_to_result(candidates[0], f"{source_name}命中")

        # 年份预排序：将年份匹配的候选提前，减少同名不同年作品的误匹配
        requested_year = str(year).strip() if year else ""

        def year_compatible(candidate):
            if not requested_year:
                return True
            candidate_year = extract_year_from_release(candidate.get("release") or "")
            if not candidate_year:
                return True
            return years_within_tolerance(requested_year, candidate_year)

        if requested_year:
            candidates = sorted(
                candidates,
                key=lambda c: 0 if extract_year_from_release(c.get("release") or "") == requested_year else 1,
            )

        # 精确/高置信标题匹配：无需 embedding/Ollama 直接命中
        import difflib as _difflib
        import re as _re
        _q_norm = _re.sub(r"[\W_]+", "", str(query_title or "").lower())
        if _q_norm:
            _exact = None
            _scores = []
            for _c in candidates:
                _ct = _re.sub(r"[\W_]+", "", str(_c.get("title") or "").lower())
                _ca = _re.sub(r"[\W_]+", "", str(_c.get("alt_title") or "").lower())
                _co = _re.sub(
                    r"[\W_]+",
                    "",
                    str((_c.get("meta") or {}).get("original_title") or "").lower(),
                )
                _s = max(
                    _difflib.SequenceMatcher(None, _q_norm, _ct).ratio() if _ct else 0.0,
                    _difflib.SequenceMatcher(None, _q_norm, _ca).ratio() if _ca else 0.0,
                    _difflib.SequenceMatcher(None, _q_norm, _co).ratio() if _co else 0.0,
                )
                _scores.append((_s, _c))
                if (
                    _ct == _q_norm
                    or _ca == _q_norm
                    or _co == _q_norm
                ) and year_compatible(_c):
                    _exact = _c
                    break
            if _exact is None and _scores:
                _scores.sort(key=lambda x: x[0], reverse=True)
                _top_s, _top_c = _scores[0]
                _second_s = _scores[1][0] if len(_scores) > 1 else 0.0
                if _top_s >= 0.90 and (_top_s - _second_s) >= 0.20 and year_compatible(_top_c):
                    _exact = _top_c
            if _exact is not None:
                return candidate_to_result(_exact, f"标题匹配/{source_name}命中")

        if source_name.startswith("TMDb") and rank_pick_allowed:
            strong_direct_hit, direct_query = self._pick_strong_tmdb_direct_hit(
                [query_title, recognized_title], year, candidates
            )
            if strong_direct_hit is not None:
                hit_msg = f"TMDb直搜首位/{source_name}命中"
                if direct_query and normalize_compare_text(direct_query) != normalize_compare_text(query_title):
                    hit_msg += " (别名直搜)"
                return candidate_to_result(strong_direct_hit, hit_msg)

        # 自动评分前置命中：在进入 embedding/AI 前尝试纯评分自动选
        score_pick, score_reason = self._auto_pick_candidate_by_score(
            query_title, year, source_name, candidates
        )
        if score_pick is not None:
            return candidate_to_result(score_pick, f"自动评分/{source_name}命中 ({score_reason})")

        prefer_ollama = bool(self.prefer_ollama.get())
        online_ready = self._can_use_online_ai_for_pick()
        ollama_ready = self._can_use_ollama_for_pick()
        ranked_candidates, _emb_pick, emb_msg = self._rerank_candidates_with_embedding(
            item, query_title, year, is_tv, source_name, candidates
        )

        def _candidate_result_from_model(label, chosen, reason):
            hit_msg = f"{label}/{source_name}命中"
            if emb_msg:
                hit_msg += f" ({emb_msg})"
            if reason:
                hit_msg += f" ({reason})"
            return candidate_to_result(chosen, hit_msg)

        ai_attempted = False

        if prefer_ollama and ollama_ready:
            ai_attempted = True
            chosen, reason = self._pick_candidate_with_ollama(
                item, query_title, year, is_tv, source_name, ranked_candidates
            )
            if chosen:
                return _candidate_result_from_model("Ollama判定", chosen, reason)

        if online_ready:
            ai_attempted = True
            chosen, reason = self._pick_candidate_with_online_ai(
                item, query_title, year, is_tv, source_name, ranked_candidates
            )
            if chosen:
                return _candidate_result_from_model("在线AI判定", chosen, reason)

        if (not prefer_ollama) and ollama_ready:
            ai_attempted = True
            chosen, reason = self._pick_candidate_with_ollama(
                item, query_title, year, is_tv, source_name, ranked_candidates
            )
            if chosen:
                return _candidate_result_from_model("Ollama判定", chosen, reason)

        manual_choice = self._request_manual_candidate_choice(
            item,
            query_title,
            source_name,
            ranked_candidates,
            recognized_title=recognized_title,
        )
        if manual_choice:
            hit_msg = f"手动选择/{source_name}命中"
            if emb_msg:
                hit_msg += f" ({emb_msg})"
            return candidate_to_result(manual_choice, hit_msg)

        pending_reason = "候选存在歧义，需手动确认"
        if ai_attempted:
            pending_reason = "候选存在歧义，AI未能稳定判定"
        elif not (online_ready or ollama_ready):
            pending_reason = "候选存在歧义，未启用AI自动判定"
        if emb_msg:
            pending_reason += f" ({emb_msg})"
        return query_title, "None", pending_reason, {}

    def _resolve_db_match(self, item, query_title, year, is_tv, mode, ai_data, g):
        """解析数据库候选，必要时调用本地模型或弹窗手动确认"""
        source_name = "TMDb" if mode == "siliconflow_tmdb" else "BGM"
        db_year = None if is_tv else year
        query_groups = build_db_query_plan(item, query_title, ai_data, g)
        merged = []
        seen_ids = set()
        used_query = query_title
        _first_hit = False

        def _search_queries(query_titles, fetch_func, limit=10):
            nonlocal used_query, _first_hit
            found = []
            for q in query_titles:
                cur = fetch_func(q)
                if not cur:
                    continue

                if not _first_hit:
                    used_query = q
                    _first_hit = True

                for cand in cur:
                    cid = str(cand.get("id") or "")
                    if not cid or cid in seen_ids:
                        continue
                    seen_ids.add(cid)
                    found.append(cand)

                if (
                    mode == "siliconflow_tmdb"
                    and self._pick_strong_tmdb_direct_hit([q], db_year, cur)[0] is not None
                ):
                    break
                if len(found) >= limit:
                    break
            return found

        for query_titles in query_groups:
            if mode == "siliconflow_tmdb":
                current = _search_queries(
                    query_titles,
                    lambda q: fetch_tmdb_candidates(
                        q, db_year, is_tv, self.tmdb_api_key.get()
                    ),
                )
            else:
                current = _search_queries(
                    query_titles,
                    lambda q: fetch_bgm_candidates(q, db_year, self.bgm_api_key.get()),
                )

            if current:
                merged.extend(current)
                break

        type_flipped = False
        if not merged and mode == "siliconflow_tmdb":
            flipped_tv = not is_tv
            flipped_year = None if flipped_tv else year
            for query_titles in query_groups:
                current = _search_queries(
                    query_titles,
                    lambda q: fetch_tmdb_candidates(
                        q, flipped_year, flipped_tv, self.tmdb_api_key.get()
                    ),
                )
                if current:
                    merged.extend(current)
                    type_flipped = True
                    break

        bgm_fallback = False
        if not merged and mode == "siliconflow_tmdb":
            for query_titles in query_groups:
                current = _search_queries(
                    query_titles,
                    lambda q: fetch_bgm_candidates(q, db_year, self.bgm_api_key.get()),
                )
                if current:
                    merged.extend(current)
                    break

            if merged:
                bgm_fallback = True
                source_name = "BGM(回退)"

        if merged:
            t_hit, tid_hit, msg_hit, meta_hit = self._select_best_db_match(
                item,
                used_query,
                db_year,
                is_tv,
                source_name,
                merged,
                recognized_title=query_title,
            )
            if tid_hit != "None" and normalize_compare_text(
                used_query
            ) != normalize_compare_text(query_title):
                msg_hit += " (备选标题)"
            if type_flipped and tid_hit != "None":
                msg_hit += " (类型翻转)"
            if bgm_fallback and tid_hit != "None":
                meta_hit["_provider"] = "bgm"
                if self.tmdb_api_key.get().strip():
                    tmdb_candidates = fetch_tmdb_candidates(
                        t_hit or used_query, db_year, is_tv, self.tmdb_api_key.get()
                    )
                    if tmdb_candidates:
                        tmdb_meta = tmdb_candidates[0].get("meta") or {}
                        if tmdb_meta.get("poster"):
                            meta_hit["poster"] = tmdb_meta["poster"]
                        if tmdb_meta.get("fanart"):
                            meta_hit["fanart"] = tmdb_meta["fanart"]
            return t_hit, tid_hit, msg_hit, meta_hit

        return query_title, "None", f"{source_name}无结果", {}

    def _async_batch_runner(self, indices, title, t_id, msg, meta):
        """异步批量处理"""
        return worker_async_batch_runner(self, indices, title, t_id, msg, meta)

    def _bg_update_single_ui(self, idx, title, t_id, msg, meta):
        """后台更新单个UI项"""
        return worker_bg_update_single_ui(self, idx, title, t_id, msg, meta)

    def _get_version_tag(self, path):
        """获取版本标签"""
        return get_version_tag(path)

    def _friendly_status_text(self, message):
        """Render coded errors to concise Chinese status text for the UI."""
        return friendly_status_text(message)

    def _build_status_text(self, *messages):
        return build_status_text(*messages)

    def _resolve_media_type(self, guess_data=None, pure_name=None, extracted_ep=None):
        """Resolve media type from UI override or parser result."""
        from utils.helpers import EXPLICIT_EP_MARKER_RE

        override = str(self.media_type_override.get() or "").strip()
        if override == "电影":
            return "movie"
        if override == "电视剧":
            return "episode"

        guessed_type = str((guess_data or {}).get("type") or "episode").strip().lower()
        if guessed_type in ("movie", "film"):
            if pure_name and EXPLICIT_EP_MARKER_RE.search(str(pure_name)):
                return "episode"
            return "movie"
        if guessed_type == "episode":
            return "episode"
        if pure_name is not None:
            text = str(pure_name or "")
            has_season_ep = bool(re.search(r"(?i)\bS\d{1,2}E\d{1,4}\b", text))
            has_ep_marker = bool(EXPLICIT_EP_MARKER_RE.search(text))
            has_season_marker = bool(
                re.search(r"(?i)(?:\bS\d{1,2}\b|Season\s*\d|第\s*\d{1,2}\s*季)", text)
            )
            if has_season_ep or has_ep_marker or has_season_marker:
                return "episode"
            if extracted_ep is None:
                return "movie"
        return "episode"

    def _reset_progress_bar(self):
        """任务结束后清零进度条，避免一直停在 100%。"""
        try:
            self.pbar["value"] = 0
            self.pbar.config(maximum=1)
        except Exception:
            pass

    def _on_preview_finished(self):
        """预览池全部跑完后的收尾（子类可覆盖以恢复选中/详情）。"""
        self._reset_progress_bar()

    def run_preview_pool(self):
        """运行预览线程池"""
        return worker_run_preview_pool(self)

    def process_task(self, i, advance_progress=True):
        """处理单个任务"""
        return worker_process_task(self, i, advance_progress=advance_progress)

    def run_execution(self, run_mode):
        """执行重命名"""
        return worker_run_execution(self, run_mode)

    def run_scrape_execution(self):
        """执行独立刮削"""
        return worker_run_scrape_execution(self)

    def process_one_file(self, item, run_mode):
        """处理单个文件"""
        return worker_process_one_file(self, item, run_mode)

    def process_one_file_scrape(self, item):
        """单独刮削单个文件"""
        return worker_process_one_file_scrape(self, item)


