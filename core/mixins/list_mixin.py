import logging
import os
import tkinter as tk

from tkinter import filedialog

from core.models.media_item import MediaItem
from utils.helpers import clear_api_cache_file


class ListMixin:
    def _default_organize_root(self, source_path):
        """Return the base directory used by in-place organize mode."""
        path = os.path.normpath(str(source_path or "").strip())
        if not path:
            return ""
        parent = os.path.dirname(path)
        return parent or path

    def add_files(self):
        """添加文件"""
        files = filedialog.askopenfilenames(parent=self.root)
        count = 0
        for file_path in files:
            source_dir = os.path.dirname(file_path)
            if self._add(
                file_path,
                source_path=source_dir,
                organize_root=source_dir,
                refresh=False,
            ):
                count += 1
        if count:
            self.refresh_tree_view()
            self.status.config(text=f"已添加 {count} 个文件")

    def add_folder(self):
        """添加文件夹"""
        folder = filedialog.askdirectory(parent=self.root)
        if folder:
            exts = self.get_media_exts()
            count = 0
            for root_dir, _, files in os.walk(folder):
                for file_name in files:
                    if file_name.lower().endswith(exts):
                        if self._add(
                            os.path.join(root_dir, file_name),
                            source_path=folder,
                            organize_root=self._default_organize_root(folder),
                            refresh=False,
                        ):
                            count += 1

            if count > 0:
                self.refresh_tree_view()
                self.status.config(text=f"已添加 {count} 个文件")

    def _add(self, path, source_path=None, organize_root=None, refresh=True):
        """添加单个文件"""
        if not os.path.exists(path):
            return False

        if any(x.path == path for x in self.file_list):
            return False

        _, ext = self.extract_lang_and_ext(os.path.basename(path))
        item_id = self._new_item_id()
        group_path = source_path or os.path.dirname(path)

        item = MediaItem(
            id=item_id,
            path=path,
            dir=os.path.dirname(path),
            old_name=os.path.basename(path),
            ext=ext,
            source_path=group_path,
            organize_root=organize_root or self._default_organize_root(group_path),
        )
        self.file_list.append(item)
        self.item_by_id[item.id] = item

        if refresh:
            self.refresh_tree_view()

        return True

    def clear_list(self):
        """清空列表"""
        for row_id in self.tree.get_children():
            self.tree.delete(row_id)

        self.file_list.clear()
        self.item_by_id.clear()
        self.expanded_groups.clear()
        self.action_scope_item_ids.clear()
        with self.cache_lock:
            for evt in self.db_resolution_events.values():
                try:
                    evt.set()
                except Exception as err:
                    logging.debug(f"释放等待事件失败: {err}")

            self.dir_cache.clear()
            self.db_cache.clear()
            self.embedding_cache.clear()
            self.manual_locks.clear()
            self.forced_seasons.clear()
            self.forced_offsets.clear()
            self.db_resolution_events.clear()

        clear_api_cache_file()
        self._set_details_content("当前没有选中任何分组或文件。", "")
        self.status.config(text="列表与缓存已清空")

    def select_all_files(self, event=None):
        """Select all rows in the file list."""
        row_ids = []
        scope_row = self._selection_scope_row_for_ctrl_a()
        if scope_row:
            if self.get_item_by_id(scope_row):
                scope_row = self.tree.parent(scope_row) or scope_row
            row_ids = self._collect_file_descendants(scope_row)
        else:
            row_ids = [item.id for item in self.file_list if self.tree.exists(item.id)]

        if row_ids:
            self.tree.selection_set(row_ids)
            self.tree.focus(row_ids[0])
            self.update_details_panel()
        return "break"

    def remove_file_by_row_id(self, row_id):
        """Remove one file entry from list/tree by Treeview row id."""
        if not row_id:
            return False

        removed = self.item_by_id.get(row_id)
        if removed is None:
            return False

        self.file_list = [it for it in self.file_list if it.id != row_id]
        self.item_by_id.pop(row_id, None)
        path_key = removed.path

        with self.cache_lock:
            self.manual_locks.pop(path_key, None)
            self.forced_seasons.pop(path_key, None)
            self.forced_offsets.pop(path_key, None)

        self.refresh_tree_view()
        self.status.config(text="已从列表删除 1 个文件")
        return True

    def remove_group_by_row_id(self, row_id):
        """Remove one grouped source path and all contained files."""
        group_path = self.source_path_from_row_id(row_id)
        if not group_path:
            return False

        removed_items = [it for it in self.file_list if it.source_path == group_path]
        if not removed_items:
            return False

        remove_ids = {it.id for it in removed_items}
        self.file_list = [it for it in self.file_list if it.source_path != group_path]
        for item in removed_items:
            self.item_by_id.pop(item.id, None)
            with self.cache_lock:
                self.manual_locks.pop(item.path, None)
                self.forced_seasons.pop(item.path, None)
                self.forced_offsets.pop(item.path, None)

        self.expanded_groups.discard(self._source_row_id(group_path))
        self.refresh_tree_view()
        self.status.config(text=f"已从列表删除分组，共 {len(remove_ids)} 个文件")
        return True

    def remove_season_group_by_row_id(self, row_id):
        """Remove one season subgroup and all contained files."""
        source_path = self.source_path_from_row_id(row_id)
        season_key = self.season_key_from_row_id(row_id)
        if not source_path or not season_key:
            return False

        removed_items = [
            it
            for it in self.file_list
            if it.source_path == source_path and self._season_group_label(it) == season_key
        ]
        if not removed_items:
            return False

        remove_ids = {it.id for it in removed_items}
        self.file_list = [
            it
            for it in self.file_list
            if not (it.source_path == source_path and self._season_group_label(it) == season_key)
        ]
        for item in removed_items:
            self.item_by_id.pop(item.id, None)
            with self.cache_lock:
                self.manual_locks.pop(item.path, None)
                self.forced_seasons.pop(item.path, None)
                self.forced_offsets.pop(item.path, None)

        self.expanded_groups.discard(self._season_row_id(source_path, season_key))
        self.refresh_tree_view()
        self.status.config(text=f"已从列表删除 {season_key}，共 {len(remove_ids)} 个文件")
        return True
