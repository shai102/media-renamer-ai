import logging
import os
import tkinter as tk

from tkinter import filedialog

from core.models.media_item import MediaItem
from utils.helpers import clear_api_cache_file


class ListMixin:
    def add_files(self):
        """添加文件"""
        files = filedialog.askopenfilenames(parent=self.root)
        for file_path in files:
            self._add(file_path)

    def add_folder(self):
        """添加文件夹"""
        folder = filedialog.askdirectory(parent=self.root)
        if folder:
            exts = self.get_media_exts()
            count = 0
            for root_dir, _, files in os.walk(folder):
                for file_name in files:
                    if file_name.lower().endswith(exts):
                        self._add(os.path.join(root_dir, file_name))
                        count += 1

            if count > 0:
                self.status.config(text=f"已添加 {count} 个文件")

    def _add(self, path):
        """添加单个文件"""
        if not os.path.exists(path):
            return

        if any(x.path == path for x in self.file_list):
            return

        _, ext = self.extract_lang_and_ext(os.path.basename(path))
        tid = self.tree.insert("", tk.END, values=(os.path.basename(path), "", "", "", "待命"))

        self.file_list.append(
            MediaItem(
                id=tid,
                path=path,
                dir=os.path.dirname(path),
                old_name=os.path.basename(path),
                ext=ext,
            )
        )

    def clear_list(self):
        """清空列表"""
        for row_id in self.tree.get_children():
            self.tree.delete(row_id)

        self.file_list.clear()
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
        self.status.config(text="列表与缓存已清空")

    def select_all_files(self, event=None):
        """Select all rows in the file list."""
        row_ids = self.tree.get_children()
        if row_ids:
            self.tree.selection_set(row_ids)
            self.tree.focus(row_ids[0])
        return "break"

    def remove_file_by_row_id(self, row_id):
        """Remove one file entry from list/tree by Treeview row id."""
        if not row_id:
            return False

        idx = next(
            (i for i, it in enumerate(self.file_list) if it.id == row_id),
            None,
        )
        if idx is None:
            return False

        removed = self.file_list.pop(idx)
        path_key = removed.path

        if self.tree.exists(row_id):
            self.tree.delete(row_id)

        with self.cache_lock:
            self.manual_locks.pop(path_key, None)
            self.forced_seasons.pop(path_key, None)
            self.forced_offsets.pop(path_key, None)

        self.status.config(text="已从列表删除 1 个文件")
        return True
