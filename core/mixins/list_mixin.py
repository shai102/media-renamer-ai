import logging
import os
import tkinter as tk

from tkinter import filedialog

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

        if any(x["path"] == path for x in self.file_list):
            return

        _, ext = self.extract_lang_and_ext(os.path.basename(path))
        tid = self.tree.insert("", tk.END, values=(os.path.basename(path), "", "", "", "待命"))

        self.file_list.append(
            {
                "id": tid,
                "path": path,
                "dir": os.path.dirname(path),
                "old_name": os.path.basename(path),
                "ext": ext,
                "metadata": {"id": "None"},
            }
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
