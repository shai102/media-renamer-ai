import json
import logging
import os
import re

from tkinter import messagebox

from utils.helpers import CONFIG_FILE, safe_int


class ConfigMixin:
    def _is_geometry_in_screen(self, x, y, w, h):
        """检查窗口坐标是否仍在当前屏幕可见区域内"""
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        return (x < sw - 80) and (y < sh - 80) and (x + w > 80) and (y + h > 80)

    def apply_saved_window_geometry(self):
        """启动时恢复上次窗口位置和大小"""
        geo = self.config.get("window_geometry", "")
        if not geo:
            return

        match = re.match(r"^(\d+)x(\d+)\+(-?\d+)\+(-?\d+)$", str(geo).strip())
        if not match:
            return

        w, h, x, y = map(int, match.groups())
        if w < 600 or h < 400:
            return

        if self._is_geometry_in_screen(x, y, w, h):
            self.root.geometry(f"{w}x{h}+{x}+{y}")

    def load_config(self):
        """加载配置"""
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as err:
                logging.error(f"加载配置失败: {err}")
        return {}

    def save_config(self, show_message=True):
        """保存配置"""
        preview_workers = self._clamp_workers(self.preview_workers.get(), 5)
        sync_workers = self._clamp_workers(self.sync_workers.get(), 5)
        execution_workers = self._clamp_workers(self.execution_workers.get(), 3)

        self.preview_workers.set(str(preview_workers))
        self.sync_workers.set(str(sync_workers))
        self.execution_workers.set(str(execution_workers))
        ai_temperature = self._clamp_temperature(self.ai_temperature.get(), 0.2)
        self.ai_temperature.set(f"{ai_temperature:.2f}")
        ai_top_p = self._clamp_top_p(self.ai_top_p.get(), 0.9)
        self.ai_top_p.set(f"{ai_top_p:.2f}")

        config_data = {
            "sf_api_key": self.sf_api_key.get().strip(),
            "sf_model": self.sf_model.get().strip(),
            "ai_temperature": ai_temperature,
            "ai_top_p": ai_top_p,
            "bgm_api_key": self.bgm_api_key.get().strip(),
            "tmdb_api_key": self.tmdb_api_key.get().strip(),
            "tv_format": self.tv_format.get(),
            "movie_format": self.movie_format.get(),
            "video_exts": self.video_exts.get(),
            "sub_audio_exts": self.sub_audio_exts.get(),
            "lang_tags": self.lang_tags.get(),
            "ollama_url": self.ollama_url.get().strip(),
            "ollama_model": self.ollama_model.get().strip(),
            "embedding_model": self.embedding_model.get().strip(),
            "prefer_ollama": self.prefer_ollama.get(),
            "use_embedding_rank": self.use_embedding_rank.get(),
            "preview_workers": preview_workers,
            "sync_workers": sync_workers,
            "execution_workers": execution_workers,
            "media_type_override": self.media_type_override.get(),
            "window_geometry": self.root.winfo_geometry(),
        }

        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(config_data, f, indent=4, ensure_ascii=False)
            if show_message:
                messagebox.showinfo(
                    "成功",
                    "所有配置与规则已保存！立即生效。",
                    parent=self.root,
                )
        except Exception as err:
            if show_message:
                messagebox.showerror("错误", f"保存失败: {err}", parent=self.root)

    def on_close(self):
        """关闭窗口时静默保存配置（含窗口位置）"""
        try:
            self.save_config(show_message=False)
        except Exception as err:
            logging.error(f"关闭时保存配置失败: {err}")
        self.root.destroy()

    def _clamp_workers(self, value, default):
        """Normalize worker count to a safe desktop range."""
        num = safe_int(value, default)
        return max(1, min(10, num))

    def _clamp_temperature(self, value, default=0.2):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return float(default)
        return max(0.0, min(2.0, number))

    def _get_ai_temperature(self):
        return self._clamp_temperature(self.ai_temperature.get(), 0.2)

    def _clamp_top_p(self, value, default=0.9):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return float(default)
        return max(0.0, min(1.0, number))

    def _get_ai_top_p(self):
        return self._clamp_top_p(self.ai_top_p.get(), 0.9)

    def _get_preview_workers(self):
        return self._clamp_workers(self.preview_workers.get(), 5)

    def _get_sync_workers(self):
        return self._clamp_workers(self.sync_workers.get(), 5)

    def _get_execution_workers(self):
        return self._clamp_workers(self.execution_workers.get(), 3)
