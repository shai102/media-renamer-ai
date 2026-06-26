import json
import logging
import os
import re

from utils.helpers import CONFIG_FILE, safe_int


class ConfigMixin:
    def _is_geometry_in_screen(self, x, y, w, h):
        """检查窗口坐标是否仍在当前屏幕可见区域内"""
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        return (x < sw - 80) and (y < sh - 80) and (x + w > 80) and (y + h > 80)

    def _parse_geometry(self, geometry_text):
        """Parse Tk geometry string into width/height/x/y integers."""
        match = re.match(
            r"^(\d+)x(\d+)\+(-?\d+)\+(-?\d+)$", str(geometry_text or "").strip()
        )
        if not match:
            return None
        return tuple(map(int, match.groups()))

    def get_saved_geometry(self, key, min_width=600, min_height=400):
        """Return a validated saved geometry string for a config key."""
        parsed = self._parse_geometry(self.config.get(key, ""))
        if not parsed:
            return ""

        w, h, x, y = parsed
        if w < min_width or h < min_height:
            return ""
        if not self._is_geometry_in_screen(x, y, w, h):
            return ""
        return f"{w}x{h}+{x}+{y}"

    def remember_window_geometry(
        self, key, geometry_text, min_width=600, min_height=400
    ):
        """Stage a validated child-window geometry into the in-memory config."""
        parsed = self._parse_geometry(geometry_text)
        if not parsed:
            return

        w, h, x, y = parsed
        if w < min_width or h < min_height:
            return
        if not self._is_geometry_in_screen(x, y, w, h):
            return
        self.config[key] = f"{w}x{h}+{x}+{y}"

    def apply_saved_window_geometry(self):
        """启动时恢复上次窗口位置和大小"""
        geo = self.get_saved_geometry("window_geometry", min_width=600, min_height=400)
        if geo:
            self.root.geometry(geo)

    def load_config(self):
        """加载配置"""
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as err:
                logging.error(f"加载配置失败: {err}")
        return {}

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
        return self._clamp_workers(self.preview_workers.get(), 1)

    def _get_sync_workers(self):
        return self._clamp_workers(self.sync_workers.get(), 5)

    def _get_execution_workers(self):
        return self._clamp_workers(self.execution_workers.get(), 5)
