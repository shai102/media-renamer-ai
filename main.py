import logging
import os
import sys
import tkinter as tk


def _resolve_log_path():
    if getattr(sys, "frozen", False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, "media_renamer.log")


def _setup_logging():
    log_path = _resolve_log_path()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )
    logging.info(f"日志系统已初始化，日志文件: {log_path}")

def main():
    _setup_logging()
    from core.app import MediaRenamerGUI

    root = tk.Tk()
    app = MediaRenamerGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
