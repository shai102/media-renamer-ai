import logging
import os
import sys


def _resolve_log_path():
    if getattr(sys, "frozen", False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, "media_renamer.log")


def _setup_logging():
    log_path = _resolve_log_path()
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )


def main():
    _setup_logging()
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication(sys.argv)
    from core.app_qt import MediaRenamerGUIQt

    main_window = __import__("PySide6.QtWidgets", fromlist=["QMainWindow"]).QMainWindow()
    gui = MediaRenamerGUIQt(main_window)
    main_window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
