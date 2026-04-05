import logging
import os

from concurrent.futures import ThreadPoolExecutor, as_completed
from tkinter import messagebox

from utils.helpers import ERROR_CODE_UNKNOWN, format_error_message


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
        if is_archive and item.full_target:
            target = item.full_target
        else:
            target = os.path.join(item.dir, item.new_name_only or item.old_name)

        if not os.path.exists(item.path):
            gui.root.after(
                0, lambda id_val=item.id: gui.tree.set(id_val, "st", "源文件不存在")
            )
            return

        target_dir = os.path.dirname(target)
        if target_dir:
            os.makedirs(target_dir, exist_ok=True)

        current_path = item.path
        same_exact_path = current_path == target
        is_case_change_only = os.path.normcase(current_path) == os.path.normcase(target)

        if not same_exact_path and not is_case_change_only and os.path.exists(target):
            gui.root.after(
                0, lambda id_val=item.id: gui.tree.set(id_val, "st", "目标已存在")
            )
            return

        if not same_exact_path:
            import shutil

            shutil.move(current_path, target)
            item.path = target

        done_text = "归档完成" if is_archive else "重命名完成"
        gui.root.after(
            0, lambda id_val=item.id, txt=done_text: gui.tree.set(id_val, "st", txt)
        )

    except PermissionError as err:
        logging.error(f"权限错误 {item.path}: {err}")
        gui.root.after(
            0, lambda id_val=item.id: gui.tree.set(id_val, "st", "权限错误")
        )
    except OSError as err:
        logging.error(f"系统错误 {item.path}: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"系统错误: {str(err)[:20]}")
        gui.root.after(
            0,
            lambda id_val=item.id, msg=err_msg: gui.tree.set(
                id_val, "st", gui._friendly_status_text(msg)
            ),
        )
    except Exception as err:
        logging.error(f"处理文件失败 {item.path}: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"失败: {str(err)[:20]}")
        gui.root.after(
            0,
            lambda id_val=item.id, msg=err_msg: gui.tree.set(
                id_val, "st", gui._friendly_status_text(msg)
            ),
        )


def run_scrape_execution(gui):
    """Run scrape-only execution with background worker pool."""
    total = len(gui.file_list)
    gui.root.after(
        0,
        lambda max_v=total: [
            gui.status.config(text="刮削中..."),
            gui.pbar.config(maximum=max_v),
            gui.pbar.configure(value=0),
        ],
    )

    try:
        with ThreadPoolExecutor(max_workers=gui._get_execution_workers()) as executor:
            futures = [
                executor.submit(gui.process_one_file_scrape, item)
                for item in gui.file_list
            ]
            for future in as_completed(futures):
                gui.root.after(0, lambda: gui.pbar.step(1))
                try:
                    future.result()
                except Exception as err:
                    logging.error(f"刮削失败: {err}")
    except Exception as err:
        logging.error(f"刮削线程池失败: {err}")
        err_msg = f"刮削失败: {err}"
        gui.root.after(
            0,
            lambda msg=err_msg: messagebox.showerror("错误", msg, parent=gui.root),
        )

    gui.root.after(0, lambda: gui.status.config(text="刮削全部完成"))


def process_one_file_scrape(gui, item):
    """Process single file scrape-only (write NFO and download images)."""
    try:
        target_path = item.path
        if not target_path or not os.path.exists(target_path):
            gui.root.after(
                0, lambda id_val=item.id: gui.tree.set(id_val, "st", "源文件不存在")
            )
            return

        gui._write_sidecar_files(item, target_path)
        gui.root.after(
            0, lambda id_val=item.id: gui.tree.set(id_val, "st", "刮削完成")
        )

    except PermissionError as err:
        logging.error(f"刮削权限错误 {item.path}: {err}")
        gui.root.after(
            0, lambda id_val=item.id: gui.tree.set(id_val, "st", "权限错误")
        )
    except OSError as err:
        logging.error(f"刮削系统错误 {item.path}: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"系统错误: {str(err)[:20]}")
        gui.root.after(
            0,
            lambda id_val=item.id, msg=err_msg: gui.tree.set(
                id_val, "st", gui._friendly_status_text(msg)
            ),
        )
    except Exception as err:
        logging.error(f"刮削文件失败 {item.path}: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"失败: {str(err)[:20]}")
        gui.root.after(
            0,
            lambda id_val=item.id, msg=err_msg: gui.tree.set(
                id_val, "st", gui._friendly_status_text(msg)
            ),
        )

