import logging
import os

from concurrent.futures import ThreadPoolExecutor, as_completed
from tkinter import messagebox

from utils.helpers import ERROR_CODE_UNKNOWN, format_error_message


def _prune_empty_dirs(start_dir, stop_before):
    """Remove empty directories upwards until reaching stop_before."""
    current = os.path.normpath(str(start_dir or "").strip())
    stop_path = os.path.normpath(str(stop_before or "").strip()) if stop_before else ""
    if not current:
        return

    while current:
        if stop_path and os.path.normcase(current) == os.path.normcase(stop_path):
            break
        if not os.path.isdir(current):
            break
        try:
            os.rmdir(current)
        except OSError:
            break
        parent = os.path.dirname(current)
        if not parent or parent == current:
            break
        current = parent


def run_execution(gui, run_mode):
    """Run rename/archive/organize execution with background worker pool."""
    active_ids = set(gui.action_scope_item_ids or [item.id for item in gui.file_list])
    target_items = [item for item in gui.file_list if item.id in active_ids]
    total = len(target_items)

    status_map = {
        "rename": "原地重命名中...",
        "archive": "归档移动中...",
        "organize": "原地整理中...",
    }
    gui.root.after(
        0,
        lambda max_v=total: [
            gui.status.config(text=status_map.get(run_mode, "执行中...")),
            gui.pbar.config(maximum=max_v),
            gui.pbar.configure(value=0),
        ],
    )

    try:
        with ThreadPoolExecutor(max_workers=gui._get_execution_workers()) as executor:
            futures = [
                executor.submit(gui.process_one_file, item, run_mode)
                for item in target_items
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


def process_one_file(gui, item, run_mode):
    """Process single file rename/archive/organize and sidecar writing."""
    try:
        if not item.new_name_only:
            gui.root.after(0, lambda: gui.update_item_display(item, status="已跳过(未预览)"))
            return

        target = gui._build_target_for_mode(item, run_mode)

        if not os.path.exists(item.path):
            gui.root.after(0, lambda: gui.update_item_display(item, status="源文件不存在"))
            return

        target_dir = os.path.dirname(target)
        if target_dir:
            os.makedirs(target_dir, exist_ok=True)

        current_path = item.path
        old_dir = os.path.dirname(current_path)
        same_exact_path = current_path == target
        is_case_change_only = os.path.normcase(current_path) == os.path.normcase(target)

        if not same_exact_path and not is_case_change_only and os.path.exists(target):
            gui.root.after(0, lambda: gui.update_item_display(item, status="目标已存在"))
            return

        if not same_exact_path:
            import shutil

            shutil.move(current_path, target)
            item.path = target
            item.dir = os.path.dirname(target)
            if run_mode == "organize":
                prune_stop = item.organize_root or item.source_path or old_dir
                _prune_empty_dirs(old_dir, prune_stop)

        final_status = {
            "rename": "重命名完成",
            "archive": "归档完成",
            "organize": "原地整理完成",
        }.get(run_mode, "处理完成")

        if run_mode == "organize":
            try:
                gui._write_sidecar_files(item, target)
                final_status = "原地整理+刮削完成"
            except PermissionError as err:
                logging.error(f"原地整理后刮削权限错误 {target}: {err}")
                final_status = "原地整理完成(刮削权限错误)"
            except OSError as err:
                logging.error(f"原地整理后刮削系统错误 {target}: {err}")
                final_status = "原地整理完成(刮削失败)"
            except Exception as err:
                logging.error(f"原地整理后刮削失败 {target}: {err}")
                final_status = "原地整理完成(刮削失败)"

        gui.root.after(
            0,
            lambda: gui.update_item_display(
                item,
                target=target,
                status=final_status,
            ),
        )

    except PermissionError as err:
        logging.error(f"权限错误 {item.path}: {err}")
        gui.root.after(0, lambda: gui.update_item_display(item, status="权限错误"))
    except OSError as err:
        logging.error(f"系统错误 {item.path}: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"系统错误: {str(err)[:20]}")
        gui.root.after(
            0,
            lambda msg=err_msg: gui.update_item_display(
                item, status=gui._friendly_status_text(msg)
            ),
        )
    except Exception as err:
        logging.error(f"处理文件失败 {item.path}: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"失败: {str(err)[:20]}")
        gui.root.after(
            0,
            lambda msg=err_msg: gui.update_item_display(
                item, status=gui._friendly_status_text(msg)
            ),
        )


def run_scrape_execution(gui):
    """Run scrape-only execution with background worker pool."""
    active_ids = set(gui.action_scope_item_ids or [item.id for item in gui.file_list])
    target_items = [item for item in gui.file_list if item.id in active_ids]
    total = len(target_items)
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
                for item in target_items
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
        if item.metadata.get("id") == "None":
            gui.root.after(0, lambda: gui.update_item_display(item, status="已跳过(未预览)"))
            return

        target_path = item.path
        if not target_path or not os.path.exists(target_path):
            gui.root.after(0, lambda: gui.update_item_display(item, status="源文件不存在"))
            return

        gui._write_sidecar_files(item, target_path)
        gui.root.after(0, lambda: gui.update_item_display(item, status="刮削完成"))

    except PermissionError as err:
        logging.error(f"刮削权限错误 {item.path}: {err}")
        gui.root.after(0, lambda: gui.update_item_display(item, status="权限错误"))
    except OSError as err:
        logging.error(f"刮削系统错误 {item.path}: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"系统错误: {str(err)[:20]}")
        gui.root.after(
            0,
            lambda msg=err_msg: gui.update_item_display(
                item, status=gui._friendly_status_text(msg)
            ),
        )
    except Exception as err:
        logging.error(f"刮削文件失败 {item.path}: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"失败: {str(err)[:20]}")
        gui.root.after(
            0,
            lambda msg=err_msg: gui.update_item_display(
                item, status=gui._friendly_status_text(msg)
            ),
        )
