import tkinter as tk
from tkinter import messagebox, ttk

from utils.helpers import center_window, safe_int


class SeasonOffsetDialog(tk.Toplevel):
    """季偏移对话框"""

    def __init__(self, parent, title_name):
        super().__init__(parent)
        self.title("高级季集映射")
        center_window(self, parent, 450, 260)
        self.result = None

        ttk.Label(
            self, text=f"已选定匹配: 【{title_name}】", font=("", 10, "bold")
        ).pack(pady=10)

        f1 = ttk.Frame(self)
        f1.pack(pady=5)
        ttk.Label(f1, text="强制指定为第几季:").pack(side=tk.LEFT)
        self.s_var = tk.StringVar(value="1")
        ttk.Entry(f1, textvariable=self.s_var, width=10).pack(side=tk.LEFT, padx=5)

        f2 = ttk.Frame(self)
        f2.pack(pady=5)
        ttk.Label(f2, text="集数增减偏移 (可选):").pack(side=tk.LEFT)
        self.o_var = tk.StringVar(value="0")
        ttk.Entry(f2, textvariable=self.o_var, width=10).pack(side=tk.LEFT, padx=5)

        ttk.Label(
            self,
            text="*提示：\n1. 普通动漫直接点确定即可 (季数填1, 偏移填0)。\n2. 若选中[13]集，但在TMDB里算作第4季第1集，\n   请填 季数: 4，偏移量: -12。",
            foreground="gray",
        ).pack(pady=10)

        ttk.Button(self, text="确定应用", command=self.on_ok).pack()

        self.transient(parent)
        self.grab_set()
        self.wait_window(self)

    def on_ok(self):
        try:
            self.result = (safe_int(self.s_var.get(), 1), safe_int(self.o_var.get(), 0))
            self.destroy()
        except ValueError:
            messagebox.showerror("错误", "请输入有效的整数！")
