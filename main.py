
import tkinter as tk
from core.app import MediaRenamerGUI

def main():
    root = tk.Tk()
    app = MediaRenamerGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
