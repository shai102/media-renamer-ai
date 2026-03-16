import tkinter as tk

def main():
    from core.app import MediaRenamerGUI

    root = tk.Tk()
    app = MediaRenamerGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
