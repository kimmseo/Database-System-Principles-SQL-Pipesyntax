import interface
import sv_ttk
from tkinter import ttk,Tk

# Main tkinter window
root = Tk()
root.resizable(width = False, height = False)
root.title("Pipe-syntax SQL from QEP")

# root size is (1,1)
root.grid_rowconfigure(0, weight=1)
root.grid_columnconfigure(0, weight=1)

# Define login and main app frames
login_frame = ttk.Frame(root)
app_frame = ttk.Frame(root)

# Assign both frames to the root partition and take up all available space
login_frame.grid(row=0, column=0, sticky="nsew")
app_frame.grid(row=0, column=1, sticky="nsew")

# Create both frames

# Show login_frame

# tkinter theme
sv_ttk.set_theme("dark")

# Start the UI thread
root.mainloop()