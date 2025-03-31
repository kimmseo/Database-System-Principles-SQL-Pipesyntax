import sv_ttk
from tkinter import ttk, Tk, scrolledtext
from preprocessing import get_qep
from pipesyntax import generate_pipe_syntax

def launch_gui():
    # Main tkinter window
    root = Tk()
    root.resizable(width=False, height=False)
    root.title("Pipe-syntax SQL from QEP")
    root.geometry("800x600")

    # Set the theme to dark mode
    sv_ttk.set_theme("dark")

    login_frame = ttk.Frame(root)
    app_frame = ttk.Frame(root)
    # Assign both frames to the root partition and take up all available space
    login_frame.grid(row=0, column=0, sticky="nsew")
    app_frame.grid(row=0, column=0, sticky="nsew")

    # Login Frame
    ttk.Label(login_frame, text="Enter DB Credentials").pack()
    login_button = ttk.Button(login_frame, text="Login", command=lambda: app_frame.tkraise())
    login_button.pack()

    # App Frame
    ttk.Label(app_frame, text="Enter SQL Query").pack()
    query_input = scrolledtext.ScrolledText(app_frame, height=10)
    query_input.pack()

    output = scrolledtext.ScrolledText(app_frame, height=15)
    output.pack()

    def run_query():
        sql = query_input.get("1.0", "end").strip()
        qep = get_qep(sql)
        pipe_sql = generate_pipe_syntax(qep)
        output.delete("1.0", "end")
        output.insert("1.0", pipe_sql)

    run_button = ttk.Button(app_frame, text="Generate Pipe-Syntax", command=run_query)
    run_button.pack()

    login_frame.tkraise()
    # Start the UI thread
    root.mainloop()
