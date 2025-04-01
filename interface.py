import sv_ttk
from tkinter import ttk, Tk, scrolledtext, StringVar

from preprocessing import get_qep
from pipesyntax import generate_pipe_syntax


def launch_gui():
    root = Tk()
    root.resizable(width=False, height=False)
    root.title("Pipe-syntax SQL from QEP")
    root.geometry("800x600")
    sv_ttk.set_theme("dark")

    login_frame = ttk.Frame(root)
    app_frame = ttk.Frame(root)

    login_frame.grid(row=0, column=0, sticky="nsew")
    app_frame.grid(row=0, column=0, sticky="nsew")

    # ---- Login Frame ----
    ttk.Label(login_frame, text="Enter Database Credentials", font=("Segoe UI", 14)).pack(pady=10)

    host = StringVar(value="localhost")
    port = StringVar(value="5432")
    user = StringVar(value="postgres")
    password = StringVar()
    dbname = StringVar(value="TPC-H")

    for label, var in [("Host:", host), ("Port:", port), ("Username:", user), ("Password:", password), ("Database:", dbname)]:
        frame = ttk.Frame(login_frame)
        frame.pack(pady=5)
        ttk.Label(frame, text=label, width=10).pack(side="left")
        entry = ttk.Entry(frame, textvariable=var, show="*" if label == "Password:" else "")
        entry.pack(side="left")

    def handle_login():
        root.db_config = {
            "host": host.get(),
            "port": port.get(),
            "user": user.get(),
            "password": password.get(),
            "dbname": dbname.get()
        }
        app_frame.tkraise()

    ttk.Button(login_frame, text="Connect", command=handle_login).pack(pady=10)

    # ---- App Frame ----
    ttk.Label(app_frame, text="Enter SQL Query", font=("Segoe UI", 12)).pack(pady=10)
    query_input = scrolledtext.ScrolledText(app_frame, height=10)
    query_input.pack(fill="x", padx=10)

    output = scrolledtext.ScrolledText(app_frame, height=15)
    output.pack(fill="x", padx=10, pady=10)

    def run_query():
        sql = query_input.get("1.0", "end").strip()
        qep = get_qep(sql, db_config=root.db_config)
        pipe_sql = generate_pipe_syntax(qep)

        output.delete("1.0", "end")
        output.insert("1.0", pipe_sql)

    ttk.Button(app_frame, text="Generate Pipe-Syntax", command=run_query).pack()

    login_frame.tkraise()
    root.mainloop()

