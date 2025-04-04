import sv_ttk
from tkinter import ttk, Tk, scrolledtext, StringVar, BooleanVar, messagebox
from preprocessing import get_qep
from pipesyntax import generate_pipe_syntax 


def build_login_frame(root, login_frame, app_frame):
    login_frame.grid(row=0, column=0, sticky="nsew")

    # Configure login_frame to center its child at the top center
    login_frame.columnconfigure(0, weight=1)  # center horizontally
    for i in range(5):  # extra padding below
        login_frame.rowconfigure(i, weight=0)
    login_frame.rowconfigure(5, weight=1)  

    inner = ttk.Frame(login_frame)
    inner.grid(row=0, column=0, pady=30)  # adjust `pady` to push it down from top

    ttk.Label(inner, text="Enter Database Credentials", font=("Segoe UI", 14)).pack(pady=10)

    host = StringVar(value="localhost")
    port = StringVar(value="5432")
    user = StringVar(value="postgres")
    password = StringVar()
    dbname = StringVar(value="TPC-H")

    for label, var in [
        ("Host:", host),
        ("Port:", port),
        ("Username:", user),
        ("Password:", password),
        ("Database:", dbname)
    ]:
        frame = ttk.Frame(inner)
        frame.pack(pady=5)
        ttk.Label(frame, text=label, width=10).pack(side="left")
        entry = ttk.Entry(frame, textvariable=var, show="*" if label == "Password:" else "")
        entry.pack(side="left")

    def handle_login():
        if not all([host.get(), port.get(), user.get(), password.get(), dbname.get()]):
            messagebox.showerror("Login Error", "All fields must be filled in.")
            return

        root.db_config = {
            "host": host.get(),
            "port": port.get(),
            "user": user.get(),
            "password": password.get(),
            "dbname": dbname.get()
        }

        try:
            import psycopg2
            conn = psycopg2.connect(**root.db_config)
            conn.close()
        except Exception as e:
            messagebox.showerror("Connection Failed", f"Could not connect to the database:\n{e}")
            return

        app_frame.tkraise()

    ttk.Button(inner, text="Connect", command=handle_login).pack(pady=10)


def build_app_frame(root, app_frame):
    # Step 1: Layout and column weights
    app_frame.grid(row=0, column=0, sticky="nsew")
    app_frame.columnconfigure(0, weight=1)  # Left
    app_frame.columnconfigure(1, weight=1)  # Right
    app_frame.rowconfigure(0, weight=1)

    # --- Left Side ---
    left_frame = ttk.Frame(app_frame)
    left_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 10), pady=10)
    left_frame.columnconfigure(0, weight=1) 
    left_frame.rowconfigure(1, weight=1)
    left_frame.rowconfigure(2, weight=1)

    ttk.Label(left_frame, text="Enter SQL Query", font=("Segoe UI", 12)).grid(row=0, column=0, sticky="w")

    query_input = scrolledtext.ScrolledText(left_frame, height=10, wrap="word")
    query_input.grid(row=1, column=0, sticky="nsew", pady=5)

    output = scrolledtext.ScrolledText(left_frame, height=10, wrap="word")
    output.grid(row=2, column=0, sticky="nsew", pady=(0, 5))

    options_frame = ttk.Frame(left_frame)
    options_frame.grid(row=3, column=0, pady=5)
    show_cost_var = BooleanVar(value=True)
    ttk.Checkbutton(options_frame, text="Show Cost", variable=show_cost_var,
                    command=lambda: update_output_from_toggle()).pack(side="left", padx=5)

    ttk.Button(left_frame, text="Generate Pipe-Syntax", command=lambda: run_query()).grid(row=4, column=0)

    # --- Right Side (QEP Tree View) ---
    right_frame = ttk.Frame(app_frame)
    right_frame.grid(row=0, column=1, sticky="nsew", padx=(0, 10), pady=10)
    right_frame.columnconfigure(0, weight=1)
    right_frame.rowconfigure(1, weight=1)        # for tree
    right_frame.rowconfigure(2, weight=0)        # for scroll X

    ttk.Label(right_frame, text="QEP Tree", font=("Segoe UI", 12)).grid(row=0, column=0, sticky="w")

    fig_label = ttk.Label(right_frame)
    fig_label.grid(row=3, column=0, columnspan=2, sticky="nsew", pady=(10, 0))

    tree = ttk.Treeview(right_frame, show="tree", selectmode="browse")
    tree.grid(row=1, column=0, sticky="nsew")

    # Scrollbars
    tree_scroll_y = ttk.Scrollbar(right_frame, orient="vertical", command=tree.yview)
    tree_scroll_y.grid(row=1, column=1, sticky="ns")
    tree_scroll_x = ttk.Scrollbar(right_frame, orient="horizontal", command=tree.xview)
    tree_scroll_x.grid(row=2, column=0, sticky="ew")

    tree.configure(yscrollcommand=tree_scroll_y.set, xscrollcommand=tree_scroll_x.set)

    # Function to update output
    def update_output_from_toggle():
        if root.last_qep:
            pipe_sql = generate_pipe_syntax(root.last_qep, show_cost=show_cost_var.get())
            output.delete("1.0", "end")
            output.insert("1.0", pipe_sql)

    def run_query():
        sql = query_input.get("1.0", "end").strip()
        if not sql:
            messagebox.showwarning("No Query", "Please enter an SQL query.")
            return
        try:
            qep = get_qep(sql, db_config=root.db_config)
            root.last_qep = qep
            pipe_sql = generate_pipe_syntax(qep, show_cost=show_cost_var.get())
            output.delete("1.0", "end")
            output.insert("1.0", pipe_sql)
            # update_tree_view(qep)
        except Exception as e:
            root.last_qep = None
            output.delete("1.0", "end")
            messagebox.showerror("Query Failed", f"An error occurred:\n{e}")

    # Function to update tree
    def update_tree_view(qep):
        tree.delete(*tree.get_children())
        for i, step in enumerate(qep):
            tree.insert("", "end", iid=f"node{i}", text=step[0])

        tree.delete(*tree.get_children())
        for i, step in enumerate(qep):
            tree.insert("", "end", iid=f"node{i}", text=step[0])

def launch_gui():
    root = Tk()
    root.title("Pipe-syntax SQL from QEP")
    root.geometry("800x600")
    root.resizable(True, True)
    sv_ttk.set_theme("dark")

    root.last_qep = None

    # Make root expandable to help center content
    root.grid_rowconfigure(0, weight=1)
    root.grid_columnconfigure(0, weight=1)

    login_frame = ttk.Frame(root)
    app_frame = ttk.Frame(root)

    build_login_frame(root, login_frame, app_frame)
    build_app_frame(root, app_frame)

    login_frame.tkraise()
    root.mainloop()
