import webbrowser
from tkinter import ttk, Tk, scrolledtext, StringVar, messagebox
from preprocessing import get_qep, get_qep_mysql
from pipesyntax import generate_pipe_syntax
from preprocessing import parse_query_explanation_to_tree, Visualizer
from sv_ttk import set_theme

def build_login_frame(root, login_frame, app_frame):
    login_frame.grid(row=0, column=0, sticky="nsew")
    login_frame.columnconfigure(0, weight=1)
    login_frame.rowconfigure(5, weight=1)

    inner = ttk.Frame(login_frame)
    inner.grid(row=0, column=0, pady=30)

    ttk.Label(inner, text="Enter Database Credentials", font=("Segoe UI", 14)).pack(pady=10)

    host = StringVar(value="localhost")
    dbms = StringVar(value="postgresql")
    port = StringVar(value="5432")
    user = StringVar(value="postgres")
    password = StringVar()
    dbname = StringVar(value="TPC-H")

    for label, var in [
        ("Host:", host),
        ("DBMS:", dbms),
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

        root.database_type = dbms.get()

        root.db_config = {
            "host": host.get(), "port": port.get(), "user": user.get(),
            "password": password.get(), "dbname": dbname.get()
        }

        if root.database_type.lower() == "postgres" or root.database_type.lower() == "postgresql":
            try:
                import psycopg2
                conn = psycopg2.connect(**root.db_config)
                conn.close()
            except Exception as e:
                messagebox.showerror("Connection Failed", f"Could not connect to the database:\n{e}")
                return
        elif root.database_type.lower() == "mysql":
            try:
                import mysql.connector
                conn = mysql.connector.connect(**root.db_config)
                conn.close()
            except Exception as e:
                messagebox.showerror("Connection Failed", f"Could not connect to the database:\n{e}")
                return
        else:
            messagebox.showerror("Connection Failed", "Unsupported database\n")
            return


        app_frame.tkraise()

    ttk.Button(inner, text="Connect", command=handle_login).pack(pady=10)

def build_app_frame(root, app_frame):
    app_frame.grid(row=0, column=0, sticky="nsew")
    app_frame.columnconfigure(0, weight=1)
    app_frame.rowconfigure(0, weight=1)

    frame = ttk.Frame(app_frame)
    frame.grid(row=0, column=0, sticky="nsew", padx=20, pady=20)
    frame.columnconfigure(0, weight=1)
    frame.rowconfigure(1, weight=1)

    ttk.Label(frame, text="Enter SQL Query", font=("Segoe UI", 12)).grid(row=0, column=0, sticky="w")

    query_input = scrolledtext.ScrolledText(frame, height=12, wrap="word")
    query_input.grid(row=1, column=0, sticky="nsew", pady=5)

    ttk.Button(frame, text="Generate Pipe-Syntax", command=lambda: run_query()).grid(row=3, column=0, pady=10)

    def run_query():
        sql = query_input.get("1.0", "end").strip()
        if not sql:
            messagebox.showwarning("Empty Query", "Please enter an SQL query.")
            return
        try:
            if root.database_type.lower() == "postgres" or root.database_type.lower() == "postgresql":
                qep = get_qep(sql, db_config=root.db_config, as_json=False)
            exec_tree = parse_query_explanation_to_tree(qep)
            exec_tree.finalize_id()
            if root.database_type.lower() == "mysql":
                qep = get_qep_mysql(sql, db_config=root.db_config)
            root.last_qep = qep
            root.exec_tree = exec_tree
            exec_tree.qep = qep
            update_plotly_browser(exec_tree, sql, root.db_config)
        except Exception as e:
            root.last_qep = None
            root.exec_tree = None
            messagebox.showerror("Query Failed", str(e))

def update_plotly_browser(exec_tree, sql, db_config):
    import html

    # 1. Visualize QEP Tree
    viz = Visualizer()
    fig = viz.visualize(exec_tree)
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False)
    )
    fig_html = fig.to_html(full_html=False, include_plotlyjs="cdn")

    # 2. Generate both pipe-syntax versions
    qep_temp = get_qep(sql, db_config=db_config)
    pipe_sql_with_cost = generate_pipe_syntax(qep_temp, show_cost=True)
    pipe_sql_without_cost = generate_pipe_syntax(qep_temp, show_cost=False)

    # Escape HTML safely
    pipe_with_cost_html = html.escape(pipe_sql_with_cost)
    pipe_without_cost_html = html.escape(pipe_sql_without_cost)

    # 1. Get cost breakdown
    cost_info = exec_tree.get_cost()
    total_cost = cost_info["total_cost"]
    startup_cost = cost_info["startup_cost"]
    cost_steps = cost_info["steps"]

    # 2. Generate cost breakdown HTML
    step_cost_html = "<ul style='padding-left: 20px;'>"
    for step in cost_steps:
        cond_str = f" — Conditions: {', '.join(step['condition'])}" if step["condition"] else ""
        step_cost_html += f"<li><b>{step['operation']}</b> → Step Cost: {step['net_cost']}{cond_str}</li>"
    step_cost_html += "</ul>"

    # 3. HTML Template with JS toggle
    combined_html = f"""
    <html>
    <head>
        <meta charset='utf-8'>
        <title>QEP Execution Tree + Pipe SQL</title>
        <script>
            function toggleCost() {{
                var showCost = document.getElementById('costToggle').checked;
                document.getElementById('with_cost').style.display = showCost ? 'block' : 'none';
                document.getElementById('without_cost').style.display = showCost ? 'none' : 'block';
            }}
        </script>
    </head>
    <body style='font-family:Segoe UI, sans-serif; background:white; color:black; padding:20px;'>
        <h2>QEP Execution Tree</h2>
        {fig_html}

        <h2 style="margin-top:40px;">Pipe-Syntax SQL</h2>
        <label><input type="checkbox" id="costToggle" onchange="toggleCost()" checked> Show Cost</label>

        <div id="with_cost" style="margin-top:10px;">
            <pre style='background:#f9f9f9;color:#222;padding:15px;border-radius:8px;'>{pipe_with_cost_html}</pre>
        </div>
        <div id="without_cost" style="display:none; margin-top:10px;">
            <pre style='background:#f9f9f9;color:#222;padding:15px;border-radius:8px;'>{pipe_without_cost_html}</pre>
        </div>
        <h2 style="margin-top:40px;">Step-wise Cost Breakdown</h2>
        <p><b>Total Startup Cost:</b> {round(startup_cost, 2)}<br>
        <b>Total Cost:</b> {round(total_cost, 2)}</p>
        {step_cost_html}
    </body>
    </html>
    """

    # 4. Write to file & open in browser
    with open("result.html", "w", encoding="utf-8") as f:
        f.write(combined_html)
    webbrowser.open("result.html")


def launch_gui():
    root = Tk()
    root.title("Pipe-syntax SQL from QEP")
    root.geometry("800x650")
    root.resizable(True, True)

    set_theme("dark")

    root.last_qep = None
    root.exec_tree = None

    login_frame = ttk.Frame(root)
    app_frame = ttk.Frame(root)

    build_login_frame(root, login_frame, app_frame)
    build_app_frame(root, app_frame)

    login_frame.tkraise()
    root.mainloop()

