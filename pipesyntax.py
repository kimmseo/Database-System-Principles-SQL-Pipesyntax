def generate_pipe_syntax(qep, show_cost=True):
    steps = []
    first_from = None

    def traverse(plan, depth=0):
        nonlocal first_from
        node_type = plan.get("Node Type", "")
        line = describe_node(plan, show_cost=show_cost)
        # Debug output
        print(f"{'  ' * depth}Processing node: {node_type}")
        print(f"{'  ' * depth}→ Generated: {line}")
        if line.startswith("FROM") and not first_from:
            first_from = line
        else:
            steps.append(line)
        for sub in plan.get("Plans", []):
            traverse(sub,depth+1)

    traverse(qep["Plan"])
    steps.reverse()
    return "\n".join([first_from] + steps if first_from else steps)

def describe_node(plan, show_cost=True):
    node_type = plan.get("Node Type", "UNKNOWN").upper()
    cost = plan.get("Total Cost", 0)
    cost_info = f"  -- Cost: {cost}" if show_cost else ""

    # Scans
    if node_type in {"SEQ SCAN", "INDEX SCAN", "INDEX ONLY SCAN"}:
        rel = plan.get("Relation Name", "<unknown_table>")
        filt = plan.get("Filter")
        return f"FROM {rel}" + (f" WHERE {filt}" if filt else "") + cost_info

    # Joins
    elif "JOIN" in node_type or node_type == "NESTED LOOP":
        join_type = plan.get("Join Type", "INNER").upper()
        if join_type == "LEFT":
            join_type = "LEFT OUTER"
        elif join_type == "RIGHT":
            join_type = "RIGHT OUTER"
        cond = plan.get("Hash Cond") or plan.get("Merge Cond") or plan.get("Join Filter") or "<missing join condition>"
        return f"|> {join_type} JOIN ON {cond}{cost_info}"

    # Aggregates
    elif node_type == "AGGREGATE":
        keys = plan.get("Group Key", [])
        key_str = ", ".join(keys) if keys else "<no group keys>"
        strategy = plan.get("Strategy", "")
        return f"|> AGGREGATE ({strategy}) GROUP BY {key_str}{cost_info}"

    # Sorting
    elif node_type == "SORT":
        sort_keys = plan.get("Sort Key", [])
        key_str = ", ".join(sort_keys) if sort_keys else "<unknown>"
        return f"|> ORDER BY {key_str}{cost_info}"

    # Limiting
    elif node_type == "LIMIT":
        return f"|> LIMIT{cost_info}"

    # Materialize, Hash, Unique, Gather
    elif node_type in {"HASH", "MATERIALIZE", "UNIQUE", "CTE SCAN"}:
        return f"|> {node_type}{cost_info}"

    elif node_type == "GATHER MERGE":
        return f"|> GATHER MERGE{cost_info}"

    # Default fallback
    return f"|> {node_type}{cost_info}"

