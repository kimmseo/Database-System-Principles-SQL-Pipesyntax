import psycopg
import decimal
import json

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
    node_type = plan.get("Node Type", "")
    cost = plan.get("Total Cost", 0)
    cost_info = f"  -- Cost: {cost}" if show_cost else ""

    if node_type in ["Seq Scan", "Index Scan", "Index Only Scan"]:
        relation = plan.get("Relation Name", "<unknown_table>")
        filter_cond = plan.get("Filter")
        if filter_cond:
            return f"FROM {relation} WHERE {filter_cond}{cost_info}"
        return f"FROM {relation}{cost_info}"

    elif node_type in ["Hash Join", "Merge Join", "Nested Loop"]:
        join_type = plan.get("Join Type", "INNER").upper()
        if join_type == "LEFT":
            join_type = "LEFT OUTER"
        cond = plan.get("Hash Cond") or plan.get("Merge Cond") or plan.get("Join Filter") or "<condition missing>"
        return f"|> {join_type} JOIN ON {cond}{cost_info}"

    elif node_type == "Aggregate":
        group_keys = plan.get("Group Key", [])
        if group_keys:
            keys = ", ".join(group_keys)
            return f"|> AGGREGATE GROUP BY {keys}{cost_info}"
        else:
            return f"|> AGGREGATE (no group keys){cost_info}"

    elif node_type == "Sort":
        sort_keys = plan.get("Sort Key", [])
        if sort_keys:
            return f"|> ORDER BY {', '.join(sort_keys)}{cost_info}"
        else:
            return f"|> ORDER BY <unknown>{cost_info}"

    elif node_type == "Hash":
        return f"|> HASH{cost_info}"

    elif node_type == "Gather Merge":
        return f"|> GATHER MERGE{cost_info}"

    elif node_type == "Limit":
        return f"|> LIMIT{cost_info}"

    # Add more node types as needed...

    return f"|> {node_type.upper()}{cost_info}"
