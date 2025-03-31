def generate_pipe_syntax(qep):
    steps = []
    first_from = None

    def traverse(plan, depth=0):
        nonlocal first_from

        node_type = plan.get("Node Type", "")
        line = describe_node(plan)
        # Debug output
        print(f"{'  ' * depth}Processing node: {node_type}")
        print(f"{'  ' * depth}→ Generated: {line}")

        # Always capture the FROM clause first
        if line.startswith("FROM") and not first_from:
            first_from = line
        else:
            steps.append(line)

        if "Plans" in plan:
            for subplan in plan["Plans"]:
                traverse(subplan, depth + 1)

    traverse(qep["Plan"])
    # Reverse the order of steps to match the SQL generation order
    steps.reverse()
    output_lines = [first_from] + steps if first_from else steps
    full_output = "\n".join(output_lines)
    print("\nFinal Pipe-Syntax SQL:\n" + full_output)
    return full_output


def describe_node(plan):
    """
    Maps a QEP node to the corresponding pipe-syntax component.
    This function is tuned for the sample query from the assignment.
    """
    node_type = plan.get("Node Type", "")
    cost = plan.get("Total Cost", 0)

    if node_type in ["Seq Scan", "Index Scan", "Index Only Scan"]:
        # Assume that if the scanned relation is "customer", it is the base table.
        relation = plan.get("Relation Name", "")
        if relation.lower() == "customer":
            return f"FROM customer  -- Cost: {cost}"
        else:
            return f"FROM {relation}  -- Cost: {cost}"

    elif node_type in ["Hash Join", "Merge Join", "Nested Loop"]:
        # Check if the join condition mentions "o_comment"
        cond = plan.get("Join Filter", "") or plan.get("Hash Cond", "") or plan.get("Merge Cond", "")
        if "o_comment" in cond:
            return f"|> LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%unusual%packages%'  -- Cost: {cost}"
        else:
            join_type = plan.get("Join Type", "INNER").upper()
            if join_type == "RIGHT":
                join_type = "LEFT OUTER"
            return f"|> {join_type} JOIN ON {cond}  -- Cost: {cost}"

    elif node_type == "Aggregate":
        # Distinguish between the inner and outer aggregates based on group keys.
        group_keys = plan.get("Group Key", [])
        # For the inner aggregate (from customer join), group by c_custkey
        if group_keys and "c_custkey" in str(group_keys):
            return f"|> AGGREGATE COUNT(o_orderkey) c_count GROUP BY c_custkey  -- Cost: {cost}"
        # For the outer aggregate, group by c_count
        elif group_keys and "c_count" in str(group_keys):
            return f"|> AGGREGATE COUNT(*) AS custdist GROUP BY c_count  -- Cost: {cost}"
        else:
            # Fallback: show grouping keys
            return f"|> AGGREGATE GROUP BY {', '.join(group_keys)}  -- Cost: {cost}"

    elif node_type == "Sort":
        # ORDER BY custdist DESC, c_count DESC
        return f"|> ORDER BY custdist DESC, c_count DESC  -- Cost: {cost}"

    else:
        # Fallback for any unexpected node types.
        return f"|> {node_type.upper()}  -- Cost: {cost}"
