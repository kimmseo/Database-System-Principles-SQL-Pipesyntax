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
        if join_type == "RIGHT":
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


# Round calculations to 2 dp
def truncate_cost(a: float) -> float:
    return round(a, 2)

# Truncate to 4 dp
def truncate(a: float) -> float:
    return float(decimal.Decimal(str(a)).quantize(decimal.Decimal('.0001'), rounding=decimal.ROUND_DOWN))


# Cache all variable from PostgreSQL
# Lasts for each session (cleared when disconnected)
cache = None
class Cache():

    def __init__(self, cur: psycopg.Cursor) -> None:
        self.dict = {}
        self.cur = cur

    def query_setting(self, setting: str) -> str:
        self.cur.execute(f"SELECT setting FROM pg_settings WHERE name = '{setting}'")
        return self.cur.fetchall()[0][0]

    # Gets the number of pages for a base relation
    def query_pagecount(self, relation: str) -> int:
        self.cur.execute(f"SELECT relpages FROM pg_class WHERE relname = '{relation}'")
        return self.cur.fetchall()[0][0]

    # Gets the number of tuples for a base relation
    def query_tuplecount(self, relation: str) -> int:
        self.cur.execute(f"SELECT reltuples FROM pg_class where relname = '{relation}'")
        return self.cur.fetchall()[0][0]

    def get_setting(self, setting: str) -> str:
        key = f"setting/{setting}"
        # query only if not present currently and save output
        if key not in self.dict:
            self.log_cb(f"Querying {key}")
            self.dict[key] = self.query_setting(setting)
        return self.dict[key]

    def get_page_count(self, relation: str) -> int:
        key = f"relpages/{relation}"
        # query only if not present currently and save output
        if key not in self.dict:
            self.log_cb(f"Querying {key}")
            self.dict[key] = self.query_pagecount(relation)
        return self.dict[key]

    def get_tuple_count(self, relation: str) -> int:
        key = f"reltuples/{relation}"
        # query only if not present currently and save output
        if key not in self.dict:
            self.log_cb(f"Querying {key}")
            self.dict[key] = self.query_tuplecount(relation)
        return self.dict[key]

    # Check if auto analysis has been done
    def set_tuple_count(self, relation: str, count: int):
        key = f"reltuples/{relation}"
        self.dict[key] = count

    def set_log_cb(self, log_cb: callable):
        self.log_cb = log_cb


# Count the number of clauses in a filter or similar condition
# Assume clause can only be connected by 'OR' or 'AND;
def count_clauses(condition: str) -> int:
    or_count = condition.count(') OR (')
    and_count = condition.count(') AND (')
    total_count = or_count + and_count + 1

    return total_count

# Return @ cost of sequential scan
def cost_seqscan(node: dict) -> float:
    cpu_tuple_cost = float(cache.get_setting("cpu_tuple_cost"))
    seq_page_cost = float(cache.get_setting("seq_page_cost"))
    rel = node["Relation Name"]
    page_count = cache.get_page_count(rel)
    row_count = cache.get_tuple_count(rel)
    workers = 1
    filter_cost = 0

    if "Filter" in node:
        filters = count_clauses(node["Filter"])
        cpu_operator_cost = float(cache.get_setting("cpu_operator_cost"))
        filter_cost = filters * cpu_operator_cost

    cpu_cost = (cpu_tuple_cost + filter_cost) * row_count

    # Parallelization
    if node["Parallel Aware"] and "Workers Planned" in node:
        workers = node["Workers Planned"]
        if cache.get_setting("parallel_leader_participation") == "on" and workers < 4:
            workers += 1 - (workers * 0.3)

    disk_cost = seq_page_cost * page_count
    cost = truncate_cost(cpu_cost / workers + disk_cost)

    # Reverse all the calculations to get the expected filter cost
    expected_cost = node["Total Cost"]
    if cost != expected_cost:
        expected_cost -= disk_cost
        expected_cost /= row_count
        expected_cost *= workers
        expected_cost -= cpu_tuple_cost

    return cost