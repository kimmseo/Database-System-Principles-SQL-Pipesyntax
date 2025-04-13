import psycopg2
import json

def get_qep(sql_query, db_config):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute(f"EXPLAIN (FORMAT JSON) {sql_query}")
    result = cur.fetchone()[0]
    lowest_cost_plan = get_best_plan(result)
    cur.close()
    conn.close()
    # Debug
    # Print the full QEP
    # print(json.dumps(result[0], indent=2))
    return lowest_cost_plan[0]  # unwrap from list

# recursively find the lowest total cost plan
def get_best_plan(plans):
    lowest_cost_plan = None
    lowest_cost = float('inf')

    for plan in plans:
        total_cost = plan.get('Total Cost', float('inf'))

        if total_cost < lowest_cost:
            lowest_cost_plan = plan
            lowest_cost = total_cost

        if 'Plans' in plan:
            child_lowest_cost_plan = get_best_plan(plan['Plans'])
            if child_lowest_cost_plan:
                child_cost = child_lowest_cost_plan.get('Total Cost', float('inf'))
                if child_cost < lowest_cost:
                    lowest_cost_plan = child_lowest_cost_plan
                    lowest_cost = child_cost

    return lowest_cost_plan
