import psycopg2
import json

def get_qep(sql_query, db_config):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute(f"EXPLAIN (FORMAT JSON) {sql_query}")
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    # Debug
    # Print the full QEP
    # print(json.dumps(result[0], indent=2))
    return result[0]  # unwrap from list
