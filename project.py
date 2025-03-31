from interface import launch_gui

if __name__ == "__main__":
    launch_gui()


# Simple test query
# Select * FROM nation WHERE n_nationkey = 1


# Sample test query (assessment)
'''
SELECT c_count, count(*) AS custdist
FROM
 (SELECT c_custkey, count(o_orderkey)
 FROM customer LEFT OUTER JOIN orders
 ON c_custkey = o_custkey
 AND o_comment not like '%unusual%packages%'
 GROUP BY c_custkey
 ) as c_orders (c_custkey, c_count)
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC;
'''