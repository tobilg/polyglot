"""Binding benchmarks. Run with the package under test on PYTHONPATH."""
import gc
import json
import statistics
import time

import polyglot_sql as sql


def measure(operation, size, function):
    function()
    samples = []
    for _ in range(5):
        gc.collect()
        start = time.perf_counter_ns()
        count = 0
        while time.perf_counter_ns() - start < 100_000_000:
            function()
            count += 1
        samples.append((time.perf_counter_ns() - start) / count / 1000)
    print(json.dumps(dict(operation=operation, size=size,
                         median_us=statistics.median(samples), samples_us=samples)), flush=True)


for depth in (32, 64, 128, 256):
    query = "SELECT " + " + ".join(["x"] * depth) + " FROM t"
    tree = sql.parse_one(query, read="postgresql")
    measure("python_walk_dfs", depth, tree.walk)
    measure("python_walk_bfs", depth, lambda: tree.walk("bfs"))
    measure("python_find_all_add", depth, lambda: tree.find_all("add"))
    measure("python_parse", depth, lambda: sql.parse_one(query, read="postgresql"))

for depth in (4, 8, 12):
    query = "SELECT * FROM (" * depth + "SELECT 1" + ") t" * depth
    measure("transpile_nested_pg_fabric", depth,
            lambda: sql.transpile(query, read="postgresql", write="fabric"))
