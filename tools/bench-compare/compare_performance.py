"""Compare JSONL produced by the performance_review benchmarks."""
import json
import sys


def read(path):
    with open(path) as file:
        rows = [json.loads(line) for line in file if line.strip()]
    return {(row['operation'], row['size']): row for row in rows}


before, after = map(read, sys.argv[1:3])
for key, old in before.items():
    new = after.get(key)
    if new:
        print(json.dumps(dict(operation=key[0], size=key[1],
                              before_us=old['median_us'], after_us=new['median_us'],
                              speedup=old['median_us'] / new['median_us'])))
