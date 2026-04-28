import csv
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/bench-hybrid-cal.csv"
rows = list(csv.DictReader(open(path)))
print(f"total rows: {len(rows)}")
g = defaultdict(list)
for r in rows:
    k = (r["version"], r["timeout_ms"])
    g[k].append((float(r["observed_ms"]), int(r["timed_out"]), r.get("error", "")))
print(f"{'ver':4s} {'to':>5s} {'n':>3s} {'tmo':>5s} {'mean':>8s} {'p50':>8s} {'max':>8s} {'over':>8s}  errs")
for k in sorted(g):
    xs = sorted(o for o, _, _ in g[k])
    tmo = sum(t for _, t, _ in g[k])
    errs = set(e for _, _, e in g[k] if e)
    n = len(xs)
    mean = sum(xs) / n
    print(f"{k[0]:4s} {k[1]:>5s} {n:>3d} {tmo:>5d} {mean:>8.2f} {xs[n//2]:>8.2f} {max(xs):>8.2f} {mean-int(k[1]):>8.2f}  {errs}")
print()
print("=== first 5 raw rows ===")
for r in rows[:5]:
    print(r)
print()
print("=== any rows with errors ===")
found = False
for r in rows:
    if r.get("error"):
        print(r)
        found = True
        break
if not found:
    print("no error rows")
