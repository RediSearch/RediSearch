import csv, sys
from collections import defaultdict
path = sys.argv[1]
text = open(path).read()
cells = defaultdict(dict)
for r in csv.DictReader(text.splitlines()):
    key = (r['topology'], r['query_type'], int(r['index_size']), int(r['timeout_ms']))
    cells[key][r['version']] = (
        float(r['observed_mean_ms']),
        float(r['overshoot_mean_ms']),
        float(r['timeout_frac']),
    )
print(f"{'topology':<9}{'qtype':<11}{'size':>8}{'TO':>5}  | {'old_obs':>8}{'old_over':>9}{'tmo%':>5}  | {'new_obs':>8}{'new_over':>9}{'tmo%':>5}  | {'delta':>8}")
print('-' * 110)
for key in sorted(cells.keys()):
    c = cells[key]
    if 'old' not in c or 'new' not in c:
        continue
    o_obs, o_over, o_tmo = c['old']
    n_obs, n_over, n_tmo = c['new']
    print(f"{key[0]:<9}{key[1]:<11}{key[2]:>8}{key[3]:>5}  | "
          f"{o_obs:>8.1f}{o_over:>9.1f}{o_tmo*100:>5.0f}  | "
          f"{n_obs:>8.1f}{n_over:>9.1f}{n_tmo*100:>5.0f}  | "
          f"{n_over-o_over:>+8.1f}")
