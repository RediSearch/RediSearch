#!/usr/bin/env python3
"""MOD-16899 measurement with many samples: exec latency (N plain runs) + loader time (M FT.PROFILE
runs). Reports median AND mean for both so drift can be judged. Output JSON per label."""
import sys, json, time, statistics
import redis

IDX = "idx:entity_events"
QUERIES = [
    ("collect",
     ["*", "LOAD", "6", "@entityName", "@eventId", "@type", "@target", "@processed", "@dueDate",
      "GROUPBY", "1", "@entityName",
      "REDUCE", "COLLECT", "18", "FIELDS", "5", "@eventId", "@type", "@target", "@processed", "@dueDate",
      "SORTBY", "6", "@target", "DESC", "@dueDate", "ASC", "@eventId", "ASC", "LIMIT", "0", "50", "AS", "events",
      "SORTBY", "2", "@entityName", "ASC", "LIMIT", "0", "50"]),
    ("s1_noSort_offset", ["*", "LOAD", "2", "@type", "@dueDate", "LIMIT", "50000", "100"]),
    ("s2_sortby_bigReply", ["*", "SORTBY", "2", "@eventId", "ASC", "LOAD", "2", "@type", "@dueDate", "LIMIT", "0", "10000"]),
]

def flatten(x, out):
    if isinstance(x, (list, tuple)):
        for e in x: flatten(e, out)
    else: out.append(x)

def one_profile(r, args):
    flat = []
    flatten(r.execute_command("FT.PROFILE", IDX, "AGGREGATE", "QUERY", *args), flat)
    flat = [str(e) for e in flat]
    lms = cnt = None
    for i, tok in enumerate(flat):
        if "Loader" in tok:
            for j in range(i + 1, min(i + 12, len(flat))):
                if flat[j] == "Time" and lms is None:
                    try: lms = float(flat[j + 1])
                    except (ValueError, IndexError): pass
                if flat[j] == "Results processed" and cnt is None:
                    try: cnt = int(flat[j + 1])
                    except (ValueError, IndexError): pass
            break
    return lms, cnt

def stats(xs):
    return {"median": round(statistics.median(xs), 2), "mean": round(statistics.fmean(xs), 2),
            "min": round(min(xs), 2), "n": len(xs)}

if __name__ == "__main__":
    port = int(sys.argv[1]); n_exec = int(sys.argv[2]); n_prof = int(sys.argv[3])
    r = redis.Redis(host="127.0.0.1", port=port, decode_responses=True, socket_timeout=600)
    out = {}
    for label, args in QUERIES:
        r.execute_command("FT.AGGREGATE", IDX, *args)  # warmup
        ex = []
        for _ in range(n_exec):
            t = time.perf_counter(); r.execute_command("FT.AGGREGATE", IDX, *args); ex.append((time.perf_counter() - t) * 1000)
        lds = []; cnt = None
        for _ in range(n_prof):
            lms, c = one_profile(r, args)
            if lms is not None: lds.append(lms)
            if c is not None: cnt = c
        out[label] = {"exec": stats(ex), "loader": stats(lds), "loader_count": cnt}
        sys.stderr.write(f"  {label}: done\n"); sys.stderr.flush()
    print(json.dumps(out))
