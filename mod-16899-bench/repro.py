#!/usr/bin/env python3
"""MOD-16899 local repro harness: load skewed JSON entity-events and hammer the
GROUPBY+COLLECT aggregate so a sampler can show JSONPath re-compilation cost.

Usage:
  repro.py load  --port P --docs N        # create index + load N JSON docs
  repro.py query --port P --seconds S     # run the aggregate in a tight loop for S seconds
  repro.py once  --port P                 # run the aggregate once, print timing + row count
"""
import argparse, random, time, sys
from datetime import date, timedelta
import redis

INDEX = "idx:entity_events"
PREFIX = "entity:"
ENTITY_NAMES = ["Entity Alpha","Entity Beta","Entity Gamma","Entity Delta","Entity Epsilon",
                "Entity Zeta","Entity Eta","Entity Theta","Entity Iota","Entity Kappa"]
EVENT_TYPES = ["TYPE_A","TYPE_B","TYPE_C"]
BOOLS = ["true","false"]
DATE0 = date(2020,1,1); DATE_RANGE = 4017
# heavy skew, scaled to N docs
HEAVY = [700_000,70_000,70_000,70_000,15_000,15_000,15_000,15_000,15_000,15_000]

def group_sizes(total):
    base_total = sum(HEAVY)
    sizes = [int(g*total/base_total) for g in HEAVY]
    rem = total - sum(sizes)
    for i in range(rem):
        sizes[i] += 1
    return sizes

def payload(rid, nbytes=512):
    tok = f"payload-{rid}-"
    return (tok * ((nbytes//len(tok))+1))[:nbytes]

def docs(total, seed=14808):
    rng = random.Random(seed)
    sizes = group_sizes(total)
    emitted = [0]*len(sizes)
    for i in range(total):
        best, best_ratio = -1, 2.0
        for gi, gs in enumerate(sizes):
            if emitted[gi] >= gs: continue
            r = emitted[gi]/gs
            if r < best_ratio: best_ratio, best = r, gi
        emitted[best] += 1
        rid = 200_000_000+i
        doc = {
            "recordId": rid,
            "entityName": ENTITY_NAMES[best],
            "event": {
                "id": 600_000_000+i,
                "type": rng.choice(EVENT_TYPES),
                "target": rng.choice(BOOLS),
                "hasNotes": rng.choice(BOOLS),
                "processed": rng.choice(BOOLS),
                "dueDate": (DATE0+timedelta(days=rng.randint(0,DATE_RANGE))).isoformat(),
                "payload": payload(rid),
            },
        }
        yield rid, doc

AGG = [
    "FT.AGGREGATE", INDEX, "*",
    "LOAD","6","@entityName","@eventId","@type","@target","@processed","@dueDate",
    "GROUPBY","1","@entityName",
    "REDUCE","COLLECT","18","FIELDS","5","@eventId","@type","@target","@processed","@dueDate",
        "SORTBY","6","@target","DESC","@dueDate","ASC","@eventId","ASC","LIMIT","0","50","AS","events",
    "SORTBY","2","@entityName","ASC","LIMIT","0","50",
]

def cmd_load(r, n):
    try: r.execute_command("FT.DROPINDEX", INDEX)
    except Exception: pass
    r.execute_command(
        "FT.CREATE", INDEX, "ON","JSON","PREFIX","1",PREFIX,"SCHEMA",
        "$.recordId","AS","recordId","NUMERIC","SORTABLE",
        "$.entityName","AS","entityName","TEXT","SORTABLE",
        "$.event.id","AS","eventId","NUMERIC","SORTABLE",
        "$.event.type","AS","type","TAG",
        "$.event.target","AS","target","TAG","SORTABLE",
        "$.event.hasNotes","AS","hasNotes","TAG",
        "$.event.processed","AS","processed","TAG",
        "$.event.dueDate","AS","dueDate","TEXT","SORTABLE",
        "$.event.payload","AS","payload","TEXT","NOINDEX",
    )
    import json
    pipe = r.pipeline(transaction=False)
    for i,(rid,doc) in enumerate(docs(n)):
        pipe.execute_command("JSON.SET", f"{PREFIX}{rid}", "$", json.dumps(doc,separators=(",",":")))
        if (i+1)%2000==0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
    pipe.execute()
    # wait for background indexing to settle
    for _ in range(120):
        info = r.execute_command("FT.INFO", INDEX)
        d = {info[j]: info[j+1] for j in range(0,len(info)-1,2)}
        if str(d.get("indexing","0")) in ("0","0.0") and int(d.get("num_docs",0))>=n:
            break
        time.sleep(0.5)
    print(f"loaded num_docs={d.get('num_docs')} indexing={d.get('indexing')}")

def cmd_once(r):
    t=time.time(); res=r.execute_command(*AGG); dt=time.time()-t
    print(f"agg returned {res[0]} groups in {dt*1000:.1f} ms")

def cmd_query(r, seconds):
    end=time.time()+seconds; n=0; t0=time.time()
    while time.time()<end:
        r.execute_command(*AGG); n+=1
    print(f"ran {n} aggregates in {time.time()-t0:.1f}s ({n/(time.time()-t0):.1f}/s)")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("mode",choices=["load","query","once"])
    ap.add_argument("--port",type=int,default=6399)
    ap.add_argument("--docs",type=int,default=30000)
    ap.add_argument("--seconds",type=int,default=20)
    a=ap.parse_args()
    r=redis.Redis(host="127.0.0.1",port=a.port,decode_responses=True,socket_timeout=120)
    if a.mode=="load": cmd_load(r,a.docs)
    elif a.mode=="once": cmd_once(r)
    else: cmd_query(r,a.seconds)
