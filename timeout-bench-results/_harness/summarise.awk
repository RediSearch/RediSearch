# Per-cell summary from raw.csv.
# Columns: 1 version 2 topology 3 query_type 4 index_size 5 timeout_ms
#          6 iter 7 observed_ms 8 timed_out 9 error 10 server_mean_ms
BEGIN { FS = "," }
NR == 1 { next }
{
  k = $1 "," $2 "," $3 "," $4 "," $5
  n[k]++
  sumobs[k] += $7
  if ($7 > maxobs[k]) maxobs[k] = $7
  if (n[k] == 1 || $7 < minobs[k]) minobs[k] = $7
  obs[k, n[k]] = $7
  if ($8 == 1) tmo[k]++
  smm[k] = $10
  topo[k] = $2; ver[k] = $1; qt[k] = $3; sz[k] = $4; to[k] = $5
}
END {
  print "version,topology,query_type,index_size,timeout_ms,n,obs_mean_ms,obs_p50_ms,obs_p95_ms,obs_max_ms,server_mean_ms,timeout_pct,overshoot_mean_ms,overshoot_p95_ms"
  for (k in n) {
    cnt = n[k]
    delete a
    for (i = 1; i <= cnt; i++) a[i] = obs[k, i]
    for (i = 2; i <= cnt; i++) {
      v = a[i]; j = i - 1
      while (j >= 1 && a[j] > v) { a[j+1] = a[j]; j-- }
      a[j+1] = v
    }
    p50 = a[int((cnt + 1) * 0.5)]
    p95 = a[int((cnt + 1) * 0.95)]
    if (p95 == "") p95 = a[cnt]
    mean = sumobs[k] / cnt
    over_mean = mean - to[k]
    over_p95  = p95 - to[k]
    tpct = (k in tmo ? tmo[k] : 0) * 100.0 / cnt
    printf "%s,%s,%s,%s,%s,%d,%.3f,%.3f,%.3f,%.3f,%s,%.1f,%.3f,%.3f\n", \
      ver[k], topo[k], qt[k], sz[k], to[k], cnt, mean, p50, p95, maxobs[k], \
      (smm[k] == "" ? "" : smm[k]), tpct, over_mean, over_p95
  }
}
