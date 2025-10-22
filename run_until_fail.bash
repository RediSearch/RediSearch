# Keep running until it fails (proves bug exists)
count=0
while make pytest TEST=test_mod_11658_workers_reduction:test_MOD_11658_workers_reduction_under_load REDIS_STANDALONE=0 COORD=oss; do
    count=$((count + 1))
    echo "Passed $count times, trying again..."
done
echo "FAILED! Bug reproduced after $count successful runs."
