# MS MARCO Benchmark Dataset Generator

Generate MS MARCO document datasets for RediSearch benchmarks with 64 tags.

**Jira**: [MOD-13349](https://redislabs.atlassian.net/browse/MOD-13349)
**PR**: [#8004](https://github.com/RediSearch/RediSearch/pull/8004)

---

## Current Status

### Dataset Generation
| Step | Status |
|------|--------|
| Download `msmarco_v2_doc.tar` | ✅ Complete (34.6 GB) |
| Extract shards | ✅ Complete (60 shards) |
| Generate 6M dataset with 64 tags | ✅ Complete (5,978,761 docs, 55 GB) |
| Upload to S3 | ✅ Complete |
| Create benchmark YAMLs | ✅ Complete (6 files) |

### CI Integration
| Step | Status | Notes |
|------|--------|-------|
| Create dedicated labels | ✅ Complete | `action:run-msmarco-benchmark`, `action:run-msmarco-benchmark-fast` |
| Configure workflow triggers | ✅ Complete | Added to `benchmark-trigger.yml`, `benchmark-runner.yml` |
| Fast benchmark (500K docs) | 🔄 In Progress | Using `oss-cluster-08-primaries` (90GB) |
| Full benchmark (6M docs) | ⏳ Pending | Needs custom 250GB setup in redisbench-admin |

### Known Issues
1. **Custom 250GB cluster not supported**: The `oss-cluster-08-primaries-250gb` setup requires changes to the `redisbench-admin` Terraform modules. For now, fast benchmarks use the existing `oss-cluster-08-primaries` (90GB).

2. **YAML structure**: Benchmark YAMLs must follow the correct pattern:
   - **Load-only benchmarks**: `ftsb_redisearch` goes in `clientconfig`
   - **Query benchmarks**: `ftsb_redisearch` goes in `dbconfig` (for pre-loading) AND `clientconfig` (for querying)

---

## Quick Start

```bash
# 1. Extract shards (if not done)
mkdir -p extracted && tar -xf msmarco_v2_doc.tar -C extracted

# 2. Generate dataset (50% sample → ~6M docs)
python3 generate_msmarco_dataset.py \
  --shards-dir ./extracted/msmarco_v2_doc \
  --sample-pct 50 \
  --dataset-name 6M-msmarco-documents \
  --output-dir ./output

# 3. Upload to S3
aws s3 cp ./output/ s3://benchmarks.redislabs/redisearch/datasets/6M-msmarco-documents/ --recursive
```

---

## Dataset Details

| Property | Value |
|----------|-------|
| **Source** | MS MARCO Document v2 |
| **Total docs** | 5,978,761 (50% sample of 12M) |
| **SETUP.csv size** | ~55 GB |
| **Tags** | 64 tags with varying cardinality |

### Tag Distribution

| Tier | Tags | Probability | Avg docs/tag |
|------|------|-------------|--------------|
| HIGH | t00-t07 (8 tags) | 45% each | ~2.7M |
| MEDIUM | t08-t23 (16 tags) | 15% each | ~900K |
| LOW | t24-t63 (40 tags) | 3% each | ~180K |

Average: **~7 tags per document**

---

## RediSearch Schema

```
FT.CREATE ms_marco_idx ON HASH PREFIX 1 doc: SCHEMA
  url TEXT
  title TEXT WEIGHT 2.0
  headings TEXT WEIGHT 1.5
  body TEXT
  tags TAG SEPARATOR ","
```

---

## Benchmark Files

Located in `tests/benchmarks/`:

| File | Description | Docs |
|------|-------------|------|
| `search-msmarco-6M-documents-load.yml` | Data ingestion benchmark | 6M |
| `search-msmarco-6M-documents-load-fast.yml` | Fast load smoke test | 500K |
| `search-msmarco-6M-documents-baseline-query.yml` | Single-word queries | 6M |
| `search-msmarco-6M-documents-baseline-query-fast.yml` | Fast query smoke test | 500K |
| `search-msmarco-6M-documents-and-query.yml` | Multi-term AND queries | 6M |
| `search-msmarco-6M-documents-phrase-query.yml` | Phrase queries | 6M |

---

## Local Testing

Test the benchmark data loading locally before running in CI:

```bash
# 1. Build RediSearch
make build

# 2. Start Redis cluster (8 primaries)
redis-server --loadmodule bin/linux-x64-release/search-community/redisearch.so --port 6379 &

# 3. Create the index
redis-cli FT.CREATE ms_marco_idx ON HASH PREFIX 1 doc: SCHEMA \
  url TEXT title TEXT WEIGHT 2.0 headings TEXT WEIGHT 1.5 body TEXT tags TAG SEPARATOR ","

# 4. Download and test with a small sample (first 1000 lines)
curl -s "https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/6M-msmarco-documents/6M-msmarco-documents.redisearch.commands.SETUP.csv" | head -1000 > sample.csv

# 5. Load using ftsb_redisearch (install from https://github.com/RediSearch/ftsb)
ftsb_redisearch -input sample.csv -workers 4

# 6. Verify
redis-cli FT.INFO ms_marco_idx | grep num_docs
```

### Using Docker

```bash
# Start Redis with RediSearch module
docker run -d --name redis-search -p 6379:6379 redis/redis-stack-server:latest

# Create index and test
redis-cli -p 6379 FT.CREATE ms_marco_idx ON HASH PREFIX 1 doc: SCHEMA \
  url TEXT title TEXT WEIGHT 2.0 headings TEXT WEIGHT 1.5 body TEXT tags TAG SEPARATOR ","
```

---

## CI Trigger Labels

| Label | Description |
|-------|-------------|
| `action:run-msmarco-benchmark` | Run full 6M document benchmark (~1 hour) |
| `action:run-msmarco-benchmark-fast` | Run fast 500K document smoke test (~15 min) |

---

## References

- [ir-datasets: msmarco-document-v2](https://ir-datasets.com/msmarco-document-v2.html)
- [ftsb - Full-Text Search Benchmark](https://github.com/RediSearch/ftsb)
- Confluence: "Performance Search - Load & Index measurements"

