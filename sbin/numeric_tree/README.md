# RediSearch Numeric Tree Tools

This directory contains Python scripts for generating, testing, parsing, and visualizing numeric indexes in RediSearch, specifically designed to test iterator performance improvements and analyze tree structures.

## Scripts Overview

### 1. `generate_numeric_trees.py` - **NEW**
Generates 3 numeric indexes with 2 fields each, using different **value insertion orders** to test how insertion patterns affect tree structure and iterator performance.

### 2. `test_numeric_queries.py` - **NEW**
Tests various numeric queries against the generated indexes to evaluate iterator performance across different tree structures.

### 3. `numeric_tree_parse.py` - **EXISTING**
Parses RediSearch numeric tree dump files and converts them to JSON format for analysis.

### 4. `numeric_tree_visualize.py` - **EXISTING**
Creates interactive visualizations of parsed numeric trees using Plotly.

## Installation

```bash
# Install Python dependencies for all tools
pip install -r requirements.txt

# For visualization tools, also install:
pip install plotly networkx

# Ensure Redis with RediSearch is running (for generation/testing tools)
redis-server --loadmodule /path/to/redisearch.so
```

## Usage Examples

## A. Data Generation & Testing (NEW Tools)

### Basic Generation

```bash
# Generate 3 indexes with different insertion orders (10K base docs, batch size 100)
./generate_numeric_trees.py --docs-per-index 10000 --batch-size 100

# Generate with smaller dataset for quick testing
./generate_numeric_trees.py --docs-per-index 1000 --batch-size 50

# Generate with larger batch size for more extreme batching effect
./generate_numeric_trees.py --docs-per-index 5000 --batch-size 200
```

### Testing Performance

```bash
# Test all query types on all 3 indexes
./test_numeric_queries.py --query-type all --iterations 100

# Test intersection queries (tests both fields within same index)
./test_numeric_queries.py --query-type intersection --range-size 100 --iterations 50

# Test single field queries only
./test_numeric_queries.py --query-type single --iterations 200
```

## B. Tree Analysis & Visualization (EXISTING Tools)

### Parse Tree Dump Files

```bash
# Parse a RediSearch numeric tree dump file
./numeric_tree_parse.py dump_numidxtree.txt tree.json

# Fast parsing mode (disable assertions for large files)
./numeric_tree_parse.py dump_numidxtree.txt tree.json --fast
```

### Visualize Parsed Trees

```bash
# Create interactive visualization
./numeric_tree_visualize.py tree.json

# Create visualization with custom spacing
./numeric_tree_visualize.py tree.json 3.0

# Show tree information only (no visualization)
./numeric_tree_visualize.py tree.json info
```

## Insertion Order Patterns

The script creates **3 indexes with identical data but different insertion orders**:

### 1. Sequential Index (`numeric_idx_sequential`)
- Values inserted in **ascending order** (0.0, 1.0, 2.0, ...)
- Creates a **balanced tree** structure
- **Best case** for tree traversal and range queries
- Simulates sorted data ingestion

### 2. Random Index (`numeric_idx_random`)
- Values inserted in **random order** (42.5, 1.2, 99.8, ...)
- Creates a **randomly balanced tree** structure
- **Average case** performance
- Simulates real-world random data ingestion

### 3. Batched Index (`numeric_idx_batched`)
- **Same value inserted multiple times** before moving to next value
- Creates **unbalanced tree** with deep branches for repeated values
- **Worst case** for tree traversal (many duplicate values)
- Simulates bulk loading of similar data

### Index Structure

Each index contains **2 numeric fields**:
- **`price`**: Primary field with controlled insertion order
- **`score`**: Secondary field (price + random variance)

This allows testing:
- **Single field queries**: `@price:[100 200]`
- **Multi-field intersection**: `@price:[100 200] @score:[150 250]`
- **Cross-index union queries**: Different insertion order effects

## Query Types

### Single Range Queries
- Test individual numeric range queries: `@field:[min max]`
- Baseline performance measurement

### Union Queries  
- Test queries across multiple fields: `@field1:[min max] | @field2:[min max]`
- Tests union iterator performance

### Intersection Queries
- Test queries requiring multiple conditions: `@field1:[min max] @field2:[min max]`
- Tests intersection iterator performance

## Performance Testing Scenarios

### Scenario 1: Insertion Order Impact on Range Queries
```bash
# Generate indexes with different insertion orders
./generate_numeric_trees.py --docs-per-index 10000 --batch-size 100 --cleanup

# Test single field range queries - compare performance across insertion orders
./test_numeric_queries.py --query-type single --iterations 200
```

### Scenario 2: Multi-Field Intersection Performance
```bash
# Test intersection queries (both fields within same index)
./test_numeric_queries.py --query-type intersection --iterations 100

# Compare how tree structure affects intersection performance
```

### Scenario 3: Batch Size Impact on Tree Structure
```bash
# Test different batch sizes for the batched index
for batch_size in 10 50 100 200 500; do
    ./generate_numeric_trees.py --docs-per-index 5000 --batch-size $batch_size --cleanup
    ./test_numeric_queries.py --query-type all --iterations 50
done
```

### Scenario 4: Dataset Size Scaling
```bash
# Test how insertion order effects scale with dataset size
for docs in 1000 5000 10000 50000; do
    ./generate_numeric_trees.py --docs-per-index $docs --batch-size 100 --cleanup
    ./test_numeric_queries.py --query-type single --iterations 100
done
```

## Output Interpretation

### Generation Output
```
✓ Connected to Redis at localhost:6379
✓ Created index: numeric_idx_1 with field: value_1
Populating index numeric_idx_1 with 10000 documents...
✓ Populated numeric_idx_1 with 10000 documents

Index Summary:
  numeric_idx_1: 10000 docs, IDs: 1-999901, Values: 0-1000
  numeric_idx_2: 10000 docs, IDs: 103-999823, Values: 1000-2000
```

### Testing Output
```
UNION Query Statistics:
  Total queries: 100
  Execution time (ms):
    Mean: 2.45
    Median: 2.31
    Min: 1.89
    Max: 4.12
    Std Dev: 0.67
  Result counts:
    Mean: 1247.3
    Median: 1198.0
    Min: 0
    Max: 3456
```

## Advanced Configuration

### Custom Redis Connection
```bash
./generate_numeric_trees.py --redis-host redis.example.com --redis-port 6380 --redis-db 1
```

### Large Scale Testing
```bash
# Generate large dataset for stress testing
./generate_numeric_trees.py --indexes 10 --docs-per-index 100000 --spread sparse --jump-size 1000
```

### Memory-Efficient Testing
```bash
# Smaller datasets for quick iteration
./generate_numeric_trees.py --indexes 3 --docs-per-index 1000 --spread consecutive
```

## Integration with Benchmarks

These scripts complement the C++ micro-benchmarks in `tests/cpptests/micro-benchmarks/`:

1. **Generate test data** with these Python scripts
2. **Run C++ benchmarks** to measure low-level iterator performance  
3. **Compare results** between different data distributions

## Troubleshooting

### Redis Connection Issues
```bash
# Check if Redis is running
redis-cli ping

# Check if RediSearch module is loaded
redis-cli MODULE LIST
```

### Index Creation Failures
```bash
# Clean up existing indexes
./generate_numeric_trees.py --cleanup --indexes 0

# Check Redis memory usage
redis-cli INFO memory
```

### Performance Variations
- Run multiple iterations to get stable measurements
- Consider system load and Redis configuration
- Use `--iterations` parameter to increase sample size

## Complete Workflow: Generation → Analysis → Visualization

### 1. Generate Test Data
```bash
# Create 3 indexes with different insertion orders
./generate_numeric_trees.py --docs-per-index 10000 --batch-size 100
```

### 2. Test Performance
```bash
# Measure query performance across different tree structures
./test_numeric_queries.py --query-type all --iterations 100
```

### 3. Dump Tree Structure (using RediSearch debug commands)
```bash
# From Redis CLI, dump each index's tree structure
redis-cli FT.DEBUG DUMP_NUMIDXTREE numeric_idx_sequential price > sequential_tree.txt
redis-cli FT.DEBUG DUMP_NUMIDXTREE numeric_idx_random price > random_tree.txt
redis-cli FT.DEBUG DUMP_NUMIDXTREE numeric_idx_batched price > batched_tree.txt
```

### 4. Parse & Visualize
```bash
# Parse each tree structure
./numeric_tree_parse.py sequential_tree.txt sequential.json
./numeric_tree_parse.py random_tree.txt random.json
./numeric_tree_parse.py batched_tree.txt batched.json

# Create interactive visualizations
./numeric_tree_visualize.py sequential.json
./numeric_tree_visualize.py random.json
./numeric_tree_visualize.py batched.json
```

This workflow allows you to:
- **Generate** identical data with different insertion orders
- **Measure** how insertion order affects iterator performance
- **Analyze** the actual tree structures created by different insertion patterns
- **Visualize** the differences in tree organization and balance

## Files

- `generate_numeric_trees.py` - **NEW** - Redis API data generation script
- `test_numeric_queries.py` - **NEW** - Query performance testing script
- `numeric_tree_parse.py` - **EXISTING** - Tree dump parser
- `numeric_tree_visualize.py` - **EXISTING** - Interactive tree visualizer
- `requirements.txt` - Python dependencies
- `README.md` - This documentation
