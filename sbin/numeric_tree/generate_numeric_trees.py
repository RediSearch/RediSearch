#!/usr/bin/env python3
"""
RediSearch Numeric Tree Generator

This script generates multiple numeric indexes in RediSearch with controllable data distribution
for testing iterator performance, especially union/intersection operations.

Usage:
    python generate_numeric_trees.py --help
    python generate_numeric_trees.py --indexes 5 --docs-per-index 10000 --spread sparse
    python generate_numeric_trees.py --indexes 3 --docs-per-index 5000 --spread consecutive --overlap 0.3
"""

import argparse
import redis
import random
import sys
import time
from typing import List, Tuple
from dataclasses import dataclass


@dataclass
class IndexConfig:
    """Configuration for a single numeric index"""
    name: str
    field1_name: str
    field2_name: str
    doc_count: int
    value_range: Tuple[float, float]
    insertion_order: str  # 'sequential', 'random', 'sparsed'
    sparse_size: int = 100


class NumericTreeGenerator:
    """Generates multiple numeric indexes with controllable data distribution"""
    
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        """Initialize Redis connection"""
        try:
            self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
            self.redis_client.ping()
            print(f"✓ Connected to Redis at {redis_host}:{redis_port}")
        except redis.ConnectionError:
            print(f"✗ Failed to connect to Redis at {redis_host}:{redis_port}")
            sys.exit(1)
    
    def cleanup_existing_indexes(self, index_names: List[str]):
        """Remove existing indexes and their documents"""
        for index_name in index_names:
            try:
                # Drop the index
                self.redis_client.execute_command('FT.DROPINDEX', index_name, 'DD')
                print(f"✓ Dropped existing index: {index_name}")
            except redis.ResponseError as e:
                if "Unknown index name" not in str(e):
                    print(f"⚠ Warning dropping index {index_name}: {e}")
    
    def create_index(self, config: IndexConfig):
        """Create a RediSearch index with two numeric fields"""
        try:
            self.redis_client.execute_command(
                'FT.CREATE', config.name,
                'ON', 'HASH',
                'PREFIX', '1', f'{config.name}:',
                'SCHEMA',
                config.field1_name, 'NUMERIC', 'SORTABLE',
                config.field2_name, 'NUMERIC', 'SORTABLE'
            )
            print(f"✓ Created index: {config.name} with fields: {config.field1_name}, {config.field2_name}")
        except redis.ResponseError as e:
            print(f"✗ Failed to create index {config.name}: {e}")
            sys.exit(1)
    
    def generate_insertion_sequence(self, config: IndexConfig) -> List[Tuple[int, float, float]]:
        """Generate sequence of (doc_id, field1_value, field2_value) based on insertion order"""
        min_val, max_val = config.value_range
        doc_count = config.doc_count

        # Generate base data: doc_id and corresponding values
        base_data = []
        for i in range(doc_count):
            key_id = i + 1
            # Field1 and Field2 values are correlated but slightly different
            field1_val = random.uniform(min_val, max_val) 
            field2_val = field1_val + 100  # Add variance
            base_data.append((key_id, field1_val, field2_val))

        if config.insertion_order == 'sequential':
            # Insert in ascending order of field1 values (sort by field1_val)
            return sorted(base_data, key=lambda x: x[1])

        elif config.insertion_order == 'random':
            # Shuffle the insertion order
            shuffled = base_data.copy()
            random.shuffle(shuffled)
            return shuffled

        elif config.insertion_order == 'sparsed':
            assert config.sparse_size > 1, "Sparse size must be greater than 1 for sparsed insertion"
            # Insert same value multiple times before moving to next
            new_sequence = []
            for key_id, field1_val, field2_val in base_data:
                # Insert this value sparse_size times with different doc_ids
                for sparse_idx in range(config.sparse_size - 1):
                    new_sequence.append((key_id, None, None))
                new_sequence.append((key_id, field1_val, field2_val))
            return new_sequence

        else:
            raise ValueError(f"Unknown insertion order: {config.insertion_order}")
    

    
    def populate_index(self, config: IndexConfig):
        """Populate an index with documents using the specified insertion order"""
        insertion_sequence = self.generate_insertion_sequence(config)

        print(f"Populating index {config.name} ({config.insertion_order} order) with {len(insertion_sequence)} documents...")

        pipe = self.redis_client.pipeline()
        batch_size = 1000
        count = 0

        for key_id, field1_val, field2_val in insertion_sequence:

            # Create document with both numeric fields
            hset_mapping = {}
            if field1_val is not None:
                hset_mapping[config.field1_name] = float(field1_val)
            if field2_val is not None:
                hset_mapping[config.field2_name] = float(field2_val)
            hset_mapping['index_name'] = str(config.name)
            hset_mapping['insertion_order'] = str(config.insertion_order)
            doc_key = f"{config.name}:{int(key_id)}"
            if field1_val is None or field2_val is None:
                continue
            pipe.hset(doc_key, mapping=hset_mapping)

            count += 1
            if count % batch_size == 0:
                pipe.execute()
                pipe = self.redis_client.pipeline()
                print(f"  Inserted {count}/{len(insertion_sequence)} documents...")

        # Execute remaining commands
        if count % batch_size != 0:
            pipe.execute()

        print(f"✓ Populated {config.name} with {len(insertion_sequence)} documents ({config.insertion_order} order)")


def generate_index_configs(docs_per_index: int, sparse_size: int) -> List[IndexConfig]:
    """Generate configurations for 3 indexes with different insertion orders"""
    insertion_orders = ['sequential', 'random', 'sparsed']
    configs = []

    for order in insertion_orders:
        config = IndexConfig(
            name=f"numeric_idx_{order}",
            field1_name="price",
            field2_name="score",
            doc_count=docs_per_index,
            value_range=(0.0, 100000.0),  # Same value range for all indexes
            insertion_order=order,
            sparse_size=sparse_size
        )
        configs.append(config)

    return configs


def main():
    parser = argparse.ArgumentParser(
        description="Generate 3 numeric indexes with different value insertion orders for performance testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 3 indexes with 10K docs each, sparse size 100
  python generate_numeric_trees.py --docs-per-index 10000 --sparse-size 100

  # Generate with smaller dataset for quick testing
  python generate_numeric_trees.py --docs-per-index 1000 --sparse-size 50

  # Generate with larger sparse size for more extreme sparsing effect
  python generate_numeric_trees.py --docs-per-index 5000 --sparse-size 200

The script creates 3 indexes:
  1. numeric_idx_sequential: Values inserted in ascending order
  2. numeric_idx_random: Values inserted in random order
  3. numeric_idx_sparsed: Same value inserted multiple times before next value
        """
    )

    parser.add_argument('--docs-per-index', '-d', type=int, default=10000,
                       help='Number of base documents per index (default: 10000)')
    parser.add_argument('--sparse-size', '-s', type=int, default=100,
                       help='Sparse size for sparsed insertion (default: 100)')
    parser.add_argument('--redis-host', default='localhost',
                       help='Redis host (default: localhost)')
    parser.add_argument('--redis-port', type=int, default=6379,
                       help='Redis port (default: 6379)')
    parser.add_argument('--redis-db', type=int, default=0,
                       help='Redis database number (default: 0)')
    parser.add_argument('--cleanup', action='store_true',
                       help='Clean up existing indexes before creating new ones')

    args = parser.parse_args()

    print(f"Generating 3 numeric indexes with different insertion orders")
    print(f"Base documents per index: {args.docs_per_index}, Sparse size: {args.sparse_size}")
    print("-" * 60)

    # Initialize generator
    generator = NumericTreeGenerator(args.redis_host, args.redis_port, args.redis_db)

    # Generate configurations for 3 indexes with different insertion orders
    configs = generate_index_configs(args.docs_per_index, args.sparse_size)

    # Cleanup existing indexes if requested
    if args.cleanup:
        index_names = [config.name for config in configs]
        generator.cleanup_existing_indexes(index_names)

    # Create and populate indexes
    start_time = time.time()

    for config in configs:
        generator.create_index(config)
        generator.populate_index(config)
        print()

    elapsed_time = time.time() - start_time

    print("=" * 60)
    print(f"✓ Successfully generated {len(configs)} indexes")
    print(f"✓ Time elapsed: {elapsed_time:.2f} seconds")

    # Print summary
    print("\nIndex Summary:")
    for config in configs:
        insertion_sequence = generator.generate_insertion_sequence(config)
        total_docs = len(insertion_sequence)
        value_range = f"{config.value_range[0]:.0f}-{config.value_range[1]:.0f}"

        print(f"  {config.name}:")
        print(f"    Insertion order: {config.insertion_order}")
        print(f"    Total documents: {total_docs}")
        print(f"    Fields: {config.field1_name}, {config.field2_name}")
        print(f"    Value range: {value_range}")
        if config.insertion_order == 'sparsed':
            print(f"    Sparse size: {config.sparse_size}")
        print()


if __name__ == '__main__':
    main()
