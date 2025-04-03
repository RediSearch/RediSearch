#!/usr/bin/env python3
import pandas as pd
import argparse
import re
import json
from collections import Counter
from tqdm import tqdm

def csv_to_redis_json(csv_path, output_path):
    """
    Convert a CSV file to JSON format for Redis HSET mappings.
    
    Args:
        csv_path: Path to the CSV file
        output_path: Path to save the JSON data
    
    Returns:
        top_words: Dictionary with most common words and median frequency words
    """
    print(f"Processing {csv_path}...")
    
    # For .dapo files, we'll strip the extension
    actual_path = csv_path
    
    # Read the CSV file
    df = pd.read_csv(actual_path)
    
    # Find the most common words for later querying
    text_data = ' '.join(df.astype(str).values.flatten())
    words = re.findall(r'\w+', text_data)
    word_counts = Counter(words)
    
    # Get word counts sorted by frequency
    sorted_words = word_counts.most_common()
    
    # Extract top common words (excluding short words and non-alphabetic terms)
    top_common_words = [word for word, count in sorted_words[:20] 
                      if len(word) > 3 and word.isalpha()][:5]
    
    # Find words around median frequency
    median_idx = len(sorted_words) // 2
    median_range_words = [word for word, count in sorted_words[median_idx-10:median_idx+10]
                         if len(word) > 3 and word.isalpha()][:5]
    
    # Create JSON records for Redis
    redis_data = {}
    
    for _, row in tqdm(df.iterrows(), total=len(df)):
        doc_id = f"doc:{row['CID']}"
        
        # Create mapping for this document
        mapping = {}
        
        for column in df.columns:
            if column != 'CID' and pd.notna(row[column]):  # Skip CID since it's used as doc ID
                # Try to convert numeric values to the appropriate type
                value = row[column]
                try:
                    # If it's a number, convert it to int or float
                    if isinstance(value, str) and value.isdigit():
                        value = int(value)
                    elif isinstance(value, str) and re.match(r'^-?\d+(\.\d+)?$', value):
                        value = float(value)
                except (ValueError, TypeError):
                    pass
                
                mapping[column] = value
        
        redis_data[doc_id] = mapping
    
    # Save as JSON file
    with open(output_path, 'w') as f:
        json.dump(redis_data, f, indent=2)
    
    top_words = {
        'most_common': top_common_words[0] if top_common_words else 'music',
        'median': median_range_words[0] if median_range_words else 'track',
        'top_common_words': top_common_words,
        'median_words': median_range_words
    }
    
    print(f"Generated JSON data saved to {output_path}")
    print(f"Most common word for querying: {top_words['most_common']}")
    print(f"Median frequency word for querying: {top_words['median']}")
    
    return top_words

def main():
    parser = argparse.ArgumentParser(description='Convert CSV to Redis JSON data')
    parser.add_argument('csv_path', help='Path to the CSV file')
    parser.add_argument('--output', '-o', help='Output file path')
    args = parser.parse_args()
    
    output_path = args.output
    if not output_path:
        output_path = args.csv_path.split("/")[-1].replace(".csv.dapo", ".json").replace(".csv", ".json")
    
    top_words = csv_to_redis_json(args.csv_path, output_path)
    print(f"Use '{top_words['most_common']}' or '{top_words['median']}' as query terms for benchmarking")

if __name__ == "__main__":
    main()