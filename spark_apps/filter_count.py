#!/usr/bin/env python3
"""
Spark Streaming Application 1: Filter + Count
Equivalent to RainStorm Test 1 (grep + count)

Usage:
    spark-submit --master spark://node1:7077 \
                 filter_count.py <input_file> <pattern> <column_num> <output_dir>
"""

import sys
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

def parse_csv_line(line):
    """Parse CSV line handling quoted fields"""
    import csv
    from io import StringIO
    reader = csv.reader(StringIO(line))
    try:
        return next(reader)
    except:
        return []

def main():
    if len(sys.argv) != 5:
        print("Usage: filter_count.py <input_file> <pattern> <column_num> <output_dir>")
        sys.exit(1)

    input_file = sys.argv[1]
    pattern = sys.argv[2]
    column_num = int(sys.argv[3])
    output_dir = sys.argv[4]

    print(f"Starting Spark Streaming: Filter + Count")
    print(f"  Input:   {input_file}")
    print(f"  Pattern: {pattern}")
    print(f"  Column:  {column_num}")
    print(f"  Output:  {output_dir}")

    # Create Spark context
    sc = SparkContext(appName="FilterCount")
    sc.setLogLevel("WARN")

    start_time = time.time()

    # Read input file
    lines = sc.textFile(input_file)

    # Stage 1: Filter by pattern
    filtered = lines.filter(lambda line: pattern in line)

    # Stage 2: Extract key (column N) and count
    def extract_key_and_count(line):
        fields = parse_csv_line(line)
        if len(fields) > column_num:
            key = fields[column_num]
            return (key, 1)
        return ("", 1)

    # Map to (key, 1) and reduce by key
    counts = filtered.map(extract_key_and_count).reduceByKey(lambda a, b: a + b)

    # Collect results (forces computation)
    results = counts.collect()

    end_time = time.time()
    elapsed = end_time - start_time

    # Write results to output
    import os
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/spark_output.txt"
    with open(output_file, 'w') as f:
        for key, count in sorted(results, key=lambda x: -x[1]):
            f.write(f"{key},{count}\n")

    # Print summary
    total_count = sum(count for _, count in results)
    unique_keys = len(results)

    print(f"\n{'='*60}")
    print(f"Spark Streaming Completed!")
    print(f"{'='*60}")
    print(f"  Time:        {elapsed:.2f} seconds")
    print(f"  Total lines: {total_count}")
    print(f"  Unique keys: {unique_keys}")
    print(f"  Throughput:  {total_count / elapsed:.2f} tuples/sec")
    print(f"  Output:      {output_file}")
    print(f"{'='*60}\n")

    sc.stop()

if __name__ == "__main__":
    main()
