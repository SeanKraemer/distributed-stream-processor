#!/usr/bin/env python3
"""
Spark Streaming Application 2: Filter + Transform
Equivalent to RainStorm Test 3 (grep + transform fields 1-3)

Usage:
    spark-submit --master spark://node1:7077 \
                 filter_transform.py <input_file> <pattern> <output_dir>
"""

import sys
import time
from pyspark import SparkContext

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
    if len(sys.argv) != 4:
        print("Usage: filter_transform.py <input_file> <pattern> <output_dir>")
        sys.exit(1)

    input_file = sys.argv[1]
    pattern = sys.argv[2]
    output_dir = sys.argv[3]

    print(f"Starting Spark Streaming: Filter + Transform")
    print(f"  Input:   {input_file}")
    print(f"  Pattern: {pattern}")
    print(f"  Output:  {output_dir}")

    # Create Spark context
    sc = SparkContext(appName="FilterTransform")
    sc.setLogLevel("WARN")

    start_time = time.time()

    # Read input file
    lines = sc.textFile(input_file)

    # Stage 1: Filter by pattern
    filtered = lines.filter(lambda line: pattern in line)

    # Stage 2: Transform - extract first 3 fields
    def extract_first_three_fields(line):
        fields = parse_csv_line(line)
        if len(fields) >= 3:
            return f"{fields[0]},{fields[1]},{fields[2]}"
        return line  # Return original if less than 3 fields

    transformed = filtered.map(extract_first_three_fields)

    # Collect results (forces computation)
    results = transformed.collect()

    end_time = time.time()
    elapsed = end_time - start_time

    # Write results to output
    import os
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/spark_output.txt"
    with open(output_file, 'w') as f:
        for line in results:
            f.write(f"{line}\n")

    # Print summary
    total_lines = len(results)

    print(f"\n{'='*60}")
    print(f"Spark Streaming Completed!")
    print(f"{'='*60}")
    print(f"  Time:        {elapsed:.2f} seconds")
    print(f"  Total lines: {total_lines}")
    print(f"  Throughput:  {total_lines / elapsed:.2f} tuples/sec")
    print(f"  Output:      {output_file}")
    print(f"{'='*60}\n")

    sc.stop()

if __name__ == "__main__":
    main()
