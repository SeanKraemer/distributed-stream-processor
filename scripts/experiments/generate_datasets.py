#!/usr/bin/env python3
"""
Generate synthetic datasets for MP4 experiments

Creates 2 synthetic datasets with realistic patterns:
- synthetic_small.csv: ~100MB, for quick testing
- synthetic_large.csv: ~500MB, for performance comparison

Each dataset has CSV format similar to demo datasets with:
- Numeric IDs, street names, sign types, regulatory categories
- Varying pattern frequencies to test filtering performance
"""

import csv
import random
import sys
from datetime import datetime, timedelta

# Seed for reproducibility
random.seed(42)

# Dataset templates
STREET_NAMES = [
    "MAIN ST", "ELM ST", "OAK AVE", "PARK DR", "LAKE RD",
    "HILL BLVD", "RIVER WAY", "FOREST LN", "MEADOW CT", "VALLEY RD",
    "SUMMIT AVE", "RIDGE DR", "CANYON PL", "CREEK WAY", "BRIDGE ST",
    "MAPLE DR", "PINE AVE", "CEDAR LN", "BIRCH CT", "WILLOW RD"
]

SIGN_TYPES = [
    "SIGN_REGULATORY", "SIGN_GUIDE", "SIGN_WARNING", "SIGN_POLE"
]

# Regulatory messages (for filter testing)
REGULATORY_MESSAGES = [
    "STOP", "YIELD", "SPEED LIMIT 25", "SPEED LIMIT 35", "SPEED LIMIT 45",
    "NO PARKING", "NO STOPPING", "NO STANDING", "ONE WAY",
    "DO NOT ENTER", "WRONG WAY", "KEEP RIGHT", "TURN RIGHT ONLY",
    "NO TURN ON RED", "STOP AHEAD", "YIELD AHEAD"
]

GUIDE_MESSAGES = [
    "STREET NAME", "EXIT 123", "DOWNTOWN", "AIRPORT",
    "HOSPITAL", "SCHOOL ZONE", "PEDESTRIAN CROSSING"
]

WARNING_MESSAGES = [
    "CURVE AHEAD", "MERGE", "HILL", "BUMP", "DIP",
    "SLIPPERY WHEN WET", "DEER CROSSING", "SCHOOL CROSSING"
]

def generate_row(row_id):
    """Generate a single CSV row"""
    # IDs
    id1 = f'"{random.randint(100000, 999999):,}"'
    id2 = f'"{random.randint(100000, 999999):,}"'

    # Sign code
    sign_code = f"{random.choice(['R', 'D', 'W'])}{random.randint(1, 9)}-{random.randint(1, 20)}"
    if random.random() < 0.1:
        sign_code += "P"

    # Street name
    street_name = random.choice(STREET_NAMES)

    # Sign type
    sign_type = random.choice(SIGN_TYPES)

    # Message based on sign type
    if sign_type == "SIGN_REGULATORY":
        message = random.choice(REGULATORY_MESSAGES)
    elif sign_type == "SIGN_GUIDE":
        message = random.choice(GUIDE_MESSAGES)
    elif sign_type == "SIGN_WARNING":
        message = random.choice(WARNING_MESSAGES)
    else:
        message = ""

    # Direction
    direction = random.choice(["N", "S", "E", "W", "NE", "NW", "SE", "SW", ""])

    # Dimensions
    width = round(random.uniform(1.0, 3.0), 1)
    height = round(random.uniform(1.0, 3.0), 1)
    offset = round(random.uniform(0.5, 2.0), 1)

    # Status
    status = random.choice(["QC Complete", "Installed", "Pending", ""])

    # Coordinates (Austin, TX area)
    lat = round(random.uniform(30.1, 30.5), 15)
    lon = round(random.uniform(-97.9, -97.6), 15)
    point = f"POINT ({lon} {lat})"

    # Dates
    base_date = datetime(2021, 1, 1)
    date1 = (base_date + timedelta(days=random.randint(0, 1400))).strftime("%Y %b %d %I:%M:%S %p")
    date2 = (base_date + timedelta(days=random.randint(1400, 1600))).strftime("%Y %b %d %I:%M:%S %p")
    date3 = (base_date + timedelta(days=random.randint(1600, 1800))).strftime("%Y %b %d %I:%M:%S %p")

    return [
        id1, id2, sign_code, street_name, "Street Name" if sign_type == "SIGN_GUIDE" else message,
        "", sign_type, message if sign_type != "SIGN_POLE" else "",
        direction, str(width), str(height), str(offset), status,
        "", "", "", date1, date2, date3, str(lat), str(lon), point
    ]

def generate_dataset(filename, num_rows):
    """Generate a synthetic dataset"""
    print(f"Generating {filename} with {num_rows:,} rows...")

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)

        for i in range(num_rows):
            row = generate_row(i)
            writer.writerow(row)

            if (i + 1) % 10000 == 0:
                print(f"  Progress: {i + 1:,} / {num_rows:,} rows ({(i + 1) / num_rows * 100:.1f}%)")

    print(f"  ✅ {filename} complete!")

def main():
    print("📊 Synthetic Dataset Generator for MP4")
    print("=" * 60)
    print()

    # Dataset 1: Small (~1.25MB, ~5K lines) - fits HyDFS buffer limits
    print("Dataset 1: Small (for testing and comparison)")
    generate_dataset("data/synthetic_medium.csv", 5000)
    print()

    # Dataset 2: Large (~2.5MB, ~10K lines) - maximum HyDFS can handle
    print("Dataset 2: Large (for performance comparison)")
    generate_dataset("data/synthetic_large.csv", 10000)
    print()

    print("=" * 60)
    print("✅ All datasets generated!")
    print()
    print("📋 Dataset Summary:")
    print(f"   synthetic_medium.csv: ~5K rows, ~1.25MB (HyDFS compatible)")
    print(f"   synthetic_large.csv: ~10K rows, ~2.5MB (HyDFS compatible)")
    print()
    print("📌 Next Steps:")
    print("   1. Upload datasets to VMs:")
    print("      ./scripts/experiments/upload_datasets.sh")
    print("   2. Run experiments:")
    print("      ./scripts/experiments/run_experiments.sh")
    print()

if __name__ == "__main__":
    main()
