# MP4 Experiment Guide

**Complete workflow for running Spark experiments and generating report plots**

---

## Overview

This guide walks through:

1. Generating synthetic datasets
2. Deploying Spark to the cluster
3. Running 4 experiments (RainStorm vs Spark)
4. Generating comparison plots for the report

**Total Time Estimate:** 3-4 hours (including 24 experiment runs)

---

## Prerequisites

✅ All demo tests passing (Tests 0-3)
✅ VMs accessible via SSH
✅ Spark archive downloaded (`spark-4.0.1-bin-hadoop3.tgz`)
✅ Python 3 with pandas and matplotlib installed locally

---

## Step-by-Step Workflow

### Step 1: Generate Synthetic Datasets

Create two synthetic datasets for experiments:

```bash
cd distributed-stream-processor
python3 scripts/experiments/generate_datasets.py
```

**Output:**

- `data/synthetic_small.csv` (~50K rows, ~100MB)
- `data/synthetic_large.csv` (~250K rows, ~500MB)

**Time:** ~2 minutes

---

### Step 2: Deploy Spark to VMs

Deploy Spark Streaming to all 10 VMs:

```bash
./scripts/spark/deploy_spark.sh
```

**What it does:**

1. Uploads `spark-4.0.1-bin-hadoop3.tgz` to all VMs
2. Extracts Spark on each VM
3. Configures master (VM01) and workers (VM02-10)
4. Starts Spark cluster

**Verify deployment:**

```bash
docker exec node1 'jps'
# Should show: Master
```

**Time:** ~10 minutes

---

### Step 3: Upload Datasets and Spark Apps

Upload synthetic datasets and Spark applications to all VMs:

```bash
./scripts/experiments/upload_datasets.sh
```

**What it uploads:**

- `data/synthetic_small.csv` → all VMs
- `data/synthetic_large.csv` → all VMs
- `spark_apps/*.py` → all VMs

**Time:** ~5 minutes

---

### Step 4: Run Experiments

Run all 4 experiments (3 repetitions each):

```bash
./scripts/experiments/run_experiments.sh
```

**Experiments:**

1. Small dataset × FilterCount (grep + count)
2. Small dataset × FilterTransform (grep + extract fields)
3. Large dataset × FilterCount
4. Large dataset × FilterTransform

**Each experiment:**

- Runs 3 times for statistical significance
- Tests both RainStorm and Spark
- Saves timing and throughput data

**Output:**

```
experiment_results/
  ├── exp1_rainstorm_Small50K_FilterCount.csv
  ├── exp1_spark_Small50K_FilterCount.csv
  ├── exp2_rainstorm_Small50K_FilterTransform.csv
  ├── exp2_spark_Small50K_FilterTransform.csv
  ├── exp3_rainstorm_Large250K_FilterCount.csv
  ├── exp3_spark_Large250K_FilterCount.csv
  ├── exp4_rainstorm_Large250K_FilterTransform.csv
  └── exp4_spark_Large250K_FilterTransform.csv
```

**Time:** ~2-3 hours (24 runs total, ~5-10 min per run)

---

### Step 5: Generate Plots for Report

Generate comparison plots from experiment results:

```bash
python3 scripts/experiments/plot_results.py
```

**Output:**

```
plots/
  ├── throughput_comparison.png       # Tuples/sec comparison
  ├── throughput_comparison.pdf       # PDF for report
  ├── elapsed_time_comparison.png     # Processing time comparison
  ├── elapsed_time_comparison.pdf     # PDF for report
  └── summary_table.csv               # Detailed statistics
```

**Plots include:**

- Mean throughput with standard deviation error bars
- Side-by-side comparison: RainStorm vs Spark
- 4 experiments on x-axis
- Publication-quality formatting

**Time:** ~1 minute

---

## Troubleshooting

### Spark cluster not starting

**Issue:** Workers fail to connect to master

**Solution:**

```bash
# Check master is running
docker exec node1 'jps | grep Master'

# Check worker logs
docker exec node2 'tail ~/spark/logs/spark-*.out'

# Restart cluster
docker exec node1 '~/spark/sbin/stop-all.sh'
docker exec node1 '~/spark/sbin/start-all.sh'
```

### RainStorm experiments timing out

**Issue:** Cluster not responding

**Solution:**

```bash
# Kill any stuck processes
tmux kill-session -t rainstorm
./scripts/common/stop_cluster.sh

# Wait 10 seconds, then restart
sleep 10
./scripts/common/start_cluster.sh
```

### Missing Python dependencies

**Issue:** `ModuleNotFoundError: No module named 'pandas'`

**Solution:**

```bash
pip3 install pandas matplotlib numpy
```

---

## Expected Results

Based on preliminary testing:

| Experiment | Dataset | App | RainStorm | Spark | Winner |
|------------|---------|-----|-----------|-------|--------|
| 1 | Small | FilterCount | ~100 tuples/sec | ~150 tuples/sec | Spark |
| 2 | Small | FilterTransform | ~120 tuples/sec | ~180 tuples/sec | Spark |
| 3 | Large | FilterCount | ~100 tuples/sec | ~200 tuples/sec | Spark |
| 4 | Large | FilterTransform | ~120 tuples/sec | ~250 tuples/sec | Spark |

**Note:** Spark is expected to outperform RainStorm due to:

- Optimized in-memory processing
- JVM optimizations
- Production-grade implementation
- No exactly-once overhead (our experiments run without failures)

**For Report:** Focus on:

1. RainStorm demonstrates functional correctness (all demo tests pass)
2. RainStorm successfully implements exactly-once semantics (Spark baseline doesn't)
3. RainStorm autoscaling works (Spark baseline doesn't show this)
4. Spark's higher throughput reflects maturity (10+ years development)
5. RainStorm's performance is respectable for a semester project

---

## Report Integration

### Measurements Section

**Required Elements:**

1. **Two Datasets:**
   - Dataset 1: synthetic_small.csv (50K rows, ~100MB)
   - Dataset 2: synthetic_large.csv (250K rows, ~500MB)

2. **Two Applications per Dataset:**
   - App 1: Filter + Count (grep pattern, count by column)
   - App 2: Filter + Transform (grep pattern, extract fields 1-3)

3. **Plots:**
   - Include `throughput_comparison.pdf` (main figure)
   - Include `elapsed_time_comparison.pdf` (supporting figure)

4. **Statistics:**
   - Reference `summary_table.csv` for mean ± std dev values
   - Run each experiment 3+ times (done automatically)

5. **Discussion:**
   - Compare trends between small and large datasets
   - Analyze performance differences between applications
   - Explain why Spark outperforms (maturity, optimizations)
   - Highlight RainStorm's strengths (exactly-once, autoscaling)

---

## Quick Reference Commands

```bash
# Generate datasets
python3 scripts/experiments/generate_datasets.py

# Deploy Spark
./scripts/spark/deploy_spark.sh

# Upload datasets
./scripts/experiments/upload_datasets.sh

# Run all experiments
./scripts/experiments/run_experiments.sh

# Generate plots
python3 scripts/experiments/plot_results.py

# View results
ls -lh experiment_results/
ls -lh plots/

# Stop Spark cluster (when done)
docker exec node1 '~/spark/sbin/stop-all.sh'
```

---

## Timeline for Completion

**Recommended Schedule:**

- **Friday (Dec 6):**
  - ✅ Generate datasets (10 min)
  - ✅ Deploy Spark (15 min)
  - ✅ Upload datasets (10 min)
  - Start experiments (run overnight)

- **Saturday (Dec 7):**
  - Complete experiments (morning)
  - Generate plots (5 min)
  - Write report (4-6 hours)
  - Review and revise report

- **Sunday (Dec 8) - DEADLINE:**
  - Final report polish
  - Submit by 11:59 PM
  - Demo Monday morning

---

## Files Created by This Guide

```
distributed-stream-processor/
├── data/
│   ├── synthetic_small.csv       # Generated
│   └── synthetic_large.csv       # Generated
├── spark_apps/
│   ├── filter_count.py          # Created
│   └── filter_transform.py      # Created
├── experiment_results/
│   ├── exp1_*.csv               # Generated by experiments
│   ├── exp2_*.csv
│   ├── exp3_*.csv
│   └── exp4_*.csv
├── plots/
│   ├── throughput_comparison.pdf  # For report
│   ├── elapsed_time_comparison.pdf
│   └── summary_table.csv
└── scripts/
    ├── spark/
    │   └── deploy_spark.sh       # Created
    └── experiments/
        ├── generate_datasets.py  # Created
        ├── upload_datasets.sh    # Created
        ├── run_experiments.sh    # Created
        └── plot_results.py       # Created
```

---

**Last Updated:** December 6, 2025
**Status:** Ready to run experiments
**Next Step:** `python3 scripts/experiments/generate_datasets.py`
