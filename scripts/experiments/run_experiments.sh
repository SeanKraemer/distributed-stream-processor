#!/bin/bash
# Run all MP4 experiments (RainStorm vs Spark)
#
# Runs 4 experiments:
#   1. Dataset1 (small) × App1 (filter+count)
#   2. Dataset1 (small) × App2 (filter+transform)
#   3. Dataset2 (large) × App1 (filter+count)
#   4. Dataset2 (large) × App2 (filter+transform)
#
# Each experiment runs 3 times for statistical analysis
#
# Usage:
#   ./scripts/experiments/run_experiments.sh

# Don't use set -e because we handle errors in experiment functions

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

RESULTS_DIR="$PROJECT_ROOT/experiment_results"
mkdir -p "$RESULTS_DIR"

echo "🔬 MP4 Experiment Suite"
echo "======================="
echo ""

# Clean up any stale processes from previous runs
echo "🧹 Cleaning up stale processes..."
tmux kill-session -t rainstorm 2>/dev/null || true
docker exec node1 "pkill -f spark-submit; pkill -f pyspark" 2>/dev/null || true
sleep 5

echo "Running 4 experiments × 3 repetitions × 2 systems = 24 runs"
echo "Results will be saved to: $RESULTS_DIR"
echo ""

# Experiment configurations
DATASETS=("synthetic_medium.csv" "synthetic_large.csv")
DATASET_NAMES=("Small_5K" "Large_10K")
DATASET_SIZES=(5000 10000)  # Input row counts for throughput calculation
APPS=("filter_count" "filter_transform")
APP_NAMES=("FilterCount" "FilterTransform")
PATTERNS=("STOP" "SIGN_REGULATORY")
COLUMNS=(4 4)  # Column for counting (App1 only)

# Number of repetitions for statistical analysis
REPETITIONS=3

# Function to run RainStorm experiment
run_rainstorm() {
    local dataset=$1
    local app=$2
    local pattern=$3
    local column=$4
    local run_num=$5
    local output_file=$6
    local input_size=$7  # Input row count for throughput calculation

    echo "   🌧️  RainStorm: Starting run $run_num..."

    # Start cluster
    "$PROJECT_ROOT/scripts/common/start_cluster.sh" > /dev/null 2>&1

    # Wait for cluster to fully stabilize (matching run_test.sh timing)
    echo "      Waiting for cluster to stabilize (25s)..."
    sleep 25

    # Upload dataset to HyDFS
    echo "      Uploading $dataset to HyDFS..."
    docker exec node1 "
        cd /app
        echo \"create data/$dataset $dataset\" | nc localhost 8003
    " 2>&1 | grep -E "^OK|^ERR" | head -1

    # Wait for replication to complete (matching run_test.sh timing)
    echo "      Waiting for replication (10s)..."
    sleep 10

    # Run application using Go CLI (matching demo test approach)
    echo "      Starting RainStorm job..."
    local start_time=$(date +%s.%N)

    if [ "$app" == "filter_count" ]; then
        docker exec node1 "
            cd /app
            go run cmd/cli/main.go 2 3 grep --pattern=\"$pattern\" --column=$column count $dataset output_exp.txt false false 0 0 0
        " > /dev/null 2>&1
    else
        # Transform application (no column parameter)
        docker exec node1 "
            cd /app
            go run cmd/cli/main.go 2 3 grep --pattern=\"$pattern\" transform $dataset output_exp.txt false false 0 0 0
        " > /dev/null 2>&1
    fi

    # Wait for processing to complete (matching run_test.sh timing)
    local wait_time=45  # Small dataset: ~5K rows (run_test.sh waits 45s for tests 0-2)
    if [[ "$dataset" == *"large"* ]]; then
        wait_time=60  # Large dataset: ~10K rows (run_test.sh waits 60s for test 3)
    fi
    echo "      Waiting for processing (${wait_time}s)..."
    sleep $wait_time

    local end_time=$(date +%s.%N)
    local elapsed=$(echo "$end_time - $start_time" | bc)

    # Get output line count from HyDFS
    echo "      Retrieving results..."
    local line_count=$(docker exec node1 "
        cd /app
        # Merge replicas first (matching run_test.sh behavior)
        echo \"merge output_exp.txt\" | nc localhost 8003 > /dev/null 2>&1
        sleep 2
        # Get the merged file
        echo \"get output_exp.txt output_exp.txt\" | nc localhost 8003 > /dev/null 2>&1
        sleep 2
        if [ -f hydfs_local/output_exp.txt ]; then
            wc -l < hydfs_local/output_exp.txt
        else
            echo 0
        fi
    " 2>/dev/null | tail -1 | tr -d ' ')

    # Default to 0 if empty
    line_count=${line_count:-0}

    # Calculate throughput based on INPUT tuples processed (not output lines)
    local throughput=$(echo "scale=2; $input_size / $elapsed" | bc)

    # Save result (input_tuples, output_lines, throughput)
    echo "$run_num,$elapsed,$input_size,$line_count,$throughput" >> "$output_file"

    echo "      ✅ Completed in ${elapsed}s (${throughput} tuples/sec, $line_count output lines)"

    # Stop cluster and wait for full cleanup
    echo "      Stopping cluster..."
    tmux kill-session -t rainstorm 2>/dev/null || true
    sleep 15  # Give VMs time to fully terminate processes
}

# Function to run Spark experiment
run_spark() {
    local dataset=$1
    local app=$2
    local pattern=$3
    local column=$4
    local run_num=$5
    local output_file=$6
    local input_size=$7  # Input row count for throughput calculation

    echo "   ⚡ Spark: Starting run $run_num..."

    # NOTE: Spark workers should be started MANUALLY before running this script
    # See demo_command_snippets_mp4.txt for worker startup commands

    # Just verify master is running
    docker exec node1 "
        /opt/spark/sbin/start-master.sh > /dev/null 2>&1 || true
    " 2>/dev/null
    sleep 5

    # Run application
    local start_time=$(date +%s.%N)

    if [ "$app" == "filter_count" ]; then
        docker exec node1 "
            cd /app
            /opt/spark/bin/spark-submit \
                --master spark://node1:7077 \
                --deploy-mode client \
                spark_apps/filter_count.py \
                data/$dataset $pattern $column spark_output_${run_num}
        " > /dev/null 2>&1
    else
        docker exec node1 "
            cd /app
            /opt/spark/bin/spark-submit \
                --master spark://node1:7077 \
                --deploy-mode client \
                spark_apps/filter_transform.py \
                data/$dataset $pattern spark_output_${run_num}
        " > /dev/null 2>&1
    fi

    local end_time=$(date +%s.%N)
    local elapsed=$(echo "$end_time - $start_time" | bc)

    # Get output line count
    local line_count=$(docker exec node1 "
        if [ -f /app/spark_output_${run_num}/spark_output.txt ]; then
            wc -l < /app/spark_output_${run_num}/spark_output.txt
        else
            echo 0
        fi
    " 2>/dev/null | tr -d ' ')

    # Default to 0 if empty
    line_count=${line_count:-0}

    # Calculate throughput based on INPUT tuples processed (not output lines)
    local throughput=$(echo "scale=2; $input_size / $elapsed" | bc)

    # Save result (input_tuples, output_lines, throughput)
    echo "$run_num,$elapsed,$input_size,$line_count,$throughput" >> "$output_file"

    echo "      ✅ Completed in ${elapsed}s (${throughput} tuples/sec, $line_count output lines)"

    # NOTE: Workers stay running across all experiments
    # Kill only the spark-submit job processes
    docker exec node1 "
        pkill -f spark-submit
        pkill -f pyspark
    " 2>/dev/null || true

    sleep 2
}

# Run all experiments
experiment_num=0
for dataset_idx in 0 1; do
    dataset="${DATASETS[$dataset_idx]}"
    dataset_name="${DATASET_NAMES[$dataset_idx]}"
    input_size="${DATASET_SIZES[$dataset_idx]}"

    for app_idx in 0 1; do
        app="${APPS[$app_idx]}"
        app_name="${APP_NAMES[$app_idx]}"
        pattern="${PATTERNS[$app_idx]}"
        column="${COLUMNS[$app_idx]}"

        ((experiment_num++))

        echo ""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "Experiment $experiment_num: $dataset_name × $app_name"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

        # Create result files
        rainstorm_results="$RESULTS_DIR/exp${experiment_num}_rainstorm_${dataset_name}_${app_name}.csv"
        spark_results="$RESULTS_DIR/exp${experiment_num}_spark_${dataset_name}_${app_name}.csv"

        echo "run,elapsed_sec,input_tuples,output_lines,throughput_tuples_per_sec" > "$rainstorm_results"
        echo "run,elapsed_sec,input_tuples,output_lines,throughput_tuples_per_sec" > "$spark_results"

        # Run repetitions
        for rep in $(seq 1 $REPETITIONS); do
            echo ""
            echo "📊 Repetition $rep / $REPETITIONS"
            echo "────────────────────────────────"

            # Run RainStorm
            run_rainstorm "$dataset" "$app" "$pattern" "$column" "$rep" "$rainstorm_results" "$input_size"

            # Run Spark
            run_spark "$dataset" "$app" "$pattern" "$column" "$rep" "$spark_results" "$input_size"
        done

        echo ""
        echo "✅ Experiment $experiment_num complete!"
    done
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🎉 All experiments complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📊 Results saved to: $RESULTS_DIR"
echo ""
echo "📌 Next Steps:"
echo "   1. Generate plots:"
echo "      python3 scripts/experiments/plot_results.py"
echo "   2. Review plots in: plots/"
echo ""
