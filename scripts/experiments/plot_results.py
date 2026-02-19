#!/usr/bin/env python3
"""
Plot MP4 experiment results comparing RainStorm vs Spark

Generates bar charts showing:
- Throughput (tuples/sec) for each experiment
- Mean/median with standard deviation error bars
- 4 experiments: 2 datasets × 2 applications
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def load_results(results_dir):
    """Load all experiment results"""
    experiments = []

    for exp_num in range(1, 5):
        # Find result files for this experiment
        rainstorm_files = list(Path(results_dir).glob(f"exp{exp_num}_rainstorm_*.csv"))
        spark_files = list(Path(results_dir).glob(f"exp{exp_num}_spark_*.csv"))

        if not rainstorm_files or not spark_files:
            print(f"⚠️  Warning: Missing results for experiment {exp_num}")
            continue

        # Load data
        rainstorm_df = pd.read_csv(rainstorm_files[0])
        spark_df = pd.read_csv(spark_files[0])

        # Extract experiment info from filename
        # Format: exp1_rainstorm_Small_5K_FilterCount.csv
        filename = rainstorm_files[0].stem
        parts = filename.split('_')
        dataset = f"{parts[2]}_{parts[3]}"  # e.g., "Small_5K"
        app = parts[4]  # e.g., "FilterCount"

        # Map application names to descriptive labels
        app_labels = {
            'FilterCount': 'Filter+Count\n(grep+count)',
            'FilterTransform': 'Filter+Transform\n(grep+replace)'
        }
        app_label = app_labels.get(app, app)

        experiments.append({
            'exp_num': exp_num,
            'dataset': dataset,
            'app': app,
            'label': f"{dataset}\n{app_label}",
            'rainstorm': rainstorm_df,
            'spark': spark_df
        })

    return experiments

def plot_throughput_comparison(experiments, output_dir):
    """Generate throughput comparison bar chart"""
    print("📊 Generating throughput comparison plot...")

    fig, ax = plt.subplots(figsize=(12, 6))

    x = np.arange(len(experiments))
    width = 0.35

    rainstorm_means = []
    rainstorm_stds = []
    spark_means = []
    spark_stds = []
    labels = []

    for exp in experiments:
        rainstorm_means.append(exp['rainstorm']['throughput_tuples_per_sec'].mean())
        rainstorm_stds.append(exp['rainstorm']['throughput_tuples_per_sec'].std())
        spark_means.append(exp['spark']['throughput_tuples_per_sec'].mean())
        spark_stds.append(exp['spark']['throughput_tuples_per_sec'].std())
        labels.append(exp['label'])

    # Plot bars
    bars1 = ax.bar(x - width/2, rainstorm_means, width, yerr=rainstorm_stds,
                   label='RainStorm', color='#3498db', alpha=0.8, capsize=5)
    bars2 = ax.bar(x + width/2, spark_means, width, yerr=spark_stds,
                   label='Spark Streaming', color='#e74c3c', alpha=0.8, capsize=5)

    # Customize plot
    ax.set_xlabel('Experiment (Dataset × Application)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Throughput (tuples/sec)', fontsize=12, fontweight='bold')
    ax.set_title('RainStorm vs Spark Streaming: Throughput Comparison', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3, linestyle='--')

    # Add value labels on top of bars (above error bars)
    for i, bars in enumerate([bars1, bars2]):
        std_values = rainstorm_stds if i == 0 else spark_stds
        for j, bar in enumerate(bars):
            height = bar.get_height()
            # Position label above the error bar with extra offset
            label_y = height + std_values[j] + (max(rainstorm_means + spark_means) * 0.02)
            ax.text(bar.get_x() + bar.get_width()/2., label_y,
                   f'{height:.1f}',
                   ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    output_file = f"{output_dir}/throughput_comparison.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"   ✅ Saved: {output_file}")

    # Also save as PDF for report
    output_pdf = f"{output_dir}/throughput_comparison.pdf"
    plt.savefig(output_pdf, bbox_inches='tight')
    print(f"   ✅ Saved: {output_pdf}")

    plt.close()

def plot_elapsed_time_comparison(experiments, output_dir):
    """Generate elapsed time comparison bar chart"""
    print("📊 Generating elapsed time comparison plot...")

    fig, ax = plt.subplots(figsize=(12, 6))

    x = np.arange(len(experiments))
    width = 0.35

    rainstorm_means = []
    rainstorm_stds = []
    spark_means = []
    spark_stds = []
    labels = []

    for exp in experiments:
        rainstorm_means.append(exp['rainstorm']['elapsed_sec'].mean())
        rainstorm_stds.append(exp['rainstorm']['elapsed_sec'].std())
        spark_means.append(exp['spark']['elapsed_sec'].mean())
        spark_stds.append(exp['spark']['elapsed_sec'].std())
        labels.append(exp['label'])

    # Plot bars
    bars1 = ax.bar(x - width/2, rainstorm_means, width, yerr=rainstorm_stds,
                   label='RainStorm', color='#3498db', alpha=0.8, capsize=5)
    bars2 = ax.bar(x + width/2, spark_means, width, yerr=spark_stds,
                   label='Spark Streaming', color='#e74c3c', alpha=0.8, capsize=5)

    # Customize plot
    ax.set_xlabel('Experiment (Dataset × Application)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Elapsed Time (seconds)', fontsize=12, fontweight='bold')
    ax.set_title('RainStorm vs Spark Streaming: Processing Time Comparison', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3, linestyle='--')

    # Add value labels on top of bars (above error bars)
    for i, bars in enumerate([bars1, bars2]):
        std_values = rainstorm_stds if i == 0 else spark_stds
        for j, bar in enumerate(bars):
            height = bar.get_height()
            # Position label above the error bar with extra offset
            label_y = height + std_values[j] + (max(rainstorm_means + spark_means) * 0.02)
            ax.text(bar.get_x() + bar.get_width()/2., label_y,
                   f'{height:.1f}s',
                   ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    output_file = f"{output_dir}/elapsed_time_comparison.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"   ✅ Saved: {output_file}")

    # Also save as PDF for report
    output_pdf = f"{output_dir}/elapsed_time_comparison.pdf"
    plt.savefig(output_pdf, bbox_inches='tight')
    print(f"   ✅ Saved: {output_pdf}")

    plt.close()

def generate_summary_table(experiments, output_dir):
    """Generate summary table of results"""
    print("📊 Generating summary table...")

    data = []
    for exp in experiments:
        data.append({
            'Experiment': exp['label'].replace('\n', ' '),
            'RainStorm Mean (tuples/sec)': f"{exp['rainstorm']['throughput_tuples_per_sec'].mean():.2f}",
            'RainStorm Std Dev': f"{exp['rainstorm']['throughput_tuples_per_sec'].std():.2f}",
            'Spark Mean (tuples/sec)': f"{exp['spark']['throughput_tuples_per_sec'].mean():.2f}",
            'Spark Std Dev': f"{exp['spark']['throughput_tuples_per_sec'].std():.2f}",
            'Winner': 'RainStorm' if exp['rainstorm']['throughput_tuples_per_sec'].mean() > exp['spark']['throughput_tuples_per_sec'].mean() else 'Spark'
        })

    df = pd.DataFrame(data)

    # Save as CSV
    output_file = f"{output_dir}/summary_table.csv"
    df.to_csv(output_file, index=False)
    print(f"   ✅ Saved: {output_file}")

    # Print to console
    print("\n📋 Results Summary:")
    print("=" * 100)
    print(df.to_string(index=False))
    print("=" * 100)

def main():
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    results_dir = project_root / "experiment_results"
    plots_dir = project_root / "plots"

    # Create plots directory
    plots_dir.mkdir(exist_ok=True)

    print("📊 MP4 Results Plotting Script")
    print("=" * 60)
    print(f"Results directory: {results_dir}")
    print(f"Plots directory:   {plots_dir}")
    print()

    # Check if results exist
    if not results_dir.exists():
        print(f"❌ Error: Results directory not found: {results_dir}")
        print("   Run experiments first: ./scripts/experiments/run_experiments.sh")
        sys.exit(1)

    # Load results
    print("📂 Loading experiment results...")
    experiments = load_results(results_dir)

    if not experiments:
        print("❌ Error: No experiment results found")
        sys.exit(1)

    print(f"   ✅ Loaded {len(experiments)} experiments")
    print()

    # Generate plots
    plot_throughput_comparison(experiments, plots_dir)
    plot_elapsed_time_comparison(experiments, plots_dir)
    generate_summary_table(experiments, plots_dir)

    print()
    print("=" * 60)
    print("✅ All plots generated successfully!")
    print()
    print(f"📁 Plots saved to: {plots_dir}")
    print("   - throughput_comparison.png/pdf")
    print("   - elapsed_time_comparison.png/pdf")
    print("   - summary_table.csv")
    print()
    print("📌 Next Steps:")
    print("   1. Review plots and include in report")
    print("   2. Write report sections (design, measurements, discussion)")
    print()

if __name__ == "__main__":
    main()
