import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import glob

def load_csv_files(data_directory="."):
    """
    Load all CSV files matching the pattern *latest.csv or *-*.csv
    Returns a dictionary with platform names as keys and DataFrames as values
    """
    csv_files = {}

    # Look for CSV files that might be your platform data
    file_patterns = ["*latest.csv", "*-*.csv", "*.csv"]

    for pattern in file_patterns:
        files = glob.glob(f"{data_directory}/{pattern}")
        for file_path in files:
            file_name = Path(file_path).stem  # Get filename without extension
            try:
                df = pd.read_csv(file_path)
                # Add chunks column
                df['chunks'] = df['bytes_written'] / (128**3)
                csv_files[file_name] = df
                print(f"Loaded {file_name}: {len(df)} rows")
            except Exception as e:
                print(f"Error loading {file_path}: {e}")

    return csv_files

def examine_data_structure(csv_files):
    """
    Print basic information about the loaded data
    """
    for platform, df in csv_files.items():
        print(f"\n=== {platform} ===")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"Chunks range: {df['chunks'].min():.2f} to {df['chunks'].max():.2f}")
        print(f"Sample data:")
        print(df.head(3))

def plot_single_platform_comparison(df, platform_name):
    """
    Create a plot comparing consolidated and vectorized methods for a single platform
    """
    # Focus on the two good methods
    method_cols = ['consolidated_time', 'vectorized_time']

    plt.figure(figsize=(10, 6))

    for method in method_cols:
        # Clean method name for legend
        method_name = method.replace('_time', '').capitalize()
        plt.plot(df['chunks'], df[method], marker='o', label=method_name, alpha=0.7, linewidth=2)

    plt.xlabel('Chunks (bytes_written / 128³)')
    plt.ylabel('Runtime (units)')
    plt.title(f'Consolidated vs Vectorized - {platform_name}')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    return plt.gcf()

def plot_method_across_platforms(csv_files, method='consolidated_time'):
    """
    Compare a single method across all platforms
    """
    plt.figure(figsize=(12, 6))

    method_name = method.replace('_time', '').capitalize()

    for platform, df in csv_files.items():
        plt.plot(df['chunks'], df[method], marker='o', label=platform, alpha=0.7)

    plt.xlabel('Chunks (bytes_written / 128³)')
    plt.ylabel('Runtime (units)')
    plt.title(f'{method_name} Method - Platform Comparison')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    return plt.gcf()

def calculate_performance_ratios(csv_files):
    """
    Calculate performance ratios using consolidated method as baseline
    Returns dictionary with platform DataFrames containing ratio columns
    """
    ratio_data = {}

    for platform, df in csv_files.items():
        df_ratios = df.copy()

        # Calculate ratio for vectorized vs consolidated (smaller ratio = better performance)
        df_ratios['vectorized_ratio'] = df['vectorized_time'] / df['consolidated_time']

        ratio_data[platform] = df_ratios

    return ratio_data

def generate_statistical_summary(csv_files):
    """
    Generate statistical summaries for consolidated and vectorized methods
    """
    methods = ['consolidated_time', 'vectorized_time']

    print("\n" + "="*80)
    print("STATISTICAL SUMMARY (Runtime) - Consolidated vs Vectorized")
    print("="*80)

    summary_data = []

    for platform, df in csv_files.items():
        print(f"\n{platform.upper()}")
        print("-" * len(platform))

        for method in methods:
            method_name = method.replace('_time', '').capitalize()
            stats = df[method].describe()

            print(f"{method_name:12} | Mean: {stats['mean']:7.1f} | Median: {stats['50%']:7.1f} | "
                  f"Std: {stats['std']:7.1f} | Min: {stats['min']:7.1f} | Max: {stats['max']:7.1f}")

            # Store for later analysis
            summary_data.append({
                'platform': platform,
                'method': method_name,
                'mean': stats['mean'],
                'median': stats['50%'],
                'std': stats['std'],
                'min': stats['min'],
                'max': stats['max']
            })

    return pd.DataFrame(summary_data)

def generate_ratio_summary(ratio_data):
    """
    Generate summary statistics for vectorized vs consolidated performance ratios
    """
    print("\n" + "="*80)
    print("VECTORIZED vs CONSOLIDATED PERFORMANCE RATIOS")
    print("Ratio < 1.0 = vectorized faster, > 1.0 = consolidated faster")
    print("="*80)

    ratio_summary = []

    for platform, df in ratio_data.items():
        stats = df['vectorized_ratio'].describe()

        # Calculate percentage faster/slower
        mean_pct = (stats['mean'] - 1) * 100
        median_pct = (stats['50%'] - 1) * 100

        print(f"\n{platform.upper()}")
        print("-" * len(platform))
        print(f"Mean ratio:    {stats['mean']:5.2f}x ({mean_pct:+5.1f}%)")
        print(f"Median ratio:  {stats['50%']:5.2f}x ({median_pct:+5.1f}%)")
        print(f"Std deviation: {stats['std']:5.2f}")
        print(f"Min ratio:     {stats['min']:5.2f}x (best vectorized performance)")
        print(f"Max ratio:     {stats['max']:5.2f}x (worst vectorized performance)")

        ratio_summary.append({
            'platform': platform,
            'mean_ratio': stats['mean'],
            'median_ratio': stats['50%'],
            'mean_pct_change': mean_pct,
            'median_pct_change': median_pct,
            'std': stats['std'],
            'min_ratio': stats['min'],
            'max_ratio': stats['max']
        })

    return pd.DataFrame(ratio_summary)

def plot_statistical_comparison(summary_df):
    """
    Create clean statistical comparison plots for consolidated vs vectorized
    """
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))

    # Mean runtime by platform and method
    pivot_mean = summary_df.pivot(index='platform', columns='method', values='mean')
    pivot_mean.plot(kind='bar', ax=ax1, rot=45, color=['#1f77b4', '#ff7f0e'])
    ax1.set_title('Mean Runtime: Consolidated vs Vectorized')
    ax1.set_ylabel('Mean Runtime (units)')
    ax1.legend(title='Method')
    ax1.grid(True, alpha=0.3)

    # Median runtime by platform and method
    pivot_median = summary_df.pivot(index='platform', columns='method', values='median')
    pivot_median.plot(kind='bar', ax=ax2, rot=45, color=['#1f77b4', '#ff7f0e'])
    ax2.set_title('Median Runtime: Consolidated vs Vectorized')
    ax2.set_ylabel('Median Runtime (units)')
    ax2.legend(title='Method')
    ax2.grid(True, alpha=0.3)

    # Standard deviation (variability)
    pivot_std = summary_df.pivot(index='platform', columns='method', values='std')
    pivot_std.plot(kind='bar', ax=ax3, rot=45, color=['#1f77b4', '#ff7f0e'])
    ax3.set_title('Runtime Variability (Std Dev)')
    ax3.set_ylabel('Standard Deviation (units)')
    ax3.legend(title='Method')
    ax3.grid(True, alpha=0.3)

    # Efficiency comparison (lower is better)
    pivot_efficiency = summary_df.pivot(index='platform', columns='method', values='mean')
    # Calculate vectorized efficiency as percentage of consolidated
    efficiency_data = []
    for platform in pivot_efficiency.index:
        consolidated_mean = pivot_efficiency.loc[platform, 'Consolidated']
        vectorized_mean = pivot_efficiency.loc[platform, 'Vectorized']
        efficiency_pct = (vectorized_mean / consolidated_mean) * 100
        efficiency_data.append({'Platform': platform, 'Vectorized Efficiency (%)': efficiency_pct})

    efficiency_df = pd.DataFrame(efficiency_data)
    bars = ax4.bar(efficiency_df['Platform'], efficiency_df['Vectorized Efficiency (%)'],
                   color='#2ca02c', alpha=0.7)
    ax4.axhline(y=100, color='red', linestyle='--', alpha=0.7, label='Consolidated baseline (100%)')
    ax4.set_title('Vectorized Efficiency vs Consolidated')
    ax4.set_ylabel('Vectorized as % of Consolidated\n(Lower = Better)')
    ax4.set_xticklabels(efficiency_df['Platform'], rotation=45)
    ax4.legend()
    ax4.grid(True, alpha=0.3)

    # Add percentage labels on bars
    for bar, pct in zip(bars, efficiency_df['Vectorized Efficiency (%)']):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height + 1,
                 f'{pct:.0f}%', ha='center', va='bottom', fontsize=10)

    plt.tight_layout()
    return fig

def plot_ratio_comparison(ratio_summary_df):
    """
    Create vectorized vs consolidated performance visualization
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # Performance ratios
    platforms = ratio_summary_df['platform']
    ratios = ratio_summary_df['mean_ratio']

    bars1 = ax1.bar(platforms, ratios, color=['#2ca02c' if r < 1 else '#d62728' for r in ratios], alpha=0.7)
    ax1.axhline(y=1.0, color='black', linestyle='--', alpha=0.7, label='Equal performance')
    ax1.set_title('Vectorized vs Consolidated Performance Ratios')
    ax1.set_ylabel('Ratio (Vectorized / Consolidated)')
    ax1.set_xticklabels(platforms, rotation=45)
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Add ratio labels on bars
    for bar, ratio in zip(bars1, ratios):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                 f'{ratio:.2f}x', ha='center', va='bottom', fontsize=10, weight='bold')

    # Percentage improvements
    pct_changes = ratio_summary_df['mean_pct_change']
    bars2 = ax2.bar(platforms, pct_changes, color=['#2ca02c' if p < 0 else '#d62728' for p in pct_changes], alpha=0.7)
    ax2.axhline(y=0, color='black', linestyle='--', alpha=0.7, label='No change')
    ax2.set_title('Vectorized Performance Change vs Consolidated')
    ax2.set_ylabel('Percentage Change (%)')
    ax2.set_xticklabels(platforms, rotation=45)
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # Add percentage labels on bars
    for bar, pct in zip(bars2, pct_changes):
        height = bar.get_height()
        label_y = height + (1 if height >= 0 else -3)
        ax2.text(bar.get_x() + bar.get_width()/2., label_y,
                 f'{pct:+.1f}%', ha='center', va='bottom' if height >= 0 else 'top',
                 fontsize=10, weight='bold')

    plt.tight_layout()
    return fig

def create_summary_plots(csv_files):
    """
    Generate a set of summary plots and statistical analysis
    """
    # Calculate performance ratios
    print("Calculating performance ratios...")
    ratio_data = calculate_performance_ratios(csv_files)

    # Generate statistical summaries
    summary_df = generate_statistical_summary(csv_files)
    ratio_summary_df = generate_ratio_summary(ratio_data)

    # Create clean statistical plots
    print("\n" + "="*50)
    print("CREATING STATISTICAL VISUALIZATIONS")
    print("="*50)

    # Statistical comparison plot
    fig1 = plot_statistical_comparison(summary_df)
    plt.savefig('statistical_summary.png', dpi=300, bbox_inches='tight')
    plt.show()

    # Ratio comparison plot
    fig2 = plot_ratio_comparison(ratio_summary_df)
    plt.savefig('performance_ratios.png', dpi=300, bbox_inches='tight')
    plt.show()

    # Save summary data to CSV for further analysis
    summary_df.to_csv('consolidated_vectorized_summary.csv', index=False)
    ratio_summary_df.to_csv('vectorized_vs_consolidated_ratios.csv', index=False)

    print(f"\nSummary data saved to:")
    print(f"- consolidated_vectorized_summary.csv")
    print(f"- vectorized_vs_consolidated_ratios.csv")
    print(f"- statistical_summary.png")
    print(f"- performance_ratios.png")

    return summary_df, ratio_summary_df

if __name__ == "__main__":
    # Load all CSV files
    data = load_csv_files()

    if not data:
        print("No CSV files found! Make sure your CSV files are in the current directory.")
        print("Expected files like: platform.csv")
    else:
        examine_data_structure(data)
        print(f"\nFound {len(data)} platform datasets")

        # Create visualizations
        print("\n" + "="*50)
        print("CREATING VISUALIZATIONS")
        print("="*50)
        create_summary_plots(data)