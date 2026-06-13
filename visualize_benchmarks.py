import matplotlib.pyplot as plt
import os

# Define the target records based on your data-generator setup
TARGET_RECORDS = 50000

def read_metrics(filename):
    if not os.path.exists(filename):
        print(f"Error: {filename} not found.")
        exit(1)
        
    with open(filename, 'r') as f:
        for line in f:
            # Skip the GNU 'time' termination warning text
            if "Command terminated by signal" in line:
                continue
            
            # Process the actual comma-separated metrics
            row = line.strip().split(',')
            if len(row) >= 4:
                wall_time = float(row[0])
                mem_mb = float(row[1]) / 1024.0
                cpu_pct = float(row[2].replace('%', ''))
                ctx_switches = int(row[3])
                return wall_time, mem_mb, cpu_pct, ctx_switches
                
    print(f"Error: No valid numeric data found in {filename}")
    exit(1)

# Read the dynamically generated CSV files
go_time, go_mem, go_cpu, go_ctx = read_metrics('go_metrics.csv')
rust_time, rust_mem, rust_cpu, rust_ctx = read_metrics('rust_metrics.csv')

# Calculate Throughput (Rows per second)
go_tps = TARGET_RECORDS / go_time
rust_tps = TARGET_RECORDS / rust_time

# Prepare the data arrays for plotting
languages = ['Golang', 'Rust']
throughput = [go_tps, rust_tps]
peak_memory = [go_mem, rust_mem]
cpu_utilization = [go_cpu, rust_cpu]
context_switches = [go_ctx, rust_ctx]

# ==========================================
# PLOT CONFIGURATION
# ==========================================
fig, axs = plt.subplots(2, 2, figsize=(12, 10))
fig.suptitle(f'Automated Benchmark: Go vs. Rust ({TARGET_RECORDS:,} Records)', fontsize=15, fontweight='bold', y=0.98)

colors = ['#00ADD8', '#CE412B']

def add_labels(ax, format_str):
    for p in ax.patches:
        val = p.get_height()
        ax.annotate(format_str.format(val), 
                    (p.get_x() + p.get_width() / 2., val),
                    ha='center', va='bottom', fontsize=11, fontweight='bold', 
                    xytext=(0, 5), textcoords='offset points')

# Chart 1: Throughput
axs[0, 0].bar(languages, throughput, color=colors, width=0.5)
axs[0, 0].set_title('Throughput (Higher is Better)', fontsize=13)
axs[0, 0].set_ylabel('Rows / Second', fontsize=11)
axs[0, 0].set_ylim(0, max(throughput) * 1.25)
add_labels(axs[0, 0], '{:,.0f}')

# Chart 2: Peak Memory
axs[0, 1].bar(languages, peak_memory, color=colors, width=0.5)
axs[0, 1].set_title('Peak Memory Footprint (Max RSS)', fontsize=13)
axs[0, 1].set_ylabel('Megabytes (Lower is Better)', fontsize=11)
axs[0, 1].set_ylim(0, max(peak_memory) * 1.25)
add_labels(axs[0, 1], '{:,.1f}') 

# Chart 3: CPU Utilization
axs[1, 0].bar(languages, cpu_utilization, color=colors, width=0.5)
axs[1, 0].set_title('CPU Utilization', fontsize=13)
axs[1, 0].set_ylabel('Percentage (%)', fontsize=11)
axs[1, 0].set_ylim(0, max(cpu_utilization) * 1.25)
add_labels(axs[1, 0], '{:,.1f}')

# Chart 4: Context Switches
axs[1, 1].bar(languages, context_switches, color=colors, width=0.5)
axs[1, 1].set_title('Voluntary Context Switches', fontsize=13)
axs[1, 1].set_ylabel('Count (Lower is Better)', fontsize=11)
axs[1, 1].set_ylim(0, max(context_switches) * 1.25)
add_labels(axs[1, 1], '{:,.0f}')

# EXPORT
plt.tight_layout(pad=3.0)
for ax in axs.flat:
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(axis='y', linestyle='--', alpha=0.7)

image_filename = 'automated_benchmark_results.png'
plt.savefig(image_filename, dpi=300, bbox_inches='tight')
print(f"✅ Success! Benchmark visualization saved as '{image_filename}'.")