import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys

def parse_resource_sms(resource_str):
    """Extract SM count from resource string like '4g.20gb' -> 4"""
    return int(resource_str.split('g.')[0])

def load_and_process_data(filename):
    """Load data from file and process it for visualization"""
    # Read the CSV file
    df = pd.read_csv(filename, names=['pod_name', 'resource', 'start_time', 'duration'])
    
    # Extract SM count from resource string
    df['sms'] = df['resource'].apply(parse_resource_sms)
    
    return df

def get_max_required_gpus(df,time_points):
    required_gpus_count = []
    for time in time_points:
        
        # Find all active pods at this time
        active_pods = df[(df['start_time'] <= time) & (df['start_time'] + df['duration'] > time)]
        total_sms = active_pods['sms'].sum()

        # Calculate GPU metrics
        required_gpus = np.ceil(total_sms / 7) if total_sms > 0 else 0
        required_gpus_count.append(required_gpus)
    return max(required_gpus_count)

def create_timeline_data(df, sms_per_gpu=7):
    """Create timeline data showing SM usage over time with GPU occupancy"""
    # Get all unique time points where resources change
    time_points = set()
    for _, row in df.iterrows():
        end_time = row['start_time'] + row['duration']
        time_points.add(row['start_time'])
        time_points.add(end_time)
    
    time_points = sorted(time_points)
    
    # Calculate SM usage at each time point
    timeline_data = []
    max_required_gpus = get_max_required_gpus(df,time_points)
    for time in time_points:
        # Find all active pods at this time
        active_pods = df[(df['start_time'] <= time) & (df['start_time'] + df['duration'] > time)]
        total_sms = active_pods['sms'].sum()
        active_count = len(active_pods)
        
        # Calculate GPU metrics
        required_gpus = np.ceil(total_sms / sms_per_gpu) if total_sms > 0 else 0
        if required_gpus > 0:
            occupancy_percent = (total_sms / (max_required_gpus * sms_per_gpu)) * 100
        else:
            occupancy_percent = 0
        
        timeline_data.append({
            'time': time,
            'total_sms': total_sms,
            'active_pods': active_count,
            'required_gpus': int(required_gpus),
            'occupancy_percent': occupancy_percent,
            'wasted_sms': int(max_required_gpus * sms_per_gpu - total_sms) if max_required_gpus > 0 else 0
        })
    
    return pd.DataFrame(timeline_data)

def create_step_visualization(timeline_df, df):
    """Create a step-style visualization showing resource usage over time"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(18, 12))
    
    # SM usage over time
    ax1.step(timeline_df['time'], timeline_df['total_sms'], where='post', 
             linewidth=2, color='#2563eb', alpha=0.8)
    ax1.fill_between(timeline_df['time'], timeline_df['total_sms'], 
                     step='post', alpha=0.3, color='#3b82f6')
    
    ax1.set_xlabel('Time', fontsize=12)
    ax1.set_ylabel('Total SM Count', fontsize=12)
    ax1.set_title('GPU Resource Usage Timeline (SM Count Over Time)', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    
    # Required GPUs over time
    ax2.step(timeline_df['time'], timeline_df['required_gpus'], where='post', 
             linewidth=2, color='#dc2626', alpha=0.8)
    ax2.fill_between(timeline_df['time'], timeline_df['required_gpus'], 
                     step='post', alpha=0.3, color='#ef4444')
    
    ax2.set_xlabel('Time', fontsize=12)
    ax2.set_ylabel('Required GPUs', fontsize=12)
    ax2.set_title('Required GPU Count Over Time', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    # GPU Occupancy over time
    ax3.step(timeline_df['time'], timeline_df['occupancy_percent'], where='post', 
             linewidth=2, color='#059669', alpha=0.8)
    ax3.fill_between(timeline_df['time'], timeline_df['occupancy_percent'], 
                     step='post', alpha=0.3, color='#10b981')
    
    ax3.set_xlabel('Time', fontsize=12)
    ax3.set_ylabel('GPU Occupancy (%)', fontsize=12)
    ax3.set_title('GPU Occupancy Percentage Over Time', fontsize=14, fontweight='bold')
    ax3.grid(True, alpha=0.3)
    ax3.set_ylim(0, 100)
    
    # Add horizontal lines for occupancy thresholds
    ax3.axhline(y=80, color='orange', linestyle='--', alpha=0.7, label='80% threshold')
    ax3.axhline(y=60, color='red', linestyle='--', alpha=0.7, label='60% threshold')
    ax3.legend()
    
    # Wasted SMs over time
    ax4.step(timeline_df['time'], timeline_df['wasted_sms'], where='post', 
             linewidth=2, color='#7c2d12', alpha=0.8)
    ax4.fill_between(timeline_df['time'], timeline_df['wasted_sms'], 
                     step='post', alpha=0.3, color='#ea580c')
    
    ax4.set_xlabel('Time', fontsize=12)
    ax4.set_ylabel('Wasted SMs', fontsize=12)
    ax4.set_title('Unused SMs (Resource Waste) Over Time', fontsize=14, fontweight='bold')
    ax4.grid(True, alpha=0.3)
    
    # Add statistics text box
    max_sms = timeline_df['total_sms'].max()
    max_gpus = timeline_df['required_gpus'].max()
    max_time = timeline_df['time'].max()
    active_periods = timeline_df[timeline_df['required_gpus'] > 0]
    avg_occupancy = active_periods['occupancy_percent'].mean() if len(active_periods) > 0 else 0
    total_pods = len(df)
    
    stats_text = f'''Peak Usage: {max_sms} SMs ({max_gpus} GPUs)
Max Time: {max_time} units
Average Occupancy: {avg_occupancy:.1f}%
Total Pods: {total_pods}
SMs per GPU: 7'''
    
    ax1.text(0.02, 0.95, stats_text, transform=ax1.transAxes, fontsize=10, 
             verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    plt.tight_layout()
    return fig

def create_gantt_chart(df):
    """Create a Gantt chart showing individual pod lifecycles"""
    fig, ax = plt.subplots(figsize=(15, 8))
    
    # Color mapping for different SM counts
    sm_colors = {1: '#fef3c7', 2: '#fde68a', 3: '#f59e0b', 4: '#d97706', 7: '#92400e'}
    
    # Sort pods by start time for better visualization
    df_sorted = df.sort_values('start_time')
    
    for i, (_, row) in enumerate(df_sorted.iterrows()):
        end_time = row['start_time'] + row['duration']
        duration = end_time - row['start_time']
        color = sm_colors.get(row['sms'], '#6b7280')
        
        ax.barh(i, duration, left=row['start_time'], height=0.8, 
                color=color, alpha=0.7, edgecolor='black', linewidth=0.5)
        
        # Add pod name
        ax.text(row['start_time'] + duration/2, i, row['pod_name'], 
                ha='center', va='center', fontsize=6)
    
    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel('Pod Index', fontsize=12)
    ax.set_title('Pod Lifecycle Gantt Chart (Color = SM Count)', fontsize=14, fontweight='bold')
    
    # Create legend
    legend_elements = [plt.Rectangle((0,0),1,1, facecolor=sm_colors[sm], alpha=0.7, 
                                   label=f'{sm} SMs') for sm in sorted(sm_colors.keys())]
    ax.legend(handles=legend_elements, loc='upper right')
    
    plt.tight_layout()
    return fig

def create_occupancy_heatmap(timeline_df):
    """Create a heatmap showing occupancy patterns"""
    fig, ax = plt.subplots(figsize=(15, 6))
    
    # Filter out periods with no GPU usage
    active_periods = timeline_df[timeline_df['required_gpus'] > 0].copy()
    
    if len(active_periods) == 0:
        ax.text(0.5, 0.5, 'No GPU usage periods found', ha='center', va='center', transform=ax.transAxes)
        return fig
    
    # Create occupancy categories
    def occupancy_category(occupancy):
        if occupancy >= 80:
            return 'High (≥80%)'
        elif occupancy >= 60:
            return 'Medium (60-79%)'
        else:
            return 'Low (<60%)'
    
    active_periods['occupancy_category'] = active_periods['occupancy_percent'].apply(occupancy_category)
    
    # Create the visualization
    colors = {'High (≥80%)': '#10b981', 'Medium (60-79%)': '#f59e0b', 'Low (<60%)': '#ef4444'}
    
    for i, (_, row) in enumerate(active_periods.iterrows()):
        color = colors[row['occupancy_category']]
        # Create bars for time segments
        if i < len(active_periods) - 1:
            next_time = active_periods.iloc[i+1]['time']
            width = next_time - row['time']
        else:
            width = 1  # Default width for last segment
        
        ax.barh(0, width, left=row['time'], height=0.8, color=color, alpha=0.7, 
                edgecolor='black', linewidth=0.3)
        
        # Add text for significant periods
        if i % max(1, len(active_periods)//15) == 0:  # Show every nth label to avoid crowding
            ax.text(row['time'] + width/2, 0, f"{row['occupancy_percent']:.0f}%", 
                   ha='center', va='center', fontsize=8, fontweight='bold')
    
    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel('GPU Occupancy', fontsize=12)
    ax.set_title('GPU Occupancy Timeline (Color-coded)', fontsize=14, fontweight='bold')
    ax.set_ylim(-0.5, 0.5)
    ax.set_yticks([0])
    ax.set_yticklabels(['Occupancy'])
    
    # Create legend
    legend_elements = [plt.Rectangle((0,0),1,1, facecolor=color, alpha=0.7, label=category) 
                      for category, color in colors.items()]
    ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(1, 1))
    
    plt.tight_layout()
    return fig

def analyze_resource_usage(df, timeline_df):
    """Print detailed analysis of resource usage"""
    print("=== GPU Resource Usage Analysis ===")
    print(f"Total number of pods: {len(df)}")
    print(f"Time range: {df['start_time'].min()} to {(df['duration'] + df['start_time']).max()}")
    print(f"Peak SM usage: {timeline_df['total_sms'].max()}")
    print(f"Average SM usage: {timeline_df['total_sms'].mean():.2f}")
    print(f"Peak concurrent pods: {timeline_df['active_pods'].max()}")
    print(f"Average concurrent pods: {timeline_df['active_pods'].mean():.2f}")
    
    # GPU-specific metrics
    print("\n=== GPU Occupancy Analysis ===")
    active_periods = timeline_df[timeline_df['required_gpus'] > 0]
    if len(active_periods) > 0:
        print(f"Peak GPU requirement: {timeline_df['required_gpus'].max()} GPUs")
        print(f"Average GPU requirement: {active_periods['required_gpus'].mean():.2f} GPUs")
        print(f"Average GPU occupancy: {active_periods['occupancy_percent'].mean():.1f}%")
        print(f"Minimum GPU occupancy: {active_periods['occupancy_percent'].min():.1f}%")
        print(f"Maximum GPU occupancy: {active_periods['occupancy_percent'].max():.1f}%")
        
        # Occupancy distribution
        high_occupancy = len(active_periods[active_periods['occupancy_percent'] >= 80])
        medium_occupancy = len(active_periods[(active_periods['occupancy_percent'] >= 60) & 
                                            (active_periods['occupancy_percent'] < 80)])
        low_occupancy = len(active_periods[active_periods['occupancy_percent'] < 60])
        
        total_active = len(active_periods)
        print(f"\nOccupancy Distribution:")
        print(f"  High (≥80%): {high_occupancy}/{total_active} periods ({high_occupancy/total_active*100:.1f}%)")
        print(f"  Medium (60-79%): {medium_occupancy}/{total_active} periods ({medium_occupancy/total_active*100:.1f}%)")
        print(f"  Low (<60%): {low_occupancy}/{total_active} periods ({low_occupancy/total_active*100:.1f}%)")
        
        # Resource waste analysis
        total_wasted_sms = active_periods['wasted_sms'].sum()
        total_used_sms = active_periods['total_sms'].sum()
        waste_percentage = (total_wasted_sms / (total_used_sms + total_wasted_sms)) * 100 if (total_used_sms + total_wasted_sms) > 0 else 0
        
        print(f"\n=== Resource Efficiency ===")
        print(f"Total SM-time units used: {total_used_sms}")
        print(f"Total SM-time units wasted: {total_wasted_sms}")
        print(f"Resource waste: {waste_percentage:.1f}%")
        print(f"Resource efficiency: {100-waste_percentage:.1f}%")
    
    # Resource distribution
    print("\n=== Resource Distribution ===")
    resource_counts = df['resource'].value_counts()
    for resource, count in resource_counts.items():
        sms = parse_resource_sms(resource)
        print(f"{resource}: {count} pods ({sms} SMs each)")
    
    # Time periods with highest usage
    print("\n=== Peak Usage Periods ===")
    peak_usage = timeline_df['total_sms'].max()
    peak_periods = timeline_df[timeline_df['total_sms'] == peak_usage]
    for _, period in peak_periods.iterrows():
        print(f"Time {period['time']}: {period['total_sms']} SMs, {period['required_gpus']} GPUs, "
              f"{period['occupancy_percent']:.1f}% occupancy")
    
    # Recommendations
    print("\n=== Optimization Recommendations ===")
    if len(active_periods) > 0:
        avg_occupancy = active_periods['occupancy_percent'].mean()
        if avg_occupancy < 60:
            print("⚠️  LOW OCCUPANCY: Consider consolidating workloads or using smaller GPU instances")
        elif avg_occupancy < 80:
            print("📊 MEDIUM OCCUPANCY: Room for optimization through better scheduling")
        else:
            print("✅ HIGH OCCUPANCY: Good GPU utilization")
        
        if waste_percentage > 30:
            print("⚠️  HIGH WASTE: Consider workload right-sizing or GPU sharing strategies")
        elif waste_percentage > 15:
            print("📊 MODERATE WASTE: Some optimization opportunities exist")
        else:
            print("✅ LOW WASTE: Efficient resource utilization")

def main(file_name):
    try:
        # Load and process data
        print(f"Loading data from {file_name}...")
        df = load_and_process_data(file_name)
        
        # Create timeline data
        print("Processing timeline data...")
        timeline_df = create_timeline_data(df)
        
        # Print analysis
        analyze_resource_usage(df, timeline_df)
        
        # Create visualizations
        print("Creating visualizations...")
        
        # Main timeline visualization (4-panel dashboard)
        fig1 = create_step_visualization(timeline_df, df)
        plt.figure(fig1.number)
        plt.show()
        
        # Occupancy heatmap
        fig3 = create_occupancy_heatmap(timeline_df)
        plt.figure(fig3.number)
        plt.show()
        
        # Gantt chart
        fig2 = create_gantt_chart(df)
        plt.figure(fig2.number)
        plt.show()
        
        # Save plots
        fig1.savefig(f'{file_name}_resource_timeline.png', dpi=300, bbox_inches='tight')
        fig2.savefig(f'{file_name}_gantt_chart.png', dpi=300, bbox_inches='tight')
        fig3.savefig(f'{file_name}_occupancy_heatmap.png', dpi=300, bbox_inches='tight')
        print(f"Plots saved as '{file_name}_resource_timeline.png', '{file_name}_gantt_chart.png', and '{file_name}_occupancy_heatmap.png'")
        
    except FileNotFoundError:
        print(f"Error: {file_name} not found. Please make sure the file exists in the current directory.")
        print("The file should contain data in the format: pod-name,resource,start_time,duration")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    file_name = sys.argv[1]
    main(file_name)


