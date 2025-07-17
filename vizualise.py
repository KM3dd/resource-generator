import matplotlib.pyplot as plt
import pandas as pd
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

def create_timeline_data(df):
    """Create timeline data showing SM usage over time"""
    # Get all unique time points where resources change
    time_points = set()
    for _, row in df.iterrows():
        end_time= row['start_time'] + row['duration']
        time_points.add(row['start_time'])
        time_points.add(end_time)
    
    time_points = sorted(time_points)
    
    # Calculate SM usage at each time point
    timeline_data = []
    for time in time_points:
        # Find all active pods at this time
        
        active_pods = df[(df['start_time'] <= time) & (df['start_time'] + df['duration'] > time)]
        total_sms = active_pods['sms'].sum()
        active_count = len(active_pods)
        
        timeline_data.append({
            'time': time,
            'total_sms': total_sms,
            'active_pods': active_count
        })
    
    return pd.DataFrame(timeline_data)

def create_step_visualization(timeline_df, df):
    """Create a step-style visualization showing resource usage over time"""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10))
    
    # Main timeline plot
    ax1.step(timeline_df['time'], timeline_df['total_sms'], where='post', 
             linewidth=2, color='#2563eb', alpha=0.8)
    ax1.fill_between(timeline_df['time'], timeline_df['total_sms'], 
                     step='post', alpha=0.3, color='#3b82f6')
    
    ax1.set_xlabel('Time', fontsize=12)
    ax1.set_ylabel('Total SM Count', fontsize=12)
    ax1.set_title('GPU Resource Usage Timeline (SM Count Over Time)', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    
    # Add some statistics as text
    max_sms = timeline_df['total_sms'].max()
    max_time = timeline_df['time'].max()
    total_pods = len(df)
    
    ax1.text(0.02, 0.95, f'Peak Usage: {max_sms} SMs\nTotal Duration: {max_time} time units\nTotal Pods: {total_pods}', 
             transform=ax1.transAxes, fontsize=10, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    # Secondary plot showing number of active pods
    ax2.step(timeline_df['time'], timeline_df['active_pods'], where='post', 
             linewidth=2, color='#059669', alpha=0.8)
    ax2.fill_between(timeline_df['time'], timeline_df['active_pods'], 
                     step='post', alpha=0.3, color='#10b981')
    
    ax2.set_xlabel('Time', fontsize=12)
    ax2.set_ylabel('Active Pod Count', fontsize=12)
    ax2.set_title('Number of Active Pods Over Time', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
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
        end_time= row['start_time'] + row['duration']
        duration = end_time - row['start_time']
        color = sm_colors.get(row['sms'], '#6b7280')
        
        ax.barh(i, duration, left=row['start_time'], height=0.8, 
                color=color, alpha=0.7, edgecolor='black', linewidth=0.5)
        
        # Add pod name if there's space
        #if duration > 20:  # Only show name if bar is wide enough
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

def analyze_resource_usage(df, timeline_df):
    """Print detailed analysis of resource usage"""
    print("=== GPU Resource Usage Analysis ===")
    print(f"Total number of pods: {len(df)}")
    print(f"Time range: {df['start_time'].min()} to {df['duration'].max()+df['start_time'].min()}")
    print(f"Peak SM usage: {timeline_df['total_sms'].max()}")
    print(f"Average SM usage: {timeline_df['total_sms'].mean():.2f}")
    print(f"Peak concurrent pods: {timeline_df['active_pods'].max()}")
    print(f"Average concurrent pods: {timeline_df['active_pods'].mean():.2f}")
    
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
        print(f"Time {period['time']}: {period['total_sms']} SMs, {period['active_pods']} active pods")

def main(file_name):
    try:
        # Load and process data
        print("Loading data from file.txt...")
        df = load_and_process_data(file_name)
        
        # Create timeline data
        print("Processing timeline data...")
        timeline_df = create_timeline_data(df)
        
        # Print analysis
        analyze_resource_usage(df, timeline_df)
        
        # Create visualizations
        print("Creating visualizations...")
        
        # Main timeline visualization
        fig1 = create_step_visualization(timeline_df, df)
        plt.figure(fig1.number)
        plt.show()
        
        # Gantt chart
        fig2 = create_gantt_chart(df)
        plt.figure(fig2.number)
        plt.show()
        
        # Save plots if needed
        fig1.savefig(f'{file_name}_resource_timeline.png', dpi=300, bbox_inches='tight')
        fig2.savefig(f'{file_name}._gantt_chart.png', dpi=300, bbox_inches='tight')
        print("Plots saved as 'gpu_resource_timeline.png' and 'pod_gantt_chart.png'")
        
    except FileNotFoundError:
        print("Error: file.txt not found. Please make sure the file exists in the current directory.")
        print("The file should contain data in the format: pod-name,resource,start_time,end_time")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    file_name=sys.argv[1]
    main(file_name)
