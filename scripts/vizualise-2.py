#!/usr/bin/env python3

import json
import sys
from datetime import datetime
import argparse
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

def parse_resource_sms(resource_str):
    """Extract SM count from resource string like '2g.10gb' -> 2"""
    return int(resource_str.split('g.')[0])

def parse_timestamp(timestamp_str):
    """Parse ISO timestamp to datetime object"""
    # Remove the 'Z' and parse
    timestamp_str = timestamp_str.rstrip('Z')
    return datetime.fromisoformat(timestamp_str)

def load_json_logs(filename):
    """Load and parse JSON log file"""
    pods = []
    
    with open(filename, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                data = json.loads(line)
                
                pod = {
                    'name': data['pod_name'],
                    'resource': data['pod_resource'],
                    'sms': parse_resource_sms(data['pod_resource']),
                    'start_time': parse_timestamp(data['start_time']),
                    'end_time': parse_timestamp(data['end_time']),
                    'wait_ms': data['wait_ms']
                }
                pods.append(pod)
                
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                print(f"Warning: Skipping malformed line {line_num}: {e}")
                continue
    
    return pods

def calculate_occupancy_timeline(pods, total_gpus, sms_per_gpu=7):
    """Calculate GPU occupancy over time"""
    if not pods:
        return pd.DataFrame()
    
    # Get all time points where resource allocation changes
    time_points = set()
    for pod in pods:
        time_points.add(pod['start_time'])
        time_points.add(pod['end_time'])
    
    time_points = sorted(time_points)
    
    # Calculate occupancy at each time point
    timeline_data = []
    for time_point in time_points:
        # Find active pods at this time
        active_pods = [pod for pod in pods 
                      if pod['start_time'] <= time_point < pod['end_time']]
        
        total_sms_used = sum(pod['sms'] for pod in active_pods)
        total_sms_available = total_gpus * sms_per_gpu
        
        occupancy_percent = (total_sms_used / total_sms_available * 100) if total_sms_available > 0 else 0
        
        # Calculate minimum required GPUs
        required_gpus = (total_sms_used + sms_per_gpu - 1) // sms_per_gpu if total_sms_used > 0 else 0  # Ceiling division
        optimal_occupancy = (total_sms_used / (required_gpus * sms_per_gpu) * 100) if required_gpus > 0 else 0
        
        # Convert datetime to seconds for plotting
        time_seconds = (time_point - time_points[0]).total_seconds()
        
        timeline_data.append({
            'time': time_seconds,
            'datetime': time_point,
            'active_pods': len(active_pods),
            'total_sms': total_sms_used,
            'total_sms_available': total_sms_available,
            'occupancy_percent': occupancy_percent,
            'required_gpus': required_gpus,
            'optimal_occupancy': optimal_occupancy,
            'wasted_sms': max(0, total_sms_available - total_sms_used),
            'active_pod_names': [pod['name'] for pod in active_pods]
        })
    
    return pd.DataFrame(timeline_data)

def create_step_visualization(timeline_df, pods, total_gpus, sms_per_gpu=7):
    """Create a step-style visualization showing resource usage over time"""
    if timeline_df.empty:
        print("No timeline data to visualize")
        return None
        
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(18, 12))
    
    # SM usage over time
    ax1.step(timeline_df['time'], timeline_df['total_sms'], where='post', 
             linewidth=2, color='#2563eb', alpha=0.8)
    ax1.fill_between(timeline_df['time'], timeline_df['total_sms'], 
                     step='post', alpha=0.3, color='#3b82f6')
    
    ax1.set_xlabel('Time (seconds)', fontsize=12)
    ax1.set_ylabel('Total SM Count', fontsize=12)
    ax1.set_title('GPU Resource Usage Timeline (SM Count Over Time)', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    
    # Required GPUs over time
    ax2.step(timeline_df['time'], timeline_df['required_gpus'], where='post', 
             linewidth=2, color='#dc2626', alpha=0.8)
    ax2.fill_between(timeline_df['time'], timeline_df['required_gpus'], 
                     step='post', alpha=0.3, color='#ef4444')
    
    ax2.set_xlabel('Time (seconds)', fontsize=12)
    ax2.set_ylabel('Required GPUs', fontsize=12)
    ax2.set_title('Required GPU Count Over Time', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    # GPU Occupancy over time
    ax3.step(timeline_df['time'], timeline_df['occupancy_percent'], where='post', 
             linewidth=2, color='#059669', alpha=0.8)
    ax3.fill_between(timeline_df['time'], timeline_df['occupancy_percent'], 
                     step='post', alpha=0.3, color='#10b981')
    
    ax3.set_xlabel('Time (seconds)', fontsize=12)
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
    
    ax4.set_xlabel('Time (seconds)', fontsize=12)
    ax4.set_ylabel('Wasted SMs', fontsize=12)
    ax4.set_title('Unused SMs (Resource Waste) Over Time', fontsize=14, fontweight='bold')
    ax4.grid(True, alpha=0.3)
    
    # Add statistics text box
    max_sms = timeline_df['total_sms'].max()
    max_gpus = timeline_df['required_gpus'].max()
    max_time = timeline_df['time'].max()
    active_periods = timeline_df[timeline_df['required_gpus'] > 0]
    avg_occupancy = active_periods['occupancy_percent'].mean() if len(active_periods) > 0 else 0
    total_pods = len(pods)
    
    stats_text = f'''Peak Usage: {max_sms} SMs ({max_gpus} GPUs)
Available: {total_gpus} GPUs ({total_gpus*sms_per_gpu} SMs)
Duration: {max_time:.1f}s
Avg Occupancy: {avg_occupancy:.1f}%
Total Pods: {total_pods}'''
    
    ax1.text(0.02, 0.95, stats_text, transform=ax1.transAxes, fontsize=10, 
             verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    plt.tight_layout()
    return fig

def create_occupancy_heatmap(timeline_df):
    """Create a heatmap showing occupancy patterns"""
    if timeline_df.empty:
        print("No timeline data for heatmap")
        return None
        
    fig, ax = plt.subplots(figsize=(15, 6))
    
    # Filter out periods with no GPU usage
    active_periods = timeline_df[timeline_df['required_gpus'] > 0].copy()
    
    if len(active_periods) == 0:
        ax.text(0.5, 0.5, 'No GPU usage periods found', ha='center', va='center', 
                transform=ax.transAxes, fontsize=14)
        ax.set_title('GPU Occupancy Heatmap (No Data)', fontsize=14, fontweight='bold')
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
        
        # Calculate segment width
        if i < len(active_periods) - 1:
            next_time = active_periods.iloc[i+1]['time']
            width = next_time - row['time']
        else:
            # For the last segment, use a reasonable default or calculate from timeline
            if len(timeline_df) > 0:
                total_duration = timeline_df['time'].max() - timeline_df['time'].min()
                width = max(1, total_duration * 0.01)  # 1% of total duration or minimum 1
            else:
                width = 1
        
        # Skip zero-width segments
        if width <= 0:
            continue
            
        ax.barh(0, width, left=row['time'], height=0.8, color=color, alpha=0.7, 
                edgecolor='black', linewidth=0.3)
        
        # Add text for significant periods (avoid overcrowding)
        label_frequency = max(1, len(active_periods) // 15)  # Show every nth label
        if i % label_frequency == 0 and width > (timeline_df['time'].max() * 0.02):  # Only for wide enough segments
            ax.text(row['time'] + width/2, 0, f"{row['occupancy_percent']:.0f}%", 
                   ha='center', va='center', fontsize=8, fontweight='bold')
    
    ax.set_xlabel('Time (seconds)', fontsize=12)
    ax.set_ylabel('GPU Occupancy', fontsize=12)
    ax.set_title('GPU Occupancy Timeline Heatmap (Color-coded by Efficiency)', fontsize=14, fontweight='bold')
    ax.set_ylim(-0.5, 0.5)
    ax.set_yticks([0])
    ax.set_yticklabels(['Occupancy Level'])
    
    # Create legend
    legend_elements = [plt.Rectangle((0,0),1,1, facecolor=color, alpha=0.7, label=category) 
                      for category, color in colors.items()]
    ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(1, 1))
    
    plt.tight_layout()
    return fig

def create_gantt_chart(pods):
    """Create a Gantt chart showing individual pod lifecycles"""
    if not pods:
        print("No pods to visualize")
        return None
        
    fig, ax = plt.subplots(figsize=(15, 8))
    
    # Color mapping for different SM counts
    sm_colors = {1: '#fef3c7', 2: '#fde68a', 3: '#f59e0b', 4: '#d97706', 7: '#92400e'}
    
    # Sort pods by start time for better visualization
    pods_sorted = sorted(pods, key=lambda x: x['start_time'])
    
    # Convert to relative time (seconds from first start)
    first_start = min(pod['start_time'] for pod in pods)
    
    for i, pod in enumerate(pods_sorted):
        start_seconds = (pod['start_time'] - first_start).total_seconds()
        end_seconds = (pod['end_time'] - first_start).total_seconds()
        duration = end_seconds - start_seconds
        
        color = sm_colors.get(pod['sms'], '#6b7280')
        
        ax.barh(i, duration, left=start_seconds, height=0.8, 
                color=color, alpha=0.7, edgecolor='black', linewidth=0.5)
        
        # Add pod name
        ax.text(start_seconds + duration/2, i, pod['name'], 
                ha='center', va='center', fontsize=6)
    
    ax.set_xlabel('Time (seconds)', fontsize=12)
    ax.set_ylabel('Pod Index', fontsize=12)
    ax.set_title('Pod Lifecycle Gantt Chart (Color = SM Count)', fontsize=14, fontweight='bold')
    
    # Create legend
    unique_sms = sorted(set(pod['sms'] for pod in pods))
    legend_elements = [plt.Rectangle((0,0),1,1, facecolor=sm_colors.get(sm, '#6b7280'), alpha=0.7, 
                                   label=f'{sm} SMs') for sm in unique_sms]
    ax.legend(handles=legend_elements, loc='upper right')
    
    plt.tight_layout()
    return fig

def print_summary_stats(timeline_df, pods, total_gpus, sms_per_gpu=7):
    """Print summary statistics"""
    if timeline_df.empty:
        print("No data to analyze")
        return
    
    print("\n" + "="*60)
    print("GPU OCCUPANCY ANALYSIS SUMMARY")
    print("="*60)
    
    # Basic stats
    total_sms_available = total_gpus * sms_per_gpu
    max_sms_used = timeline_df['total_sms'].max()
    max_gpus_needed = timeline_df['required_gpus'].max()
    duration = timeline_df['time'].max()
    
    print(f"Total GPUs Available: {total_gpus}")
    print(f"Total SMs Available: {total_sms_available}")
    print(f"Analysis Duration: {duration:.1f} seconds")
    print(f"Total Pods Processed: {len(pods)}")
    
    print(f"\nPeak Usage:")
    print(f"  Maximum SMs Used: {max_sms_used}")
    print(f"  Maximum GPUs Needed: {max_gpus_needed}")
    print(f"  Peak Occupancy: {(max_sms_used / total_sms_available * 100):.1f}%")
    
    # Active periods only
    active_periods = timeline_df[timeline_df['total_sms'] > 0]
    if len(active_periods) > 0:
        avg_occupancy = active_periods['occupancy_percent'].mean()
        avg_wasted_sms = active_periods['wasted_sms'].mean()
        print(f"\nDuring Active Periods:")
        print(f"  Average Occupancy: {avg_occupancy:.1f}%")
        print(f"  Average Wasted SMs: {avg_wasted_sms:.1f}")
        
        # Time below thresholds
        low_occupancy = active_periods[active_periods['occupancy_percent'] < 60]
        med_occupancy = active_periods[(active_periods['occupancy_percent'] >= 60) & 
                                     (active_periods['occupancy_percent'] < 80)]
        high_occupancy = active_periods[active_periods['occupancy_percent'] >= 80]
        
        print(f"\nOccupancy Distribution:")
        print(f"  High (≥80%): {len(high_occupancy)} time periods")
        print(f"  Medium (60-80%): {len(med_occupancy)} time periods") 
        print(f"  Low (<60%): {len(low_occupancy)} time periods")

def main():
    parser = argparse.ArgumentParser(description='Calculate GPU occupancy from pod logs')
    parser.add_argument('filename', help='JSON log file to analyze')
    parser.add_argument('gpus', type=int, help='Total number of GPUs available')
    parser.add_argument('--details', action='store_true', help='Show detailed timeline')
    parser.add_argument('--sms-per-gpu', type=int, default=7, help='SMs per GPU (default: 7)')
    parser.add_argument('--no-plots', action='store_true', help='Skip generating plots')
    parser.add_argument('--save-plots', help='Save plots to file (provide base filename)')
    
    args = parser.parse_args()
    
    try:
        # Load and parse data
        print(f"Loading data from {args.filename}...")
        pods = load_json_logs(args.filename)
        
        if not pods:
            print("No valid pod data found in file!")
            return
        
        print(f"Loaded {len(pods)} pods")
        
        # Calculate occupancy timeline
        print("Calculating occupancy timeline...")
        timeline = calculate_occupancy_timeline(pods, args.gpus, args.sms_per_gpu)
        
        # Print summary statistics
        print_summary_stats(timeline, pods, args.gpus, args.sms_per_gpu)
        
        # Show detailed timeline if requested
        if args.details and not timeline.empty:
            print(f"\nDetailed Timeline:")
            print("-" * 80)
            for _, row in timeline.iterrows():
                print(f"Time: {row['time']:6.1f}s | "
                      f"Active Pods: {row['active_pods']:2d} | "
                      f"SMs Used: {row['total_sms']:2d} | "
                      f"GPUs Needed: {row['required_gpus']:2d} | "
                      f"Occupancy: {row['occupancy_percent']:5.1f}%")
        
        # Create and show visualizations
        if not args.no_plots:
            print("\nGenerating visualizations...")
            
            # Create step visualization
            fig1 = create_step_visualization(timeline, pods, args.gpus, args.sms_per_gpu)
            if fig1:
                if args.save_plots:
                    fig1.savefig(f"{args.save_plots}_occupancy.png", dpi=300, bbox_inches='tight')
                    print(f"Saved occupancy plot to {args.save_plots}_occupancy.png")
            
            # Create occupancy heatmap
            fig2 = create_occupancy_heatmap(timeline)
            if fig2:
                if args.save_plots:
                    fig2.savefig(f"{args.save_plots}_heatmap.png", dpi=300, bbox_inches='tight')
                    print(f"Saved occupancy heatmap to {args.save_plots}_heatmap.png")
            
            # Create Gantt chart
            fig3 = create_gantt_chart(pods)
            if fig3:
                if args.save_plots:
                    fig3.savefig(f"{args.save_plots}_gantt.png", dpi=300, bbox_inches='tight')
                    print(f"Saved Gantt chart to {args.save_plots}_gantt.png")
            
            # Show plots if not saving
            if not args.save_plots:
                plt.show()
        
    except FileNotFoundError:
        print(f"Error: File '{args.filename}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()