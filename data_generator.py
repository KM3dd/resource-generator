#!/usr/bin/env python3
"""
MIG Resource Request Data Generator
Generates synthetic data for simulating pod arrivals requesting MIG resources on a cluster
"""

import csv
import os
import random
import argparse
import math
from typing import List, Tuple

class MIGDataGenerator:
    def __init__(self):
        # Define common MIG resource configurations (GPU memory, RAM)
        self.mig_profiles = [
            "1g.5gb",   # MIG 1g.5gb (1/7 of A100)
            "2g.10gb",  # MIG 2g.10gb (2/7 of A100) 
            "3g.20gb",  # MIG 3g.20gb (3/7 of A100)
            "4g.20gb",  # MIG 4g.20gb (4/7 of A100)
            "7g.40gb",  # MIG 7g.40gb (full A100)
        ]
        
        # Weights for different resource types (smaller instances more common)
        self.profile_weights = [0.4, 0.25, 0.15, 0.15, 0.05]
        
        # Duration ranges for different workload types (in seconds)
        self.duration_ranges = {
            "short": (30, 90),      # 30s - 1.5min
            "medium": (90, 200),     # 1.5min - 5min  300 -> 200 
            "long": (200, 400),      # 5min - 10min 200,400
        }
        
        # Workload type probabilities
        self.workload_weights = {"1g.5gb":[0.6, 0.3, 0.1],
                                 "2g.10gb":[0.5, 0.3, 0.2],
                                 "3g.20gb":[0.4, 0.4, 0.2],
                                 "4g.20gb":[0.3, 0.4, 0.3],
                                 "7g.40gb":[0.2, 0.4, 0.4]}  # short, medium, long
    
    def calculate_num_pods(self, lambda_rate: float, time_span: int) -> int:
        """Calculate expected number of pods based on lambda rate and timespan"""
        return int(lambda_rate * time_span)
    
    def generate_arrival_times(self, lambda_rate: float, time_span: int, 
                             pattern: str = "poisson") -> List[int]:
        """Generate arrival times based on different patterns"""
        # Calculate number of pods based on lambda and timespan
        num_pods = self.calculate_num_pods(lambda_rate, time_span)
        arrival_times = []
        
        if pattern == "poisson":
            # Poisson arrivals (exponential inter-arrival times)
            current_time = 0
            
            while current_time < time_span:
                # Exponential inter-arrival times
                inter_arrival = random.expovariate(lambda_rate)
                current_time += inter_arrival
                if current_time < time_span:
                    arrival_times.append(int(current_time))
        
        elif pattern == "uniform":
            # Uniform random arrivals
            arrival_times = sorted([random.randint(0, time_span) 
                                  for _ in range(num_pods)])
        
        elif pattern == "burst":
            # Bursty arrivals with periods of high activity
            burst_periods = 3
            burst_duration = time_span // (burst_periods * 2)
            
            for i in range(num_pods):
                burst_start = (i % burst_periods) * (time_span // burst_periods)
                arrival_time = burst_start + random.randint(0, burst_duration)
                arrival_times.append(arrival_time)
            
            arrival_times.sort()
        
        return arrival_times
    
    def generate_pod_data(self, lambda_rate: float, time_span: int,
                         arrival_pattern: str = "poisson") -> List[Tuple[str, str, int, int]]:
        """Generate complete pod data"""
        data = []
        
        # Generate arrival times
        arrival_times = self.generate_arrival_times(lambda_rate, time_span, arrival_pattern)
        num_pods = len(arrival_times)
        
        for i in range(num_pods):
            # Generate pod name
            pod_name = f"pod-{i+1}"
            
            # Select resource profile
            resource = random.choices(self.mig_profiles, 
                                    weights=self.profile_weights)[0]
            
            # Select workload type and duration
            workload_type = random.choices(["short", "medium", "long"],
                                         weights=self.workload_weights[resource])[0]
            duration_range = self.duration_ranges[workload_type]
            duration = random.randint(*duration_range)
            
            # Get arrival time
            arrival_time = arrival_times[i]
            
            data.append((pod_name, resource, arrival_time, duration))
        
        return data
    
    def save_to_txt(self, data: List[Tuple[str, str, int, int]], filename: str):
        """Save data to TXT file in the requested format"""
        with open(filename, 'w') as txtfile:
            txtfile.write("<podname>,<requested resources>,<arrival time (s)>,<duration (s)>\n")
            for pod_name, resources, arrival, duration in data:
                txtfile.write(f"{pod_name},{resources},{arrival},{duration}\n")
    
    def print_data(self, data: List[Tuple[str, str, int, int]]):
        """Print data in the requested format"""
        print("<podname>,<requested resources>,<arrival time (s)>,<duration (s)>")
        for pod_name, resources, arrival, duration in data:
            print(f"{pod_name},{resources},{arrival},{duration}")

def main():
    parser = argparse.ArgumentParser(description='Generate MIG resource request data')
    parser.add_argument('-l', '--lambda-rate', type=float, required=True,
                       help='Lambda rate (pods per second)')
    parser.add_argument('-t', '--time-span', type=int, default=400,
                       help='Time span for arrivals in seconds (default: 400)')
    parser.add_argument('-p', '--pattern', choices=['poisson', 'uniform', 'burst'],
                       default='poisson', help='Arrival pattern (default: poisson)')
    parser.add_argument('-o', '--output', type=str,
                       help='Output TXT filename (optional)')
    parser.add_argument('--seed', type=int, 
                       help='Random seed for reproducible results')
    
    args = parser.parse_args()
    
    # Set random seed if provided
    if args.seed:
        random.seed(args.seed)
    
    # Generate data
    generator = MIGDataGenerator()
    
    # Calculate and show expected number of pods
    expected_pods = generator.calculate_num_pods(args.lambda_rate, args.time_span)
    print(f"Expected number of pods: {expected_pods} (λ={args.lambda_rate}, timespan={args.time_span}s)")
    
    data = generator.generate_pod_data(
        lambda_rate=args.lambda_rate,
        time_span=args.time_span,
        arrival_pattern=args.pattern
    )
    
    print(f"Generated {len(data)} pods")
    
    # Output results
    if args.output:
        generator.save_to_txt(data, args.output)
        print(f"Data saved to {args.output}")
    else:
        generator.print_data(data)

if __name__ == "__main__":
    #file_name = os.args
    main()