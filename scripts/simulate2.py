#!/usr/bin/env python3
"""
MIG GPU Scheduler Simulator

This script simulates the arrival, scheduling, and departure of pods requesting
MIG GPU slices on A100 GPUs. It handles scheduling iterations every minute
and implements first-fit placement with MIG constraints.
"""

import csv
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict
import statistics

@dataclass
class Pod:
    name: str
    resource: str
    arrival_time: int  # seconds
    duration: int      # seconds
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    wait_time: Optional[int] = None

class MIGScheduler:
    # MIG slice configurations: resource -> (size, valid_start_indexes)
    MIG_CONFIG = {
        '1g.5gb': (1, [0, 1, 2, 3, 4, 5, 6]),
        '2g.10gb': (2, [0, 2, 4, 6]),
        '3g.20gb': (3, [0, 4]),
        '4g.20gb': (4, [0]),
        '7g.40gb': (7, [0])
    }
    
    def __init__(self, num_gpus: int = 1, alpha: float = 0.5, beta: float = 0.5):
        self.num_gpus = num_gpus
        # Each GPU has 7 MIG slices (0-6)
        self.gpu_slices = [[False] * 7 for _ in range(num_gpus)]
        self.pods = []
        self.waiting_queue = []
        self.running_pods = []
        self.completed_pods = []
        self.current_time = 0
        self.scheduling_interval = 60  # 1 minute in seconds
        self.alpha = alpha  # Weight for resource size
        self.beta = beta    # Weight for age
        
    def load_pods(self, filename: str):
        """Load pods from CSV file"""
        with open(filename, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                if len(row) == 4:
                    name, resource, arrival_time, duration = row
                    pod = Pod(
                        name=name.strip(),
                        resource=resource.strip(),
                        arrival_time=int(arrival_time),
                        duration=int(duration)
                    )
                    self.pods.append(pod)
        
        # Sort pods by arrival time
        self.pods.sort(key=lambda p: p.arrival_time)
        print(f"Loaded {len(self.pods)} pods")
    def calculate_pod_score(self, pod: Pod, max_resource_size: int, min_resource_size: int, max_age: int) -> float:
        """
        Calculate priority score for a pod based on resource size and age.
        Higher score = higher priority
        """
        resource_size, _ = self.MIG_CONFIG[pod.resource]
        pod_age = self.current_time - pod.arrival_time
        
        # normalized1 = (max - x) / (max - min) for resource size
        if max_resource_size == min_resource_size:
            normalized_resource = 1.0
        else:
            normalized_resource = (max_resource_size - resource_size) / (max_resource_size - min_resource_size)
        
        # normalized2 = (max - x) / max for age
        if max_age == 0:
            normalized_age = 1.0
        else:
            normalized_age = (max_age - pod_age) / max_age
        
        score = self.alpha * normalized_resource + self.beta * normalized_age
        return score
    
    def find_best_fit_placement(self, resource: str, pod_duration: int) -> Optional[Tuple[int, int]]:
        """
        Find best-fit placement for a pod with given resource.
        Returns (gpu_id, start_index) if placement is possible, None otherwise.
        Prioritizes rightmost placement when multiple options are available.
        """
        if resource not in self.MIG_CONFIG:
            return None
            
        size, valid_starts = self.MIG_CONFIG[resource]
        pod_end_time = self.current_time + pod_duration
        
        available_placements = []
        
        # Try each GPU
        for gpu_id in range(self.num_gpus):
            # Try each valid starting index
            for start_idx in valid_starts:
                # Check if we have enough consecutive slices
                if start_idx + size <= 7:
                    can_place = True
                    
                    # Check each required slice for conflicts
                    for slice_idx in range(start_idx, start_idx + size):
                        # Check if slice is currently occupied
                        if self.gpu_slices[gpu_id][slice_idx]:
                            can_place = False
                            break
                        
                        # Check for conflicts with running pods
                        for running_pod, running_gpu_id, running_start_idx in self.running_pods:
                            if running_gpu_id == gpu_id:
                                running_size, _ = self.MIG_CONFIG[running_pod.resource]
                                running_end_idx = running_start_idx + running_size - 1
                                
                                # Check if this slice overlaps with the running pod's slices
                                if (running_start_idx <= slice_idx <= running_end_idx and
                                    running_pod.end_time > self.current_time):
                                    can_place = False
                                    break
                        
                        if not can_place:
                            break
                    
                    if can_place:
                        available_placements.append((gpu_id, start_idx))
        
        if not available_placements:
            return None
        
        # Best-fit: choose the rightmost placement (highest start_idx)
        # Sort by GPU first, then by start_idx in descending order (rightmost first)
        available_placements.sort(key=lambda x: (x[0], -x[1]))
        return available_placements[0]
    
    def allocate_pod(self, pod: Pod, gpu_id: int, start_idx: int):
        """Allocate GPU slices to a pod"""
        size, _ = self.MIG_CONFIG[pod.resource]
        
        # Mark slices as occupied
        for i in range(start_idx, start_idx + size):
            self.gpu_slices[gpu_id][i] = True
        
        # Set pod timing
        pod.start_time = self.current_time
        pod.end_time = self.current_time + pod.duration
        pod.wait_time = self.current_time - pod.arrival_time
        
        self.running_pods.append((pod, gpu_id, start_idx))
        print(f"  Allocated {pod.name} ({pod.resource}) on GPU {gpu_id}, slices {start_idx}-{start_idx + size - 1}")
    
    def deallocate_pod(self, pod: Pod, gpu_id: int, start_idx: int):
        """Deallocate GPU slices from a completed pod"""
        size, _ = self.MIG_CONFIG[pod.resource]
        
        # Mark slices as free
        for i in range(start_idx, start_idx + size):
            self.gpu_slices[gpu_id][i] = False
        
        self.completed_pods.append(pod)
        print(f"  Deallocated {pod.name} ({pod.resource}) from GPU {gpu_id}")
    
    def process_arrivals(self):
        """Add newly arrived pods to waiting queue"""
        while self.pods and self.pods[0].arrival_time <= self.current_time:
            pod = self.pods.pop(0)
            self.waiting_queue.append(pod)
            print(f"  Pod {pod.name} arrived (resource: {pod.resource})")
    
    def process_departures(self):
        """Remove completed pods and free their resources"""
        completed = []
        for i, (pod, gpu_id, start_idx) in enumerate(self.running_pods):
            if pod.end_time <= self.current_time:
                completed.append(i)
                self.deallocate_pod(pod, gpu_id, start_idx)
        
        # Remove completed pods from running list (in reverse order to maintain indices)
        for i in reversed(completed):
            self.running_pods.pop(i)
    
    def schedule_waiting_pods(self):
        """
        Schedule pods using ranking-based approach with best-fit placement.
        Ranks pods by score (resource size + age) and places highest priority first.
        """
        if not self.waiting_queue:
            return
        
        # Calculate normalization values
        resource_sizes = [self.MIG_CONFIG[pod.resource][0] for pod in self.waiting_queue]
        ages = [self.current_time - pod.arrival_time for pod in self.waiting_queue]
        
        max_resource_size = max(resource_sizes) if resource_sizes else 1
        min_resource_size = min(resource_sizes) if resource_sizes else 1
        max_age = max(ages) if ages else 1
        
        # Calculate scores and sort pods by priority (highest score first)
        pod_scores = []
        for pod in self.waiting_queue:
            score = self.calculate_pod_score(pod, max_resource_size, min_resource_size, max_age)
            pod_scores.append((pod, score))
        
        # Sort by score descending (highest priority first)
        pod_scores.sort(key=lambda x: x[1], reverse=True)
        
        print(f"  Pod ranking for scheduling:")
        for pod, score in pod_scores:
            age = self.current_time - pod.arrival_time
            print(f"    {pod.name} ({pod.resource}): score={score:.3f}, age={age}s")
        
        # Try to place pods in priority order
        scheduled = []
        scheduled_pods = []
        
        for i, (pod, score) in enumerate(pod_scores):
            placement = self.find_best_fit_placement(pod.resource, pod.duration)
            if placement:
                gpu_id, start_idx = placement
                self.allocate_pod(pod, gpu_id, start_idx)
                scheduled_pods.append(pod)
                
                # Find original index in waiting_queue to mark for removal
                original_idx = self.waiting_queue.index(pod)
                scheduled.append(original_idx)
        
        # Remove scheduled pods from waiting queue (in reverse order to maintain indices)
        for idx in sorted(scheduled, reverse=True):
            self.waiting_queue.pop(idx)
    
    def print_status(self):
        """Print current scheduler status"""
        print(f"\nTime: {self.current_time}s")
        print(f"Waiting: {len(self.waiting_queue)} pods")
        print(f"Running: {len(self.running_pods)} pods")
        print(f"Completed: {len(self.completed_pods)} pods")
        
        # Print GPU utilization
        for gpu_id in range(self.num_gpus):
            slices = ''.join(['X' if occupied else '.' for occupied in self.gpu_slices[gpu_id]])
            print(f"GPU {gpu_id}: [{slices}]")
    
    def run_simulation(self):
        """Run the complete scheduling simulation"""
        if not self.pods:
            print("No pods to schedule")
            return
        
        # Start simulation at first pod's arrival time
        self.current_time = self.pods[0].arrival_time
        next_schedule_time = ((self.current_time // self.scheduling_interval) + 1) * self.scheduling_interval
        
        print(f"Starting simulation at time {self.current_time}s")
        print(f"First scheduling iteration at time {next_schedule_time}s")
        
        while self.pods or self.waiting_queue or self.running_pods:
            # Process arrivals at current time
            self.process_arrivals()
            
            # Process departures at current time
            self.process_departures()
            
            # Check if it's time for scheduling iteration
            if self.current_time >= next_schedule_time:
                print(f"\n--- Scheduling iteration at time {self.current_time}s ---")
                self.schedule_waiting_pods()
                next_schedule_time += self.scheduling_interval
            
            # Print status every minute or when something changes
            if self.current_time % 60 == 0 or not self.pods:
                self.print_status()
            
            # Advance time
            self.current_time += 1
            
            # Safety check to prevent infinite loops
            if self.current_time > 10000:
                print("Simulation timeout - stopping")
                break
        
        print(f"\nSimulation completed at time {self.current_time}s")
    
    def save_results(self, filename: str = "results.json"):
        """Save results to JSON file"""
        # Use a base timestamp for the simulation
        base_time = datetime.now()
        
        with open(filename, 'w') as file:
            for pod in self.completed_pods:
                # Calculate actual timestamps based on simulation time
                start_timestamp = base_time + timedelta(seconds=pod.start_time)
                end_timestamp = base_time + timedelta(seconds=pod.end_time)
                
                result = {
                    "pod_name": pod.name,
                    "pod_resource": pod.resource,
                    "start_time": start_timestamp.isoformat() + "Z",
                    "wait_ms": pod.wait_time * 1000,  # Convert to milliseconds
                    "end_time": end_timestamp.isoformat() + "Z"
                }
                file.write(json.dumps(result) + "\n")
        
        print(f"Results saved to {filename}")
    
    def calculate_statistics(self):
        """Calculate and print waiting time statistics"""
        if not self.completed_pods:
            print("No completed pods to analyze")
            return
        
        wait_times = [pod.wait_time for pod in self.completed_pods]
        
        print(f"\n--- Overall Waiting Time Statistics ---")
        print(f"Total pods completed: {len(self.completed_pods)}")
        print(f"Average waiting time: {statistics.mean(wait_times):.2f} seconds")
        print(f"Minimum waiting time: {min(wait_times)} seconds")
        print(f"Maximum waiting time: {max(wait_times)} seconds")
        print(f"Median waiting time: {statistics.median(wait_times):.2f} seconds")
        
        # Calculate per-profile statistics
        profile_stats = defaultdict(list)
        for pod in self.completed_pods:
            profile_stats[pod.resource].append(pod.wait_time)
        
        print(f"\n--- Per-Profile Waiting Time Statistics ---")
        for resource in sorted(profile_stats.keys()):
            wait_times_for_resource = profile_stats[resource]
            count = len(wait_times_for_resource)
            avg_wait = statistics.mean(wait_times_for_resource)
            max_wait = max(wait_times_for_resource)
            min_wait = min(wait_times_for_resource)
            
            print(f"{resource}:")
            print(f"  Count: {count} pods")
            print(f"  Average wait time: {avg_wait:.2f} seconds")
            print(f"  Maximum wait time: {max_wait} seconds")
            print(f"  Minimum wait time: {min_wait} seconds")
        
        # Handle remaining pods in queue
        if self.waiting_queue:
            print(f"\n--- Pods Still Waiting ---")
            print(f"Total pods still waiting: {len(self.waiting_queue)}")
            
            waiting_profile_stats = defaultdict(list)
            for pod in self.waiting_queue:
                current_wait = self.current_time - pod.arrival_time
                waiting_profile_stats[pod.resource].append((pod.name, current_wait))
            
            for resource in sorted(waiting_profile_stats.keys()):
                pods_waiting = waiting_profile_stats[resource]
                print(f"{resource}: {len(pods_waiting)} pods")
                for pod_name, wait_time in pods_waiting:
                    print(f"  {pod_name}: waiting {wait_time} seconds so far")

def main():
    # Configuration

    input_file = "data/wkld0.5/wkld0.5-3"
    num_gpus = 21
    alpha = 0.7  # Weight for resource size
    beta = 0.3 # Weight for age
    
    
    try:
        with open(input_file, 'r') as f:
            pass  # File exists
    except FileNotFoundError:
        print(f"filenotfound: {input_file}")

    
    try:
        with open(input_file, 'r') as f:
            pass  # File exists
    except FileNotFoundError:
        print(f"Creating sample input file: {input_file}")
        with open(input_file, 'w') as file:
            for line in sample_data:
                file.write(line + '\n')
    
    # Run simulation
    print(f"Running simulation with alpha={alpha}, beta={beta}")
    scheduler = MIGScheduler(num_gpus=num_gpus, alpha=alpha, beta=beta)
    scheduler.load_pods(input_file)
    scheduler.run_simulation()
    scheduler.save_results()
    scheduler.calculate_statistics()

if __name__ == "__main__":
    main()