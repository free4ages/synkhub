#!/usr/bin/env python3
"""
Example usage of the date partition generator.

This script demonstrates how to use the date partition generator to create
partitions for date ranges with day as the minimum unit.
"""

from datetime import date
from .date_partition_generator import generate_date_partitions, DatePartition

def example_daily_partitions():
    """Example of daily partitions"""
    print("=== Daily Partitions Example ===")
    start = date(2023, 1, 1)
    end = date(2023, 1, 10)
    step = 1
    unit = "day"
    
    print(f"Generating daily partitions from {start} to {end}")
    partitions = list(generate_date_partitions(start, end, step, unit, bounded=False))
    
    for i, partition in enumerate(partitions, 1):
        print(f"  Partition {i}: {partition.start} -> {partition.end}")
    print()

def example_weekly_partitions():
    """Example of weekly partitions"""
    print("=== Weekly Partitions Example ===")
    start = date(2023, 1, 1)
    end = date(2023, 2, 1)
    step = 1
    unit = "week"
    
    print(f"Generating weekly partitions from {start} to {end}")
    partitions = list(generate_date_partitions(start, end, step, unit, bounded=True))
    
    for i, partition in enumerate(partitions, 1):
        print(f"  Partition {i}: {partition.start} -> {partition.end}")
    print()

def example_monthly_partitions():
    """Example of monthly partitions"""
    print("=== Monthly Partitions Example ===")
    start = date(2023, 1, 15)
    end = date(2023, 6, 20)
    step = 1
    unit = "month"
    
    print(f"Generating monthly partitions from {start} to {end}")
    partitions = list(generate_date_partitions(start, end, step, unit, bounded=True))
    
    for i, partition in enumerate(partitions, 1):
        print(f"  Partition {i}: {partition.start} -> {partition.end}")
    print()

def example_multi_day_partitions():
    """Example of multi-day partitions"""
    print("=== Multi-Day Partitions Example ===")
    start = date(2023, 1, 1)
    end = date(2023, 1, 25)
    step = 5  # 5-day partitions
    unit = "day"
    
    print(f"Generating 5-day partitions from {start} to {end}")
    partitions = list(generate_date_partitions(start, end, step, unit, bounded=False))
    
    for i, partition in enumerate(partitions, 1):
        days = (partition.end - partition.start).days
        print(f"  Partition {i}: {partition.start} -> {partition.end} ({days} days)")
    print()

def example_quarterly_partitions():
    """Example of quarterly partitions"""
    print("=== Quarterly Partitions Example ===")
    start = date(2022, 6, 15)
    end = date(2024, 3, 10)
    step = 1
    unit = "quarter"
    
    print(f"Generating quarterly partitions from {start} to {end}")
    partitions = list(generate_date_partitions(start, end, step, unit, bounded=True))
    
    for i, partition in enumerate(partitions, 1):
        print(f"  Partition {i}: {partition.start} -> {partition.end}")
    print()

def example_bounded_vs_unbounded():
    """Example showing difference between bounded and unbounded partitions"""
    print("=== Bounded vs Unbounded Example ===")
    start = date(2023, 1, 15)  # Mid-month start
    end = date(2023, 3, 10)    # Mid-month end
    step = 1
    unit = "month"
    
    print(f"Date range: {start} to {end}")
    
    print("\nUnbounded partitions (aligned to month boundaries):")
    unbounded = list(generate_date_partitions(start, end, step, unit, bounded=False))
    for i, partition in enumerate(unbounded, 1):
        print(f"  Partition {i}: {partition.start} -> {partition.end}")
    
    print("\nBounded partitions (respect input boundaries):")
    bounded = list(generate_date_partitions(start, end, step, unit, bounded=True))
    for i, partition in enumerate(bounded, 1):
        print(f"  Partition {i}: {partition.start} -> {partition.end}")
    print()

if __name__ == "__main__":
    print("Date Partition Generator Examples")
    print("=" * 50)
    print()
    
    example_daily_partitions()
    example_weekly_partitions()
    example_monthly_partitions()
    example_multi_day_partitions()
    example_quarterly_partitions()
    example_bounded_vs_unbounded()
    
    print("All examples completed successfully!")
