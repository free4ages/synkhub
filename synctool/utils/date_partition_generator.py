from datetime import date, timedelta
from typing import Generator, Tuple
from calendar import monthrange
from dateutil.relativedelta import relativedelta

epoch_date = date(1970, 1, 1)
epoch_monday = date(1970, 1, 5)  # first Monday in 1970
# Date-only helper functions for handling dates with day as minimum unit
def start_of_date(d: date) -> date:
    """Return the start of the date (which is just the date itself)"""
    return d

def end_of_date(d: date) -> date:
    """Return the end of the date (which is the date itself for inclusive ranges)"""
    return d + timedelta(days=1)

def next_date(d: date) -> date:
    """Return the next date"""
    return d + timedelta(days=1)

def add_days(d: date, days: int) -> date:
    """Add days to a date"""
    return d + timedelta(days=days)

def days_between(start: date, end: date) -> int:
    """Calculate number of days between two dates"""
    return (end - start).days

def start_of_week(d: date) -> date:
    """Return the start of the week (Monday) for the given date"""
    return d - timedelta(days=d.weekday())

def start_of_month(d: date) -> date:
    """Return the start of the month for the given date"""
    return date(d.year, d.month, 1)

def start_of_quarter(d: date) -> date:
    """Return the start of the quarter for the given date"""
    return date(d.year, (d.month - 1) // 3 * 3 + 1, 1)

def start_of_year(d: date) -> date:
    """Return the start of the year for the given date"""
    return date(d.year, 1, 1)

def end_of_week(d: date) -> date:
    """Return the end of the week (Sunday) for the given date"""
    week_start = start_of_week(d)
    return week_start + timedelta(weeks=1)

def end_of_month(d: date) -> date:
    """Return the end of the month for the given date"""
    month_start = start_of_month(d)
    return month_start + relativedelta(months=1)
    # _, last_day = monthrange(d.year, d.month)
    # return date(d.year, d.month, last_day)

def end_of_quarter(d: date) -> date:
    """Return the end of the quarter for the given date"""
    quarter_start = start_of_quarter(d)
    return quarter_start + relativedelta(months=3)
    # if quarter_start.month == 1:  # Q1
    #     return date(quarter_start.year, 3, 31)
    # elif quarter_start.month == 4:  # Q2
    #     return date(quarter_start.year, 6, 30)
    # elif quarter_start.month == 7:  # Q3
    #     return date(quarter_start.year, 9, 30)
    # else:  # Q4
    #     return date(quarter_start.year, 12, 31)

def end_of_year(d: date) -> date:
    """Return the end of the year for the given date"""
    year_start = start_of_year(d)
    return year_start + relativedelta(years=1)
    # return date(d.year, 12, 31)

def add_months(d: date, months: int) -> date:
    """Add months to a date"""
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    day = min(d.day, monthrange(year, month)[1])
    return date(year, month, day)

def add_years(d: date, years: int) -> date:
    """Add years to a date"""
    try:
        return date(d.year + years, d.month, d.day)
    except ValueError:  # Handle leap year edge case (Feb 29)
        return date(d.year + years, d.month, 28)

def add_quarters(d: date, quarters: int) -> date:
    """Add quarters to a date"""
    return add_months(d, quarters * 3)

def days_since_epoch(d): return (d - epoch_date).days
def weeks_since_epoch(d): return (start_of_week(d) - epoch_monday).days // 7
def months_since_epoch(d): return (d.year - 1970) * 12 + (d.month - 1)
def quarters_since_epoch(d): return (d.year - 1970) * 4 + (d.month - 1) // 3
def years_since_epoch(d): return d.year - 1970

# ----- helper aligners -----
def align_to_boundary(d: date, step: int, unit: str) -> date:
    if unit == "day":
        idx = (days_since_epoch(d) // step) * step
        return epoch_date + timedelta(days=idx)
    elif unit == "week":
        idx = (weeks_since_epoch(d) // step) * step
        return epoch_monday + timedelta(weeks=idx)
    elif unit == "month":
        idx = (months_since_epoch(d) // step) * step
        y = 1970 + idx // 12
        m = (idx % 12) + 1
        return date(y, m, 1)
    elif unit == "quarter":
        idx = (quarters_since_epoch(d) // step) * step
        return date(1970 + idx // 4, (idx % 4) * 3 + 1, 1)
    elif unit == "year":
        idx = (years_since_epoch(d) // step) * step
        return date(1970 + idx, 1, 1)
    else:
        raise NotImplementedError(f"Unsupported unit: {unit}")

def add_units(d: date, count: int, step: int, unit: str) -> date:
    if unit == "day":
        return d + timedelta(days=step * count)
    elif unit == "week":
        return d + timedelta(weeks=step * count)
    elif unit == "month":
        return add_months(d, step * count)
    elif unit == "quarter":
        return add_quarters(d, step * count)
    elif unit == "year":
        return add_years(d, step * count)

def floor_to_unit(d: date, unit: str) -> date:
    if unit == "day": return start_of_date(d)
    if unit == "week": return start_of_week(d)
    if unit == "month": return start_of_month(d)
    if unit == "quarter": return start_of_quarter(d)
    if unit == "year": return start_of_year(d)

def ceil_to_unit(d: date, step: int, unit: str) -> date:
    if unit == "day": return end_of_date(d)
    if unit == "week": return end_of_week(d)
    if unit == "month": return end_of_month(d)
    if unit == "quarter": return end_of_quarter(d)
    if unit == "year": return end_of_year(d)

def ceil_to_next_unit(d: date, step: int, unit: str) -> date:
    floored = align_to_boundary(d, step, unit)
    if floored == d:
        return d
    return add_units(floored, 1)

def get_boundary(d: date, step: int, unit: str) -> date:
    start = align_to_boundary(d, step, unit)
    end = add_units(start, 1, step, unit)
    return start, end

def get_partition_id(dt: date, step: int, unit: str) -> int:
    if unit == "day":
        return days_since_epoch(dt)//step
    elif unit == "week":
        return weeks_since_epoch(dt)//step
    elif unit == "month":
        return months_since_epoch(dt)//step
    elif unit == "quarter":
        return quarters_since_epoch(dt)//step
    elif unit == "year":
        return years_since_epoch(dt)//step
    else:
        raise NotImplementedError(f"Unsupported unit: {unit}")



def calculate_offset(start: date, unit: str) -> date:
    if unit == "day":
        effective_start = start_of_date(start)
        return effective_start, effective_start
    elif unit == "week":
        effective_start = start_of_week(start)
        return weeks_since_epoch(effective_start), effective_start
    elif unit == "month":
        effective_start = start_of_month(start)
        return months_since_epoch(effective_start), effective_start
    elif unit == "quarter":
        effective_start = start_of_quarter(start)
        return quarters_since_epoch(effective_start), effective_start
    elif unit == "year":
        effective_start = start_of_year(start)
        return years_since_epoch(effective_start), effective_start
    else:
        raise NotImplementedError(f"Unsupported unit: {unit}")

def generate_date_partitions(
    start: date,
    end: date,
    step: int,
    step_unit: str,
    bounded: bool = False
) -> Generator[Tuple[date, date, int], None, None]:
    """
    Generate date-based partitions with day as the minimum unit.
    
    Args:
        start: Start date
        end: End date
        step: Number of step_units per partition
        step_unit: Unit of partitioning ('day', 'week', 'month', 'quarter', 'year')
        bounded: If True, partitions are bounded by start/end dates
    
    Yields:
        DatePartition objects with start and end dates
    """
    unit = step_unit.lower()


    # ----- helper indexers -----


    # ----- main logic -----
    if not bounded:
        # Align to partition boundaries
        current_start = align_to_boundary(start, step, unit)
        effective_end = ceil_to_next_unit(end, step, unit)
        while current_start < effective_end:
            current_end = add_units(current_start, 1, step, unit)
            partition_id = get_partition_id(current_start, step, unit)
            yield (current_start, min(current_end, effective_end), partition_id)
            current_start = current_end
    else:
        # Stay within the provided bounds
        current_start = floor_to_unit(start, unit)
        effective_end = end  # do not ceil
        partition_id = 0
        while current_start < effective_end:
            current_end = add_units(current_start, 1, step, unit)
            yield (max(current_start, start), min(current_end, effective_end), partition_id)
            partition_id += 1
            current_start = current_end

def check_date_partitions_diff(result, expected):
    """Helper function to verify date partition results"""
    print(f"  Expected: {len(expected)} partitions")
    print(f"  Actual: {len(result)} partitions")
    
    # Verify length matches
    assert len(result) == len(expected), f"Expected {len(expected)} partitions, got {len(result)}"
    
    # Verify each partition matches
    for i, (actual, expected_partition) in enumerate(zip(result, expected)):
        actual_start, actual_end, actual_partition_id = actual
        expected_start, expected_end, expected_partition_id = expected_partition
        print(f"  Partition {i+1}: {actual_start} -> {actual_end} (ID: {actual_partition_id})")
        assert actual_start == expected_start, f"Partition {i+1} start mismatch: expected {expected_start}, got {actual_start}"
        assert actual_end == expected_end, f"Partition {i+1} end mismatch: expected {expected_end}, got {actual_end}"
        assert actual_partition_id == expected_partition_id, f"Partition {i+1} ID mismatch: expected {expected_partition_id}, got {actual_partition_id}"
    
    print("  ✓ All assertions passed")
    print()
    return result


def test_generate_date_partitions():
    """Test generate_date_partitions with comprehensive test cases covering all units and scenarios"""
    import random
    
    # Set seed for reproducible results
    random.seed(42)
    
    test_cases = []
    
    # Test Case 1-10: Daily partitions
    test_cases.extend([
        # Basic daily partitions
        (date(2023, 1, 1), date(2023, 1, 5), 1, "day", False),
        (date(2023, 1, 2), date(2023, 1, 6), 1, "day", True),
        (date(2023, 1, 1), date(2023, 1, 8), 2, "day", False),
        (date(2023, 1, 3), date(2023, 1, 10), 3, "day", True),
        (date(2023, 1, 1), date(2023, 1, 16), 5, "day", False),
        (date(2023, 1, 4), date(2023, 1, 18), 7, "day", True),
        (date(2023, 1, 1), date(2023, 2, 1), 10, "day", False),
        (date(2023, 1, 5), date(2023, 2, 5), 15, "day", True),
        (date(2023, 1, 1), date(2023, 3, 1), 30, "day", False),
        (date(2023, 1, 10), date(2023, 2, 20), 20, "day", True),
    ])
    
    # Test Case 11-20: Weekly partitions
    test_cases.extend([
        (date(2023, 1, 2), date(2023, 1, 23), 1, "week", False),  # Monday to Monday
        (date(2023, 1, 4), date(2023, 1, 25), 1, "week", True),   # Wednesday to Wednesday
        (date(2023, 1, 2), date(2023, 2, 13), 2, "week", False),
        (date(2023, 1, 9), date(2023, 2, 20), 2, "week", True),
        (date(2023, 1, 2), date(2023, 3, 6), 3, "week", False),
        (date(2023, 1, 16), date(2023, 3, 20), 3, "week", True),
        (date(2023, 1, 2), date(2023, 4, 3), 4, "week", False),
        (date(2023, 1, 23), date(2023, 4, 24), 4, "week", True),
        (date(2023, 1, 2), date(2023, 6, 5), 8, "week", False),
        (date(2023, 2, 6), date(2023, 7, 10), 10, "week", True),
    ])
    
    # Test Case 21-30: Monthly partitions
    test_cases.extend([
        (date(2023, 1, 1), date(2023, 4, 1), 1, "month", False),
        (date(2023, 2, 15), date(2023, 6, 15), 1, "month", True),
        (date(2023, 1, 1), date(2023, 7, 1), 2, "month", False),
        (date(2023, 3, 10), date(2023, 9, 10), 3, "month", True),
        (date(2023, 1, 1), date(2024, 1, 1), 6, "month", False),
        (date(2023, 4, 1), date(2024, 4, 1), 4, "month", True),
        (date(2023, 1, 1), date(2024, 7, 1), 3, "month", False),
        (date(2023, 5, 15), date(2024, 5, 15), 2, "month", True),
        (date(2023, 1, 1), date(2025, 1, 1), 12, "month", False),
        (date(2023, 6, 10), date(2024, 6, 10), 6, "month", True),
    ])
    
    # Test Case 31-40: Quarterly partitions
    test_cases.extend([
        (date(2023, 1, 1), date(2024, 1, 1), 1, "quarter", False),
        (date(2023, 2, 15), date(2024, 2, 15), 1, "quarter", True),
        (date(2023, 1, 1), date(2024, 7, 1), 2, "quarter", False),
        (date(2023, 3, 10), date(2024, 9, 10), 2, "quarter", True),
        (date(2022, 1, 1), date(2024, 1, 1), 3, "quarter", False),
        (date(2022, 4, 1), date(2024, 4, 1), 3, "quarter", True),
        (date(2021, 1, 1), date(2024, 1, 1), 4, "quarter", False),
        (date(2021, 7, 15), date(2024, 7, 15), 4, "quarter", True),
        (date(2020, 1, 1), date(2025, 1, 1), 8, "quarter", False),
        (date(2020, 10, 10), date(2025, 10, 10), 6, "quarter", True),
    ])
    
    # Test Case 41-50: Yearly partitions
    test_cases.extend([
        (date(2020, 1, 1), date(2023, 1, 1), 1, "year", False),
        (date(2020, 6, 15), date(2023, 6, 15), 1, "year", True),
        (date(2020, 1, 1), date(2026, 1, 1), 2, "year", False),
        (date(2021, 3, 10), date(2024, 3, 10), 1, "year", True),
        (date(2020, 1, 1), date(2030, 1, 1), 3, "year", False),
        (date(2021, 8, 20), date(2026, 8, 20), 2, "year", True),
        (date(2015, 1, 1), date(2025, 1, 1), 5, "year", False),
        (date(2016, 12, 31), date(2021, 12, 31), 1, "year", True),
        (date(2010, 1, 1), date(2030, 1, 1), 10, "year", False),
        (date(2012, 4, 15), date(2022, 4, 15), 5, "year", True),
    ])
    
    print("Running 50 comprehensive test cases for generate_date_partitions:")
    print("=" * 80)
    
    for i, (start, end, step, unit, bounded) in enumerate(test_cases, 1):
        try:
            result = list(generate_date_partitions(start, end, step, unit, bounded))
            
            # Print concise summary
            print(f"Test {i:2d}: {unit:9s} step={step:2d} {'bounded' if bounded else 'unbounded':9s} "
                  f"({start} to {end}) "
                  f"-> {len(result):2d} partitions")
            
            # Basic validation
            assert len(result) > 0, f"Test {i}: No partitions generated"
            
            # Verify partitions are contiguous and within bounds
            for j, partition in enumerate(result):
                partition_start, partition_end, partition_id = partition
                if j > 0:
                    # For date partitions, check if they're adjacent (end of previous should be start of current or one day before)
                    prev_start, prev_end, prev_id = result[j-1]
                    # Allow for both inclusive and exclusive ranges
                    assert prev_end == partition_start or (prev_end + timedelta(days=1)) == partition_start, \
                        f"Test {i}: Gap between partitions {j-1} and {j}: {prev_end} -> {partition_start}"
                
                if bounded:
                    assert partition_start >= start, f"Test {i}: Partition {j} starts before range start"
                    assert partition_end <= end, f"Test {i}: Partition {j} ends after range end"
            
            # Verify first and last partitions
            if bounded:
                first_start, first_end, first_id = result[0]
                last_start, last_end, last_id = result[-1]
                assert first_start >= start, f"Test {i}: First partition starts before range start"
                assert last_end <= end, f"Test {i}: Last partition ends after range end"
            
        except Exception as e:
            print(f"Test {i:2d}: FAILED - {str(e)}")
            raise
    
    print("=" * 80)
    print("✓ All 50 test cases completed successfully!")
    
    # Add some specific detailed test cases
    print("\nRunning detailed regression tests:")
    print("-" * 40)
    
    # Test 1: Simple daily partitions
    start, end, step, unit, bounded = date(2023, 1, 1), date(2023, 1, 5), 1, "day", False
    result = list(generate_date_partitions(start, end, step, unit, bounded))
    print(f"Detailed Test 1 - {'Bounded' if bounded else 'Unbounded'} {unit} partitions ({start} - {end}, step={step}):")
    expected = [
        (date(2023, 1, 1), date(2023, 1, 2), 19358),
        (date(2023, 1, 2), date(2023, 1, 3), 19359),
        (date(2023, 1, 3), date(2023, 1, 4), 19360),
        (date(2023, 1, 4), date(2023, 1, 5), 19361)
    ]
    check_date_partitions_diff(result, expected)

    # Test 2: Weekly partitions with bounds
    start, end, step, unit, bounded = date(2023, 1, 4), date(2023, 1, 25), 1, "week", True
    result = list(generate_date_partitions(start, end, step, unit, bounded))
    print(f"Detailed Test 2 - {'Bounded' if bounded else 'Unbounded'} {unit} partitions ({start} - {end}, step={step}):")
    expected = [
        (date(2023, 1, 4), date(2023, 1, 9), 0),
        (date(2023, 1, 9), date(2023, 1, 16), 1),
        (date(2023, 1, 16), date(2023, 1, 23), 2),
        (date(2023, 1, 23), date(2023, 1, 25), 3)
    ]
    check_date_partitions_diff(result, expected)

    # Test 3: Monthly partitions
    start, end, step, unit, bounded = date(2023, 2, 15), date(2023, 6, 15), 1, "month", True
    result = list(generate_date_partitions(start, end, step, unit, bounded))
    print(f"Detailed Test 3 - {'Bounded' if bounded else 'Unbounded'} {unit} partitions ({start} - {end}, step={step}):")
    expected = [
        (date(2023, 2, 15), date(2023, 3, 1), 0),
        (date(2023, 3, 1), date(2023, 4, 1), 1),
        (date(2023, 4, 1), date(2023, 5, 1), 2),
        (date(2023, 5, 1), date(2023, 6, 1), 3),
        (date(2023, 6, 1), date(2023, 6, 15), 4)
    ]
    check_date_partitions_diff(result, expected)

    # Test 4: Multi-day steps
    start, end, step, unit, bounded = date(2023, 1, 1), date(2023, 1, 15), 3, "day", False
    result = list(generate_date_partitions(start, end, step, unit, bounded))
    print(f"Detailed Test 4 - {'Bounded' if bounded else 'Unbounded'} {unit} partitions ({start} - {end}, step={step}):")
    expected = [
        (date(2022, 12, 30), date(2023, 1, 2), 6452),
        (date(2023, 1, 2), date(2023, 1, 5), 6453),
        (date(2023, 1, 5), date(2023, 1, 8), 6454),
        (date(2023, 1, 8), date(2023, 1, 11), 6455),
        (date(2023, 1, 11), date(2023, 1, 14), 6456),
        (date(2023, 1, 14), date(2023, 1, 17), 6457)
    ]
    check_date_partitions_diff(result, expected)


if __name__ == "__main__":
    # Run the test cases when script is executed directly
    test_generate_date_partitions()
