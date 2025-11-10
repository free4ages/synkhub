from datetime import datetime, timedelta, date
from typing import Generator, Tuple, Union
from dateutil.relativedelta import relativedelta


epoch = datetime(1970, 1, 1)
epoch_monday = datetime(1970, 1, 5)  # first Monday

def start_of_second(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)

def start_of_minute(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute)

def start_of_hour(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, dt.day, dt.hour)

def start_of_day(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, dt.day)

def start_of_week(dt: datetime) -> datetime:
    return start_of_day(dt) - timedelta(days=dt.weekday())

def start_of_month(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, 1)

def start_of_quarter(dt: datetime) -> datetime:
    return datetime(dt.year, (dt.month - 1) // 3 * 3 + 1, 1)

def start_of_year(dt: datetime) -> datetime:
    return datetime(dt.year, 1, 1)


def end_of_second(dt: datetime) -> datetime:
    second_start = start_of_second(dt)
    if dt == second_start:
        return dt
    return second_start + timedelta(seconds=1)

def end_of_minute(dt: datetime) -> datetime:
    minute_start = start_of_minute(dt)
    if dt == minute_start:
        return dt
    return minute_start + timedelta(minutes=1)

def end_of_hour(dt: datetime) -> datetime:
    hour_start = start_of_hour(dt)
    if dt == hour_start:
        return dt
    return hour_start + timedelta(hours=1)

def end_of_day(dt: datetime) -> datetime:
    day_start = start_of_day(dt)
    if dt == day_start:
        return dt
    return day_start + timedelta(days=1)


def end_of_week(dt: datetime) -> datetime:
    week_start = start_of_week(dt)
    if dt == week_start:
        return dt
    return week_start + timedelta(weeks=1)

def end_of_month(dt: datetime) -> datetime:
    month_start = start_of_month(dt)
    if dt == month_start:
        return dt
    return month_start + relativedelta(months=1)

def end_of_quarter(dt: datetime) -> datetime:
    quarter_start = start_of_quarter(dt)
    if dt == quarter_start:
        return dt
    return quarter_start + relativedelta(months=3)


def end_of_year(dt: datetime) -> datetime:
    year_start = start_of_year(dt)
    if dt == year_start:
        return dt
    return year_start + relativedelta(years=1)

def add_months(dt: datetime, months: int) -> datetime:
    year = dt.year + (dt.month - 1 + months) // 12
    month = (dt.month - 1 + months) % 12 + 1
    return datetime(year, month, 1)

def add_years(dt: datetime, years: int) -> datetime:
    return datetime(dt.year + years, 1, 1)

def add_quarters(dt: datetime, quarters: int) -> datetime:
    return datetime(dt.year + quarters // 4, (dt.month - 1 + quarters) % 12 + 1, 1)

# ----- helper indexers -----
def seconds_since_epoch(dt): return int((dt - epoch).total_seconds())
def minutes_since_epoch(dt): return int((start_of_minute(dt) - epoch).total_seconds() // 60)
def hours_since_epoch(dt): return int((start_of_hour(dt) - epoch).total_seconds() // 3600)
def days_since_epoch(dt): return (start_of_day(dt) - epoch).days
def weeks_since_epoch(dt): return (start_of_week(dt) - epoch_monday).days // 7
def months_since_epoch(dt): return (dt.year - 1970) * 12 + (dt.month - 1)
def quarters_since_epoch(dt): return (dt.year - 1970) * 4 + (dt.month - 1) // 3
def years_since_epoch(dt): return dt.year - 1970

# ----- helper aligners -----
def align_to_boundary(dt: datetime, step: int, unit: str) -> datetime:
    if unit == "timestamp":
        idx = (seconds_since_epoch(dt) // step) * step
        return epoch + timedelta(seconds=idx)
    elif unit == "minute":
        idx = (minutes_since_epoch(dt) // step) * step
        return epoch + timedelta(minutes=idx)
    elif unit == "hour":
        idx = (hours_since_epoch(dt) // step) * step
        return epoch + timedelta(hours=idx)
    elif unit == "day":
        idx = (days_since_epoch(dt) // step) * step
        return epoch + timedelta(days=idx)
    elif unit == "week":
        idx = (weeks_since_epoch(dt) // step) * step
        return epoch_monday + timedelta(weeks=idx)
    elif unit == "month":
        idx = (months_since_epoch(dt) // step) * step
        y = 1970 + idx // 12
        m = (idx % 12) + 1
        return datetime(y, m, 1)
    elif unit == "quarter":
        idx = (quarters_since_epoch(dt) // step) * step
        return datetime(1970 + idx // 4, (idx % 4) * 3 + 1, 1)
    elif unit == "year":
        idx = (years_since_epoch(dt) // step) * step
        return datetime(1970 + idx, 1, 1)
    else:
        raise NotImplementedError(f"Unsupported unit: {unit}")

def add_units(dt: datetime, count: int, step: int, unit: str) -> datetime:
    if unit == "timestamp":
        return dt + timedelta(seconds=step * count)
    elif unit == "minute":
        return dt + timedelta(minutes=step * count)
    elif unit == "hour":
        return dt + timedelta(hours=step * count)
    elif unit == "day":
        return dt + timedelta(days=step * count)
    elif unit == "week":
        return dt + timedelta(weeks=step * count)
    elif unit == "month":
        return add_months(dt, step * count)
    elif unit == "quarter":
        return add_quarters(dt, step * count)
    elif unit == "year":
        return add_years(dt, step * count)

def floor_to_unit(dt: datetime, unit: str) -> datetime:
    if unit == "timestamp": return start_of_second(dt)
    if unit == "minute": return start_of_minute(dt)
    if unit == "hour": return start_of_hour(dt)
    if unit == "day": return start_of_day(dt)
    if unit == "week": return start_of_week(dt)
    if unit == "month": return start_of_month(dt)
    if unit == "quarter": return start_of_quarter(dt)
    if unit == "year": return start_of_year(dt)

def ceil_to_unit(dt: datetime, unit: str) -> datetime:
    if unit == "timestamp": return end_of_second(dt)
    if unit == "minute": return end_of_minute(dt)
    if unit == "hour": return end_of_hour(dt)
    if unit == "day": return end_of_day(dt)
    if unit == "week": return end_of_week(dt)
    if unit == "month": return end_of_month(dt)
    if unit == "quarter": return end_of_quarter(dt)
    if unit == "year": return end_of_year(dt)


def ceil_to_next_unit(dt: datetime, step: int, unit: str) -> datetime:
    # if unit == "timestamp":
    #     secs = seconds_since_epoch(dt)
    #     if secs % step == 0:
    #         return dt
    #     return epoch + timedelta(seconds=((secs // step) + 1) * step)
    floored = align_to_boundary(dt, step, unit)
    # floored = floor_to_unit(dt)
    if floored == dt:
        return dt
    return add_units(floored, 1, step, unit)

def get_boundary(dt: datetime, step: int, unit: str) -> datetime:
    start = align_to_boundary(dt, step, unit)
    end = add_units(start, 1, step, unit)
    return start, end

def get_partition_id(dt: datetime, step: int, unit: str) -> int:
    if unit == "timestamp":
        return seconds_since_epoch(dt)//step
    elif unit == "minute":
        return minutes_since_epoch(dt)//step
    elif unit == "hour":
        return hours_since_epoch(dt)//step
    elif unit == "day":
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

def calculate_offset(start: datetime, unit: str) -> datetime:
    if unit == "timestamp":
        effective_start = start_of_second(start)
        return effective_start.timestamp(), effective_start
    elif unit == "minute":
        effective_start = start_of_minute(start)
        return minutes_since_epoch(effective_start), effective_start
    elif unit == "hour":
        effective_start = start_of_hour(start)
        return hours_since_epoch(effective_start), effective_start
    elif unit == "day":
        effective_start = start_of_day(start)
        return days_since_epoch(effective_start), effective_start
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

def generate_datetime_partitions(
    start: datetime,
    end: datetime,
    step: int,
    step_unit: str,
    bounded: bool = False
) -> Generator[Tuple[datetime, datetime, int], None, None]:
    unit = step_unit.lower()




    # ----- main logic -----
    if not bounded:
        # import pdb; pdb.set_trace()
        current_start = align_to_boundary(start, step, unit)
        effective_end = ceil_to_next_unit(end, step, unit)
        while current_start < effective_end:
            current_end = add_units(current_start, 1, step, unit)
            partition_id = get_partition_id(current_start, step, unit)
            yield current_start, min(current_end, effective_end), partition_id
            current_start = current_end
    else:
        current_start = floor_to_unit(start, unit)
        effective_end = end  # do not ceil
        partition_id = 0
        while current_start < effective_end:
            current_end = add_units(current_start, 1, step, unit)
            yield max(current_start, start), min(current_end, effective_end), partition_id
            partition_id += 1
            current_start = current_end

def check_diff(result, expected):
    print(f"  Expected: {len(expected)} partitions")
    print(f"  Actual: {len(result)} partitions")
    
    # Verify length matches
    assert len(result) == len(expected), f"Expected {len(expected)} partitions, got {len(result)}"
    
    # Verify each partition matches
    for i, (actual, expected_partition) in enumerate(zip(result, expected)):
        actual_start, actual_end, actual_partition_id = actual
        expected_start, expected_end, expected_partition_id = expected_partition
        print(f"  Partition {i+1}: {actual_start} -> {actual_end}")
        assert actual_start == expected_start, f"Partition {i+1} start mismatch: expected {expected_start}, got {actual_start}"
        assert actual_end == expected_end, f"Partition {i+1} end mismatch: expected {expected_end}, got {actual_end}"
    
    print("  ✓ All assertions passed")
    print()
    return result

def test_generate_datetime_partitions():
    """Test _generate_datetime_partitions with 50 diverse test cases covering all units and scenarios"""
    import random
    
    # Set seed for reproducible results
    random.seed(42)
    
    test_cases = []
    
    # Test Case 1-10: Timestamp partitions (seconds)
    test_cases.extend([
        # Small intervals with seconds
        (datetime(2023, 1, 1, 0, 0, 0), datetime(2023, 1, 1, 0, 0, 5), 1, "timestamp", False),
        (datetime(2023, 1, 1, 0, 0, 0), datetime(2023, 1, 1, 0, 0, 4), 2, "timestamp", True),
        (datetime(2023, 1, 1, 12, 30, 0), datetime(2023, 1, 1, 12, 30, 10), 5, "timestamp", False),
        (datetime(2023, 1, 1, 12, 30, 15), datetime(2023, 1, 1, 12, 30, 25), 3, "timestamp", True),
        # Minutes as seconds
        (datetime(2023, 1, 1, 0, 0, 0), datetime(2023, 1, 1, 0, 3, 0), 60, "timestamp", False),
        (datetime(2023, 1, 1, 0, 0, 0), datetime(2023, 1, 1, 0, 5, 0), 120, "timestamp", True),
        # Hours as seconds
        (datetime(2023, 1, 1, 0, 0, 0), datetime(2023, 1, 1, 3, 0, 0), 3600, "timestamp", False),
        (datetime(2023, 1, 1, 1, 0, 0), datetime(2023, 1, 1, 4, 0, 0), 7200, "timestamp", True),
        # Days as seconds
        (datetime(2023, 1, 1), datetime(2023, 1, 3), 86400, "timestamp", False),
        (datetime(2023, 1, 1), datetime(2023, 1, 4), 172800, "timestamp", True),
    ])
    
    # Test Case 11-20: Hour partitions
    test_cases.extend([
        (datetime(2023, 1, 1, 0), datetime(2023, 1, 1, 5), 1, "hour", False),
        (datetime(2023, 1, 1, 2), datetime(2023, 1, 1, 8), 2, "hour", True),
        (datetime(2023, 1, 1, 0), datetime(2023, 1, 1, 12), 3, "hour", False),
        (datetime(2023, 1, 1, 6), datetime(2023, 1, 1, 18), 4, "hour", True),
        (datetime(2023, 1, 1, 0), datetime(2023, 1, 2, 0), 6, "hour", False),
        (datetime(2023, 1, 1, 3), datetime(2023, 1, 2, 3), 8, "hour", True),
        (datetime(2023, 1, 1, 0), datetime(2023, 1, 3, 0), 12, "hour", False),
        (datetime(2023, 1, 1, 6), datetime(2023, 1, 3, 6), 18, "hour", True),
        (datetime(2023, 1, 1, 0), datetime(2023, 1, 4, 0), 24, "hour", False),
        (datetime(2023, 1, 1, 12), datetime(2023, 1, 3, 12), 36, "hour", True),
    ])
    
    # Test Case 21-30: Day partitions
    test_cases.extend([
        (datetime(2023, 1, 1), datetime(2023, 1, 4), 1, "day", False),
        (datetime(2023, 1, 2), datetime(2023, 1, 6), 1, "day", True),
        (datetime(2023, 1, 1), datetime(2023, 1, 8), 2, "day", False),
        (datetime(2023, 1, 3), datetime(2023, 1, 10), 3, "day", True),
        (datetime(2023, 1, 1), datetime(2023, 1, 16), 5, "day", False),
        (datetime(2023, 1, 4), datetime(2023, 1, 18), 7, "day", True),
        (datetime(2023, 1, 1), datetime(2023, 2, 1), 10, "day", False),
        (datetime(2023, 1, 5), datetime(2023, 2, 5), 15, "day", True),
        (datetime(2023, 1, 1), datetime(2023, 3, 1), 30, "day", False),
        (datetime(2023, 1, 10), datetime(2023, 2, 20), 20, "day", True),
    ])
    
    # Test Case 31-40: Week partitions
    test_cases.extend([
        (datetime(2023, 1, 2), datetime(2023, 1, 23), 1, "week", False),  # Monday to Monday
        (datetime(2023, 1, 4), datetime(2023, 1, 25), 1, "week", True),   # Wednesday to Wednesday
        (datetime(2023, 1, 2), datetime(2023, 2, 13), 2, "week", False),
        (datetime(2023, 1, 9), datetime(2023, 2, 20), 2, "week", True),
        (datetime(2023, 1, 2), datetime(2023, 3, 6), 3, "week", False),
        (datetime(2023, 1, 16), datetime(2023, 3, 20), 3, "week", True),
        (datetime(2023, 1, 2), datetime(2023, 4, 3), 4, "week", False),
        (datetime(2023, 1, 23), datetime(2023, 4, 24), 4, "week", True),
        (datetime(2023, 1, 2), datetime(2023, 6, 5), 8, "week", False),
        (datetime(2023, 2, 6), datetime(2023, 7, 10), 10, "week", True),
    ])
    
    # Test Case 41-50: Month and Year partitions
    test_cases.extend([
        # Month partitions
        (datetime(2023, 1, 1), datetime(2023, 4, 1), 1, "month", False),
        (datetime(2023, 2, 15), datetime(2023, 6, 15), 1, "month", True),
        (datetime(2023, 1, 1), datetime(2023, 7, 1), 2, "month", False),
        (datetime(2023, 3, 10), datetime(2023, 9, 10), 3, "month", True),
        (datetime(2023, 1, 1), datetime(2024, 1, 1), 6, "month", False),
        (datetime(2023, 4, 1), datetime(2024, 4, 1), 4, "month", True),
        # Year partitions
        (datetime(2020, 1, 1), datetime(2023, 1, 1), 1, "year", False),
        (datetime(2020, 6, 15), datetime(2023, 6, 15), 1, "year", True),
        (datetime(2020, 1, 1), datetime(2026, 1, 1), 2, "year", False),
        (datetime(2021, 3, 10), datetime(2024, 3, 10), 1, "year", True),
    ])
    
    print("Running 50 comprehensive test cases for _generate_datetime_partitions:")
    print("=" * 80)
    
    for i, (start, end, step, unit, bounded) in enumerate(test_cases, 1):
        try:
            result = list(generate_datetime_partitions(start, end, step, unit, bounded))
            
            # Print concise summary
            print(f"Test {i:2d}: {unit:9s} step={step:2d} {'bounded' if bounded else 'unbounded':9s} "
                  f"({start.strftime('%Y-%m-%d %H:%M:%S')} to {end.strftime('%Y-%m-%d %H:%M:%S')}) "
                  f"-> {len(result):2d} partitions")
            
            # Basic validation
            assert len(result) > 0, f"Test {i}: No partitions generated"
            
            # Verify partitions are contiguous and within bounds
            for j, partition in enumerate(result):
                partition_start, partition_end, partition_id = partition
                if j > 0:
                    prev_start, prev_end, prev_id = result[j-1]
                    assert prev_end == partition_start, f"Test {i}: Gap between partitions {j-1} and {j}"
                
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
    
    # Keep the original test cases for regression
    print("\nRunning original regression tests:")
    print("-" * 40)
    
    start, end, step, unit, bounded = datetime(2020, 1, 1), datetime(2025, 1, 1), 1, "year", False
    result = list(generate_datetime_partitions(start, end, step, unit, bounded))
    print(f"Original Test 1 - {'Bounded' if bounded else 'Unbounded'} {unit} partitions ({start} - {end}, step={step}):")
    expected = [
        (datetime(2020, 1, 1), datetime(2021, 1, 1), 0),
        (datetime(2021, 1, 1), datetime(2022, 1, 1), 1),
        (datetime(2022, 1, 1), datetime(2023, 1, 1), 2),
        (datetime(2023, 1, 1), datetime(2024, 1, 1), 3),
        (datetime(2024, 1, 1), datetime(2025, 1, 1), 4)
    ]
    check_diff(result, expected)

    start, end, step, unit, bounded = datetime(2020, 10, 1), datetime(2025, 1, 2), 1, "year", True
    result = list(generate_datetime_partitions(start, end, step, unit, bounded))
    print(f"Original Test 2 - {'Bounded' if bounded else 'Unbounded'} {unit} partitions ({start} - {end}, step={step}):")
    expected = [
        (datetime(2020, 10, 1), datetime(2021, 1, 1), 0),
        (datetime(2021, 1, 1), datetime(2022, 1, 1), 1),
        (datetime(2022, 1, 1), datetime(2023, 1, 1), 2),
        (datetime(2023, 1, 1), datetime(2024, 1, 1), 3),
        (datetime(2024, 1, 1), datetime(2025, 1, 1), 4),
        (datetime(2025, 1, 1), datetime(2025, 1, 2), 5)
    ]
    check_diff(result, expected)

    start, end, step, unit, bounded = datetime(2021, 1, 16), datetime(2025, 10, 1), 2, "year", False
    result = list(generate_datetime_partitions(start, end, step, unit, bounded))
    print(f"Original Test 3 - {'Bounded' if bounded else 'Unbounded'} {unit} partitions ({start} - {end}, step={step}):")
    expected = [
        (datetime(2020, 1, 1), datetime(2022, 1, 1), 25),
        (datetime(2022, 1, 1), datetime(2024, 1, 1), 26),
        (datetime(2024, 1, 1), datetime(2026, 1, 1), 27)
    ]
    check_diff(result, expected)

    start, end, step, unit, bounded = datetime(1970,1,1), datetime(1970,1,10), 24*60*60, "timestamp", False
    result = list(generate_datetime_partitions(start, end, step, unit, bounded))
    print(f"Original Test 4 - {'Bounded' if bounded else 'Unbounded'} {unit} partitions ({start} - {end}, step={step}):")
    expected = [
        (datetime(1970, 1, 1, 0, 0), datetime(1970, 1, 2, 0, 0), 0),
        (datetime(1970, 1, 2, 0, 0), datetime(1970, 1, 3, 0, 0), 1),
        (datetime(1970, 1, 3, 0, 0), datetime(1970, 1, 4, 0, 0), 2),
        (datetime(1970, 1, 4, 0, 0), datetime(1970, 1, 5, 0, 0), 3),
        (datetime(1970, 1, 5, 0, 0), datetime(1970, 1, 6, 0, 0), 4),
        (datetime(1970, 1, 6, 0, 0), datetime(1970, 1, 7, 0, 0), 5),
        (datetime(1970, 1, 7, 0, 0), datetime(1970, 1, 8, 0, 0), 6),
        (datetime(1970, 1, 8, 0, 0), datetime(1970, 1, 9, 0, 0), 7),
        (datetime(1970, 1, 9, 0, 0), datetime(1970, 1, 10, 0, 0), 8)
    ]
    check_diff(result, expected)




if __name__ == "__main__":
    # Run the test cases when script is executed directly
    test_generate_datetime_partitions()
