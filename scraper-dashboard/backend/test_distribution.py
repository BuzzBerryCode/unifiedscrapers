#!/usr/bin/env python3

from datetime import datetime, timedelta
import random

# Test the distribution logic
now = datetime.utcnow()
print(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Today is: {now.strftime('%A, %B %d, %Y')}")
print()

print("Testing distribution logic:")
print("=" * 50)

for i in range(14):  # Test first 14 creators (2 full cycles)
    # Calculate which day this creator should be assigned to (0-6)
    day_offset = i % 7
    
    # Calculate the target date: spread across PAST 7 days
    days_ago = 7 - day_offset
    target_date = now - timedelta(days=days_ago)
    
    # Add random time within the day
    random_hours = random.randint(6, 18)
    random_minutes = random.randint(0, 59)
    random_seconds = random.randint(0, 59)
    
    target_date = target_date.replace(
        hour=random_hours, 
        minute=random_minutes, 
        second=random_seconds, 
        microsecond=random.randint(0, 999999)
    )
    
    # Calculate when this creator will be due (7 days after updated_at)
    due_date = target_date + timedelta(days=7)
    
    print(f"Creator {i:2d}: day_offset={day_offset}, days_ago={days_ago}")
    print(f"           updated_at={target_date.strftime('%Y-%m-%d %H:%M:%S')} ({target_date.strftime('%A')})")
    print(f"           due_date  ={due_date.strftime('%Y-%m-%d %H:%M:%S')} ({due_date.strftime('%A')})")
    print()

print("Summary of due dates:")
print("=" * 30)
due_dates = {}
for i in range(21):  # Test 21 creators (3 full cycles)
    day_offset = i % 7
    days_ago = 7 - day_offset
    target_date = now - timedelta(days=days_ago)
    due_date = target_date + timedelta(days=7)
    due_day = due_date.strftime('%A')
    
    if due_day not in due_dates:
        due_dates[due_day] = 0
    due_dates[due_day] += 1

for day, count in sorted(due_dates.items()):
    print(f"{day}: {count} creators")
