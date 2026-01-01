#!/usr/bin/env python
"""Quick script to check database contents."""
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
os.environ['POSTGRES_HOST'] = 'localhost'
django.setup()

from django.db.models import Count, Avg
from core.models import HealthRecord, SleepLog, NutritionLog, DailySummary

print("=" * 50)
print("DATABASE SUMMARY")
print("=" * 50)
print(f"HealthRecords: {HealthRecord.objects.count():,}")
print(f"SleepLogs: {SleepLog.objects.count():,}")
print(f"NutritionLogs: {NutritionLog.objects.count():,}")
print(f"DailySummaries: {DailySummary.objects.count():,}")

print("\n" + "=" * 50)
print("HEALTH RECORDS BY TYPE")
print("=" * 50)
for r in HealthRecord.objects.values('metric_type').annotate(count=Count('id')).order_by('-count'):
    print(f"  {r['metric_type']}: {r['count']:,}")

print("\n" + "=" * 50)
print("SAMPLE DAILY SUMMARY (Dec 20, 2025)")
print("=" * 50)
from datetime import date
ds = DailySummary.objects.filter(date=date(2025, 12, 20)).first()
if ds:
    print(f"Date: {ds.date}")
    print(f"  NUTRITION:")
    print(f"    Calories: {ds.calories} kcal")
    print(f"    Protein: {ds.protein_g}g ({ds.protein_pct}%)")
    print(f"    Carbs: {ds.carbs_g}g ({ds.carbs_pct}%)")
    print(f"    Fat: {ds.fat_g}g ({ds.fat_pct}%)")
    print(f"  SLEEP:")
    print(f"    Duration: {ds.sleep_duration_min} min")
    print(f"    Deep: {ds.deep_sleep_min} min")
    print(f"    REM: {ds.rem_sleep_min} min")
    print(f"    Light: {ds.light_sleep_min} min")
    print(f"    Score: {ds.sleep_score}")
    print(f"  ACTIVITY:")
    print(f"    Steps: {ds.steps:,}" if ds.steps else "    Steps: None")
    print(f"    Distance: {ds.distance_km} km")
    print(f"  VITALS:")
    print(f"    Resting HR: {ds.resting_hr} bpm")
    print(f"    HRV: {ds.hrv_rmssd}")
    print(f"    Readiness: {ds.readiness_score}")
    print(f"  Completeness: {ds.data_completeness}%")
else:
    print("No data for this date")

print("\n" + "=" * 50)
print("30-DAY AVERAGES")
print("=" * 50)
from core.services import InsightsService
avgs = InsightsService.get_averages(days=30)
for key, val in avgs.items():
    if val:
        print(f"  {key}: {val:.1f}")
