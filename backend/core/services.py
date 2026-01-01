"""
Core services for data aggregation and insights generation.
"""

from datetime import date, timedelta
from typing import Optional
from django.db.models import Avg, Min, Max, Sum
from django.contrib.auth.models import User

from .models import (
    DailySummary, HealthRecord, SleepLog, NutritionLog,
    MetricType
)


class DailySummaryService:
    """
    Service to build and update DailySummary records from normalized data.
    """
    
    @classmethod
    def build_summary(
        cls,
        target_date: date,
        user: Optional[User] = None
    ) -> DailySummary:
        """
        Build or update a DailySummary for a specific date.
        Aggregates data from HealthRecord, SleepLog, and NutritionLog.
        """
        # Get or create the summary
        summary, created = DailySummary.objects.get_or_create(
            user=user,
            date=target_date
        )
        
        # Populate from each data source
        cls._populate_nutrition(summary, target_date, user)
        cls._populate_sleep(summary, target_date, user)
        cls._populate_activity(summary, target_date, user)
        cls._populate_vitals(summary, target_date, user)
        cls._populate_scores(summary, target_date, user)
        
        summary.save()
        return summary
    
    @classmethod
    def build_range(
        cls,
        start_date: date,
        end_date: date,
        user: Optional[User] = None
    ) -> list[DailySummary]:
        """
        Build summaries for a date range.
        """
        summaries = []
        current = start_date
        while current <= end_date:
            summary = cls.build_summary(current, user)
            summaries.append(summary)
            current += timedelta(days=1)
        return summaries
    
    @classmethod
    def rebuild_all(cls, user: Optional[User] = None) -> int:
        """
        Rebuild all summaries based on available data.
        Returns count of summaries created/updated.
        """
        # Find date range from all data sources
        dates = set()
        
        # Get dates from HealthRecords
        hr_dates = HealthRecord.objects.filter(user=user).values_list('date', flat=True).distinct()
        dates.update(d for d in hr_dates if d)
        
        # Get dates from SleepLogs
        sleep_dates = SleepLog.objects.filter(user=user).values_list('date_of_sleep', flat=True).distinct()
        dates.update(d for d in sleep_dates if d)
        
        # Get dates from NutritionLogs
        nutrition_dates = NutritionLog.objects.filter(user=user).values_list('date', flat=True).distinct()
        dates.update(d for d in nutrition_dates if d)
        
        # Build summary for each date
        for d in sorted(dates):
            cls.build_summary(d, user)
        
        return len(dates)
    
    @classmethod
    def _populate_nutrition(
        cls,
        summary: DailySummary,
        target_date: date,
        user: Optional[User]
    ):
        """Populate nutrition fields from NutritionLog."""
        try:
            nutrition = NutritionLog.objects.get(user=user, date=target_date)
            summary.calories = nutrition.calories
            summary.protein_g = nutrition.protein_g
            summary.carbs_g = nutrition.carbs_g
            summary.fat_g = nutrition.fat_g
            summary.fiber_g = nutrition.fiber_g
            summary.sodium_mg = nutrition.sodium_mg
        except NutritionLog.DoesNotExist:
            pass
    
    @classmethod
    def _populate_sleep(
        cls,
        summary: DailySummary,
        target_date: date,
        user: Optional[User]
    ):
        """
        Populate sleep fields from SleepLog.
        Uses the sleep that ENDED on this date (previous night's sleep).
        """
        try:
            # Get main sleep for this date
            sleep = SleepLog.objects.filter(
                user=user,
                date_of_sleep=target_date
            ).order_by('-duration_minutes').first()
            
            if sleep:
                summary.sleep_duration_min = sleep.duration_minutes
                summary.sleep_minutes_asleep = sleep.minutes_asleep
                summary.sleep_minutes_awake = sleep.minutes_awake
                summary.deep_sleep_min = sleep.deep_sleep_minutes
                summary.light_sleep_min = sleep.light_sleep_minutes
                summary.rem_sleep_min = sleep.rem_sleep_minutes
                summary.sleep_efficiency = sleep.efficiency
                summary.sleep_score = sleep.sleep_score
                if sleep.start_time:
                    summary.sleep_start_time = sleep.start_time.time()
                if sleep.end_time:
                    summary.sleep_end_time = sleep.end_time.time()
        except Exception:
            pass
    
    @classmethod
    def _populate_activity(
        cls,
        summary: DailySummary,
        target_date: date,
        user: Optional[User]
    ):
        """Populate activity fields from HealthRecord."""
        records = HealthRecord.objects.filter(user=user, date=target_date)
        
        # Steps
        steps_record = records.filter(metric_type='steps').first()
        if steps_record:
            summary.steps = int(steps_record.value)
        
        # Distance
        distance_record = records.filter(metric_type='distance').first()
        if distance_record:
            summary.distance_km = distance_record.value
        
        # Active Zone Minutes (from AZM records)
        azm_record = records.filter(metric_type='active_zone_minutes').first()
        if azm_record:
            summary.active_zone_minutes = int(azm_record.value)
        
        # Activity breakdown by intensity
        for level in ['very_active', 'moderately_active', 'lightly_active', 'sedentary']:
            record = records.filter(
                metric_type='active_minutes',
                metadata__activity_level=level
            ).first()
            if record:
                setattr(summary, f'{level}_minutes', int(record.value))
    
    @classmethod
    def _populate_vitals(
        cls,
        summary: DailySummary,
        target_date: date,
        user: Optional[User]
    ):
        """Populate vital signs from HealthRecord."""
        records = HealthRecord.objects.filter(user=user, date=target_date)
        
        # Resting Heart Rate (stored as 'resting_heart_rate' from UserSleepScores)
        rhr_record = records.filter(metric_type='resting_heart_rate').first()
        if rhr_record:
            summary.resting_hr = int(rhr_record.value)
        
        # HRV (stored as 'hrv_rmssd' in the database)
        hrv_record = records.filter(metric_type='hrv_rmssd').first()
        if hrv_record:
            summary.hrv_rmssd = hrv_record.value
            # Check for deep sleep HRV in metadata
            if hrv_record.metadata and 'deep_rmssd' in hrv_record.metadata:
                summary.hrv_deep_rmssd = hrv_record.metadata['deep_rmssd']
        
        # SpO2
        spo2_record = records.filter(metric_type='spo2').first()
        if spo2_record:
            summary.spo2_avg = spo2_record.value
            if spo2_record.metadata:
                summary.spo2_min = spo2_record.metadata.get('min_value')
        
        # Skin Temperature
        temp_record = records.filter(metric_type='skin_temperature').first()
        if temp_record:
            summary.skin_temp_deviation = temp_record.value
    
    @classmethod
    def _populate_scores(
        cls,
        summary: DailySummary,
        target_date: date,
        user: Optional[User]
    ):
        """Populate readiness and stress scores from HealthRecord."""
        records = HealthRecord.objects.filter(user=user, date=target_date)
        
        # Readiness Score
        readiness_record = records.filter(metric_type='readiness_score').first()
        if readiness_record:
            summary.readiness_score = int(readiness_record.value)
        
        # Stress Score
        stress_record = records.filter(metric_type='stress_score').first()
        if stress_record:
            summary.stress_score = int(stress_record.value)
        
        # Sleep Score (from HealthRecord, not SleepLog)
        sleep_score_record = records.filter(metric_type='sleep_score').first()
        if sleep_score_record:
            summary.sleep_score = int(sleep_score_record.value)


class InsightsService:
    """
    Service to generate insights and correlations from DailySummary data.
    """
    
    @classmethod
    def get_averages(
        cls,
        user: Optional[User] = None,
        days: int = 30
    ) -> dict:
        """
        Get rolling averages for key metrics over the past N days.
        """
        cutoff = date.today() - timedelta(days=days)
        summaries = DailySummary.objects.filter(
            user=user,
            date__gte=cutoff
        )
        
        return summaries.aggregate(
            avg_calories=Avg('calories'),
            avg_protein=Avg('protein_g'),
            avg_steps=Avg('steps'),
            avg_sleep=Avg('sleep_duration_min'),
            avg_deep_sleep=Avg('deep_sleep_min'),
            avg_rem_sleep=Avg('rem_sleep_min'),
            avg_resting_hr=Avg('resting_hr'),
            avg_hrv=Avg('hrv_rmssd'),
            avg_sleep_score=Avg('sleep_score'),
            avg_readiness=Avg('readiness_score'),
        )
    
    @classmethod
    def get_correlations(
        cls,
        user: Optional[User] = None,
        days: int = 90
    ) -> dict:
        """
        Calculate correlations between metrics.
        Returns correlation coefficients for key relationships.
        
        Note: For production, use numpy/scipy for proper correlation calculation.
        This is a simplified placeholder.
        """
        cutoff = date.today() - timedelta(days=days)
        summaries = list(DailySummary.objects.filter(
            user=user,
            date__gte=cutoff
        ).values(
            'date', 'calories', 'protein_g', 'steps', 'sleep_duration_min',
            'deep_sleep_min', 'resting_hr', 'hrv_rmssd', 'sleep_score',
            'readiness_score'
        ))
        
        if len(summaries) < 7:
            return {'error': 'Insufficient data for correlation analysis'}
        
        # TODO: Implement proper correlation calculation with numpy
        # For now, return structure placeholder
        return {
            'sample_size': len(summaries),
            'date_range': {
                'start': cutoff.isoformat(),
                'end': date.today().isoformat()
            },
            'correlations': {
                'protein_vs_deep_sleep': None,  # Calculate with numpy
                'steps_vs_sleep_quality': None,
                'hrv_vs_readiness': None,
                'calories_vs_next_day_hrv': None,
            }
        }
    
    @classmethod
    def find_anomalies(
        cls,
        user: Optional[User] = None,
        days: int = 30,
        std_threshold: float = 2.0
    ) -> list[dict]:
        """
        Find days where metrics deviate significantly from baseline.
        """
        # Get baseline averages
        averages = cls.get_averages(user, days)
        
        # Get recent data
        cutoff = date.today() - timedelta(days=days)
        summaries = DailySummary.objects.filter(
            user=user,
            date__gte=cutoff
        )
        
        anomalies = []
        
        for summary in summaries:
            day_anomalies = []
            
            # Check HRV
            if summary.hrv_rmssd and averages['avg_hrv']:
                hrv_diff = abs(summary.hrv_rmssd - averages['avg_hrv'])
                if hrv_diff > averages['avg_hrv'] * 0.3:  # 30% deviation
                    day_anomalies.append({
                        'metric': 'hrv',
                        'value': summary.hrv_rmssd,
                        'baseline': averages['avg_hrv'],
                        'direction': 'high' if summary.hrv_rmssd > averages['avg_hrv'] else 'low'
                    })
            
            # Check Resting HR
            if summary.resting_hr and averages['avg_resting_hr']:
                hr_diff = abs(summary.resting_hr - averages['avg_resting_hr'])
                if hr_diff > 8:  # 8 bpm deviation
                    day_anomalies.append({
                        'metric': 'resting_hr',
                        'value': summary.resting_hr,
                        'baseline': averages['avg_resting_hr'],
                        'direction': 'high' if summary.resting_hr > averages['avg_resting_hr'] else 'low'
                    })
            
            # Check Sleep
            if summary.sleep_duration_min and averages['avg_sleep']:
                sleep_diff = abs(summary.sleep_duration_min - averages['avg_sleep'])
                if sleep_diff > 90:  # 1.5 hour deviation
                    day_anomalies.append({
                        'metric': 'sleep',
                        'value': summary.sleep_duration_min,
                        'baseline': averages['avg_sleep'],
                        'direction': 'high' if summary.sleep_duration_min > averages['avg_sleep'] else 'low'
                    })
            
            if day_anomalies:
                anomalies.append({
                    'date': summary.date.isoformat(),
                    'anomalies': day_anomalies
                })
        
        return anomalies
    
    @classmethod
    def generate_weekly_report(
        cls,
        user: Optional[User] = None,
        week_start: Optional[date] = None
    ) -> dict:
        """
        Generate a weekly summary report.
        """
        if not week_start:
            # Default to current week (Monday start)
            today = date.today()
            week_start = today - timedelta(days=today.weekday())
        
        week_end = week_start + timedelta(days=6)
        
        summaries = DailySummary.objects.filter(
            user=user,
            date__gte=week_start,
            date__lte=week_end
        )
        
        days_with_data = summaries.count()
        
        return {
            'week_start': week_start.isoformat(),
            'week_end': week_end.isoformat(),
            'days_with_data': days_with_data,
            'averages': summaries.aggregate(
                avg_calories=Avg('calories'),
                avg_protein=Avg('protein_g'),
                avg_steps=Avg('steps'),
                avg_sleep_min=Avg('sleep_duration_min'),
                avg_deep_sleep=Avg('deep_sleep_min'),
                avg_hrv=Avg('hrv_rmssd'),
                avg_resting_hr=Avg('resting_hr'),
                avg_sleep_score=Avg('sleep_score'),
                avg_readiness=Avg('readiness_score'),
            ),
            'totals': summaries.aggregate(
                total_steps=Sum('steps'),
                total_active_minutes=Sum('active_zone_minutes'),
            ),
            'bests': {
                'best_sleep': summaries.aggregate(Max('sleep_score'))['sleep_score__max'],
                'highest_hrv': summaries.aggregate(Max('hrv_rmssd'))['hrv_rmssd__max'],
                'most_steps': summaries.aggregate(Max('steps'))['steps__max'],
            }
        }
