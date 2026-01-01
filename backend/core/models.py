from django.db import models
from django.contrib.auth.models import User


class DataSource(models.TextChoices):
    """Supported data source providers."""
    FITBIT = 'fitbit', 'Fitbit'
    GARMIN = 'garmin', 'Garmin'
    OURA = 'oura', 'Oura'
    APPLE_HEALTH = 'apple_health', 'Apple Health'
    CRONOMETER = 'cronometer', 'Cronometer'
    MYFITNESSPAL = 'myfitnesspal', 'MyFitnessPal'
    MANUAL = 'manual', 'Manual Entry'


class MetricType(models.TextChoices):
    """Standardized metric types across all sources."""
    # Activity
    STEPS = 'steps', 'Steps'
    DISTANCE = 'distance', 'Distance'
    CALORIES_BURNED = 'calories_burned', 'Calories Burned'
    ACTIVE_MINUTES = 'active_minutes', 'Active Minutes'
    FLOORS_CLIMBED = 'floors_climbed', 'Floors Climbed'
    
    # Heart
    HEART_RATE = 'heart_rate', 'Heart Rate'
    RESTING_HEART_RATE = 'resting_heart_rate', 'Resting Heart Rate'
    HRV = 'hrv', 'Heart Rate Variability'
    
    # Sleep
    SLEEP_DURATION = 'sleep_duration', 'Sleep Duration'
    SLEEP_DEEP = 'sleep_deep', 'Deep Sleep'
    SLEEP_LIGHT = 'sleep_light', 'Light Sleep'
    SLEEP_REM = 'sleep_rem', 'REM Sleep'
    SLEEP_AWAKE = 'sleep_awake', 'Time Awake'
    SLEEP_SCORE = 'sleep_score', 'Sleep Score'
    
    # Body
    WEIGHT = 'weight', 'Weight'
    BODY_FAT = 'body_fat', 'Body Fat Percentage'
    SPO2 = 'spo2', 'Blood Oxygen (SpO2)'
    SKIN_TEMP = 'skin_temp', 'Skin Temperature'
    
    # Stress/Recovery
    STRESS_SCORE = 'stress_score', 'Stress Score'
    READINESS_SCORE = 'readiness_score', 'Readiness Score'


class HealthRecord(models.Model):
    """
    Universal model for normalized health metrics from any wearable.
    Each record represents a single data point at a specific timestamp.
    """
    user = models.ForeignKey(
        User, 
        on_delete=models.CASCADE, 
        related_name='health_records',
        null=True,  # Allow null for initial development
        blank=True
    )
    
    # Source tracking
    source = models.CharField(
        max_length=50, 
        choices=DataSource.choices,
        db_index=True
    )
    
    # The standardized metric type
    metric_type = models.CharField(
        max_length=50, 
        choices=MetricType.choices,
        db_index=True
    )
    
    # The value and unit
    value = models.FloatField()
    unit = models.CharField(max_length=20)
    
    # When this reading was taken (UTC)
    timestamp = models.DateTimeField(db_index=True)
    
    # For daily aggregates (steps, sleep totals, etc.)
    date = models.DateField(null=True, blank=True, db_index=True)
    
    # Store raw original data for debugging/audit
    raw_data = models.JSONField(null=True, blank=True)
    
    # Extra context (e.g., heart_zone, sleep_stage, activity_type)
    metadata = models.JSONField(default=dict, blank=True)
    
    # Import tracking
    import_batch_id = models.UUIDField(null=True, blank=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=['source', 'metric_type', 'timestamp']),
            models.Index(fields=['metric_type', 'date']),
            models.Index(fields=['user', 'metric_type', 'timestamp']),
        ]
        ordering = ['-timestamp']

    def __str__(self):
        return f"{self.metric_type}: {self.value} {self.unit} ({self.timestamp})"


class SleepLog(models.Model):
    """
    Detailed sleep session data. One record per sleep session.
    More structured than individual HealthRecords for sleep analysis.
    """
    user = models.ForeignKey(
        User, 
        on_delete=models.CASCADE, 
        related_name='sleep_logs',
        null=True,
        blank=True
    )
    
    source = models.CharField(max_length=50, choices=DataSource.choices)
    source_log_id = models.CharField(max_length=100, blank=True)  # Original ID from source
    
    date_of_sleep = models.DateField(db_index=True)
    start_time = models.DateTimeField()
    end_time = models.DateTimeField()
    
    # Duration in minutes
    duration_minutes = models.IntegerField()
    minutes_asleep = models.IntegerField(null=True, blank=True)
    minutes_awake = models.IntegerField(null=True, blank=True)
    
    # Sleep stages in minutes
    deep_sleep_minutes = models.IntegerField(null=True, blank=True)
    light_sleep_minutes = models.IntegerField(null=True, blank=True)
    rem_sleep_minutes = models.IntegerField(null=True, blank=True)
    
    # Scores
    efficiency = models.IntegerField(null=True, blank=True)  # Percentage
    sleep_score = models.IntegerField(null=True, blank=True)
    
    # Raw stage-by-stage data
    stages_data = models.JSONField(null=True, blank=True)
    
    raw_data = models.JSONField(null=True, blank=True)
    import_batch_id = models.UUIDField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=['user', 'date_of_sleep']),
            models.Index(fields=['source', 'source_log_id']),
        ]
        ordering = ['-date_of_sleep']

    def __str__(self):
        return f"Sleep on {self.date_of_sleep}: {self.duration_minutes}min"


class NutritionLog(models.Model):
    """
    Daily nutrition data from food tracking apps.
    """
    user = models.ForeignKey(
        User, 
        on_delete=models.CASCADE, 
        related_name='nutrition_logs',
        null=True,
        blank=True
    )
    
    source = models.CharField(max_length=50, choices=DataSource.choices)
    date = models.DateField(db_index=True)
    
    # Macronutrients (grams)
    calories = models.FloatField(null=True, blank=True)
    protein_g = models.FloatField(null=True, blank=True)
    carbs_g = models.FloatField(null=True, blank=True)
    fat_g = models.FloatField(null=True, blank=True)
    fiber_g = models.FloatField(null=True, blank=True)
    sugar_g = models.FloatField(null=True, blank=True)
    
    # Key micronutrients (store what's available)
    sodium_mg = models.FloatField(null=True, blank=True)
    potassium_mg = models.FloatField(null=True, blank=True)
    vitamin_d_iu = models.FloatField(null=True, blank=True)
    iron_mg = models.FloatField(null=True, blank=True)
    magnesium_mg = models.FloatField(null=True, blank=True)
    
    # Full micronutrient data (varies by source)
    micronutrients = models.JSONField(default=dict, blank=True)
    
    # Water intake (ml)
    water_ml = models.FloatField(null=True, blank=True)
    
    # Alcohol (grams)
    alcohol_g = models.FloatField(null=True, blank=True)
    
    # Caffeine (mg)
    caffeine_mg = models.FloatField(null=True, blank=True)
    
    raw_data = models.JSONField(null=True, blank=True)
    import_batch_id = models.UUIDField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=['user', 'date']),
        ]
        ordering = ['-date']

    def __str__(self):
        return f"Nutrition {self.date}: {self.calories} kcal"


class BloodworkResult(models.Model):
    """
    Lab test results. Each row is one biomarker from one test date.
    """
    user = models.ForeignKey(
        User, 
        on_delete=models.CASCADE, 
        related_name='bloodwork_results',
        null=True,
        blank=True
    )
    
    test_date = models.DateField(db_index=True)
    
    # The biomarker name (standardized)
    biomarker = models.CharField(max_length=100, db_index=True)
    
    value = models.FloatField()
    unit = models.CharField(max_length=30)
    
    # Reference ranges (from lab report)
    ref_range_low = models.FloatField(null=True, blank=True)
    ref_range_high = models.FloatField(null=True, blank=True)
    
    # Is this value flagged as out of range?
    is_flagged = models.BooleanField(default=False)
    flag_type = models.CharField(
        max_length=10, 
        choices=[('high', 'High'), ('low', 'Low')],
        blank=True
    )
    
    # Lab/source info
    lab_name = models.CharField(max_length=100, blank=True)
    notes = models.TextField(blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=['user', 'biomarker', 'test_date']),
            models.Index(fields=['biomarker', 'test_date']),
        ]
        ordering = ['-test_date', 'biomarker']

    def __str__(self):
        return f"{self.biomarker}: {self.value} {self.unit} ({self.test_date})"


class DailySummary(models.Model):
    """
    Denormalized daily summary combining all health metrics for fast querying.
    One row per day - perfect for trend analysis and insights.
    """
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='daily_summaries',
        null=True,
        blank=True
    )
    
    date = models.DateField(db_index=True)
    
    # -------------------------------------------------------------------------
    # Nutrition (from NutritionLog / Cronometer)
    # -------------------------------------------------------------------------
    calories = models.FloatField(null=True, blank=True)
    protein_g = models.FloatField(null=True, blank=True)
    carbs_g = models.FloatField(null=True, blank=True)
    fat_g = models.FloatField(null=True, blank=True)
    fiber_g = models.FloatField(null=True, blank=True)
    sodium_mg = models.FloatField(null=True, blank=True)
    
    # -------------------------------------------------------------------------
    # Sleep (from SleepLog - previous night's sleep)
    # -------------------------------------------------------------------------
    sleep_duration_min = models.IntegerField(null=True, blank=True)
    sleep_minutes_asleep = models.IntegerField(null=True, blank=True)
    sleep_minutes_awake = models.IntegerField(null=True, blank=True)
    deep_sleep_min = models.IntegerField(null=True, blank=True)
    light_sleep_min = models.IntegerField(null=True, blank=True)
    rem_sleep_min = models.IntegerField(null=True, blank=True)
    sleep_efficiency = models.IntegerField(null=True, blank=True)  # percentage
    sleep_score = models.IntegerField(null=True, blank=True)
    sleep_start_time = models.TimeField(null=True, blank=True)
    sleep_end_time = models.TimeField(null=True, blank=True)
    
    # -------------------------------------------------------------------------
    # Activity (from HealthRecord)
    # -------------------------------------------------------------------------
    steps = models.IntegerField(null=True, blank=True)
    distance_km = models.FloatField(null=True, blank=True)
    active_zone_minutes = models.IntegerField(null=True, blank=True)
    very_active_minutes = models.IntegerField(null=True, blank=True)
    moderately_active_minutes = models.IntegerField(null=True, blank=True)
    lightly_active_minutes = models.IntegerField(null=True, blank=True)
    sedentary_minutes = models.IntegerField(null=True, blank=True)
    
    # -------------------------------------------------------------------------
    # Vitals (from HealthRecord)
    # -------------------------------------------------------------------------
    resting_hr = models.IntegerField(null=True, blank=True)  # bpm
    hrv_rmssd = models.FloatField(null=True, blank=True)  # ms
    hrv_deep_rmssd = models.FloatField(null=True, blank=True)  # during deep sleep
    spo2_avg = models.FloatField(null=True, blank=True)  # percentage
    spo2_min = models.FloatField(null=True, blank=True)
    skin_temp_deviation = models.FloatField(null=True, blank=True)  # °C from baseline
    
    # -------------------------------------------------------------------------
    # Scores (from HealthRecord)
    # -------------------------------------------------------------------------
    readiness_score = models.IntegerField(null=True, blank=True)
    stress_score = models.IntegerField(null=True, blank=True)
    
    # -------------------------------------------------------------------------
    # Computed / Derived Fields
    # -------------------------------------------------------------------------
    # Macro percentages (computed on save)
    protein_pct = models.FloatField(null=True, blank=True)
    carbs_pct = models.FloatField(null=True, blank=True)
    fat_pct = models.FloatField(null=True, blank=True)
    
    # Data completeness score (0-100)
    data_completeness = models.IntegerField(default=0)
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ['user', 'date']
        indexes = [
            models.Index(fields=['user', 'date']),
            models.Index(fields=['date']),
        ]
        ordering = ['-date']

    def __str__(self):
        return f"Summary {self.date}: {self.steps or 0} steps, {self.calories or 0} kcal"

    def save(self, *args, **kwargs):
        # Compute macro percentages
        if self.calories and self.calories > 0:
            if self.protein_g:
                self.protein_pct = round((self.protein_g * 4 / self.calories) * 100, 1)
            if self.carbs_g:
                self.carbs_pct = round((self.carbs_g * 4 / self.calories) * 100, 1)
            if self.fat_g:
                self.fat_pct = round((self.fat_g * 9 / self.calories) * 100, 1)
        
        # Compute data completeness
        fields_to_check = [
            self.calories, self.steps, self.sleep_duration_min,
            self.resting_hr, self.hrv_rmssd, self.sleep_score
        ]
        filled = sum(1 for f in fields_to_check if f is not None)
        self.data_completeness = int((filled / len(fields_to_check)) * 100)
        
        super().save(*args, **kwargs)


class DataImportLog(models.Model):
    """
    Tracks each import operation for debugging and preventing duplicates.
    """
    user = models.ForeignKey(
        User, 
        on_delete=models.CASCADE, 
        related_name='import_logs',
        null=True,
        blank=True
    )
    
    batch_id = models.UUIDField(unique=True, db_index=True)
    source = models.CharField(max_length=50, choices=DataSource.choices)
    
    status = models.CharField(
        max_length=20,
        choices=[
            ('pending', 'Pending'),
            ('processing', 'Processing'),
            ('completed', 'Completed'),
            ('failed', 'Failed'),
        ],
        default='pending'
    )
    
    # What was imported
    file_name = models.CharField(max_length=255, blank=True)
    file_type = models.CharField(max_length=50, blank=True)  # csv, json, zip
    
    # Results
    records_processed = models.IntegerField(default=0)
    records_created = models.IntegerField(default=0)
    records_skipped = models.IntegerField(default=0)
    errors = models.JSONField(default=list, blank=True)
    
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-started_at']

    def __str__(self):
        return f"{self.source} import {self.batch_id} - {self.status}"
