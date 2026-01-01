"""
DRF Serializers for the ingestion API.
"""

from rest_framework import serializers
from core.models import (
    HealthRecord, 
    SleepLog, 
    NutritionLog, 
    BloodworkResult,
    DataImportLog,
    DataSource,
    MetricType
)


class HealthRecordSerializer(serializers.ModelSerializer):
    """Serializer for health metric records."""
    
    class Meta:
        model = HealthRecord
        fields = [
            'id',
            'source',
            'metric_type',
            'value',
            'unit',
            'timestamp',
            'date',
            'metadata',
            'created_at',
        ]
        read_only_fields = ['id', 'created_at']


class SleepLogSerializer(serializers.ModelSerializer):
    """Serializer for sleep session records."""
    
    class Meta:
        model = SleepLog
        fields = [
            'id',
            'source',
            'source_log_id',
            'date_of_sleep',
            'start_time',
            'end_time',
            'duration_minutes',
            'minutes_asleep',
            'minutes_awake',
            'deep_sleep_minutes',
            'light_sleep_minutes',
            'rem_sleep_minutes',
            'efficiency',
            'sleep_score',
            'created_at',
        ]
        read_only_fields = ['id', 'created_at']


class NutritionLogSerializer(serializers.ModelSerializer):
    """Serializer for nutrition records."""
    
    class Meta:
        model = NutritionLog
        fields = [
            'id',
            'source',
            'date',
            'calories',
            'protein_g',
            'carbs_g',
            'fat_g',
            'fiber_g',
            'sugar_g',
            'sodium_mg',
            'water_ml',
            'alcohol_g',
            'caffeine_mg',
            'micronutrients',
            'created_at',
        ]
        read_only_fields = ['id', 'created_at']


class BloodworkResultSerializer(serializers.ModelSerializer):
    """Serializer for bloodwork/lab results."""
    
    class Meta:
        model = BloodworkResult
        fields = [
            'id',
            'test_date',
            'biomarker',
            'value',
            'unit',
            'ref_range_low',
            'ref_range_high',
            'is_flagged',
            'flag_type',
            'lab_name',
            'notes',
            'created_at',
        ]
        read_only_fields = ['id', 'created_at']
    
    def validate(self, data):
        """Auto-flag if value is outside reference range."""
        value = data.get('value')
        ref_low = data.get('ref_range_low')
        ref_high = data.get('ref_range_high')
        
        if value is not None:
            if ref_low is not None and value < ref_low:
                data['is_flagged'] = True
                data['flag_type'] = 'low'
            elif ref_high is not None and value > ref_high:
                data['is_flagged'] = True
                data['flag_type'] = 'high'
        
        return data


class DataImportLogSerializer(serializers.ModelSerializer):
    """Serializer for import tracking."""
    
    class Meta:
        model = DataImportLog
        fields = [
            'id',
            'batch_id',
            'source',
            'status',
            'file_name',
            'file_type',
            'records_processed',
            'records_created',
            'records_skipped',
            'errors',
            'started_at',
            'completed_at',
        ]
        read_only_fields = fields


class FileUploadSerializer(serializers.Serializer):
    """Serializer for file upload requests."""
    
    file = serializers.FileField()
    source = serializers.ChoiceField(
        choices=DataSource.choices,
        required=False,
        help_text="Data source type. If not provided, will be auto-detected."
    )


class DataSourceInfoSerializer(serializers.Serializer):
    """Information about a supported data source."""
    
    name = serializers.CharField()
    label = serializers.CharField()
    supported_formats = serializers.ListField(child=serializers.CharField())
    description = serializers.CharField()


class MetricSummarySerializer(serializers.Serializer):
    """Summary statistics for a metric type."""
    
    metric_type = serializers.CharField()
    count = serializers.IntegerField()
    min_value = serializers.FloatField()
    max_value = serializers.FloatField()
    avg_value = serializers.FloatField()
    first_date = serializers.DateField()
    last_date = serializers.DateField()
