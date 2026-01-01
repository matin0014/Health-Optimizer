from django.contrib import admin
from .models import HealthRecord, SleepLog, NutritionLog, BloodworkResult, DataImportLog


@admin.register(HealthRecord)
class HealthRecordAdmin(admin.ModelAdmin):
    list_display = ('metric_type', 'value', 'unit', 'timestamp', 'source', 'date')
    list_filter = ('metric_type', 'source', 'date')
    search_fields = ('metric_type', 'source')
    date_hierarchy = 'timestamp'
    readonly_fields = ('created_at', 'import_batch_id')


@admin.register(SleepLog)
class SleepLogAdmin(admin.ModelAdmin):
    list_display = ('date_of_sleep', 'duration_minutes', 'deep_sleep_minutes', 'efficiency', 'source')
    list_filter = ('source', 'date_of_sleep')
    date_hierarchy = 'date_of_sleep'
    readonly_fields = ('created_at', 'import_batch_id')


@admin.register(NutritionLog)
class NutritionLogAdmin(admin.ModelAdmin):
    list_display = ('date', 'calories', 'protein_g', 'carbs_g', 'fat_g', 'source')
    list_filter = ('source', 'date')
    date_hierarchy = 'date'
    readonly_fields = ('created_at', 'import_batch_id')


@admin.register(BloodworkResult)
class BloodworkResultAdmin(admin.ModelAdmin):
    list_display = ('biomarker', 'value', 'unit', 'test_date', 'is_flagged', 'flag_type')
    list_filter = ('biomarker', 'is_flagged', 'test_date')
    search_fields = ('biomarker', 'lab_name')
    date_hierarchy = 'test_date'
    readonly_fields = ('created_at',)


@admin.register(DataImportLog)
class DataImportLogAdmin(admin.ModelAdmin):
    list_display = ('batch_id', 'source', 'status', 'records_created', 'records_skipped', 'started_at')
    list_filter = ('source', 'status', 'started_at')
    readonly_fields = ('batch_id', 'started_at', 'completed_at')

