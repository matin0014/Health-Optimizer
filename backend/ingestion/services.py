"""
Ingestion service - orchestrates parsing and saving data to the database.
"""

from datetime import datetime
from pathlib import Path
from uuid import uuid4, UUID
import logging

from django.db import transaction
from django.contrib.auth.models import User

from core.models import (
    HealthRecord, 
    SleepLog, 
    NutritionLog, 
    DataImportLog,
    DataSource
)
from core.services import DailySummaryService
from .adapters.base import BaseAdapter, ParsedRecord, AdapterRegistry

logger = logging.getLogger(__name__)


class IngestionService:
    """
    Service for ingesting data from various sources into the database.
    
    Usage:
        service = IngestionService(user=request.user)
        result = service.ingest_file(path, source='fitbit')
        # or
        result = service.ingest_from_adapter(adapter, path)
    """
    
    def __init__(self, user: User | None = None):
        self.user = user
        self.batch_id: UUID | None = None
        self.import_log: DataImportLog | None = None
    
    def ingest_file(
        self, 
        path: Path | str, 
        source: str | None = None
    ) -> DataImportLog:
        """
        Ingest a file or directory, auto-detecting the source if not provided.
        
        Args:
            path: Path to file or directory
            source: Optional source name (e.g., 'fitbit'). Auto-detected if None.
            
        Returns:
            DataImportLog with results
        """
        path = Path(path)
        self.batch_id = uuid4()
        
        # Create import log
        self.import_log = DataImportLog.objects.create(
            user=self.user,
            batch_id=self.batch_id,
            source=source or 'unknown',
            status='processing',
            file_name=path.name,
            file_type=path.suffix or 'directory'
        )
        
        try:
            # Get adapter
            if source:
                adapter = AdapterRegistry.get_adapter_by_name(source, self.batch_id)
            else:
                adapter = AdapterRegistry.get_adapter_for(path, self.batch_id)
            
            if adapter is None:
                raise ValueError(f"No adapter found for path: {path}")
            
            # Update source if it was auto-detected
            if not source:
                self.import_log.source = adapter.SOURCE_NAME
                self.import_log.save()
            
            return self._process_with_adapter(adapter, path)
            
        except Exception as e:
            logger.exception(f"Import failed for {path}")
            self.import_log.status = 'failed'
            self.import_log.errors = [str(e)]
            self.import_log.completed_at = datetime.utcnow()
            self.import_log.save()
            return self.import_log
    
    def ingest_from_adapter(
        self, 
        adapter: BaseAdapter, 
        path: Path | str
    ) -> DataImportLog:
        """
        Ingest using a specific adapter instance.
        """
        path = Path(path)
        self.batch_id = adapter.batch_id or uuid4()
        
        self.import_log = DataImportLog.objects.create(
            user=self.user,
            batch_id=self.batch_id,
            source=adapter.SOURCE_NAME,
            status='processing',
            file_name=path.name,
            file_type=path.suffix or 'directory'
        )
        
        try:
            return self._process_with_adapter(adapter, path)
        except Exception as e:
            logger.exception(f"Import failed for {path}")
            self.import_log.status = 'failed'
            self.import_log.errors = [str(e)]
            self.import_log.completed_at = datetime.utcnow()
            self.import_log.save()
            return self.import_log
    
    def _process_with_adapter(
        self, 
        adapter: BaseAdapter, 
        path: Path
    ) -> DataImportLog:
        """Process a path with the given adapter."""
        result = adapter.parse(path)
        
        records_created = 0
        records_skipped = 0
        dates_affected = set()  # Track dates for summary building
        
        # Process records in batches
        with transaction.atomic():
            for record in result.records:
                try:
                    created = self._save_record(record)
                    if created:
                        records_created += 1
                        # Track the date for summary building
                        if record.date:
                            dates_affected.add(record.date)
                    else:
                        records_skipped += 1
                except Exception as e:
                    logger.warning(f"Failed to save record: {e}")
                    records_skipped += 1
                    result.errors.append(str(e))
        
        # Build daily summaries for affected dates
        if dates_affected:
            logger.info(f"Building daily summaries for {len(dates_affected)} dates...")
            for d in sorted(dates_affected):
                try:
                    DailySummaryService.build_summary(d, self.user)
                except Exception as e:
                    logger.warning(f"Failed to build summary for {d}: {e}")
        
        # Update import log
        self.import_log.status = 'completed' if result.success else 'completed'
        self.import_log.records_processed = result.records_parsed
        self.import_log.records_created = records_created
        self.import_log.records_skipped = records_skipped
        self.import_log.errors = result.errors[:100]  # Limit stored errors
        self.import_log.completed_at = datetime.utcnow()
        self.import_log.save()
        
        logger.info(
            f"Import complete: {records_created} created, "
            f"{records_skipped} skipped, {len(result.errors)} errors, "
            f"{len(dates_affected)} daily summaries updated"
        )
        
        return self.import_log
    
    def _save_record(self, record: ParsedRecord) -> bool:
        """
        Save a ParsedRecord to the appropriate model.
        
        Returns:
            True if record was created, False if skipped (duplicate)
        """
        if record.record_type == 'health_record':
            return self._save_health_record(record)
        elif record.record_type == 'sleep_log':
            return self._save_sleep_log(record)
        elif record.record_type in ('nutrition_log', 'nutrition'):
            return self._save_nutrition_log(record)
        else:
            logger.warning(f"Unknown record type: {record.record_type}")
            return False
    
    def _save_health_record(self, record: ParsedRecord) -> bool:
        """Save a health metric record."""
        _, created = HealthRecord.objects.get_or_create(
            user=self.user,
            source=record.source,
            metric_type=record.metric_type,
            timestamp=record.timestamp,
            defaults={
                'value': record.value,
                'unit': record.unit,
                'date': record.date,
                'metadata': record.metadata,
                'raw_data': record.raw_data,
                'import_batch_id': self.batch_id,
            }
        )
        return created
    
    def _save_sleep_log(self, record: ParsedRecord) -> bool:
        """Save a sleep session record."""
        sleep_data = record.sleep_data
        
        _, created = SleepLog.objects.get_or_create(
            user=self.user,
            source=record.source,
            source_log_id=sleep_data.get('source_log_id', ''),
            defaults={
                'date_of_sleep': record.date,
                'start_time': record.start_time,
                'end_time': record.end_time,
                'duration_minutes': record.duration_minutes or 0,
                'minutes_asleep': sleep_data.get('minutes_asleep'),
                'minutes_awake': sleep_data.get('minutes_awake'),
                'efficiency': sleep_data.get('efficiency'),
                'deep_sleep_minutes': sleep_data.get('deep_sleep_minutes'),
                'light_sleep_minutes': sleep_data.get('light_sleep_minutes'),
                'rem_sleep_minutes': sleep_data.get('rem_sleep_minutes'),
                'stages_data': sleep_data.get('stages_data'),
                'raw_data': record.raw_data,
                'import_batch_id': self.batch_id,
            }
        )
        return created
    
    def _save_nutrition_log(self, record: ParsedRecord) -> bool:
        """Save a nutrition record."""
        # Handle both nutrition_data dict and metadata dict formats
        nutrition_data = record.nutrition_data or record.metadata or {}
        
        _, created = NutritionLog.objects.get_or_create(
            user=self.user,
            source=record.source,
            date=record.date,
            defaults={
                'calories': nutrition_data.get('calories') or record.value,
                'protein_g': nutrition_data.get('protein_g'),
                'carbs_g': nutrition_data.get('carbs_g'),
                'fat_g': nutrition_data.get('fat_g'),
                'fiber_g': nutrition_data.get('fiber_g'),
                'sugar_g': nutrition_data.get('sugar_g'),
                'sodium_mg': nutrition_data.get('sodium_mg'),
                'water_ml': nutrition_data.get('water_ml'),
                'micronutrients': nutrition_data.get('micronutrients', {}),
                'raw_data': record.raw_data,
                'import_batch_id': self.batch_id,
            }
        )
        return created
