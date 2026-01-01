"""
Management command to ingest data from a local directory.

Usage:
    python manage.py ingest_data /path/to/fitbit/export --source fitbit
    python manage.py ingest_data /path/to/data --source fitbit --dry-run
"""

from pathlib import Path
from django.core.management.base import BaseCommand, CommandError

from ingestion.services import IngestionService
from ingestion.adapters.base import AdapterRegistry


class Command(BaseCommand):
    help = 'Ingest health data from a local file or directory'

    def add_arguments(self, parser):
        parser.add_argument(
            'path',
            type=str,
            help='Path to file or directory to ingest'
        )
        parser.add_argument(
            '--source',
            type=str,
            choices=['fitbit', 'garmin', 'oura', 'cronometer', 'apple_health'],
            help='Data source type (auto-detected if not provided)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Parse and show stats without saving to database'
        )

    def handle(self, *args, **options):
        path = Path(options['path'])
        source = options.get('source')
        dry_run = options.get('dry_run', False)
        
        if not path.exists():
            raise CommandError(f"Path does not exist: {path}")
        
        self.stdout.write(f"Processing: {path}")
        self.stdout.write(f"Source: {source or 'auto-detect'}")
        self.stdout.write(f"Dry run: {dry_run}")
        self.stdout.write("-" * 50)
        
        if dry_run:
            self._dry_run(path, source)
        else:
            self._ingest(path, source)
    
    def _dry_run(self, path: Path, source: str | None):
        """Parse files without saving, just show stats."""
        from ingestion.adapters.base import AdapterRegistry
        
        if source:
            adapter = AdapterRegistry.get_adapter_by_name(source)
        else:
            adapter = AdapterRegistry.get_adapter_for(path)
        
        if adapter is None:
            raise CommandError(f"No adapter found for path: {path}")
        
        self.stdout.write(f"Using adapter: {adapter.SOURCE_NAME}")
        
        result = adapter.parse(path)
        
        # Count by record type
        type_counts = {}
        metric_counts = {}
        
        for record in result.records:
            type_counts[record.record_type] = type_counts.get(record.record_type, 0) + 1
            if record.metric_type:
                metric_counts[record.metric_type] = metric_counts.get(record.metric_type, 0) + 1
        
        self.stdout.write(self.style.SUCCESS(f"\nParsed {result.records_parsed} records"))
        
        self.stdout.write("\nBy record type:")
        for rtype, count in sorted(type_counts.items()):
            self.stdout.write(f"  {rtype}: {count}")
        
        self.stdout.write("\nBy metric type:")
        for mtype, count in sorted(metric_counts.items()):
            self.stdout.write(f"  {mtype}: {count}")
        
        if result.errors:
            self.stdout.write(self.style.WARNING(f"\nErrors ({len(result.errors)}):"))
            for error in result.errors[:10]:
                self.stdout.write(f"  - {error}")
            if len(result.errors) > 10:
                self.stdout.write(f"  ... and {len(result.errors) - 10} more")
    
    def _ingest(self, path: Path, source: str | None):
        """Actually ingest data to the database."""
        service = IngestionService(user=None)  # No user for CLI
        
        import_log = service.ingest_file(path, source=source)
        
        if import_log.status == 'completed':
            self.stdout.write(self.style.SUCCESS(
                f"\nImport completed successfully!"
            ))
        else:
            self.stdout.write(self.style.ERROR(
                f"\nImport failed: {import_log.status}"
            ))
        
        self.stdout.write(f"Batch ID: {import_log.batch_id}")
        self.stdout.write(f"Records processed: {import_log.records_processed}")
        self.stdout.write(f"Records created: {import_log.records_created}")
        self.stdout.write(f"Records skipped: {import_log.records_skipped}")
        
        if import_log.errors:
            self.stdout.write(self.style.WARNING(f"\nErrors ({len(import_log.errors)}):"))
            for error in import_log.errors[:10]:
                self.stdout.write(f"  - {error}")
