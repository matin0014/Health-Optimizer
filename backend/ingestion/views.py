"""
REST API views for the ingestion module.
"""

import tempfile
import zipfile
from pathlib import Path
from uuid import uuid4

from django.db.models import Count, Min, Max, Avg
from django.db.models.functions import TruncDate
from rest_framework import viewsets, status, views
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.permissions import AllowAny  # Change to IsAuthenticated in production

from core.models import (
    HealthRecord, 
    SleepLog, 
    NutritionLog, 
    BloodworkResult,
    DataImportLog,
    DataSource,
)
from .serializers import (
    HealthRecordSerializer,
    SleepLogSerializer,
    NutritionLogSerializer,
    BloodworkResultSerializer,
    DataImportLogSerializer,
    FileUploadSerializer,
    DataSourceInfoSerializer,
    MetricSummarySerializer,
)
from .services import IngestionService
from .adapters import FitbitAdapter
from .adapters.base import AdapterRegistry


class HealthRecordViewSet(viewsets.ModelViewSet):
    """
    API endpoint for health records.
    
    GET /api/v1/health-records/ - List all records
    GET /api/v1/health-records/?metric_type=heart_rate - Filter by type
    GET /api/v1/health-records/?source=fitbit - Filter by source
    GET /api/v1/health-records/?date=2024-12-01 - Filter by date
    GET /api/v1/health-records/{id}/ - Get single record
    """
    
    queryset = HealthRecord.objects.all()
    serializer_class = HealthRecordSerializer
    permission_classes = [AllowAny]  # TODO: Change to IsAuthenticated
    
    def get_queryset(self):
        queryset = super().get_queryset()
        
        # Filter by user when authentication is enabled
        # if self.request.user.is_authenticated:
        #     queryset = queryset.filter(user=self.request.user)
        
        # Apply filters from query params
        metric_type = self.request.query_params.get('metric_type')
        source = self.request.query_params.get('source')
        date = self.request.query_params.get('date')
        start_date = self.request.query_params.get('start_date')
        end_date = self.request.query_params.get('end_date')
        
        if metric_type:
            queryset = queryset.filter(metric_type=metric_type)
        if source:
            queryset = queryset.filter(source=source)
        if date:
            queryset = queryset.filter(date=date)
        if start_date:
            queryset = queryset.filter(date__gte=start_date)
        if end_date:
            queryset = queryset.filter(date__lte=end_date)
        
        return queryset
    
    @action(detail=False, methods=['get'])
    def summary(self, request):
        """
        GET /api/v1/health-records/summary/
        Get summary statistics grouped by metric type.
        """
        queryset = self.get_queryset()
        
        summaries = queryset.values('metric_type').annotate(
            count=Count('id'),
            min_value=Min('value'),
            max_value=Max('value'),
            avg_value=Avg('value'),
            first_date=Min('date'),
            last_date=Max('date'),
        ).order_by('metric_type')
        
        serializer = MetricSummarySerializer(summaries, many=True)
        return Response(serializer.data)


class SleepLogViewSet(viewsets.ModelViewSet):
    """
    API endpoint for sleep logs.
    
    GET /api/v1/sleep-logs/ - List all sleep sessions
    GET /api/v1/sleep-logs/?start_date=2024-12-01&end_date=2024-12-31
    """
    
    queryset = SleepLog.objects.all()
    serializer_class = SleepLogSerializer
    permission_classes = [AllowAny]
    
    def get_queryset(self):
        queryset = super().get_queryset()
        
        start_date = self.request.query_params.get('start_date')
        end_date = self.request.query_params.get('end_date')
        
        if start_date:
            queryset = queryset.filter(date_of_sleep__gte=start_date)
        if end_date:
            queryset = queryset.filter(date_of_sleep__lte=end_date)
        
        return queryset


class NutritionLogViewSet(viewsets.ModelViewSet):
    """
    API endpoint for nutrition logs.
    """
    
    queryset = NutritionLog.objects.all()
    serializer_class = NutritionLogSerializer
    permission_classes = [AllowAny]
    
    def get_queryset(self):
        queryset = super().get_queryset()
        
        start_date = self.request.query_params.get('start_date')
        end_date = self.request.query_params.get('end_date')
        
        if start_date:
            queryset = queryset.filter(date__gte=start_date)
        if end_date:
            queryset = queryset.filter(date__lte=end_date)
        
        return queryset


class BloodworkResultViewSet(viewsets.ModelViewSet):
    """
    API endpoint for bloodwork/lab results.
    
    POST /api/v1/bloodwork/ - Add new lab result
    GET /api/v1/bloodwork/?biomarker=vitamin_d - Filter by biomarker
    """
    
    queryset = BloodworkResult.objects.all()
    serializer_class = BloodworkResultSerializer
    permission_classes = [AllowAny]
    
    def get_queryset(self):
        queryset = super().get_queryset()
        
        biomarker = self.request.query_params.get('biomarker')
        start_date = self.request.query_params.get('start_date')
        end_date = self.request.query_params.get('end_date')
        flagged_only = self.request.query_params.get('flagged_only')
        
        if biomarker:
            queryset = queryset.filter(biomarker=biomarker)
        if start_date:
            queryset = queryset.filter(test_date__gte=start_date)
        if end_date:
            queryset = queryset.filter(test_date__lte=end_date)
        if flagged_only and flagged_only.lower() == 'true':
            queryset = queryset.filter(is_flagged=True)
        
        return queryset
    
    @action(detail=False, methods=['get'])
    def biomarkers(self, request):
        """
        GET /api/v1/bloodwork/biomarkers/
        List all unique biomarkers with their latest values.
        """
        # Get distinct biomarkers with counts
        biomarkers = self.get_queryset().values('biomarker').annotate(
            count=Count('id'),
            first_test=Min('test_date'),
            last_test=Max('test_date'),
        ).order_by('biomarker')
        
        return Response(list(biomarkers))


class DataImportLogViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint for import history (read-only).
    
    GET /api/v1/imports/ - List all imports
    GET /api/v1/imports/{batch_id}/ - Get import details
    """
    
    queryset = DataImportLog.objects.all()
    serializer_class = DataImportLogSerializer
    permission_classes = [AllowAny]
    lookup_field = 'batch_id'


class FileUploadView(views.APIView):
    """
    Upload data files for ingestion.
    
    POST /api/v1/ingest/upload/
    
    Accepts:
    - Single JSON/CSV files
    - ZIP archives containing export folders
    
    Body (multipart/form-data):
    - file: The file to upload
    - source: (optional) Data source type (fitbit, garmin, etc.)
    """
    
    parser_classes = [MultiPartParser, FormParser]
    permission_classes = [AllowAny]
    
    def post(self, request):
        serializer = FileUploadSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        uploaded_file = serializer.validated_data['file']
        source = serializer.validated_data.get('source')
        
        # Save uploaded file to temp location
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            file_path = tmpdir / uploaded_file.name
            
            with open(file_path, 'wb') as f:
                for chunk in uploaded_file.chunks():
                    f.write(chunk)
            
            # If it's a ZIP, extract it
            if file_path.suffix.lower() == '.zip':
                extract_dir = tmpdir / 'extracted'
                extract_dir.mkdir()
                
                with zipfile.ZipFile(file_path, 'r') as zf:
                    zf.extractall(extract_dir)
                
                # Find the actual data folder (might be nested)
                file_path = self._find_data_root(extract_dir)
            
            # Process the file
            user = request.user if request.user.is_authenticated else None
            service = IngestionService(user=user)
            
            import_log = service.ingest_file(file_path, source=source)
            
            return Response(
                DataImportLogSerializer(import_log).data,
                status=status.HTTP_201_CREATED if import_log.status == 'completed' else status.HTTP_400_BAD_REQUEST
            )
    
    def _find_data_root(self, extract_dir: Path) -> Path:
        """
        Find the actual data folder in an extracted ZIP.
        Handles cases where the ZIP has a single root folder.
        """
        children = list(extract_dir.iterdir())
        
        # If there's only one child and it's a directory, go into it
        if len(children) == 1 and children[0].is_dir():
            return self._find_data_root(children[0])
        
        return extract_dir


class DataSourcesView(views.APIView):
    """
    List supported data sources and their capabilities.
    
    GET /api/v1/ingest/sources/
    """
    
    permission_classes = [AllowAny]
    
    def get(self, request):
        sources = [
            {
                'name': 'fitbit',
                'label': 'Fitbit (Google Takeout)',
                'supported_formats': ['zip', 'json', 'csv'],
                'description': 'Upload your Google Takeout export containing Fitbit data.',
            },
            {
                'name': 'garmin',
                'label': 'Garmin Connect',
                'supported_formats': ['zip', 'fit', 'csv'],
                'description': 'Coming soon: Garmin Connect data export.',
            },
            {
                'name': 'oura',
                'label': 'Oura Ring',
                'supported_formats': ['json', 'csv'],
                'description': 'Coming soon: Oura Ring data export.',
            },
            {
                'name': 'cronometer',
                'label': 'Cronometer',
                'supported_formats': ['csv'],
                'description': 'Coming soon: Cronometer nutrition export.',
            },
            {
                'name': 'apple_health',
                'label': 'Apple Health',
                'supported_formats': ['zip', 'xml'],
                'description': 'Coming soon: Apple Health export.',
            },
        ]
        
        serializer = DataSourceInfoSerializer(sources, many=True)
        return Response(serializer.data)
