"""
URL routes for the ingestion API.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter

from .views import (
    HealthRecordViewSet,
    SleepLogViewSet,
    NutritionLogViewSet,
    BloodworkResultViewSet,
    DataImportLogViewSet,
    FileUploadView,
    DataSourcesView,
)

router = DefaultRouter()
router.register(r'health-records', HealthRecordViewSet, basename='health-record')
router.register(r'sleep-logs', SleepLogViewSet, basename='sleep-log')
router.register(r'nutrition-logs', NutritionLogViewSet, basename='nutrition-log')
router.register(r'bloodwork', BloodworkResultViewSet, basename='bloodwork')
router.register(r'imports', DataImportLogViewSet, basename='import-log')

urlpatterns = [
    # ViewSet routes
    path('', include(router.urls)),
    
    # File upload endpoint
    path('ingest/upload/', FileUploadView.as_view(), name='file-upload'),
    
    # Data sources info
    path('ingest/sources/', DataSourcesView.as_view(), name='data-sources'),
]
