"""
Base adapter interface for all data source parsers.

Each data source (Fitbit, Garmin, Oura, etc.) implements this interface
to normalize their proprietary export formats into our unified schema.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Generator, Any
from uuid import UUID
import logging

logger = logging.getLogger(__name__)


@dataclass
class ParsedRecord:
    """
    A normalized record ready to be saved to the database.
    This is the common format that all adapters output.
    """
    record_type: str  # 'health_record', 'sleep_log', 'nutrition_log'
    
    # Core fields
    source: str
    metric_type: str | None = None  # For health_record
    value: float | None = None
    unit: str = ''
    timestamp: datetime | None = None
    date: Any = None  # date object
    
    # Sleep-specific
    start_time: datetime | None = None
    end_time: datetime | None = None
    duration_minutes: int | None = None
    sleep_data: dict = field(default_factory=dict)
    
    # Nutrition-specific
    nutrition_data: dict = field(default_factory=dict)
    
    # Common
    metadata: dict = field(default_factory=dict)
    raw_data: dict | None = None


@dataclass
class ParseResult:
    """Result of parsing an entire file or export."""
    success: bool
    records: list[ParsedRecord] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    records_parsed: int = 0
    file_path: str = ''


class BaseAdapter(ABC):
    """
    Abstract base class for all data source adapters.
    
    Each adapter is responsible for:
    1. Detecting if it can handle a given file/folder
    2. Parsing the data into ParsedRecord objects
    3. Handling source-specific quirks (date formats, nested structures, etc.)
    
    Usage:
        adapter = FitbitAdapter()
        if adapter.can_handle(file_path):
            result = adapter.parse(file_path)
            for record in result.records:
                # Save to database
    """
    
    # Override in subclasses
    SOURCE_NAME: str = 'unknown'
    SUPPORTED_FILE_TYPES: tuple[str, ...] = ()
    
    def __init__(self, batch_id: UUID | None = None):
        self.batch_id = batch_id
        self.errors: list[str] = []
    
    @abstractmethod
    def can_handle(self, path: Path) -> bool:
        """
        Check if this adapter can handle the given file or directory.
        
        Args:
            path: Path to file or directory
            
        Returns:
            True if this adapter can parse the given path
        """
        pass
    
    @abstractmethod
    def parse(self, path: Path) -> ParseResult:
        """
        Parse the file/directory and yield normalized records.
        
        Args:
            path: Path to file or directory to parse
            
        Returns:
            ParseResult with list of ParsedRecord objects
        """
        pass
    
    def parse_streaming(self, path: Path) -> Generator[ParsedRecord, None, None]:
        """
        Parse and yield records one at a time (memory efficient for large files).
        Default implementation just wraps parse(), override for true streaming.
        """
        result = self.parse(path)
        yield from result.records
    
    def _log_error(self, message: str, exception: Exception | None = None):
        """Log and track an error during parsing."""
        if exception:
            message = f"{message}: {str(exception)}"
        logger.error(f"[{self.SOURCE_NAME}] {message}")
        self.errors.append(message)
    
    def _parse_datetime(self, value: str, formats: list[str]) -> datetime | None:
        """
        Try parsing a datetime string with multiple format options.
        
        Args:
            value: The datetime string to parse
            formats: List of strptime format strings to try
            
        Returns:
            Parsed datetime or None if all formats fail
        """
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        
        self._log_error(f"Could not parse datetime: {value}")
        return None


class AdapterRegistry:
    """
    Registry of all available adapters.
    Use this to find the right adapter for a given file.
    """
    
    _adapters: list[type[BaseAdapter]] = []
    
    @classmethod
    def register(cls, adapter_class: type[BaseAdapter]):
        """Register an adapter class."""
        cls._adapters.append(adapter_class)
        return adapter_class
    
    @classmethod
    def get_adapter_for(cls, path: Path, batch_id: UUID | None = None) -> BaseAdapter | None:
        """
        Find an adapter that can handle the given path.
        
        Args:
            path: Path to file or directory
            batch_id: Optional batch ID for import tracking
            
        Returns:
            An adapter instance or None if no adapter matches
        """
        for adapter_class in cls._adapters:
            adapter = adapter_class(batch_id=batch_id)
            if adapter.can_handle(path):
                return adapter
        return None
    
    @classmethod
    def get_adapter_by_name(cls, name: str, batch_id: UUID | None = None) -> BaseAdapter | None:
        """Get an adapter by its SOURCE_NAME."""
        for adapter_class in cls._adapters:
            if adapter_class.SOURCE_NAME == name:
                return adapter_class(batch_id=batch_id)
        return None
    
    @classmethod
    def list_adapters(cls) -> list[str]:
        """List all registered adapter names."""
        return [a.SOURCE_NAME for a in cls._adapters]
