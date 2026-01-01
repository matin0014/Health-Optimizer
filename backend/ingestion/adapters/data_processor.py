"""
Pandas-based data processor for cleaning and aggregating health data.

Handles:
- Aggregating minute-level data into daily totals (steps, calories, distance)
- Cleaning data (deduplication, missing values, outlier detection)
- Parsing various date formats consistently
"""

import pandas as pd
import numpy as np
import json
import csv
from datetime import datetime, date
from pathlib import Path
from typing import Generator
import logging

logger = logging.getLogger(__name__)


class DataProcessor:
    """
    Utility class for processing health data with pandas.
    """
    
    # Fitbit's datetime formats
    DATETIME_FORMATS = [
        '%m/%d/%y %H:%M:%S',      # 07/27/24 06:53:41
        '%Y-%m-%dT%H:%M:%S.%f',   # 2024-08-25T03:44:00.000
        '%Y-%m-%dT%H:%M:%S',      # 2024-08-25T03:44:00
        '%Y-%m-%dT%H:%M:%SZ',     # 2024-08-25T03:44:00Z
        '%Y-%m-%dT%H:%M',         # 2024-07-27T10:56
        '%Y-%m-%d',               # 2024-08-25
    ]
    
    @classmethod
    def parse_datetime(cls, dt_string: str) -> datetime | None:
        """Try multiple datetime formats to parse a string."""
        if not dt_string:
            return None
        
        for fmt in cls.DATETIME_FORMATS:
            try:
                return datetime.strptime(dt_string, fmt)
            except ValueError:
                continue
        
        # Try pandas as fallback (handles many formats)
        try:
            return pd.to_datetime(dt_string).to_pydatetime()
        except:
            logger.warning(f"Could not parse datetime: {dt_string}")
            return None
    
    @classmethod
    def load_json_to_dataframe(cls, file_path: Path) -> pd.DataFrame:
        """
        Load a Fitbit JSON file into a pandas DataFrame.
        Handles the [{dateTime, value}, ...] format.
        """
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        return df
    
    @classmethod
    def aggregate_minute_data_to_daily(
        cls, 
        file_path: Path,
        value_column: str = 'value',
        agg_func: str = 'sum',
        value_type: type = float
    ) -> pd.DataFrame:
        """
        Aggregate minute-level JSON data to daily totals.
        
        Args:
            file_path: Path to the JSON file
            value_column: Column containing the value to aggregate
            agg_func: Aggregation function ('sum', 'mean', 'max', 'min')
            value_type: Type to cast value to
            
        Returns:
            DataFrame with columns: date, value, record_count
        """
        df = cls.load_json_to_dataframe(file_path)
        
        if df.empty:
            return pd.DataFrame(columns=['date', 'value', 'record_count'])
        
        # Parse datetime
        df['datetime'] = df['dateTime'].apply(cls.parse_datetime)
        df = df.dropna(subset=['datetime'])
        
        # Extract date
        df['date'] = df['datetime'].dt.date
        
        # Convert value to numeric, coercing errors
        df['value'] = pd.to_numeric(df[value_column], errors='coerce')
        df = df.dropna(subset=['value'])
        
        # Aggregate by date
        if agg_func == 'sum':
            result = df.groupby('date').agg(
                value=('value', 'sum'),
                record_count=('value', 'count'),
                min_value=('value', 'min'),
                max_value=('value', 'max'),
            ).reset_index()
        elif agg_func == 'mean':
            result = df.groupby('date').agg(
                value=('value', 'mean'),
                record_count=('value', 'count'),
                min_value=('value', 'min'),
                max_value=('value', 'max'),
            ).reset_index()
        else:
            result = df.groupby('date').agg(
                value=('value', agg_func),
                record_count=('value', 'count'),
            ).reset_index()
        
        return result
    
    @classmethod
    def aggregate_steps_json(cls, file_path: Path) -> pd.DataFrame:
        """
        Aggregate minute-level steps data to daily totals.
        
        Returns DataFrame with: date, total_steps, step_records, first_step_time, last_step_time
        """
        df = cls.load_json_to_dataframe(file_path)
        
        if df.empty:
            return pd.DataFrame()
        
        # Parse datetime
        df['datetime'] = df['dateTime'].apply(cls.parse_datetime)
        df = df.dropna(subset=['datetime'])
        df['date'] = df['datetime'].dt.date
        
        # Convert steps to numeric
        df['steps'] = pd.to_numeric(df['value'], errors='coerce').fillna(0).astype(int)
        
        # Only keep records with steps > 0 for time analysis
        steps_df = df[df['steps'] > 0]
        
        # Aggregate
        result = df.groupby('date').agg(
            total_steps=('steps', 'sum'),
            records_count=('steps', 'count'),
        ).reset_index()
        
        # Add first/last step time
        if not steps_df.empty:
            time_info = steps_df.groupby('date').agg(
                first_step_time=('datetime', 'min'),
                last_step_time=('datetime', 'max'),
            ).reset_index()
            result = result.merge(time_info, on='date', how='left')
        
        return result
    
    @classmethod
    def aggregate_calories_json(cls, file_path: Path) -> pd.DataFrame:
        """
        Aggregate minute-level calories data to daily totals.
        
        Returns DataFrame with: date, total_calories, avg_per_minute, active_minutes
        """
        df = cls.load_json_to_dataframe(file_path)
        
        if df.empty:
            return pd.DataFrame()
        
        df['datetime'] = df['dateTime'].apply(cls.parse_datetime)
        df = df.dropna(subset=['datetime'])
        df['date'] = df['datetime'].dt.date
        
        # Calories are per-minute burn rate
        df['calories'] = pd.to_numeric(df['value'], errors='coerce').fillna(0)
        
        # Base metabolic rate is ~1.2 cal/min at rest
        # Count "active" as anything above that threshold
        BASE_RATE = 1.3
        df['is_active'] = df['calories'] > BASE_RATE
        
        result = df.groupby('date').agg(
            total_calories=('calories', 'sum'),
            avg_per_minute=('calories', 'mean'),
            active_minutes=('is_active', 'sum'),
            records_count=('calories', 'count'),
        ).reset_index()
        
        # Round appropriately
        result['total_calories'] = result['total_calories'].round(0).astype(int)
        result['avg_per_minute'] = result['avg_per_minute'].round(2)
        
        return result
    
    @classmethod
    def aggregate_distance_json(cls, file_path: Path) -> pd.DataFrame:
        """
        Aggregate minute-level distance data to daily totals.
        Distance in Fitbit export is in centimeters (needs verification).
        
        Returns DataFrame with: date, total_distance_km, total_distance_miles
        """
        df = cls.load_json_to_dataframe(file_path)
        
        if df.empty:
            return pd.DataFrame()
        
        df['datetime'] = df['dateTime'].apply(cls.parse_datetime)
        df = df.dropna(subset=['datetime'])
        df['date'] = df['datetime'].dt.date
        
        # Distance values appear to be in some small unit - check actual data
        df['distance'] = pd.to_numeric(df['value'], errors='coerce').fillna(0)
        
        result = df.groupby('date').agg(
            total_distance=('distance', 'sum'),
            records_count=('distance', 'count'),
        ).reset_index()
        
        # Convert to km (assuming mm input based on typical step length)
        result['total_distance_km'] = (result['total_distance'] / 1_000_000).round(2)
        result['total_distance_miles'] = (result['total_distance_km'] * 0.621371).round(2)
        
        return result
    
    @classmethod
    def load_daily_csv_with_date(
        cls, 
        file_path: Path, 
        date_column: str = 'timestamp'
    ) -> pd.DataFrame:
        """
        Load a CSV file that already has daily data.
        Parses the date column and cleans the data.
        """
        df = pd.read_csv(file_path)
        
        if df.empty or date_column not in df.columns:
            return pd.DataFrame()
        
        df['parsed_date'] = df[date_column].apply(cls.parse_datetime)
        df = df.dropna(subset=['parsed_date'])
        df['date'] = df['parsed_date'].dt.date
        
        return df
    
    @classmethod
    def parse_daily_hrv_csv(cls, file_path: Path) -> pd.DataFrame:
        """
        Parse Daily Heart Rate Variability Summary CSV.
        
        Expected columns: timestamp, rmssd, nremhr, entropy, etc.
        """
        df = pd.read_csv(file_path)
        
        if df.empty:
            return pd.DataFrame()
        
        # Find date column
        date_col = None
        for col in ['timestamp', 'date', 'DATE']:
            if col in df.columns:
                date_col = col
                break
        
        if not date_col:
            logger.warning(f"No date column found in {file_path}")
            return pd.DataFrame()
        
        df['datetime'] = df[date_col].apply(cls.parse_datetime)
        df = df.dropna(subset=['datetime'])
        df['date'] = df['datetime'].dt.date
        
        # Convert numeric columns
        numeric_cols = ['rmssd', 'nremhr', 'entropy']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    @classmethod
    def parse_daily_spo2_csv(cls, file_path: Path) -> pd.DataFrame:
        """
        Parse Daily SpO2 CSV files.
        
        Expected columns: timestamp, average_value, lower_bound, upper_bound
        """
        df = pd.read_csv(file_path)
        
        if df.empty:
            return pd.DataFrame()
        
        # Find date column
        date_col = None
        for col in ['timestamp', 'date', 'DATE']:
            if col in df.columns:
                date_col = col
                break
        
        if not date_col:
            return pd.DataFrame()
        
        df['datetime'] = df[date_col].apply(cls.parse_datetime)
        df = df.dropna(subset=['datetime'])
        df['date'] = df['datetime'].dt.date
        
        # Convert numeric columns
        numeric_cols = ['average_value', 'lower_bound', 'upper_bound']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    @classmethod
    def parse_daily_readiness_csv(cls, file_path: Path) -> pd.DataFrame:
        """
        Parse Daily Readiness Score CSV.
        """
        df = pd.read_csv(file_path)
        
        if df.empty:
            return pd.DataFrame()
        
        # Find date column
        date_col = None
        for col in ['timestamp', 'date', 'DATE', 'TIMESTAMP']:
            if col in df.columns:
                date_col = col
                break
        
        if not date_col:
            return pd.DataFrame()
        
        df['datetime'] = df[date_col].apply(cls.parse_datetime)
        df = df.dropna(subset=['datetime'])
        df['date'] = df['datetime'].dt.date
        
        return df
    
    @classmethod
    def clean_dataframe(
        cls, 
        df: pd.DataFrame, 
        dedup_columns: list[str] | None = None,
        remove_outliers: bool = False,
        outlier_column: str | None = None,
        outlier_std: float = 3.0
    ) -> pd.DataFrame:
        """
        Clean a DataFrame by removing duplicates and optionally outliers.
        
        Args:
            df: Input DataFrame
            dedup_columns: Columns to use for deduplication
            remove_outliers: Whether to remove outliers
            outlier_column: Column to use for outlier detection
            outlier_std: Number of standard deviations for outlier threshold
        """
        if df.empty:
            return df
        
        result = df.copy()
        
        # Remove duplicates
        if dedup_columns:
            result = result.drop_duplicates(subset=dedup_columns, keep='last')
        
        # Remove outliers using z-score method
        if remove_outliers and outlier_column and outlier_column in result.columns:
            mean = result[outlier_column].mean()
            std = result[outlier_column].std()
            result = result[
                (result[outlier_column] >= mean - outlier_std * std) &
                (result[outlier_column] <= mean + outlier_std * std)
            ]
        
        return result
    
    @classmethod
    def validate_steps(cls, value: float) -> bool:
        """Validate step count is reasonable."""
        return 0 <= value <= 100000  # Max 100k steps/day
    
    @classmethod
    def validate_calories(cls, value: float) -> bool:
        """Validate calorie count is reasonable."""
        return 0 <= value <= 10000  # Max 10k calories/day
    
    @classmethod
    def validate_heart_rate(cls, value: float) -> bool:
        """Validate heart rate is reasonable."""
        return 30 <= value <= 250
    
    @classmethod
    def validate_spo2(cls, value: float) -> bool:
        """Validate SpO2 is reasonable."""
        return 70 <= value <= 100
