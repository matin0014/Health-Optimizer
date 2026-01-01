"""
Fitbit Google Takeout Export Adapter

Handles the folder structure from Google Takeout Fitbit exports:
- Global Export Data/ (heart_rate, sleep, steps, calories, etc.)
- Sleep Score/
- Active Zone Minutes (AZM)/
- Stress Score/
- Heart Rate Variability/
- Oxygen Saturation (SpO2)/
- Daily Readiness/

Fitbit exports are a mix of JSON and CSV files with inconsistent naming.

Uses pandas for efficient data cleaning and aggregation of minute-level data
into daily summaries.
"""

import json
import csv
from datetime import datetime, date
from pathlib import Path
from typing import Generator
import logging

import pandas as pd

from .base import BaseAdapter, ParsedRecord, ParseResult, AdapterRegistry
from .data_processor import DataProcessor

logger = logging.getLogger(__name__)


@AdapterRegistry.register
class FitbitAdapter(BaseAdapter):
    """
    Parser for Fitbit data exported via Google Takeout.
    """
    
    SOURCE_NAME = 'fitbit'
    SUPPORTED_FILE_TYPES = ('.json', '.csv')
    
    # Fitbit's datetime formats (they use several!)
    DATETIME_FORMATS = [
        '%m/%d/%y %H:%M:%S',      # 07/27/24 06:53:41 (heart rate)
        '%Y-%m-%dT%H:%M:%S.%f',   # 2024-08-25T03:44:00.000 (sleep)
        '%Y-%m-%dT%H:%M:%S',      # 2024-08-25T03:44:00
        '%Y-%m-%dT%H:%M',         # 2024-07-27T10:56 (AZM csv)
        '%Y-%m-%d',               # 2024-08-25
    ]
    
    def can_handle(self, path: Path) -> bool:
        """
        Check if this looks like a Fitbit export.
        Look for characteristic folder names or file patterns.
        """
        if path.is_dir():
            # Check for Fitbit folder structure
            indicators = [
                path / 'Global Export Data',
                path / 'Sleep Score',
                path / 'Active Zone Minutes (AZM)',
            ]
            return any(p.exists() for p in indicators)
        
        if path.is_file():
            name = path.name.lower()
            # Fitbit file naming patterns
            patterns = [
                'heart_rate-',
                'sleep-',
                'steps-',
                'calories-',
                'sleep_score',
                'active zone minutes',
            ]
            return any(p in name for p in patterns)
        
        return False
    
    def parse(self, path: Path) -> ParseResult:
        """
        Parse a Fitbit export directory or individual file.
        """
        records: list[ParsedRecord] = []
        self.errors = []
        
        if path.is_dir():
            records = self._parse_directory(path)
        elif path.is_file():
            records = list(self._parse_file(path))
        
        return ParseResult(
            success=len(self.errors) == 0,
            records=records,
            errors=self.errors,
            records_parsed=len(records),
            file_path=str(path)
        )
    
    def _parse_directory(self, root: Path) -> list[ParsedRecord]:
        """Parse an entire Fitbit export directory."""
        records: list[ParsedRecord] = []
        
        # Parse Global Export Data (main data folder)
        global_data = root / 'Global Export Data'
        if global_data.exists():
            for file in global_data.iterdir():
                if file.suffix in self.SUPPORTED_FILE_TYPES:
                    records.extend(self._parse_file(file))
        
        # Parse Sleep Score
        sleep_score = root / 'Sleep Score' / 'sleep_score.csv'
        if sleep_score.exists():
            records.extend(self._parse_sleep_score_csv(sleep_score))
        
        # Parse Stress Score
        stress_score = root / 'Stress Score' / 'Stress Score.csv'
        if stress_score.exists():
            records.extend(self._parse_stress_score_csv(stress_score))
        
        # Parse Active Zone Minutes
        azm_folder = root / 'Active Zone Minutes (AZM)'
        if azm_folder.exists():
            for file in azm_folder.glob('*.csv'):
                records.extend(self._parse_azm_csv(file))
        
        # Parse Daily HRV Summary files
        hrv_folder = root / 'Heart Rate Variability'
        if hrv_folder.exists():
            for file in hrv_folder.glob('Daily Heart Rate Variability Summary*.csv'):
                records.extend(self._parse_daily_hrv_csv(file))
        
        # Parse Daily SpO2 files
        spo2_folder = root / 'Oxygen Saturation (SpO2)'
        if spo2_folder.exists():
            for file in spo2_folder.glob('Daily SpO2*.csv'):
                records.extend(self._parse_daily_spo2_csv(file))
        
        # Parse Daily Readiness files
        readiness_folder = root / 'Daily Readiness'
        if readiness_folder.exists():
            for file in readiness_folder.glob('Daily Readiness Score*.csv'):
                records.extend(self._parse_daily_readiness_csv(file))
        
        # Parse Temperature files
        temp_folder = root / 'Temperature'
        if temp_folder.exists():
            for file in temp_folder.glob('Computed Temperature*.csv'):
                records.extend(self._parse_temperature_csv(file))
        
        # Parse Health Fitness Data_GoogleData (UserSleepScores for RHR, UserSleeps for times)
        health_fitness_folder = root / 'Health Fitness Data_GoogleData'
        if health_fitness_folder.exists():
            for file in health_fitness_folder.glob('UserSleepScores_*.csv'):
                records.extend(self._parse_user_sleep_scores_csv(file))
        
        return records
    
    def _parse_file(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """Route a file to the appropriate parser based on filename."""
        name = file.name.lower()
        
        try:
            if file.suffix == '.json':
                if 'heart_rate-' in name:
                    # Skip minute-level heart rate data (~17k records/day)
                    # We'll use resting_heart_rate and daily HRV instead
                    pass
                elif 'calories-' in name:
                    # Skip calories burned - wearable TDEE estimates are inaccurate
                    # We use food_logs (Cronometer) for actual nutrition tracking
                    pass
                elif 'sleep-' in name:
                    yield from self._parse_sleep_json(file)
                elif 'steps-' in name:
                    yield from self._parse_steps_json(file)
                elif 'resting_heart_rate-' in name:
                    yield from self._parse_resting_hr_json(file)
                elif 'distance-' in name:
                    yield from self._parse_distance_json(file)
                elif 'very_active_minutes-' in name:
                    yield from self._parse_active_minutes_json(file, 'very_active')
                elif 'moderately_active_minutes-' in name:
                    yield from self._parse_active_minutes_json(file, 'moderately_active')
                elif 'lightly_active_minutes-' in name:
                    yield from self._parse_active_minutes_json(file, 'lightly_active')
                elif 'sedentary_minutes-' in name:
                    yield from self._parse_active_minutes_json(file, 'sedentary')
                elif 'food_logs-' in name:
                    yield from self._parse_food_logs_json(file)
                    
            elif file.suffix == '.csv':
                if 'sleep_score' in name:
                    yield from self._parse_sleep_score_csv(file)
                elif 'stress score' in name:
                    yield from self._parse_stress_score_csv(file)
                elif 'active zone minutes' in name:
                    yield from self._parse_azm_csv(file)
                    
        except Exception as e:
            self._log_error(f"Error parsing {file.name}", e)
    
    # -------------------------------------------------------------------------
    # JSON Parsers
    # -------------------------------------------------------------------------
    
    def _parse_heart_rate_json(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """Parse heart_rate-YYYY-MM-DD.json files."""
        try:
            with open(file, 'r') as f:
                data = json.load(f)
            
            for entry in data:
                dt = self._parse_datetime(entry['dateTime'], self.DATETIME_FORMATS)
                if dt is None:
                    continue
                
                yield ParsedRecord(
                    record_type='health_record',
                    source=self.SOURCE_NAME,
                    metric_type='heart_rate',
                    value=float(entry['value']['bpm']),
                    unit='bpm',
                    timestamp=dt,
                    date=dt.date(),
                    metadata={
                        'confidence': entry['value'].get('confidence', 0)
                    },
                    raw_data=entry
                )
        except Exception as e:
            self._log_error(f"Error parsing heart rate file {file.name}", e)
    
    def _parse_sleep_json(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """Parse sleep-YYYY-MM-DD.json files into SleepLog records."""
        try:
            with open(file, 'r') as f:
                data = json.load(f)
            
            for entry in data:
                start_time = self._parse_datetime(entry['startTime'], self.DATETIME_FORMATS)
                end_time = self._parse_datetime(entry['endTime'], self.DATETIME_FORMATS)
                
                if start_time is None or end_time is None:
                    continue
                
                # Extract sleep stage summary if available
                levels = entry.get('levels', {})
                summary = levels.get('summary', {})
                
                yield ParsedRecord(
                    record_type='sleep_log',
                    source=self.SOURCE_NAME,
                    timestamp=start_time,
                    date=datetime.strptime(entry['dateOfSleep'], '%Y-%m-%d').date(),
                    start_time=start_time,
                    end_time=end_time,
                    duration_minutes=entry.get('duration', 0) // 60000,  # ms to minutes
                    sleep_data={
                        'source_log_id': str(entry.get('logId', '')),
                        'date_of_sleep': entry['dateOfSleep'],
                        'minutes_asleep': entry.get('minutesAsleep'),
                        'minutes_awake': entry.get('minutesAwake'),
                        'efficiency': entry.get('efficiency'),
                        'deep_sleep_minutes': summary.get('deep', {}).get('minutes'),
                        'light_sleep_minutes': summary.get('light', {}).get('minutes'),
                        'rem_sleep_minutes': summary.get('rem', {}).get('minutes'),
                        'stages_data': levels.get('data', []),
                    },
                    metadata={
                        'type': entry.get('type'),
                        'log_type': entry.get('logType'),
                        'info_code': entry.get('infoCode'),
                    },
                    raw_data=entry
                )
        except Exception as e:
            self._log_error(f"Error parsing sleep file {file.name}", e)
    
    def _parse_steps_json(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """
        Parse steps-YYYY-MM-DD.json files.
        
        Uses pandas to aggregate minute-level data into daily totals.
        Instead of storing ~60,000 records per file, we store one per day.
        """
        try:
            df = DataProcessor.aggregate_steps_json(file)
            
            if df.empty:
                return
            
            for _, row in df.iterrows():
                # Validate the step count
                if not DataProcessor.validate_steps(row['total_steps']):
                    continue
                
                yield ParsedRecord(
                    record_type='health_record',
                    source=self.SOURCE_NAME,
                    metric_type='steps',
                    value=float(row['total_steps']),
                    unit='steps',
                    timestamp=datetime.combine(row['date'], datetime.min.time()),
                    date=row['date'],
                    metadata={
                        'aggregation': 'daily_total',
                        'records_count': int(row['records_count']),
                        'first_step_time': str(row.get('first_step_time', '')),
                        'last_step_time': str(row.get('last_step_time', '')),
                    },
                    raw_data=None  # Don't store ~60k raw records
                )
        except Exception as e:
            self._log_error(f"Error parsing steps file {file.name}", e)
        except Exception as e:
            self._log_error(f"Error parsing steps file {file.name}", e)
    
    def _parse_calories_json(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """
        Parse calories-YYYY-MM-DD.json files.
        
        Uses pandas to aggregate minute-level data into daily totals.
        """
        try:
            df = DataProcessor.aggregate_calories_json(file)
            
            if df.empty:
                return
            
            for _, row in df.iterrows():
                # Validate the calorie count
                if not DataProcessor.validate_calories(row['total_calories']):
                    continue
                
                yield ParsedRecord(
                    record_type='health_record',
                    source=self.SOURCE_NAME,
                    metric_type='calories_burned',
                    value=float(row['total_calories']),
                    unit='kcal',
                    timestamp=datetime.combine(row['date'], datetime.min.time()),
                    date=row['date'],
                    metadata={
                        'aggregation': 'daily_total',
                        'avg_per_minute': float(row['avg_per_minute']),
                        'active_minutes': int(row['active_minutes']),
                        'records_count': int(row['records_count']),
                    },
                    raw_data=None
                )
        except Exception as e:
            self._log_error(f"Error parsing calories file {file.name}", e)
    
    def _parse_resting_hr_json(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """Parse resting_heart_rate-YYYY-MM-DD.json files."""
        try:
            with open(file, 'r') as f:
                data = json.load(f)
            
            for entry in data:
                dt = self._parse_datetime(entry['dateTime'], self.DATETIME_FORMATS)
                if dt is None:
                    # Try parsing just the date
                    try:
                        d = datetime.strptime(entry['dateTime'], '%m/%d/%y')
                        dt = d
                    except ValueError:
                        continue
                
                rhr = entry.get('value', {})
                if isinstance(rhr, dict):
                    value = rhr.get('value', rhr.get('restingHeartRate', 0))
                else:
                    value = float(rhr)
                
                if value:
                    yield ParsedRecord(
                        record_type='health_record',
                        source=self.SOURCE_NAME,
                        metric_type='resting_heart_rate',
                        value=float(value),
                        unit='bpm',
                        timestamp=dt,
                        date=dt.date() if dt else None,
                        raw_data=entry
                    )
        except Exception as e:
            self._log_error(f"Error parsing resting HR file {file.name}", e)
    
    def _parse_distance_json(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """
        Parse distance-YYYY-MM-DD.json files.
        
        Uses pandas to aggregate minute-level data into daily totals.
        """
        try:
            df = DataProcessor.aggregate_distance_json(file)
            
            if df.empty:
                return
            
            for _, row in df.iterrows():
                yield ParsedRecord(
                    record_type='health_record',
                    source=self.SOURCE_NAME,
                    metric_type='distance',
                    value=float(row['total_distance_km']),
                    unit='km',
                    timestamp=datetime.combine(row['date'], datetime.min.time()),
                    date=row['date'],
                    metadata={
                        'aggregation': 'daily_total',
                        'distance_miles': float(row['total_distance_miles']),
                        'records_count': int(row['records_count']),
                    },
                    raw_data=None
                )
        except Exception as e:
            self._log_error(f"Error parsing distance file {file.name}", e)
    
    def _parse_active_minutes_json(
        self, file: Path, activity_level: str
    ) -> Generator[ParsedRecord, None, None]:
        """Parse *_active_minutes-YYYY-MM-DD.json files."""
        try:
            with open(file, 'r') as f:
                data = json.load(f)
            
            for entry in data:
                dt = self._parse_datetime(entry['dateTime'], self.DATETIME_FORMATS)
                if dt is None:
                    continue
                
                yield ParsedRecord(
                    record_type='health_record',
                    source=self.SOURCE_NAME,
                    metric_type='active_minutes',
                    value=float(entry['value']),
                    unit='minutes',
                    timestamp=dt,
                    date=dt.date(),
                    metadata={'activity_level': activity_level},
                    raw_data=entry
                )
        except Exception as e:
            self._log_error(f"Error parsing active minutes file {file.name}", e)
    
    # -------------------------------------------------------------------------
    # CSV Parsers
    # -------------------------------------------------------------------------
    
    def _parse_sleep_score_csv(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """Parse Sleep Score/sleep_score.csv."""
        try:
            with open(file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    ts = self._parse_datetime(row['timestamp'], self.DATETIME_FORMATS + ['%Y-%m-%dT%H:%M:%SZ'])
                    if ts is None:
                        continue
                    
                    score = row.get('overall_score', '')
                    if not score:
                        continue
                    
                    yield ParsedRecord(
                        record_type='health_record',
                        source=self.SOURCE_NAME,
                        metric_type='sleep_score',
                        value=float(score),
                        unit='score',
                        timestamp=ts,
                        date=ts.date(),
                        metadata={
                            'composition_score': row.get('composition_score') or None,
                            'revitalization_score': row.get('revitalization_score') or None,
                            'duration_score': row.get('duration_score') or None,
                            'deep_sleep_minutes': row.get('deep_sleep_in_minutes') or None,
                            'resting_heart_rate': row.get('resting_heart_rate') or None,
                            'restlessness': row.get('restlessness') or None,
                        },
                        raw_data=dict(row)
                    )
        except Exception as e:
            self._log_error(f"Error parsing sleep score CSV {file.name}", e)
    
    def _parse_stress_score_csv(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """Parse Stress Score/Stress Score.csv."""
        try:
            with open(file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    # Stress score files have DATE and STRESS_SCORE columns (check actual format)
                    date_str = row.get('DATE') or row.get('date') or row.get('timestamp')
                    score = row.get('STRESS_SCORE') or row.get('stress_score') or row.get('overall_score')
                    
                    if not date_str or not score:
                        continue
                    
                    dt = self._parse_datetime(date_str, self.DATETIME_FORMATS + ['%Y-%m-%dT%H:%M:%SZ'])
                    if dt is None:
                        continue
                    
                    yield ParsedRecord(
                        record_type='health_record',
                        source=self.SOURCE_NAME,
                        metric_type='stress_score',
                        value=float(score),
                        unit='score',
                        timestamp=dt,
                        date=dt.date(),
                        raw_data=dict(row)
                    )
        except Exception as e:
            self._log_error(f"Error parsing stress score CSV {file.name}", e)
    
    def _parse_azm_csv(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """Parse Active Zone Minutes CSV files."""
        try:
            with open(file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    dt = self._parse_datetime(row['date_time'], self.DATETIME_FORMATS)
                    if dt is None:
                        continue
                    
                    zone = row.get('heart_zone_id', 'unknown')
                    minutes = row.get('total_minutes', 0)
                    
                    yield ParsedRecord(
                        record_type='health_record',
                        source=self.SOURCE_NAME,
                        metric_type='active_minutes',
                        value=float(minutes),
                        unit='minutes',
                        timestamp=dt,
                        date=dt.date(),
                        metadata={'heart_zone': zone},
                        raw_data=dict(row)
                    )
        except Exception as e:
            self._log_error(f"Error parsing AZM CSV {file.name}", e)

    # -------------------------------------------------------------------------
    # Daily Aggregate Parsers (using pandas for efficient processing)
    # -------------------------------------------------------------------------

    def _parse_daily_hrv_csv(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """
        Parse Daily Heart Rate Variability Summary CSV files.
        
        These files contain daily HRV metrics:
        - rmssd: Root mean square of successive differences (ms)
        - nremhr: Heart rate during NREM sleep
        - entropy: HRV complexity measure
        """
        try:
            df = DataProcessor.parse_daily_hrv_csv(file)
            
            if df.empty:
                return
            
            for _, row in df.iterrows():
                rmssd = row.get('rmssd')
                if pd.isna(rmssd):
                    continue
                
                yield ParsedRecord(
                    record_type='health_record',
                    source=self.SOURCE_NAME,
                    metric_type='hrv_rmssd',
                    value=float(rmssd),
                    unit='ms',
                    timestamp=row['datetime'],
                    date=row['date'],
                    metadata={
                        'nremhr': float(row['nremhr']) if pd.notna(row.get('nremhr')) else None,
                        'entropy': float(row['entropy']) if pd.notna(row.get('entropy')) else None,
                        'aggregation': 'daily',
                    },
                    raw_data=None
                )
        except Exception as e:
            self._log_error(f"Error parsing HRV CSV {file.name}", e)

    def _parse_daily_spo2_csv(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """
        Parse Daily SpO2 (Oxygen Saturation) CSV files.
        
        Contains nightly SpO2 readings:
        - average_value: Average SpO2 during sleep
        - lower_bound: Lowest SpO2 reading
        - upper_bound: Highest SpO2 reading
        """
        try:
            df = DataProcessor.parse_daily_spo2_csv(file)
            
            if df.empty:
                return
            
            for _, row in df.iterrows():
                avg_value = row.get('average_value')
                if pd.isna(avg_value):
                    continue
                
                # Validate SpO2 range
                if not DataProcessor.validate_spo2(avg_value):
                    continue
                
                yield ParsedRecord(
                    record_type='health_record',
                    source=self.SOURCE_NAME,
                    metric_type='spo2',
                    value=float(avg_value),
                    unit='%',
                    timestamp=row['datetime'],
                    date=row['date'],
                    metadata={
                        'lower_bound': float(row['lower_bound']) if pd.notna(row.get('lower_bound')) else None,
                        'upper_bound': float(row['upper_bound']) if pd.notna(row.get('upper_bound')) else None,
                        'aggregation': 'daily',
                    },
                    raw_data=None
                )
        except Exception as e:
            self._log_error(f"Error parsing SpO2 CSV {file.name}", e)

    def _parse_daily_readiness_csv(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """
        Parse Daily Readiness Score CSV files.
        
        Fitbit's readiness score combines:
        - HRV changes
        - Sleep quality
        - Recent activity levels
        """
        try:
            df = DataProcessor.parse_daily_readiness_csv(file)
            
            if df.empty:
                return
            
            # Find the score column
            score_col = None
            for col in ['readiness_score_value', 'READINESS_SCORE_VALUE', 'score']:
                if col in df.columns:
                    score_col = col
                    break
            
            if not score_col:
                return
            
            for _, row in df.iterrows():
                score = row.get(score_col)
                if pd.isna(score):
                    continue
                
                metadata = {'aggregation': 'daily'}
                
                # Capture sub-components if available
                for sub in ['hrv_subcomponent', 'sleep_subcomponent', 'activity_subcomponent']:
                    if sub in row and pd.notna(row.get(sub)):
                        metadata[sub] = float(row[sub])
                
                yield ParsedRecord(
                    record_type='health_record',
                    source=self.SOURCE_NAME,
                    metric_type='readiness_score',
                    value=float(score),
                    unit='score',
                    timestamp=row['datetime'],
                    date=row['date'],
                    metadata=metadata,
                    raw_data=None
                )
        except Exception as e:
            self._log_error(f"Error parsing Readiness CSV {file.name}", e)

    def _parse_temperature_csv(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """
        Parse Computed Temperature CSV files.
        
        Contains nightly skin temperature readings:
        - nightly_temperature: Relative temperature deviation from baseline
        """
        try:
            df = pd.read_csv(file)
            
            if df.empty:
                return
            
            # Find date column
            date_col = None
            for col in ['timestamp', 'date', 'sleep_start']:
                if col in df.columns:
                    date_col = col
                    break
            
            if not date_col:
                return
            
            df['datetime'] = df[date_col].apply(DataProcessor.parse_datetime)
            df = df.dropna(subset=['datetime'])
            
            # Find temperature column
            temp_col = None
            for col in ['nightly_temperature', 'temperature_samples', 'computed_temperature']:
                if col in df.columns:
                    temp_col = col
                    break
            
            if not temp_col:
                return
            
            for _, row in df.iterrows():
                temp = row.get(temp_col)
                if pd.isna(temp):
                    continue
                
                yield ParsedRecord(
                    record_type='health_record',
                    source=self.SOURCE_NAME,
                    metric_type='skin_temperature',
                    value=float(temp),
                    unit='°C',  # Usually relative deviation
                    timestamp=row['datetime'],
                    date=row['datetime'].date(),
                    metadata={'aggregation': 'nightly'},
                    raw_data=None
                )
        except Exception as e:
            self._log_error(f"Error parsing Temperature CSV {file.name}", e)

    def _parse_user_sleep_scores_csv(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """
        Parse UserSleepScores_*.csv from Health Fitness Data_GoogleData.
        
        This file contains:
        - resting_heart_rate: Daily RHR from overnight measurement
        - overall_score: Sleep score
        - deep_sleep_minutes, rem_sleep_percent, etc.
        - score_time: When the sleep ended (used as date reference)
        """
        try:
            df = pd.read_csv(file)
            
            if df.empty:
                return
            
            for _, row in df.iterrows():
                # Parse the score_time to get the date
                score_time_str = row.get('score_time')
                if pd.isna(score_time_str):
                    continue
                
                # Parse datetime like "2025-10-08 08:43:30+0000"
                try:
                    dt = pd.to_datetime(score_time_str, format='%Y-%m-%d %H:%M:%S%z')
                    if dt is None:
                        continue
                    target_date = dt.date()
                except Exception:
                    continue
                
                # Extract resting heart rate
                rhr = row.get('resting_heart_rate')
                if not pd.isna(rhr) and float(rhr) > 0:
                    yield ParsedRecord(
                        record_type='health_record',
                        source=self.SOURCE_NAME,
                        metric_type='resting_heart_rate',
                        value=float(rhr),
                        unit='bpm',
                        timestamp=dt.to_pydatetime(),
                        date=target_date,
                        metadata={
                            'source_file': 'UserSleepScores',
                            'sleep_id': str(row.get('sleep_id', '')),
                        },
                        raw_data=None
                    )
        except Exception as e:
            self._log_error(f"Error parsing UserSleepScores CSV {file.name}", e)

    def _parse_food_logs_json(self, file: Path) -> Generator[ParsedRecord, None, None]:
        """
        Parse food_logs-*.json files.
        
        These contain daily nutrition data synced from Cronometer:
        - calories consumed (NOT burned - that's calories-*.json)
        - macros: protein, carbs, fat, fiber, sodium
        
        Each entry is already a daily aggregate from Cronometer.
        """
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Group by date in case there are multiple entries per day
            daily_totals = {}
            
            for entry in data:
                log_date = entry.get('logDate')
                if not log_date:
                    continue
                
                try:
                    d = datetime.strptime(log_date, '%Y-%m-%d').date()
                except ValueError:
                    continue
                
                nutrition = entry.get('nutritionalValues', {})
                
                if d not in daily_totals:
                    daily_totals[d] = {
                        'calories': 0,
                        'protein': 0,
                        'carbs': 0,
                        'fat': 0,
                        'fiber': 0,
                        'sodium': 0,
                        'entries': 0
                    }
                
                # Accumulate values (in case of multiple meals)
                daily_totals[d]['calories'] += nutrition.get('calories', 0) or 0
                daily_totals[d]['protein'] += nutrition.get('protein', 0) or 0
                daily_totals[d]['carbs'] += nutrition.get('carbs', 0) or 0
                daily_totals[d]['fat'] += nutrition.get('fat', 0) or 0
                daily_totals[d]['fiber'] += nutrition.get('fiber', 0) or 0
                daily_totals[d]['sodium'] += nutrition.get('sodium', 0) or 0
                daily_totals[d]['entries'] += 1
            
            # Yield a record for each day
            for d, totals in sorted(daily_totals.items()):
                if totals['calories'] <= 0:
                    continue
                
                yield ParsedRecord(
                    record_type='nutrition',
                    source=self.SOURCE_NAME,
                    metric_type='nutrition_daily',
                    value=float(totals['calories']),
                    unit='kcal',
                    timestamp=datetime.combine(d, datetime.min.time()),
                    date=d,
                    metadata={
                        'calories': float(totals['calories']),
                        'protein_g': round(totals['protein'], 2),
                        'carbs_g': round(totals['carbs'], 2),
                        'fat_g': round(totals['fat'], 2),
                        'fiber_g': round(totals['fiber'], 2),
                        'sodium_mg': round(totals['sodium'], 2),
                        'meal_entries': totals['entries'],
                        'data_source': 'cronometer'
                    },
                    raw_data=None
                )
        except Exception as e:
            self._log_error(f"Error parsing food_logs file {file.name}", e)
