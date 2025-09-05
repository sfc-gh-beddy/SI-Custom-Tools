-- SI tool for healthcare-specific date/time calculations
-- Useful for age calculations, date ranges, and temporal analysis

-- Function to calculate age in various formats
CREATE OR REPLACE FUNCTION Calculate_age(
    birth_date STRING,
    reference_date STRING DEFAULT NULL,
    output_format STRING DEFAULT 'years'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('snowflake-snowpark-python',)
HANDLER = 'calculate_age'
AS
$$
from datetime import datetime, date
import json

def calculate_age(birth_date, reference_date=None, output_format='years'):
    try:
        # Parse birth date
        try:
            if '-' in birth_date:
                birth = datetime.strptime(birth_date, '%Y-%m-%d').date()
            elif '/' in birth_date:
                birth = datetime.strptime(birth_date, '%m/%d/%Y').date()
            else:
                return json.dumps({"error": "Invalid birth date format. Use YYYY-MM-DD or MM/DD/YYYY"})
        except ValueError:
            return json.dumps({"error": "Invalid birth date format"})
        
        # Parse reference date (default to today)
        if reference_date:
            try:
                if '-' in reference_date:
                    ref = datetime.strptime(reference_date, '%Y-%m-%d').date()
                elif '/' in reference_date:
                    ref = datetime.strptime(reference_date, '%m/%d/%Y').date()
                else:
                    ref = date.today()
            except ValueError:
                ref = date.today()
        else:
            ref = date.today()
        
        # Calculate age
        age_delta = ref - birth
        
        result = {
            "birth_date": birth.strftime('%Y-%m-%d'),
            "reference_date": ref.strftime('%Y-%m-%d'),
            "age_in_days": age_delta.days
        }
        
        # Calculate different formats
        years = age_delta.days // 365.25
        months = age_delta.days // 30.44
        weeks = age_delta.days // 7
        
        result["age_in_years"] = int(years)
        result["age_in_months"] = int(months)
        result["age_in_weeks"] = int(weeks)
        
        # More precise age calculation
        years_precise = ref.year - birth.year
        if ref.month < birth.month or (ref.month == birth.month and ref.day < birth.day):
            years_precise -= 1
        
        result["age_precise_years"] = years_precise
        
        # Age groups for healthcare analysis
        if years_precise < 1:
            result["age_group"] = "Infant (0-1)"
        elif years_precise < 5:
            result["age_group"] = "Toddler (1-4)"
        elif years_precise < 13:
            result["age_group"] = "Child (5-12)"
        elif years_precise < 18:
            result["age_group"] = "Adolescent (13-17)"
        elif years_precise < 65:
            result["age_group"] = "Adult (18-64)"
        else:
            result["age_group"] = "Senior (65+)"
        
        # Return specific format if requested
        if output_format.lower() == 'years':
            return str(years_precise)
        elif output_format.lower() == 'months':
            return str(int(months))
        elif output_format.lower() == 'days':
            return str(age_delta.days)
        elif output_format.lower() == 'group':
            return result["age_group"]
        else:
            return json.dumps(result)
            
    except Exception as e:
        return json.dumps({"error": f"Age calculation error: {str(e)}"})
$$;

-- Function to create date ranges for healthcare analysis
CREATE OR REPLACE FUNCTION Create_date_range(
    start_date STRING,
    end_date STRING,
    interval_type STRING DEFAULT 'month',
    format_output STRING DEFAULT 'json'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('snowflake-snowpark-python',)
HANDLER = 'create_range'
AS
$$
from datetime import datetime, timedelta
import json
import calendar

def create_range(start_date, end_date, interval_type='month', format_output='json'):
    try:
        # Parse dates
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        if start > end:
            return json.dumps({"error": "Start date must be before end date"})
        
        ranges = []
        current = start
        
        if interval_type.lower() == 'day':
            while current <= end:
                ranges.append({
                    "start_date": current.strftime('%Y-%m-%d'),
                    "end_date": current.strftime('%Y-%m-%d'),
                    "period": current.strftime('%Y-%m-%d')
                })
                current += timedelta(days=1)
                
        elif interval_type.lower() == 'week':
            # Start from Monday of the week containing start_date
            days_since_monday = current.weekday()
            week_start = current - timedelta(days=days_since_monday)
            
            while week_start <= end:
                week_end = min(week_start + timedelta(days=6), end)
                ranges.append({
                    "start_date": week_start.strftime('%Y-%m-%d'),
                    "end_date": week_end.strftime('%Y-%m-%d'),
                    "period": f"Week of {week_start.strftime('%Y-%m-%d')}"
                })
                week_start += timedelta(days=7)
                
        elif interval_type.lower() == 'month':
            while current <= end:
                # Get last day of current month
                last_day = calendar.monthrange(current.year, current.month)[1]
                month_end = current.replace(day=last_day)
                month_end = min(month_end, end)
                
                ranges.append({
                    "start_date": current.replace(day=1).strftime('%Y-%m-%d'),
                    "end_date": month_end.strftime('%Y-%m-%d'),
                    "period": current.strftime('%Y-%m')
                })
                
                # Move to next month
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1, day=1)
                else:
                    current = current.replace(month=current.month + 1, day=1)
                    
        elif interval_type.lower() == 'quarter':
            # Start from beginning of quarter
            quarter = (current.month - 1) // 3 + 1
            quarter_start_month = (quarter - 1) * 3 + 1
            current = current.replace(month=quarter_start_month, day=1)
            
            while current <= end:
                # Calculate quarter end
                quarter_end_month = min(current.month + 2, 12)
                if quarter_end_month == 12:
                    last_day = 31
                else:
                    last_day = calendar.monthrange(current.year, quarter_end_month)[1]
                
                quarter_end = current.replace(month=quarter_end_month, day=last_day)
                quarter_end = min(quarter_end, end)
                
                quarter_num = (current.month - 1) // 3 + 1
                ranges.append({
                    "start_date": current.strftime('%Y-%m-%d'),
                    "end_date": quarter_end.strftime('%Y-%m-%d'),
                    "period": f"{current.year} Q{quarter_num}"
                })
                
                # Move to next quarter
                if quarter_num == 4:
                    current = current.replace(year=current.year + 1, month=1, day=1)
                else:
                    current = current.replace(month=current.month + 3, day=1)
                    
        elif interval_type.lower() == 'year':
            while current <= end:
                year_end = current.replace(month=12, day=31)
                year_end = min(year_end, end)
                
                ranges.append({
                    "start_date": current.replace(month=1, day=1).strftime('%Y-%m-%d'),
                    "end_date": year_end.strftime('%Y-%m-%d'),
                    "period": str(current.year)
                })
                
                current = current.replace(year=current.year + 1, month=1, day=1)
        
        result = {
            "total_periods": len(ranges),
            "interval_type": interval_type,
            "ranges": ranges
        }
        
        if format_output.lower() == 'simple':
            return '\n'.join([f"{r['period']}: {r['start_date']} to {r['end_date']}" for r in ranges])
        else:
            return json.dumps(result)
            
    except Exception as e:
        return json.dumps({"error": f"Date range error: {str(e)}"})
$$;

-- Function to calculate healthcare-specific date metrics
CREATE OR REPLACE FUNCTION Healthcare_date_metrics(
    admit_date STRING,
    discharge_date STRING DEFAULT NULL,
    birth_date STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('snowflake-snowpark-python',)
HANDLER = 'calculate_metrics'
AS
$$
from datetime import datetime, date
import json

def calculate_metrics(admit_date, discharge_date=None, birth_date=None):
    try:
        # Parse admit date
        admit = datetime.strptime(admit_date, '%Y-%m-%d').date()
        result = {
            "admit_date": admit.strftime('%Y-%m-%d'),
            "metrics": {}
        }
        
        # Calculate length of stay
        if discharge_date:
            discharge = datetime.strptime(discharge_date, '%Y-%m-%d').date()
            result["discharge_date"] = discharge.strftime('%Y-%m-%d')
            
            los_days = (discharge - admit).days
            result["metrics"]["length_of_stay_days"] = los_days
            
            # LOS categories for healthcare analysis
            if los_days == 0:
                result["metrics"]["los_category"] = "Same Day"
            elif los_days <= 2:
                result["metrics"]["los_category"] = "Short Stay (1-2 days)"
            elif los_days <= 7:
                result["metrics"]["los_category"] = "Medium Stay (3-7 days)"
            elif los_days <= 30:
                result["metrics"]["los_category"] = "Long Stay (8-30 days)"
            else:
                result["metrics"]["los_category"] = "Extended Stay (30+ days)"
        
        # Calculate age at admission
        if birth_date:
            birth = datetime.strptime(birth_date, '%Y-%m-%d').date()
            result["birth_date"] = birth.strftime('%Y-%m-%d')
            
            age_at_admit = admit.year - birth.year
            if admit.month < birth.month or (admit.month == birth.month and admit.day < birth.day):
                age_at_admit -= 1
            
            result["metrics"]["age_at_admission"] = age_at_admit
            
            # Age-based risk categories
            if age_at_admit < 18:
                result["metrics"]["age_risk_category"] = "Pediatric"
            elif age_at_admit >= 65:
                result["metrics"]["age_risk_category"] = "Geriatric"
            else:
                result["metrics"]["age_risk_category"] = "Adult"
        
        # Day of week analysis
        result["metrics"]["admit_day_of_week"] = admit.strftime('%A')
        result["metrics"]["admit_month"] = admit.strftime('%B')
        result["metrics"]["admit_quarter"] = f"Q{(admit.month - 1) // 3 + 1}"
        
        # Weekend vs weekday
        if admit.weekday() >= 5:  # Saturday = 5, Sunday = 6
            result["metrics"]["admit_timing"] = "Weekend"
        else:
            result["metrics"]["admit_timing"] = "Weekday"
        
        return json.dumps(result)
        
    except Exception as e:
        return json.dumps({"error": f"Healthcare date metrics error: {str(e)}"})
$$;

-- Test the datetime utility functions
SELECT Calculate_age('1980-05-15', '2024-01-01', 'years') as age_years;
SELECT Calculate_age('1980-05-15', '2024-01-01', 'full') as age_full;
SELECT Create_date_range('2023-01-01', '2023-12-31', 'quarter', 'json') as quarterly_ranges;
SELECT Healthcare_date_metrics('2024-01-15', '2024-01-20', '1965-03-10') as stay_metrics;
