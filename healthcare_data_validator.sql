-- SI tool for validating healthcare data quality and compliance
-- Useful for ensuring data integrity in healthcare intelligence queries

-- Function to validate common healthcare data formats
CREATE OR REPLACE FUNCTION Validate_healthcare_data(
    data_type STRING,
    value STRING,
    validation_rules STRING DEFAULT '{}'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('snowflake-snowpark-python',)
HANDLER = 'validate_data'
AS
$$
import re
import json
from datetime import datetime, date

def validate_data(data_type, value, validation_rules='{}'):
    try:
        rules = json.loads(validation_rules) if validation_rules else {}
        results = {"valid": True, "errors": [], "warnings": []}
        
        if not value or value.strip() == '':
            results["valid"] = False
            results["errors"].append("Value is empty or null")
            return json.dumps(results)
        
        value = value.strip()
        
        if data_type.upper() == 'MRN':  # Medical Record Number
            # Typically 6-10 digits
            if not re.match(r'^\d{6,10}$', value):
                results["valid"] = False
                results["errors"].append("MRN must be 6-10 digits")
                
        elif data_type.upper() == 'NPI':  # National Provider Identifier
            # Must be 10 digits
            if not re.match(r'^\d{10}$', value):
                results["valid"] = False
                results["errors"].append("NPI must be exactly 10 digits")
            else:
                # Luhn algorithm check for NPI
                digits = [int(d) for d in value]
                checksum = 0
                for i, digit in enumerate(reversed(digits[:-1])):
                    if i % 2 == 0:
                        digit *= 2
                        if digit > 9:
                            digit = digit // 10 + digit % 10
                    checksum += digit
                if (checksum + digits[-1]) % 10 != 0:
                    results["valid"] = False
                    results["errors"].append("NPI fails Luhn algorithm validation")
                    
        elif data_type.upper() == 'ICD10':  # ICD-10 Code
            # Pattern: Letter followed by 2 digits, optional decimal and up to 4 more characters
            if not re.match(r'^[A-Z]\d{2}(\.\w{1,4})?$', value.upper()):
                results["valid"] = False
                results["errors"].append("ICD-10 code format invalid (e.g., A12.345)")
                
        elif data_type.upper() == 'CPT':  # CPT Code
            # 5 digits
            if not re.match(r'^\d{5}$', value):
                results["valid"] = False
                results["errors"].append("CPT code must be exactly 5 digits")
                
        elif data_type.upper() == 'HCPCS':  # HCPCS Code
            # Letter followed by 4 digits
            if not re.match(r'^[A-Z]\d{4}$', value.upper()):
                results["valid"] = False
                results["errors"].append("HCPCS code format invalid (e.g., A1234)")
                
        elif data_type.upper() == 'DATE_OF_BIRTH':
            try:
                # Try to parse date
                if re.match(r'^\d{4}-\d{2}-\d{2}$', value):
                    birth_date = datetime.strptime(value, '%Y-%m-%d').date()
                elif re.match(r'^\d{2}/\d{2}/\d{4}$', value):
                    birth_date = datetime.strptime(value, '%m/%d/%Y').date()
                else:
                    raise ValueError("Invalid date format")
                
                # Check if date is reasonable
                today = date.today()
                age = (today - birth_date).days / 365.25
                
                if birth_date > today:
                    results["valid"] = False
                    results["errors"].append("Date of birth cannot be in the future")
                elif age > 150:
                    results["warnings"].append("Age over 150 years - please verify")
                elif age < 0:
                    results["valid"] = False
                    results["errors"].append("Invalid birth date")
                    
            except ValueError:
                results["valid"] = False
                results["errors"].append("Invalid date format (use YYYY-MM-DD or MM/DD/YYYY)")
                
        elif data_type.upper() == 'PHONE':
            # Remove all non-digits
            digits_only = re.sub(r'\D', '', value)
            if len(digits_only) == 10:
                # Format as (XXX) XXX-XXXX
                results["formatted"] = f"({digits_only[:3]}) {digits_only[3:6]}-{digits_only[6:]}"
            elif len(digits_only) == 11 and digits_only[0] == '1':
                # US number with country code
                results["formatted"] = f"1-({digits_only[1:4]}) {digits_only[4:7]}-{digits_only[7:]}"
            else:
                results["valid"] = False
                results["errors"].append("Phone number must be 10 digits (or 11 with country code)")
                
        elif data_type.upper() == 'EMAIL':
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if not re.match(email_pattern, value):
                results["valid"] = False
                results["errors"].append("Invalid email format")
                
        elif data_type.upper() == 'SSN':
            # Remove hyphens and check format
            ssn_digits = re.sub(r'-', '', value)
            if not re.match(r'^\d{9}$', ssn_digits):
                results["valid"] = False
                results["errors"].append("SSN must be 9 digits (XXX-XX-XXXX)")
            else:
                # Check for invalid SSN patterns
                if ssn_digits in ['000000000', '123456789'] or ssn_digits[0:3] == '000':
                    results["valid"] = False
                    results["errors"].append("Invalid SSN pattern")
                else:
                    results["formatted"] = f"{ssn_digits[:3]}-{ssn_digits[3:5]}-{ssn_digits[5:]}"
                    
        elif data_type.upper() == 'AMOUNT':
            try:
                # Remove currency symbols and commas
                clean_value = re.sub(r'[$,]', '', value)
                amount = float(clean_value)
                
                if amount < 0 and rules.get('allow_negative', True) == False:
                    results["valid"] = False
                    results["errors"].append("Negative amounts not allowed")
                elif amount > rules.get('max_amount', 1000000):
                    results["warnings"].append(f"Amount exceeds typical range: ${amount:,.2f}")
                    
                results["formatted"] = f"${amount:,.2f}"
                
            except ValueError:
                results["valid"] = False
                results["errors"].append("Invalid amount format")
                
        else:
            results["warnings"].append(f"Unknown data type: {data_type}")
        
        return json.dumps(results)
        
    except Exception as e:
        return json.dumps({"valid": False, "errors": [f"Validation error: {str(e)}"]})
$$;

-- Function to validate a batch of healthcare records
CREATE OR REPLACE FUNCTION Validate_healthcare_record(record_json STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('snowflake-snowpark-python',)
HANDLER = 'validate_record'
AS
$$
import json

def validate_record(record_json):
    try:
        record = json.loads(record_json)
        validation_results = {
            "record_valid": True,
            "field_validations": {},
            "summary": {"errors": 0, "warnings": 0}
        }
        
        # Define validation mappings
        field_mappings = {
            'mrn': 'MRN',
            'medical_record_number': 'MRN',
            'npi': 'NPI',
            'provider_id': 'NPI',
            'icd10': 'ICD10',
            'diagnosis_code': 'ICD10',
            'cpt': 'CPT',
            'procedure_code': 'CPT',
            'hcpcs': 'HCPCS',
            'date_of_birth': 'DATE_OF_BIRTH',
            'dob': 'DATE_OF_BIRTH',
            'phone': 'PHONE',
            'phone_number': 'PHONE',
            'email': 'EMAIL',
            'email_address': 'EMAIL',
            'ssn': 'SSN',
            'social_security_number': 'SSN',
            'amount': 'AMOUNT',
            'claim_amount': 'AMOUNT',
            'payment_amount': 'AMOUNT'
        }
        
        # Validate each field
        for field_name, field_value in record.items():
            field_lower = field_name.lower()
            if field_lower in field_mappings:
                data_type = field_mappings[field_lower]
                
                # Call validation function (simplified version)
                result = validate_single_field(data_type, str(field_value))
                validation_results["field_validations"][field_name] = result
                
                if not result.get("valid", True):
                    validation_results["record_valid"] = False
                    validation_results["summary"]["errors"] += len(result.get("errors", []))
                
                validation_results["summary"]["warnings"] += len(result.get("warnings", []))
        
        return json.dumps(validation_results)
        
    except Exception as e:
        return json.dumps({"record_valid": False, "error": f"Record validation error: {str(e)}"})

def validate_single_field(data_type, value):
    # Simplified validation logic for batch processing
    import re
    
    results = {"valid": True, "errors": [], "warnings": []}
    
    if not value or value.strip() == '':
        results["valid"] = False
        results["errors"].append("Empty value")
        return results
    
    value = value.strip()
    
    if data_type == 'MRN':
        if not re.match(r'^\d{6,10}$', value):
            results["valid"] = False
            results["errors"].append("Invalid MRN format")
    elif data_type == 'NPI':
        if not re.match(r'^\d{10}$', value):
            results["valid"] = False
            results["errors"].append("Invalid NPI format")
    elif data_type == 'ICD10':
        if not re.match(r'^[A-Z]\d{2}(\.\w{1,4})?$', value.upper()):
            results["valid"] = False
            results["errors"].append("Invalid ICD-10 format")
    # Add other validations as needed
    
    return results
$$;

-- Test the validation functions
SELECT Validate_healthcare_data('MRN', '1234567') as mrn_validation;
SELECT Validate_healthcare_data('NPI', '1234567890') as npi_validation;
SELECT Validate_healthcare_data('ICD10', 'A12.345') as icd10_validation;
SELECT Validate_healthcare_data('DATE_OF_BIRTH', '1985-03-15') as dob_validation;
SELECT Validate_healthcare_data('AMOUNT', '$1,250.75') as amount_validation;
