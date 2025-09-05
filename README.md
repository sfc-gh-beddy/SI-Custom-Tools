# Snowflake Intelligence Custom Tools for Healthcare Data

This repository contains custom tools designed to enhance your Snowflake Intelligence agent for healthcare data analysis and communication.

## Tool Overview

### 1. **Email Sender Tools**
- `email_sender.sql` - Basic email functionality
- `enhanced_email_sender.sql` - Enhanced version with validation and multi-recipient support

**Features:**
- Email validation
- Support for HTML and plain text formats
- Multi-recipient capability
- Error handling and reporting

**Usage Example:**
```sql
CALL send_mail('doctor@hospital.com', 'Patient Analysis Report', '<h1>Report Content</h1>');
CALL send_mail_multiple('doctor1@hospital.com,doctor2@hospital.com', 'Bulk Report', 'Content');
```

### 2. **Web Scraping Tools**
- `web_scraper.sql` - Basic web scraping
- `enhanced_web_scraper.sql` - Enhanced version with security and extraction options

**Features:**
- Secure network rules for healthcare domains
- Multiple extraction types (text, title, links, headers, meta)
- URL status checking
- Error handling and timeouts

**Usage Example:**
```sql
SELECT Web_scrape('https://www.cdc.gov/health-guidelines', 'text');
SELECT Check_url_status('https://www.cms.gov/regulations');
```

### 3. **PDF Report Generator**
- `pdf_report_generator.sql` - Create formatted PDF reports

**Features:**
- Professional PDF formatting
- Table generation from JSON data
- Automatic styling and layout
- Support for large datasets

**Usage Example:**
```sql
SELECT Generate_pdf_report(
    'Monthly Claims Analysis',
    '{"total_claims": 1250, "avg_amount": 2500.75}',
    'Healthcare Analytics Team'
);
```

### 4. **Healthcare Data Validator**
- `healthcare_data_validator.sql` - Validate healthcare-specific data formats

**Features:**
- Validates MRN, NPI, ICD-10, CPT, HCPCS codes
- Date of birth, phone, email, SSN validation
- Batch record validation
- Compliance checking

**Usage Example:**
```sql
SELECT Validate_healthcare_data('NPI', '1234567890');
SELECT Validate_healthcare_data('ICD10', 'A12.345');
SELECT Validate_healthcare_record('{"mrn": "1234567", "npi": "1234567890"}');
```

### 5. **Healthcare DateTime Utilities**
- `healthcare_datetime_utils.sql` - Specialized date/time calculations

**Features:**
- Accurate age calculations
- Healthcare-specific age groupings
- Date range generation for analysis periods
- Length of stay calculations
- Admission timing analysis

**Usage Example:**
```sql
SELECT Calculate_age('1980-05-15', '2024-01-01', 'years');
SELECT Create_date_range('2023-01-01', '2023-12-31', 'quarter');
SELECT Healthcare_date_metrics('2024-01-15', '2024-01-20', '1965-03-10');
```

### 6. **Data Formatter**
- `data_formatter.sql` - Format data for reports and emails

**Features:**
- HTML email formatting
- Executive summary generation
- Table and list formatting options
- Healthcare-specific metrics
- Summary statistics

**Usage Example:**
```sql
SELECT Format_data_for_email(
    '[{"patient_id": 1, "claim_amount": 1250.75}]',
    'table',
    'Claims Report'
);
SELECT Create_executive_summary('[data...]', 'claims');
```

## Implementation Strategy

### Prerequisites
1. **Role Permissions**: Ensure you have `snowflake_intelligence_admin_rl` role
2. **Account Admin**: Some integrations require `accountadmin` privileges
3. **Network Access**: Configure appropriate network rules for external access

### Deployment Steps
1. Run the permission setup from `enhanced_email_sender.sql`
2. Deploy each tool in order of dependencies
3. Test each tool with the provided examples
4. Configure network rules for your organization's domains

### Security Considerations
- **Network Rules**: The enhanced web scraper includes specific domain allowlists
- **Data Validation**: All healthcare data is validated for compliance
- **Error Handling**: Comprehensive error handling prevents information leakage
- **Access Control**: All tools respect Snowflake's role-based access control

## Integration Patterns

### Complete Healthcare Analysis Workflow
```sql
-- 1. Validate incoming data
SELECT Validate_healthcare_record('[patient_data]');

-- 2. Perform analysis with date calculations
SELECT Calculate_age(birth_date) as patient_age,
       Healthcare_date_metrics(admit_date, discharge_date) as stay_info
FROM patient_data;

-- 3. Format results for stakeholders
SELECT Format_data_for_email(
    query_results,
    'table',
    'Weekly Healthcare Intelligence Report'
) as email_content;

-- 4. Send formatted report
CALL send_mail_multiple(
    'medical-team@hospital.com',
    'Weekly Healthcare Intelligence Report',
    email_content
);

-- 5. Generate executive summary
SELECT Create_executive_summary(query_results, 'patients');
```

### Claims Analysis Pipeline
```sql
-- Validate claim data
SELECT Validate_healthcare_data('ICD10', diagnosis_code),
       Validate_healthcare_data('CPT', procedure_code)
FROM claims_data;

-- Analyze claims with date ranges
SELECT Create_date_range('2024-01-01', '2024-12-31', 'month') as analysis_periods;

-- Generate comprehensive report
SELECT Generate_pdf_report(
    'Annual Claims Analysis',
    claims_summary_json,
    'Claims Analytics Department'
);
```

## Best Practices

### Email Communications
- Use HTML formatting for better readability
- Include executive summaries for leadership
- Validate all email addresses before sending
- Limit data in emails for security

### Data Validation
- Always validate healthcare data before analysis
- Use batch validation for large datasets
- Monitor validation error rates
- Document data quality issues

### Report Generation
- Include timestamps and authorship
- Limit table sizes for readability
- Use appropriate formatting for audience
- Include summary statistics

### Security & Compliance
- Validate all healthcare identifiers
- Use secure network rules
- Monitor access patterns
- Audit tool usage regularly

## Troubleshooting

### Common Issues
1. **Permission Errors**: Ensure proper role assignments
2. **Network Access**: Check external access integrations
3. **Data Format Errors**: Validate input data structure
4. **Email Delivery**: Verify notification integration setup

### Monitoring
- Monitor tool execution times
- Track validation error rates
- Review email delivery success
- Audit data access patterns

## Future Enhancements

Consider adding:
- **FHIR Integration**: Direct HL7 FHIR API connectivity
- **Real-time Dashboards**: Live data visualization tools
- **ML Model Integration**: Predictive analytics capabilities
- **Audit Logging**: Enhanced compliance tracking
- **Mobile Notifications**: SMS/push notification support

---

**Note**: These tools are designed specifically for healthcare intelligence applications. Always ensure compliance with HIPAA, PHI handling requirements, and your organization's data governance policies.
