-- SI tool for formatting data for reports and emails
-- Useful for creating readable output from complex healthcare data queries

-- Function to format query results for email or reports
CREATE OR REPLACE FUNCTION Format_data_for_email(
    data_json STRING,
    format_type STRING DEFAULT 'table',
    title STRING DEFAULT 'Data Report',
    include_summary BOOLEAN DEFAULT TRUE
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('snowflake-snowpark-python',)
HANDLER = 'format_for_email'
AS
$$
import json
from datetime import datetime

def format_for_email(data_json, format_type='table', title='Data Report', include_summary=True):
    try:
        data = json.loads(data_json)
        
        # HTML email template start
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
                h2 {{ color: #34495e; margin-top: 25px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
                th {{ background-color: #3498db; color: white; padding: 12px; text-align: left; }}
                td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
                tr:nth-child(even) {{ background-color: #f8f9fa; }}
                .summary {{ background-color: #e8f4fd; padding: 15px; border-radius: 5px; margin: 15px 0; }}
                .metric {{ display: inline-block; margin: 10px 15px; }}
                .metric-value {{ font-size: 1.2em; font-weight: bold; color: #2c3e50; }}
                .timestamp {{ color: #7f8c8d; font-size: 0.9em; }}
            </style>
        </head>
        <body>
        <h1>{title}</h1>
        <p class="timestamp">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        """
        
        if isinstance(data, list) and len(data) > 0:
            # Handle list of records
            if include_summary:
                html += f"""
                <div class="summary">
                    <h2>Summary</h2>
                    <div class="metric">
                        <div>Total Records</div>
                        <div class="metric-value">{len(data)}</div>
                    </div>
                """
                
                # Add summary statistics if numeric data is present
                if isinstance(data[0], dict):
                    numeric_fields = []
                    for key, value in data[0].items():
                        try:
                            float(str(value))
                            numeric_fields.append(key)
                        except:
                            pass
                    
                    for field in numeric_fields[:3]:  # Show up to 3 numeric summaries
                        try:
                            values = [float(str(record.get(field, 0))) for record in data if record.get(field) is not None]
                            if values:
                                avg_val = sum(values) / len(values)
                                max_val = max(values)
                                min_val = min(values)
                                html += f"""
                                <div class="metric">
                                    <div>{field.replace('_', ' ').title()}</div>
                                    <div class="metric-value">Avg: {avg_val:.2f}</div>
                                    <div>Min: {min_val:.2f} | Max: {max_val:.2f}</div>
                                </div>
                                """
                        except:
                            pass
                
                html += "</div>"
            
            if format_type.lower() == 'table':
                # Table format
                if isinstance(data[0], dict):
                    headers = list(data[0].keys())
                    html += "<h2>Detailed Data</h2><table><tr>"
                    
                    for header in headers:
                        html += f"<th>{header.replace('_', ' ').title()}</th>"
                    html += "</tr>"
                    
                    for record in data[:100]:  # Limit to 100 rows for email
                        html += "<tr>"
                        for header in headers:
                            value = record.get(header, '')
                            # Format different types of values
                            if isinstance(value, float):
                                if header.lower() in ['amount', 'cost', 'price', 'payment']:
                                    formatted_value = f"${value:,.2f}"
                                elif header.lower() in ['rate', 'percentage', 'percent']:
                                    formatted_value = f"{value:.1f}%"
                                else:
                                    formatted_value = f"{value:.2f}"
                            else:
                                formatted_value = str(value)
                            html += f"<td>{formatted_value}</td>"
                        html += "</tr>"
                    
                    html += "</table>"
                    
                    if len(data) > 100:
                        html += f"<p><em>Showing first 100 of {len(data)} records</em></p>"
                
            elif format_type.lower() == 'list':
                # List format
                html += "<h2>Records</h2>"
                for i, record in enumerate(data[:50], 1):  # Limit to 50 for readability
                    html += f"<h3>Record {i}</h3><ul>"
                    if isinstance(record, dict):
                        for key, value in record.items():
                            html += f"<li><strong>{key.replace('_', ' ').title()}:</strong> {value}</li>"
                    else:
                        html += f"<li>{record}</li>"
                    html += "</ul>"
                
                if len(data) > 50:
                    html += f"<p><em>Showing first 50 of {len(data)} records</em></p>"
        
        elif isinstance(data, dict):
            # Handle single record or summary data
            html += "<h2>Data</h2><table>"
            for key, value in data.items():
                formatted_key = key.replace('_', ' ').title()
                if isinstance(value, (list, dict)):
                    formatted_value = json.dumps(value, indent=2)
                elif isinstance(value, float):
                    if key.lower() in ['amount', 'cost', 'price', 'payment']:
                        formatted_value = f"${value:,.2f}"
                    elif key.lower() in ['rate', 'percentage', 'percent']:
                        formatted_value = f"{value:.1f}%"
                    else:
                        formatted_value = f"{value:.2f}"
                else:
                    formatted_value = str(value)
                
                html += f"<tr><th>{formatted_key}</th><td>{formatted_value}</td></tr>"
            html += "</table>"
        
        else:
            # Handle simple data
            html += f"<div class='summary'><h2>Result</h2><p>{str(data)}</p></div>"
        
        html += """
        </body>
        </html>
        """
        
        return html
        
    except Exception as e:
        return f"<html><body><h1>Error</h1><p>Error formatting data: {str(e)}</p></body></html>"
$$;

-- Function to create executive summary from healthcare data
CREATE OR REPLACE FUNCTION Create_executive_summary(
    data_json STRING,
    analysis_type STRING DEFAULT 'general',
    key_metrics STRING DEFAULT '[]'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('snowflake-snowpark-python',)
HANDLER = 'create_summary'
AS
$$
import json
from datetime import datetime

def create_summary(data_json, analysis_type='general', key_metrics='[]'):
    try:
        data = json.loads(data_json)
        metrics = json.loads(key_metrics) if key_metrics else []
        
        summary = {
            "title": f"Healthcare Intelligence Summary - {analysis_type.title()}",
            "generated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "key_findings": [],
            "recommendations": [],
            "metrics": {}
        }
        
        if isinstance(data, list) and len(data) > 0:
            summary["metrics"]["total_records"] = len(data)
            
            # Analyze based on type
            if analysis_type.lower() == 'claims':
                # Claims analysis
                if isinstance(data[0], dict):
                    # Calculate claims metrics
                    amount_fields = ['amount', 'claim_amount', 'payment_amount', 'total_amount']
                    for field in amount_fields:
                        if field in data[0]:
                            amounts = [float(record.get(field, 0)) for record in data if record.get(field)]
                            if amounts:
                                summary["metrics"][f"total_{field}"] = sum(amounts)
                                summary["metrics"][f"average_{field}"] = sum(amounts) / len(amounts)
                                summary["metrics"][f"max_{field}"] = max(amounts)
                            break
                    
                    # Common findings for claims
                    summary["key_findings"].append(f"Analyzed {len(data)} claims records")
                    if "total_claim_amount" in summary["metrics"]:
                        total = summary["metrics"]["total_claim_amount"]
                        avg = summary["metrics"]["average_claim_amount"]
                        summary["key_findings"].append(f"Total claim value: ${total:,.2f}")
                        summary["key_findings"].append(f"Average claim amount: ${avg:,.2f}")
                    
                    summary["recommendations"].append("Monitor high-value claims for potential fraud")
                    summary["recommendations"].append("Review claims processing efficiency")
                
            elif analysis_type.lower() == 'patients':
                # Patient analysis
                summary["key_findings"].append(f"Patient cohort includes {len(data)} individuals")
                
                # Age analysis if available
                age_fields = ['age', 'age_years', 'patient_age']
                for field in age_fields:
                    if field in str(data[0]):
                        ages = []
                        for record in data:
                            if isinstance(record, dict) and field in record:
                                try:
                                    ages.append(int(record[field]))
                                except:
                                    pass
                        
                        if ages:
                            avg_age = sum(ages) / len(ages)
                            summary["metrics"]["average_age"] = avg_age
                            summary["key_findings"].append(f"Average patient age: {avg_age:.1f} years")
                            
                            # Age distribution
                            pediatric = len([a for a in ages if a < 18])
                            geriatric = len([a for a in ages if a >= 65])
                            summary["metrics"]["pediatric_patients"] = pediatric
                            summary["metrics"]["geriatric_patients"] = geriatric
                            
                            if geriatric > len(ages) * 0.3:
                                summary["recommendations"].append("Consider geriatric care protocols")
                            if pediatric > 0:
                                summary["recommendations"].append("Ensure pediatric specialists available")
                        break
                
            elif analysis_type.lower() == 'utilization':
                # Utilization analysis
                summary["key_findings"].append(f"Utilization data covers {len(data)} episodes")
                
                # Look for common utilization fields
                utilization_fields = ['length_of_stay', 'los', 'days', 'visits']
                for field in utilization_fields:
                    if field in str(data[0]):
                        values = []
                        for record in data:
                            if isinstance(record, dict) and field in record:
                                try:
                                    values.append(float(record[field]))
                                except:
                                    pass
                        
                        if values:
                            avg_val = sum(values) / len(values)
                            summary["metrics"][f"average_{field}"] = avg_val
                            summary["key_findings"].append(f"Average {field.replace('_', ' ')}: {avg_val:.1f}")
                        break
                
                summary["recommendations"].append("Monitor for outliers in utilization patterns")
                summary["recommendations"].append("Identify opportunities for care coordination")
        
        # Add custom metrics
        for metric in metrics:
            if metric in summary["metrics"]:
                summary["key_findings"].append(f"{metric.replace('_', ' ').title()}: {summary['metrics'][metric]}")
        
        # Format as readable text
        output = f"""
EXECUTIVE SUMMARY: {summary['title']}
Generated: {summary['generated']}

KEY FINDINGS:
{chr(10).join(['• ' + finding for finding in summary['key_findings']])}

RECOMMENDATIONS:
{chr(10).join(['• ' + rec for rec in summary['recommendations']])}

METRICS:
{chr(10).join([f"• {k.replace('_', ' ').title()}: {v}" for k, v in summary['metrics'].items()])}
        """
        
        return output.strip()
        
    except Exception as e:
        return f"Error creating executive summary: {str(e)}"
$$;

-- Test the formatting functions
SELECT Format_data_for_email(
    '[{"patient_id": 1, "age": 45, "diagnosis": "Hypertension", "claim_amount": 1250.75}, {"patient_id": 2, "age": 67, "diagnosis": "Diabetes", "claim_amount": 2100.50}]',
    'table',
    'Patient Claims Report',
    true
) as formatted_email;

SELECT Create_executive_summary(
    '[{"claim_amount": 1250.75, "age": 45}, {"claim_amount": 2100.50, "age": 67}]',
    'claims',
    '["total_claims", "average_age"]'
) as executive_summary;
