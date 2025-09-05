-- SI tool for generating PDF reports from healthcare data queries
-- Useful for creating formatted reports to share with stakeholders

CREATE OR REPLACE FUNCTION Generate_pdf_report(
    title STRING,
    content STRING,
    author STRING DEFAULT 'Healthcare Intelligence Agent',
    include_timestamp BOOLEAN DEFAULT TRUE
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('reportlab', 'snowflake-snowpark-python')
HANDLER = 'create_pdf_report'
AS
$$
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
import tempfile
import base64
from datetime import datetime
import json

def create_pdf_report(title, content, author='Healthcare Intelligence Agent', include_timestamp=True):
    try:
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
            doc = SimpleDocTemplate(tmp_file.name, pagesize=letter)
            
            # Define styles
            styles = getSampleStyleSheet()
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=16,
                spaceAfter=30,
                alignment=1  # Center alignment
            )
            
            # Build the PDF content
            story = []
            
            # Add title
            story.append(Paragraph(title, title_style))
            story.append(Spacer(1, 12))
            
            # Add timestamp if requested
            if include_timestamp:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                story.append(Paragraph(f"Generated: {timestamp}", styles['Normal']))
                story.append(Paragraph(f"Author: {author}", styles['Normal']))
                story.append(Spacer(1, 20))
            
            # Try to parse content as JSON (for structured data)
            try:
                data = json.loads(content)
                if isinstance(data, list) and len(data) > 0:
                    # Create table from list of dictionaries
                    if isinstance(data[0], dict):
                        headers = list(data[0].keys())
                        table_data = [headers]
                        for row in data[:50]:  # Limit to 50 rows
                            table_data.append([str(row.get(h, '')) for h in headers])
                        
                        table = Table(table_data)
                        table.setStyle(TableStyle([
                            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('FONTSIZE', (0, 0), (-1, 0), 10),
                            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                            ('GRID', (0, 0), (-1, -1), 1, colors.black)
                        ]))
                        story.append(table)
                    else:
                        # Simple list
                        for item in data:
                            story.append(Paragraph(str(item), styles['Normal']))
                else:
                    # Single object or simple data
                    story.append(Paragraph(str(data), styles['Normal']))
            except:
                # Not JSON, treat as plain text
                paragraphs = content.split('\n')
                for para in paragraphs:
                    if para.strip():
                        story.append(Paragraph(para, styles['Normal']))
                        story.append(Spacer(1, 6))
            
            # Build PDF
            doc.build(story)
            
            # Read the PDF and encode as base64
            with open(tmp_file.name, 'rb') as pdf_file:
                pdf_content = pdf_file.read()
                encoded_pdf = base64.b64encode(pdf_content).decode('utf-8')
            
            return f"PDF generated successfully. Size: {len(pdf_content)} bytes. Base64 encoded content available."
            
    except Exception as e:
        return f"Error generating PDF: {str(e)}"
$$;

-- Helper function to format query results for PDF
CREATE OR REPLACE FUNCTION Format_query_for_pdf(query_result VARIANT)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('snowflake-snowpark-python',)
HANDLER = 'format_result'
AS
$$
import json

def format_result(query_result):
    try:
        # Convert Snowflake VARIANT to JSON string
        if query_result is None:
            return json.dumps({"message": "No data returned"})
        
        # Handle different data types
        if isinstance(query_result, (dict, list)):
            return json.dumps(query_result, indent=2)
        else:
            return json.dumps({"data": str(query_result)})
            
    except Exception as e:
        return json.dumps({"error": f"Error formatting data: {str(e)}"})
$$;

-- Test the PDF generator
SELECT Generate_pdf_report(
    'Healthcare Claims Analysis Report',
    '{"total_claims": 1250, "avg_claim_amount": 2500.75, "top_diagnosis": "Hypertension", "claims_trend": "increasing"}',
    'Healthcare Intelligence Agent',
    TRUE
) as pdf_status;
