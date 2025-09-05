-- Enhanced SI email tool with error handling and validation
-- notification integration required by the SP
use role accountadmin;
grant CREATE INTEGRATION on ACCOUNT to role snowflake_intelligence_admin_rl;
use role snowflake_intelligence_admin_rl;

CREATE OR REPLACE NOTIFICATION INTEGRATION ai_email_int
  TYPE=EMAIL
  ENABLED=TRUE;

-- Enhanced stored proc that sends email with validation and error handling
CREATE OR REPLACE PROCEDURE send_mail(recipient TEXT, subject TEXT, text TEXT, content_type TEXT DEFAULT 'text/html')
RETURNS TEXT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'send_mail'
AS
$$
import re

def send_mail(session, recipient, subject, text, content_type='text/html'):
    try:
        # Basic email validation
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, recipient):
            return f'Error: Invalid email format for {recipient}'
        
        # Validate content type
        if content_type not in ['text/html', 'text/plain']:
            content_type = 'text/html'
        
        # Send email
        session.call(
            'SYSTEM$SEND_EMAIL',
            'ai_email_int',
            recipient,
            subject,
            text,
            content_type
        )
        return f'Email successfully sent to {recipient} with subject: "{subject}"'
        
    except Exception as e:
        return f'Error sending email: {str(e)}'
$$;

-- Enhanced procedure for sending to multiple recipients
CREATE OR REPLACE PROCEDURE send_mail_multiple(recipients TEXT, subject TEXT, text TEXT, content_type TEXT DEFAULT 'text/html')
RETURNS TEXT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'send_mail_multiple'
AS
$$
import re

def send_mail_multiple(session, recipients, subject, text, content_type='text/html'):
    try:
        # Split recipients by comma or semicolon
        recipient_list = [email.strip() for email in re.split('[,;]', recipients)]
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        results = []
        
        for recipient in recipient_list:
            if not re.match(email_pattern, recipient):
                results.append(f'Error: Invalid email format for {recipient}')
                continue
                
            try:
                session.call(
                    'SYSTEM$SEND_EMAIL',
                    'ai_email_int',
                    recipient,
                    subject,
                    text,
                    content_type
                )
                results.append(f'Success: Email sent to {recipient}')
            except Exception as e:
                results.append(f'Error sending to {recipient}: {str(e)}')
        
        return '; '.join(results)
        
    except Exception as e:
        return f'Error processing multiple emails: {str(e)}'
$$;

--test
CALL send_mail('olivier.sinquin@snowflake.com', 'Test Email', 'This is a test email sent from Snowflake.');
CALL send_mail_multiple('olivier.sinquin@snowflake.com,user2@example.com', 'Bulk Test', 'This is a bulk test email.');
