-- SI web tool for email sending

-- notification integration required by the SP
use role accountadmin;
grant CREATE INTEGRATION on ACCOUNT to role snowflake_intelligence_admin_rl;
use role snowflake_intelligence_admin_rl;

CREATE OR REPLACE NOTIFICATION INTEGRATION ai_email_int
  TYPE=EMAIL
  ENABLED=TRUE;

-- Stored proc that sends email to a specified recipient
CREATE OR REPLACE PROCEDURE send_mail(recipient TEXT, subject TEXT, text TEXT)
RETURNS TEXT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'send_mail'
AS
$$
def send_mail(session, recipient, subject, text):
    session.call(
        'SYSTEM$SEND_EMAIL',
        'ai_email_int',
        recipient,
        subject,
        text,
        'text/html'
    )
    return f'Email was sent to {recipient} with subject: "{subject}".'
$$;

--test
CALL send_mail('bracken.eddy@snowflake.com', 'Test Email', 'This is a test email sent from Snowflake.');
