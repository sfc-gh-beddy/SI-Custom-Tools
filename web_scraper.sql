-- SI web tool for web scrapping
-- Stored proc that scrape from a website using beautiful soup
  -- NETWORK rule is part of db schema
CREATE OR REPLACE NETWORK RULE Snowflake_intelligence_WebAccessRule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('0.0.0.0:80', '0.0.0.0:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION Snowflake_intelligence_ExternalAccess_Integration
  ALLOWED_NETWORK_RULES = (Snowflake_intelligence_WebAccessRule)
  ENABLED = true;

CREATE OR REPLACE FUNCTION Web_scrape(weburl STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'get_page'
EXTERNAL_ACCESS_INTEGRATIONS = (Snowflake_intelligence_ExternalAccess_Integration)
PACKAGES = ('requests', 'beautifulsoup4')
AS
$$
import _snowflake
import requests
from bs4 import BeautifulSoup

def get_page(weburl):
  url = f"{weburl}"
  response = requests.get(url)
  soup = BeautifulSoup(response.text)
  return soup.get_text()
$$;

--test
select Web_scrape('https://www.snowflake.com/en/blog/ISO-IEC-42001-AI-certification/');
