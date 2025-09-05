-- Enhanced SI web tool for secure web scraping with error handling
-- More specific network rules for better security
CREATE OR REPLACE NETWORK RULE Snowflake_intelligence_WebAccessRule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'www.snowflake.com:443',
    'docs.snowflake.com:443', 
    'community.snowflake.com:443',
    'www.cdc.gov:443',
    'www.cms.gov:443',
    'www.hl7.org:443',
    -- Add specific healthcare/industry domains you need
    'httpbin.org:80',
    'httpbin.org:443'
  );

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION Snowflake_intelligence_ExternalAccess_Integration
  ALLOWED_NETWORK_RULES = (Snowflake_intelligence_WebAccessRule)
  ENABLED = true;

-- Enhanced web scraping function with error handling and options
CREATE OR REPLACE FUNCTION Web_scrape(weburl STRING, extraction_type STRING DEFAULT 'text')
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'get_page'
EXTERNAL_ACCESS_INTEGRATIONS = (Snowflake_intelligence_ExternalAccess_Integration)
PACKAGES = ('requests', 'beautifulsoup4')
AS
$$
import requests
from bs4 import BeautifulSoup
import json
import time

def get_page(weburl, extraction_type='text'):
    try:
        # Add timeout and headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; Snowflake Web Scraper)'
        }
        
        response = requests.get(weburl, timeout=30, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Different extraction types
        if extraction_type == 'text':
            return soup.get_text(strip=True, separator=' ')
        elif extraction_type == 'title':
            title = soup.find('title')
            return title.get_text() if title else 'No title found'
        elif extraction_type == 'links':
            links = [a.get('href') for a in soup.find_all('a', href=True)]
            return json.dumps(links[:50])  # Limit to first 50 links
        elif extraction_type == 'headers':
            headers = []
            for i in range(1, 7):
                headers.extend([h.get_text(strip=True) for h in soup.find_all(f'h{i}')])
            return json.dumps(headers[:20])  # Limit to first 20 headers
        elif extraction_type == 'meta':
            meta_data = {}
            for meta in soup.find_all('meta'):
                name = meta.get('name') or meta.get('property')
                content = meta.get('content')
                if name and content:
                    meta_data[name] = content
            return json.dumps(meta_data)
        else:
            return soup.get_text(strip=True, separator=' ')
            
    except requests.exceptions.Timeout:
        return 'Error: Request timed out after 30 seconds'
    except requests.exceptions.RequestException as e:
        return f'Error: HTTP request failed - {str(e)}'
    except Exception as e:
        return f'Error: {str(e)}'
$$;

-- Function to check if a URL is accessible
CREATE OR REPLACE FUNCTION Check_url_status(weburl STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'check_status'
EXTERNAL_ACCESS_INTEGRATIONS = (Snowflake_intelligence_ExternalAccess_Integration)
PACKAGES = ('requests',)
AS
$$
import requests

def check_status(weburl):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; Snowflake URL Checker)'
        }
        response = requests.head(weburl, timeout=10, headers=headers, allow_redirects=True)
        return f'Status: {response.status_code}, URL accessible: {weburl}'
    except requests.exceptions.Timeout:
        return f'Status: Timeout, URL: {weburl}'
    except requests.exceptions.RequestException as e:
        return f'Status: Error - {str(e)}, URL: {weburl}'
$$;

--test different extraction types
SELECT Web_scrape('https://www.snowflake.com/en/blog/ISO-IEC-42001-AI-certification/', 'text') as full_text;
SELECT Web_scrape('https://www.snowflake.com/en/blog/ISO-IEC-42001-AI-certification/', 'title') as page_title;
SELECT Web_scrape('https://www.snowflake.com/en/blog/ISO-IEC-42001-AI-certification/', 'headers') as page_headers;
SELECT Check_url_status('https://www.snowflake.com/en/blog/ISO-IEC-42001-AI-certification/') as url_status;
