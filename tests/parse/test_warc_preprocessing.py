import os
import pytest
from fastwarc.warc import ArchiveIterator, WarcRecord, WarcRecordType
from resilipipe.parse.warc_preprocessing import basic_parsing, parse_ows_headers, parse_warc_headers, \
    parse_url, parse_http_headers, parse_content_type, basic_html_parsing, detailed_html_parsing

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))


@pytest.fixture
def sample_record() -> WarcRecord:
    sample_path = os.path.join(DATA_DIR, "sample.warc.gz")
    archive_it = ArchiveIterator(open(sample_path, 'rb'), record_types=WarcRecordType.response)
    return next(archive_it)


# Basic parsing
def test_basic_parsing(sample_record):
    results = basic_parsing(record=sample_record, basic_modules=[], module_input={})
    assert results["id"] == "41b07364376b982b49a519be67af49abda917d75f001301e9f3026c32ab9e9e5"
    assert results["record_id"] == "00000000-0000-0000-0000-000000000001"
    assert results["url"] == "https://home.example.com/lifestyle;jsessionid=0001?query=hello&sort=newest#print"


def test_parse_ows_headers(sample_record):
    warc_headers = sample_record.headers.asdict()
    results = parse_ows_headers(warc_headers=warc_headers)
    assert results["ows_canonical"] is None
    assert results["ows_resource_type"] == "OWLER"
    assert results["ows_curlielabel"] == "Business"
    assert results["ows_index"] is True
    assert results["ows_genai"] is True
    assert results["ows_genai_details"] is None
    assert results["ows_fetch_response_time"] == 600
    assert results["ows_fetch_num_errors"] is None


def test_parse_warc_headers(sample_record):
    warc_headers = sample_record.headers.asdict()
    results = parse_warc_headers(warc_headers=warc_headers)
    assert results["warc_date"] == "2024-01-01T00:00:02Z"
    assert results["warc_ip"] == "123.12.34.567"


def test_parse_http_headers(sample_record):
    http_headers = sample_record.http_headers.asdict()
    results = parse_http_headers(http_headers=http_headers)
    assert results["http_server"] == "cloudflare"


def test_parse_content_type(sample_record):
    http_headers = sample_record.http_headers.asdict()
    results = parse_content_type(http_headers.get('Content-Type', None))
    assert results["mime_type"] == "text/html"
    assert results["charset"] == "utf-8"
    assert results["content_type_other"] is None


def test_parse_url(sample_record):
    url = basic_parsing(record=sample_record, basic_modules=[], module_input={})["url"]
    results = parse_url(url=url)
    assert results["url_scheme"] == "https"
    assert results["url_path"] == "/lifestyle"
    assert results["url_params"] == "jsessionid=0001"
    assert results["url_query"] == "query=hello&sort=newest"
    assert results["url_fragment"] == "print"
    assert results["url_subdomain"] == "home"
    assert results["url_domain"] == "example"
    assert results["url_suffix"] == "com"


# HTML Parsing
def test_basic_html_parsing(sample_record):
    results, tree, html = basic_html_parsing(record=sample_record)
    assert results["title"] == "Example page"
    assert results["plain_text"] == "Example Corporation Example Corporation logo" \
                                    "\n\nHello world!\n\nHello DOM!\n\nYour browser does not support the video tag."


def test_detailed_html_parsing(sample_record):
    html_results, tree, html = basic_html_parsing(sample_record)
    results = detailed_html_parsing(tree=tree, html=html, results=html_results, html_modules=[], module_input={})
    assert eval(results["microdata"]) == [
        {'type': 'https://schema.org/Organization',
         'properties':
             {'url': 'https://www.example.com',
              'logo': 'https://www.example.com/logo.png',
              'contactPoint':
                  {'type': 'https://schema.org/ContactPoint',
                   'properties':
                       {'contactType': 'Customer Service',
                        'telephone': '+1-800-555-5555',
                        'areaServed': 'US',
                        'availableLanguage': ['English', 'Spanish']
                        }
                   },
              'address':
                  {'type': 'https://schema.org/PostalAddress',
                   'properties':
                       {'streetAddress': '123 Example Street',
                        'addressLocality': 'Example City',
                        'addressRegion': 'EX',
                        'postalCode': '12345',
                        'addressCountry': 'US'
                        }
                   },
              'sameAs': [
                  {'type': 'https://schema.org/URL',
                   'properties':
                       {'url': 'https://www.facebook.com/example'}},
                  {'type': 'https://schema.org/URL',
                   'properties': {'url': 'https://www.twitter.com/example'}},
                  {'type': 'https://schema.org/URL',
                   'properties': {'url': 'https://www.linkedin.com/company/example'}}
              ],
              'founder':
                  {'type': 'https://schema.org/Person',
                   'properties': {'name': 'Jane Doe'}
                   },
              'foundingDate': '2000-01-01',
              'description': 'Example Corporation is a leading provider of exemplary services.'
              }
         }
    ]
    assert eval(results["json-ld"]) == [
        {'@context': 'https://schema.org',
         '@type': 'Organization',
         'name': 'Example Corporation',
         'url': 'https://www.example.com',
         'logo': 'https://www.example.com/logo.png',
         'sameAs': [
             'https://www.facebook.com/example',
             'https://www.twitter.com/example',
             'https://www.linkedin.com/company/example'
         ],
         'contactPoint':
             {'@type': 'ContactPoint',
              'telephone': '+1-800-555-5555',
              'contactType': 'Customer Service',
              'areaServed': 'US',
              'availableLanguage': ['English', 'Spanish']
              },
         'address':
             {'@type': 'PostalAddress',
              'streetAddress': '123 Example Street',
              'addressLocality': 'Example City',
              'addressRegion': 'EX',
              'postalCode': '12345',
              'addressCountry': 'US'
              },
         'founder':
             {'@type': 'Person',
              'name': 'Jane Doe'},
         'foundingDate': '2000-01-01',
         'description': 'Example Corporation is a leading provider of exemplary services.'
         }
    ]


