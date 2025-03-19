from dataclasses import asdict
from email.message import Message
import extruct
import json
from fastwarc.warc import ArchiveIterator, WarcRecordType, WarcRecord, is_http
from fastwarc.stream_io import BytesIOStream, FileStream
from hashlib import sha256
from logging import getLogger, Logger
from pantomime import normalize_mimetype
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.encoding import detect_encoding, bytes_to_str
from resiliparse.parse.html import HTMLTree
import tldextract as tld
from tqdm import tqdm
from typing import Dict, Sequence, Tuple, Union
from urllib3.response import HTTPResponse
from urllib.parse import urlparse
from url_normalize import url_normalize

from resilipipe.parse import language
from resilipipe.parse.modules.abstract import BasicModule, HTMLModule, PostProcessingModule


def parse_warc(warc_input: Union[HTTPResponse, str, bytes], modules: Sequence, module_input: Dict,
               logger_name: str = None, warc_path: str = None, schema_metadata: Sequence[Tuple[str, str]] = None) \
        -> Sequence[Dict]:
    '''
    Creates an Arrow Table of metadata for a given WARC file and returns it
    :param warc_input:      HTTPResponse or bytes containing content from a WARC file, or a Path to a WARC file
    :param modules:         List of parsing modules to apply to each record; Tuples of (MODULE_NAME, MODULE_CLASS)
    :param module_input:    Dictionary of content prepared for the modules
    :param logger_name:     Optional name to identify an instance of logging.Logger for exceptions
    :param warc_path:       Optional path to the WARC file if it was passed as a HTTP response from S3
    :param schema_metadata: Optional list of key-value-tuples giving information about the schema of the parsing results
    :return:              Table with metadata
    '''
    # Instantiate a logger if a name was provided
    logger = getLogger(logger_name) if logger_name else None

    # Get a string to identify the file by (warc_input if it is not a HTTPResponse; Else warc_name)
    warc_path = warc_input if isinstance(warc_input, str) else warc_path

    if isinstance(warc_input, HTTPResponse):
        stream = warc_input
    elif isinstance(warc_input, str):
        stream = FileStream(warc_input)
    else:
        stream = BytesIOStream(warc_input)

    num_results = 0
    archive_it = ArchiveIterator(stream, record_types=WarcRecordType.response, func_filter=is_http)
    for record in tqdm(archive_it, desc=f'Parsing records in {warc_path}'):
        num_results += 1

        result = parse_http_record(record, modules, module_input, logger=logger)
        result.update({"valid": True, "warc_file": warc_path, "schema_metadata": schema_metadata})
        yield result

    # Handle empty results (that can occur due to encoding issues)
    if num_results == 0:
        raise Exception(
            'No records found for current file. ' + (f'Please check {warc_path}.' if warc_path is not None else '')
        )


def parse_http_record(record: WarcRecord, modules: Sequence, module_input: Dict, logger: Logger = None,
                      warc_path: str = None) -> Dict:
    '''
    Extract relevant information from a http-record
    :param record:        A http WARC-Record
    :param modules:       List of parsing modules to apply to each record; Tuples of (MODULE_NAME, MODULE_CLASS)
    :param module_input:  Dictionary of content prepared for the modules
    :param logger:        Optional instance of logging.Logger for exceptions
    :param warc_path:     Optional path to the WARC file if it was passed as a HTTP response from S3
    :return:              Dictionary with relevant parsing results
    '''
    # Split the modules according to their type
    basic_modules = [m for m in modules if issubclass(m[1], BasicModule)]
    html_modules = [m for m in modules if issubclass(m[1], HTMLModule)]
    post_proc_modules = [m for m in modules if issubclass(m[1], PostProcessingModule)]

    # Get the basic information
    results = basic_parsing(record=record, basic_modules=basic_modules, module_input=module_input, logger=logger,
                            warc_path=warc_path)

    # Apply further parsing (like plain text extraction and language detection) if mime-type is text
    if results['mime_type'] is not None and results['mime_type'].startswith('text'):
        html_results, tree, html = basic_html_parsing(record)
        results.update(html_results)
        results['language'] = language.process_tree_and_text(tree=tree, plain_text=results['plain_text'])

        # Finally, get additional metadata if the mime-type is html
        if results['mime_type'].startswith('text/html'):
            results = detailed_html_parsing(tree=tree, html=html, results=results,
                                            html_modules=html_modules, module_input=module_input,
                                            logger=logger, warc_path=warc_path)

    # Apply post-processing modules
    results = _apply_record_level_modules(module_list=post_proc_modules,
                                          module_input=module_input,
                                          results=results,
                                          logger=logger,
                                          warc_path=warc_path)
    return results


def _apply_record_level_modules(module_list: Sequence, module_input: Dict, results: Dict, logger: Logger = None,
                                warc_path: str = None):
    for module_tuple in module_list:
        try:
            module_output = module_tuple[1].process_record(record_dict=results,
                                                           **module_input.get(module_tuple[0]))
            results.update(module_output)
        except Exception as e:
            _log_exception(logger=logger, exception=e, msg_prefix=f'Module {module_tuple[0]} failed',
                           record_id=results['record_id'], warc_path=warc_path)
            continue
    return results


def basic_parsing(record: WarcRecord, basic_modules: Sequence, module_input: Dict, logger: Logger = None,
                  warc_path: str = None) -> Dict:
    '''
    Very basic parsing on WARC record level. Applied to all records
    :param record:          Instance of fastwarc.warc.WarcRecord
    :param basic_modules:   List of parsing modules to apply to each record; Tuples of (MODULE_NAME, MODULE_CLASS)
    :param module_input:    Dictionary of content prepared for the modules
    :param logger:          Optional instance of logging.Logger for exceptions
    :param warc_path:       Optional path to the WARC file if it was passed as a HTTP response from S3
    :return:                Dictionary with the following keys:
                            - id,
                            - record_id
                            - warc_date
                            - warc_ip
                            - [all keys from parse_url()]
    '''
    # Parse warc and http headers (warc header parsing includes the results from parse_url)
    warc_headers = record.headers.asdict()
    warc_headers_parsed = parse_warc_headers(warc_headers, logger=logger, warc_path=warc_path)
    http_headers = record.http_headers.asdict()
    http_headers_parsed = parse_http_headers(http_headers)

    # Get the record id and calculate a hash for the url id
    url = warc_headers_parsed.get('url', None)
    url_id = sha256(url.encode('utf-8')).hexdigest() if url else None
    record_id = record.record_id.replace('<urn:uuid:', '').replace('>', '')

    results = {'id': url_id, 'record_id': record_id, **warc_headers_parsed, **http_headers_parsed}

    # Parse the OWS-specific headers
    results.update(parse_ows_headers(warc_headers=warc_headers))

    # Apply the basic modules
    results = _apply_record_level_modules(module_list=basic_modules,
                                          module_input=module_input,
                                          results=results,
                                          logger=logger,
                                          warc_path=warc_path)
    return results


def parse_ows_headers(warc_headers: Dict) -> Dict:
    results = {}
    ows_headers = {
        "OWS.CANONICAL": str,
        "OWS.RESOURCE-TYPE": str,
        "OWS.CURLIELABEL": str,
        "OWS.INDEX": bool,
        "OWS.GENAI": bool,
        "OWS.GENAI-DETAILS": str,
        "OWS.FETCH.RESPONSE-TIME": int,
        "OWS.FETCH.NUM-ERRORS": str
    }
    for header, type_ in ows_headers.items():
        result_header = header.lower().replace(".", "_").replace("-", "_")
        if header in warc_headers:
            results[result_header] = type_(warc_headers[header])
        else:
            results[result_header] = None

    # Capitalize the curlielabel if it is present
    if results["ows_curlielabel"]:
        results["ows_curlielabel"] = results["ows_curlielabel"].capitalize()
    return results


def parse_warc_headers(warc_headers: Dict, logger: Logger = None, warc_path: str = None) -> Dict:
    '''
    Takes in a dictionary corresponding to the WARC headers of a record. Returns relevant parts
    :param warc_header:     Dictionary to be parsed
    :param logger:          Optional instance of logging.Logger for exceptions
    :param warc_path:       Optional path to the WARC file if it was passed as a HTTP response from S3
    :return:                Dictionary of WARC date and IP address, and result from parse_url()
    '''
    url_dict = parse_url(warc_headers['WARC-Target-URI'], logger=logger, warc_path=warc_path)

    return {'warc_date': warc_headers.get('WARC-Date', None),
            'warc_ip': warc_headers.get('WARC-IP-Address', None),
            **url_dict}


def parse_http_headers(http_headers: Dict) -> Dict:
    '''
    Takes in a dictionary corresponding to the HTTP header of a record. Returns relevant parts
    :param http_headers:    Dictionary to be parsed
    :return:                Dictionary of mime_type, charset, content_type_other and http_server
    '''
    http_server = http_headers.get('Server', None)
    parsed_content_type = parse_content_type(http_headers.get('Content-Type', None))
    return {**parsed_content_type, 'http_server': http_server}


def parse_content_type(http_content_type: str = None):
    """
    Parse a content type string into its components (using the email parser) and normalize them.
    Anything beyond MIME-Type and charset is put in a list of tuples (key, value)
    :param http_content_type:   Content-Type string from the HTTP-Header
    :return:                    Dictionary of mime_type, charset, and http_server
    """
    # Set default values
    mime_type = "application/octet-stream"
    charset = None
    content_type_other = None

    # Parse and normalize the content type
    if http_content_type:
        email = Message()
        email["content-type"] = http_content_type
        params = email.get_params()

        mime_type = normalize_mimetype(params[0][0])
        content_type_other = dict(params[1:])

        # Extract the charset if possible and turn the content_type_other dict into a list of tuples (k, v)
        charset = content_type_other.pop("charset", None)
        charset = charset.strip("'").strip('"').lower() if charset else None
        content_type_other = [(k, v) for k, v in content_type_other.items()] if len(content_type_other) > 0 else None

    return {"mime_type": mime_type, "charset": charset, "content_type_other": content_type_other}


def parse_url(url: str, logger: Logger = None, warc_path: str = None) -> Dict[str, str]:
    '''
    Takes in a complete URL to parse it into its components using urllib and tldextract
    :param url:             URL to be parsed
    :param logger:          Optional instance of logging.Logger for exceptions
    :param warc_path:       Optional path to the WARC file if it was passed as a HTTP response from S3
    :return:                Dictionary with the following keys:
                            - url_scheme:       URL scheme specifier
                            - url_path:         Hierarchical path after TLD
                            - url_params:       Parameters for last path element
                            - url_query:        Query component
                            - url_fragment:     Fragment identifier
                            - url_subdomain:    Subdomain of the network location
                            - url_domain:       Domain of the network location
                            - url_suffix:       Suffix according to the Public Suffix List
                            - url_is_private:   If the URL has a private suffix
    '''
    # Normalize the URL
    url = url_normalize(url)

    # First parse the URL into its components, then the netloc into subdomain, domain and suffix
    parse_result = {'scheme': None, 'netloc': None, 'path': None, 'params': None, 'query': None, 'fragment': None}
    netloc_result = {'subdomain': None, 'domain': None, 'suffix': None, 'is_private': False}

    try:
        parse_result.update(urlparse(url)._asdict())
        try:
            netloc_result.update(asdict(tld.extract(parse_result.pop('netloc'))))
            if (netloc_result['subdomain'] == '') & (netloc_result['domain'] == 'www'):
                netloc_result = _correct_faulty_netloc(netloc_result)
        except:
            pass

    except Exception as e:
        _log_exception(logger=logger, exception=e, msg_prefix=f'URL parsing failed for {url}', record_id=None,
                       warc_path=warc_path)

    result = {**parse_result, **netloc_result}
    result = {k: v if v != '' else None for k, v in result.items()}

    # Add URL as a prefix to all dictionary keys for clear identification
    with_prefix = {f'url_{key}': val for key, val in result.items()}

    return {'url': url, **with_prefix}


def _correct_faulty_netloc(netloc_result: Dict) -> Dict:
    false_suffix = netloc_result['suffix'].split('.')
    netloc_result['subdomain'] = netloc_result['domain']
    netloc_result['domain'] = false_suffix[0]
    try:
        netloc_result['suffix'] = false_suffix[1]
    except:
        netloc_result['suffix'] = ''
    return netloc_result


def basic_html_parsing(record: WarcRecord) -> Tuple[Dict, HTMLTree, str]:
    '''
    Very basic parsing for records with MIME type "text" (mostly text/html)
    :param record:      Instance of fastwarc.warc.WarcRecord
    :return:            Dictionary title and plain text, HTMLTree instance, HTML as string
    '''
    # Parse the html
    html = _get_html_from_record(record)
    tree = HTMLTree.parse(html)
    plain_text = extract_plain_text(tree)

    # Look for the title
    try:
        title_nodes = tree.head.get_elements_by_tag_name('title')
        title_nodes = tree.body.get_elements_by_tag_name('title') if len(title_nodes) == 0 else title_nodes
        title = title_nodes[0].text
    except:
        title = None
    return {'title': title, 'plain_text': plain_text}, tree, html


def _get_html_from_record(record: WarcRecord) -> str:
    '''
    Read a http-record and return the html as a string
    :param record:  A http WARC-Record
    :return:        The decoded html bytes as a string
    '''
    record_bytes = record.reader.read()
    return bytes_to_str(record_bytes, detect_encoding(record_bytes))


def detailed_html_parsing(tree: HTMLTree, html: str, results: Dict, html_modules: Sequence, module_input: Dict,
                          logger: Logger = None, warc_path: str = None) \
        -> Dict:
    '''
    Apply further parsing on records that are of type text/html.
    :param tree:            Instance of HTMLTree
    :param html:            The HTML as a string
    :param results:         Dictionary with the data that has been extracted by previous steps
    :param html_modules:    List of parsing modules to apply to each record; Tuples of (MODULE_NAME, MODULE_CLASS)
    :param module_input:    Dictionary of content prepared for the html_modules
    :param logger:          Optional instance of logging.Logger for exceptions
    :param warc_path:       Optional path to the WARC file if it was passed as a HTTP response from S3
    :return:                Updated results dictionary with the following additional keys:
                            - json-ld:      String list of JSON-LD (https://www.w3.org/TR/json-ld/#embedding-json-ld-in-html-documents)
                            - microdata:    String list of HTML Microdata (http://www.w3.org/TR/microdata/#json)
                            - [all data extracted from the html_modules]
    '''
    extruct_result = apply_extruct_extractors(html)
    results.update(extruct_result)
    json_ld = eval(results['json-ld']) if results['json-ld'] is not None else None
    microdata = eval(results['microdata']) if results['microdata'] is not None else None

    for module_tuple in html_modules:
        try:
            module_output = module_tuple[1].process_record(tree=tree,
                                                           plain_text=results['plain_text'],
                                                           language=results['language'],
                                                           json_ld=json_ld,
                                                           microdata=microdata,
                                                           **module_input.get(module_tuple[0]))
            results.update(module_output)
        except Exception as e:
            _log_exception(logger=logger, exception=e, msg_prefix=f'Module {module_tuple[0]} failed',
                           record_id=results['record_id'], warc_path=warc_path)
            continue
    return results


def apply_extruct_extractors(html: str) -> Dict:
    try:
        # Turn the resulting nested dictionary into a string for compatability with arrow
        extruct_result = extruct.extract(html, syntaxes=['json-ld', 'microdata'])
        return {k: (str(v) if len(v) > 0 else None) for k, v in extruct_result.items()}
    except:
        return {'json-ld': None, 'microdata': None}


def _log_exception(logger: Logger, msg_prefix: str, exception: Exception = None, record_id: str = None,
                   warc_path: str = None):
    msg = msg_prefix
    if record_id:
        msg += f' on record {record_id}'
    if warc_path:
        msg += f' in {warc_path}'
    if exception:
        msg += f' with exception: {exception}'
    if logger:
        logger.info(msg)
    else:
        print(msg)
