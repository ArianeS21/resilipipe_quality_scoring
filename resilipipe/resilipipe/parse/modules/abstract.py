from abc import ABC, abstractmethod
from pathlib import Path
from pyarrow import field
from pyspark.sql.types import StructField
from resiliparse.parse.html import HTMLTree
from typing import Dict, Sequence


class Module(ABC):
    @staticmethod
    @abstractmethod
    def prepare(resources_path: Path) -> Dict:
        """Method to prepare the module upon first deployment (e.g. to download necessary files).
        Called in resilipipe.parse.prepare_modules
        :param resources_path:        Path to the directory containing resources
        """
        pass

    @staticmethod
    @abstractmethod
    def load_input(resources_path) -> Dict:
        """Method to load necessary input data or instantiate classes. Return output as dictionary
        :param resources_path:        Path to the directory containing resources"""
        pass

    @staticmethod
    @abstractmethod
    def get_pyarrow_schema() -> Sequence[field]:
        """Return a list of pyarrow fields that defines the output signature of the module as an Arrow schema
           Each field consists of (COLUMN NAME, PYARROW TYPE, NULLABLE [True, False])
        """
        pass

    @staticmethod
    @abstractmethod
    def get_spark_schema() -> Sequence[StructField]:
        """Return a list of StructFields that defines the output signature of the module as a Spark schema
           Each tuple consists of StructField(COLUMN NAME, SPARK TYPE, True)
        """
        pass


class BasicModule(Module):
    @staticmethod
    @abstractmethod
    def process_record(record_dict: Dict, **kwargs) -> Dict:
        '''
        Method to process the given record dictionary with data from the http and warc headers and produce a
         dictionary of results.
        :param record_dict:     Dictionary with the following keys:
                                - id,
                                - record_id
                                - warc_date
                                - warc_ip
                                - url_scheme:       URL scheme specifier
                                - url_path:         Hierarchical path after TLD
                                - url_params:       Parameters for last path element
                                - url_query:        Query component
                                - url_fragment:     Fragment identifier
                                - url_subdomain:    Subdomain of the network location
                                - url_domain:       Domain of the network location
                                - url_suffix:       Suffix according to the Public Suffix List
        :param kwargs:          Dictionary of content prepared by the "load_input" method
        '''
        pass


class HTMLModule(Module):
    @staticmethod
    @abstractmethod
    def process_record(tree: HTMLTree, plain_text: str, language: str, json_ld: Sequence, microdata: Sequence,
                       **kwargs) -> Dict:
        '''
        Method to process the given HTMLTree and plain text and produce a dictionary of results.
        :param tree:        The HTMLTree instance
        :param plain_text:  The plain text as extracted by resiliparse.extract.html2text
        :param language:    The language of the HTML record in ISO-639-3 notation
        :param json_ld:     List of JSON-LD (https://www.w3.org/TR/json-ld/#embedding-json-ld-in-html-documents)
        :param microdata:   List of HTML Microdata (http://www.w3.org/TR/microdata/#json)
        :param kwargs:      Dictionary of content prepared by the "load_input" method
        '''
        pass


class PostProcessingModule(Module):
    @staticmethod
    @abstractmethod
    def process_record(record_dict: Dict, **kwargs) -> Dict:
        '''
        Method to process the given record dictionary with data from all previous processing steps and return a
         dictionary of results.
        :param record_dict:     Dictionary with the results of all previous processing steps.
                                The exact content depends on the processing order but the dictionary should at least
                                contain all keys found in the schema in resilipipe.conf.config

        :param kwargs:          Dictionary of content prepared by the "load_input" method
        '''
        pass
