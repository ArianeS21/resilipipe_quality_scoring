from abc import ABC, abstractmethod
from logging import Logger
import os
from minio import Minio
from pathlib import Path
import pyarrow as pa
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
import re
from typing import Callable, Dict, Sequence, Union
import yaml
from pyspark import SparkFiles

from resilipipe import RESILIPIPE_DIR
from resilipipe.conf.config import Configurator
from resilipipe.parse.modules.abstract import PostProcessingModule


# Module class
class Module(PostProcessingModule):
    @staticmethod
    def prepare(resources_path: Path):
        pass

    @staticmethod
    def load_input(resources_path: Path) -> Dict:
        # 1. Load the spark schema
        try:
            configurator = Configurator(modules_yaml=RESILIPIPE_DIR / 'conf/modules.yaml')
        except:
            configurator = Configurator(modules_yaml=SparkFiles.get('modules.yaml'))
        schema = StructType(configurator.get_spark_schema())

        # 2. Create a minio client
        client = Minio(endpoint=os.getenv("MINIO_ENDPOINT").replace("https://", ""),
                       access_key=os.getenv("MINIO_ACCESS_KEY"), secret_key=os.getenv("MINIO_SECRET_KEY"))
        bucket_name = os.getenv("MINIO_BUCKET_NAME")

        # 3. Get the index_filters for the core indices. These are dictionaries of [NAME]: [DICT OF FILTERS]
        # Get the name of all yaml-Files that define core indices and extract the contained filters
        ci_yaml_files = [x.object_name for x in
                         client.list_objects(bucket_name, "shared/config/core-index/system/system-ci")]

        # 4. Get the filter definitions
        filter_definitions = {}
        for file in ci_yaml_files:
            indices_dict = yaml.safe_load(client.get_object(bucket_name, file).read())
            for name, filters in _parse_indices_dict(indices_dict).items():
                if name in filter_definitions:
                    print(f"A collection index with the name {name} was already defined. It won't be added again.")
                    continue
                filter_definitions[name] = filters

        # 5. Return the filters and schema as module_input
        module_input = {"ci_filters": filter_definitions, "schema": schema}
        return module_input

    @staticmethod
    def get_pyarrow_schema() -> Sequence[pa.field]:
        schema = [pa.field("collection_indices", pa.list_(pa.string()), True)]
        return schema

    @staticmethod
    def get_spark_schema() -> Sequence[StructField]:
        schema = [StructField("collection_indices", ArrayType(StringType()), True)]
        return schema

    @staticmethod
    def process_record(record_dict: Dict, **kwargs) -> Dict[str, Union[Sequence[str], None]]:
        '''
        Take a dictionary of information about a WARC record and return a list of collection indices it belongs to.

        :param record_dict:     Dictionary with record information
        :param kwargs:          Dictionary of content prepared by the "load_input" method;
                                Contains a dictionary of functions called "ci_filters" with filter conditions and
                                "schema" with the pyspark schema
        :return:                List of collections indices or None if it does not belong to a specified index
        '''
        ci_filters = kwargs["ci_filters"]
        schema = kwargs["schema"]

        collection_indices = []
        for name, filters in ci_filters.items():
            function = DictParser.parse(filters=filters, schema=schema)
            if function(record_dict):
                collection_indices.append(name)

        return {'collection_indices': None if len(collection_indices) == 0 else collection_indices}


# Helper function
def _parse_indices_dict(indices_dict: Dict) -> Dict[str, Dict[str, str]]:
    """
    The indices_dict contains information beyond the description of filters.
    This function extract the name of a collection index and the filters defined for it
    """
    index_filters = {}
    for index_desc in indices_dict["indices"]:
        name = index_desc.get("name", None)
        filters = index_desc.get("filters", None)
        if filters and name:
            index_filters[name] = filters
    return index_filters


# Classes for filter parsing
class FilterParser(ABC):
    @staticmethod
    @abstractmethod
    def parse(filters: Dict[str, str], schema: StructType, logger: Logger = None):
        """
        Method to turn the filters specified as a dictionary into applicable filters for the use case.
        Example: WHERE clauses for filtering a pypark dataframe, Functions to be applied to a dictionary
        :param filters:     Dict of [KEY TO FILTER ON]: [FILTER CONDITION]
                            The filtering key can have the following suffixes:
                            - '+': Filtering conditions are connected with AND
                            - '*': Filtering conditions should be interpreted as regular expressions
        :param schema:      Instance of a pyspark Schema to verify the filtering keys and their datatypes
        :param logger:      Optional instance of a logger
        """
        pass

    @staticmethod
    @abstractmethod
    def _regex_filter(key: str, value: str, schema: StructType, logger: Logger = None):
        """
        Parse a filtering condition as a regular expression. Concrete implementation depends on the use case.
        """
        pass


class SparkParser(FilterParser):
    @staticmethod
    def parse(filters: Dict[str, str], schema: StructType, logger: Logger = None):
        """
        Method to turn the filters specified as a dictionary into WHERE clauses for a pyspark dataframe
        :param filters:     Dict of [KEY TO FILTER ON]: [FILTER CONDITION]
                            The filtering key can have the following suffixes:
                            - '+': Filtering conditions are connected with AND
                            - '*': Filtering conditions should be interpreted as regular expressions
        :param schema:      Instance of a pyspark Schema to verify the filtering keys and their datatypes
        :param logger:      Optional instance of a logger
        """
        where_clauses = []
        for key, value in filters.items():
            value_conditions = []

            # 1. Check for and connection and if the filter should be interpreted as regex
            and_connected = key.endswith('+')
            # TODO: Build and_connected part
            if and_connected:
                key = key[:-1]
            regex_filter = key.endswith('*')
            if regex_filter:
                key = key[:-1]

            # 2. Confirm that the key is in the schema
            if key not in schema.names:
                if logger:
                    logger.warning(f"Filter key {key} not in schema")
                continue

            # 3. Prepare value_conditions either with regex or direct comparisons
            if regex_filter:
                value_conditions += SparkParser._regex_filter(key=key, value=value, schema=schema, logger=logger)
            else:
                value_conditions += [f"{key} = '{v}'" for v in value.split('|')]

            # 4. Construct the where clause
            combined_condition = " OR ".join(value_conditions)
            where_clauses.append(f"({combined_condition})")

        return where_clauses

    @staticmethod
    def _regex_filter(key: str, value: str, schema: StructType, logger: Logger = None) -> Sequence[str]:
        """
        Parse a filtering condition into regular expressions for a pypark dataframe.
        """
        value_conditions = []
        field = schema[key]
        if isinstance(field.dataType, StringType):
            value_conditions = [f"{key} RLIKE '{regex}'" for regex in value.split("|")]
        elif isinstance(field.dataType, ArrayType):
            value_conditions = [
                f"size(filter({key}, x -> x RLIKE '{regex}')) > 0" for regex in value.split("|")
            ]
        elif logger:
            logger.warning(f"Unknown data type for applying regex filter {key} {str(field.dataType)}.")
        return value_conditions


class DictParser(FilterParser):
    @staticmethod
    def parse(filters: Dict[str, str], schema: StructType, logger: Logger = None) -> Callable:
        """
        Method to create a function that checks if the criteria specified by the filters are met.
        Therefore, each filter in the filters dictionary is turned into a function to be applied to a record dictionary.
        These filtering_functions will return True if their condition is met.

        To apply all conditions from the dictionary, the filtering_functions are combined into one high level function
        that is then returned.

        :param filters:     Dict of [KEY TO FILTER ON]: [FILTER CONDITION]
                            The filtering key can have the following suffixes:
                            - '+': Filtering conditions are connected with AND
                            - '*': Filtering conditions should be interpreted as regular expressions
        :param schema:      Instance of a pyspark Schema to verify the filtering keys and their datatypes
        :param logger:      Optional instance of a logger
        """
        filtering_functions = []
        for key, value in filters.items():
            # 1. Check for and connection and if the filter should be interpreted as regex
            and_connected = key.endswith('+')
            # TODO: Build and_connected part
            if and_connected:
                key = key[:-1]
            regex_filter = key.endswith('*')
            if regex_filter:
                key = key[:-1]

            # 2. Confirm that the key is in the schema
            if key not in schema.names:
                if logger:
                    logger.warning(f"Filter key {key} not in schema")
                continue

            # 3. Add a new filtering function either with regex or direct comparisons
            if regex_filter:
                filtering_functions.append(DictParser._regex_filter(key=key, value=value, schema=schema, logger=logger))
            else:
                filtering_functions += DictParser._exact_filter(key=key, value=value)

        # Construct the high level function based on the list of functions
        def index_func(dict_: Dict):
            return any([func(dict_) for func in filtering_functions])

        return index_func

    @staticmethod
    def _regex_filter(key: str, value: str, schema: StructType, logger: Logger = None) -> Union[Callable, None]:
        """
        Turn a filtering condition into a function that applies a regex pattern based on the field type.
        """
        pattern = re.compile(value, re.IGNORECASE)
        field = schema[key]
        if isinstance(field.dataType, StringType):
            def filter_func(dict_: Dict, filter_key=key, filter_pattern=pattern):
                search_str = dict_[filter_key]
                if search_str:
                    return filter_pattern.search(search_str) is not None
                else:
                    return False

            return filter_func

        elif isinstance(field.dataType, ArrayType):
            def filter_func(dict_: Dict, filter_key=key, filter_pattern=pattern):
                search_array = dict_[filter_key]
                if search_array:
                    return any([filter_pattern.search(x) is not None for x in search_array])
                else:
                    return False

            return filter_func

        else:
            if logger:
                logger.warning(f"Unknown data type for applying regex filter {key} {str(field.dataType)}.")
            return None

    @staticmethod
    def _exact_filter(key: str, value: str) -> Sequence[Callable]:
        """
        Create a function that looks for exact matches
        """
        filtering_functions = []
        for v in value.split('|'):
            def filtering_func(dict_: Dict, filter_key=key, filter_value=v):
                return dict_[filter_key] == filter_value

            filtering_functions.append(filtering_func)

        return filtering_functions
