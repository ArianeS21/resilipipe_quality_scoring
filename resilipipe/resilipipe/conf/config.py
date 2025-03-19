import importlib
import pyarrow as pa
import yaml
from pyspark.sql.types import BooleanType, IntegerType, MapType, StructField, StringType

SCHEMA_VERSION = "0.1.0"


class Configurator:
    '''
    Class to provide necessary configuration to jobs
    '''
    def __init__(self, modules_yaml: str = None, data_centers_yaml: str = None):
        '''
        Initialize configuration attributes
        :param modules_yaml:        Path to the yaml containing all modules to be added to the pipeline
        :param data_centers_yaml:   Path to the yaml containing data center parameters (used in job submission)
        '''
        # Define the schemas without the HTML modules
        self.pyarrow_schema = [
            pa.field("id", pa.string(), False),
            pa.field("record_id", pa.string(), False),
            pa.field("title", pa.string(), True),
            pa.field("plain_text", pa.string(), True),
            pa.field("json-ld", pa.string(), True),
            pa.field("microdata", pa.string(), True),
            pa.field("warc_date", pa.string(), True),
            pa.field("warc_ip", pa.string(), True),
            pa.field("url", pa.string(), True),
            pa.field("url_scheme", pa.string(), True),
            pa.field("url_path", pa.string(), True),
            pa.field("url_params", pa.string(), True),
            pa.field("url_query", pa.string(), True),
            pa.field("url_fragment", pa.string(), True),
            pa.field("url_subdomain", pa.string(), True),
            pa.field("url_domain", pa.string(), True),
            pa.field("url_suffix", pa.string(), True),
            pa.field("url_is_private", pa.bool_(), True),
            pa.field("mime_type", pa.string(), True),
            pa.field("charset", pa.string(), True),
            pa.field("content_type_other", pa.map_(pa.string(), pa.string()), True),
            pa.field("http_server", pa.string(), True),
            pa.field("language", pa.string(), True),
            pa.field("valid", pa.bool_(), True),
            pa.field("warc_file", pa.string(), True),
            pa.field("ows_canonical", pa.string(), True),
            pa.field("ows_resource_type", pa.string(), True),
            pa.field("ows_curlielabel", pa.string(), True),
            pa.field("ows_index", pa.bool_(), True),
            pa.field("ows_genai", pa.bool_(), True),
            pa.field("ows_genai_details", pa.string(), True),
            pa.field("ows_fetch_response_time", pa.int32(), True),
            pa.field("ows_fetch_num_errors", pa.string(), True),
            pa.field("schema_metadata", pa.map_(pa.string(), pa.string()), True)
        ]

        self.spark_schema = [
            StructField("id", StringType(), False),
            StructField("record_id", StringType(), False),
            StructField("title", StringType(), True),
            StructField("plain_text", StringType(), True),
            StructField("json-ld", StringType(), True),
            StructField("microdata", StringType(), True),
            StructField("warc_date", StringType(), True),
            StructField("warc_ip", StringType(), True),
            StructField("url", StringType(), True),
            StructField("url_scheme", StringType(), True),
            StructField("url_path", StringType(), True),
            StructField("url_params", StringType(), True),
            StructField("url_query", StringType(), True),
            StructField("url_fragment", StringType(), True),
            StructField("url_subdomain", StringType(), True),
            StructField("url_domain", StringType(), True),
            StructField("url_suffix", StringType(), True),
            StructField("url_is_private", BooleanType(), True),
            StructField("mime_type", StringType(), True),
            StructField("charset", StringType(), True),
            StructField("content_type_other", MapType(StringType(), StringType()), True),
            StructField("http_server", StringType(), True),
            StructField("language", StringType(), True),
            StructField("valid", BooleanType(), True),
            StructField("warc_file", StringType(), True),
            StructField("ows_canonical", StringType(), True),
            StructField("ows_resource_type", StringType(), True),
            StructField("ows_curlielabel", StringType(), True),
            StructField("ows_index", BooleanType(), True),
            StructField("ows_genai", BooleanType(), True),
            StructField("ows_genai_details", StringType(), True),
            StructField("ows_fetch_response_time", IntegerType(), True),
            StructField("ows_fetch_num_errors", StringType(), True),
            StructField("schema_metadata", MapType(StringType(), StringType()), True)
        ]

        # Load the modules, if available
        if modules_yaml is not None:
            with open(modules_yaml, 'r') as stream:
                module_names = yaml.safe_load(stream)['modules']

            self.modules = []
            if module_names is not None:
                for name in module_names:
                    try:
                        self.modules.append((
                                name, getattr(importlib.import_module(f"resilipipe.parse.modules.{name}"), "Module")
                            ))
                    except Exception as e:
                        print(f'Loading module "{name}" failed due to Exception: {e}')
                        continue

                # Expand the schemas with those of the HTML modules
                for module_tuple in self.modules:
                    self.pyarrow_schema += module_tuple[1].get_pyarrow_schema()
                    self.spark_schema += module_tuple[1].get_spark_schema()

        # Load the data center information, if available
        if data_centers_yaml is not None:
            with open(data_centers_yaml, 'r') as stream:
                self.data_centers = yaml.safe_load(stream)

    def get_data_centers(self):
        '''
        :return:    Dictionary with information per data center used to estimate resource requirements for jobs.
                    See estimate_resources.py for its usage
        '''
        return self.data_centers

    def get_modules(self):
        '''
        :return:    List of parsing modules to apply to each record; Tuples of (MODULE_NAME, MODULE_CLASS)
        '''
        return self.modules

    def get_pyarrow_schema(self):
        '''
        :return:    List of column names with associated Arrow data types
        '''
        return self.pyarrow_schema

    def get_spark_schema(self):
        '''
        :return:    List of column names with associated Spark data types
        '''
        return self.spark_schema

    @staticmethod
    def get_schema_metadata():
        return [("schema_version", SCHEMA_VERSION)]
