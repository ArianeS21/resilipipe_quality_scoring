import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, udf
from pyspark.sql.types import ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual
import pytest

from resilipipe import RESILIPIPE_DIR, RESOURCES_DIR
from resilipipe.conf.config import Configurator
from resilipipe.jobs.spark import parse_warc_partition, run
from resilipipe.log.statistics import calculate_df_stats

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
EXPECTED_DATA = [
        {'id': '41b07364376b982b49a519be67af49abda917d75f001301e9f3026c32ab9e9e5',
         'record_id': '00000000-0000-0000-0000-000000000001',
         'title': 'Example page',
         'plain_text':
             'Example Corporation Example Corporation logo\n\n'
             'Hello world!\n\nHello DOM!\n\nYour browser does not support the video tag.',
         'json-ld': str([{
             '@context': 'https://schema.org',
              '@type': 'Organization',
              'name': 'Example Corporation',
              'url': 'https://www.example.com',
              'logo': 'https://www.example.com/logo.png',
              'sameAs': ['https://www.facebook.com/example',
                         'https://www.twitter.com/example',
                         'https://www.linkedin.com/company/example'],
              'contactPoint': {'@type': 'ContactPoint',
                               'telephone': '+1-800-555-5555',
                               'contactType': 'Customer Service',
                               'areaServed': 'US',
                               'availableLanguage': ['English', 'Spanish']},
              'address': {'@type': 'PostalAddress',
                          'streetAddress': '123 Example Street',
                          'addressLocality': 'Example City',
                          'addressRegion': 'EX',
                          'postalCode': '12345',
                          'addressCountry': 'US'},
              'founder': {'@type': 'Person',
                          'name': 'Jane Doe'},
              'foundingDate': '2000-01-01',
              'description': 'Example Corporation is a leading provider of exemplary services.'
         }]),
         'microdata': str([{
             'type': 'https://schema.org/Organization',
             'properties': {'url': 'https://www.example.com',
                            'logo': 'https://www.example.com/logo.png',
                            'contactPoint': {'type': 'https://schema.org/ContactPoint',
                                             'properties': {'contactType': 'Customer Service',
                                                            'telephone': '+1-800-555-5555',
                                                            'areaServed': 'US',
                                                            'availableLanguage': ['English', 'Spanish']}
                                             },
                            'address': {'type': 'https://schema.org/PostalAddress',
                                        'properties': {'streetAddress': '123 Example Street',
                                                       'addressLocality': 'Example City',
                                                       'addressRegion': 'EX',
                                                       'postalCode': '12345',
                                                       'addressCountry': 'US'}
                                        },
                            'sameAs': [{'type': 'https://schema.org/URL',
                                        'properties': {'url': 'https://www.facebook.com/example'}
                                        },
                                       {'type': 'https://schema.org/URL',
                                        'properties': {'url': 'https://www.twitter.com/example'}
                                        },
                                       {'type': 'https://schema.org/URL',
                                        'properties': {'url': 'https://www.linkedin.com/company/example'}
                                        }],
                            'founder': {'type': 'https://schema.org/Person',
                                        'properties': {'name': 'Jane Doe'}},
                            'foundingDate': '2000-01-01',
                            'description': 'Example Corporation is a leading provider of exemplary services.'
                            }
         }]),
         'warc_date': '2024-01-01T00:00:02Z',
         'warc_ip': '123.12.34.567',
         'url': 'https://home.example.com/lifestyle;jsessionid=0001?query=hello&sort=newest#print',
         'url_scheme': 'https',
         'url_path': '/lifestyle',
         'url_params': 'jsessionid=0001',
         'url_query': 'query=hello&sort=newest',
         'url_fragment': 'print',
         'url_subdomain': 'home',
         'url_domain': 'example',
         'url_suffix': 'com',
         'url_is_private': False,
         'mime_type': 'text/html',
         'charset': 'utf-8',
         'content_type_other': None,
         'http_server': 'cloudflare',
         'language': 'eng',
         'valid': True,
         'warc_file': f'file:{DATA_DIR}/sample.warc.gz',
         'ows_canonical': None,
         'ows_resource_type': 'OWLER',
         'ows_curlielabel': 'Business',
         'ows_index': True,
         'ows_genai': True,
         'ows_genai_details': None,
         'ows_fetch_response_time': 600,
         'ows_fetch_num_errors': None,
         'schema_metadata': None,
         'outgoing_links': ['https://www.example.com', 'https://example.com'],
         'image_links': [{"src": 'https://www.example.com/logo.png', "width": None, "height": None}],
         'video_links': [{"src": 'https://www.youtube.com/watch?v=dQw4w9WgXcQ', "width": '250', "height": None}],
         'iframes': None,
         'curlielabels': None,
         'curlielabels_en': None,
         'address': [{"loc": {"streetAddress": '123 Example Street', "postalCode": '12345',
                              "addressLocality": 'Example City', "addressCountry": 'US'},
                      "geo": None}],
         'collection_indices': ['curlie_full']}]


@pytest.fixture
def spark_fixture():
    # Create the SparkSession
    conf = SparkConf().setMaster("local[4]").setAppName("Spark Job Test")
    conf.set("spark.driver.memory", "10g")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # Get the configurator
    configurator = Configurator(modules_yaml=RESILIPIPE_DIR / 'conf' / 'modules.yaml')
    yield spark, configurator


def test_run(spark_fixture):
    spark, configurator = spark_fixture

    # Create the pyspark Dataframe of expected values
    schema = StructType(configurator.get_spark_schema())
    expected_df = spark.createDataFrame(EXPECTED_DATA, schema=schema)

    # Run the spark job
    target_path = os.path.join(DATA_DIR,  "./", "sample_output")
    run(object_path=DATA_DIR, target_path=target_path, test=True)

    # Load the dataframe and assert correctness
    df = spark.read.parquet(target_path)
    assertDataFrameEqual(df, expected_df)


def test_calculate_df_stats(spark_fixture):
    spark, configurator = spark_fixture

    # Create the pyspark Dataframe of expected values and get the statistics
    schema = StructType(configurator.get_spark_schema())
    expected_df = spark.createDataFrame(EXPECTED_DATA, schema=schema)
    stat_df = calculate_df_stats(df=expected_df, spark=spark, ts_start=0, ts_end=1)

    # Compare the stat_df with the expected stats
    expected_stats = [{
        "warc_file": f'file:{DATA_DIR}/sample.warc.gz',
        "num_records": 1,
        "min_ts": 1704067202,
        "max_ts": 1704067202,
        "domain_1": ('example.com', 1),
        "languages": {'eng': 1},
        "creation_ts": 0
    }]

    # Create the schema
    schema = StructType([StructField('warc_file', StringType(), True),
                         StructField('num_records', LongType(), True),
                         StructField('min_ts', IntegerType(), True),
                         StructField('max_ts', IntegerType(), True),
                         StructField('domain_1', StructType([
                             StructField('0', StringType(), True),
                             StructField('1', LongType(), True)]),
                                     True),
                         StructField('languages', MapType(
                             StringType(), LongType(), False),
                                     False),
                         StructField('creation_ts', IntegerType(), True)])
    expected_stat_df = spark.createDataFrame(expected_stats, schema)
    assertDataFrameEqual(stat_df, expected_stat_df)


def test_parse_warc_partition(spark_fixture):
    spark, configurator = spark_fixture
    sc = spark.sparkContext

    # Create the pyspark Dataframe of expected values
    schema = StructType(configurator.get_spark_schema())
    expected_df = spark.createDataFrame(EXPECTED_DATA, schema=schema)

    # Prepare the job
    modules = configurator.get_modules()
    modules_bc = sc.broadcast(modules)
    module_input_bc = sc.broadcast(
        {module_tuple[0]: module_tuple[1].load_input(RESOURCES_DIR) for module_tuple in modules})
    schema_bc = sc.broadcast(configurator.get_spark_schema())
    schema_metadata_bc = sc.broadcast(configurator.get_schema_metadata())

    parse_udf = udf(
        lambda warc_content, warc_path: parse_warc_partition(warc_content=bytes(warc_content),
                                                             warc_path=warc_path,
                                                             modules_bc=modules_bc,
                                                             module_input_bc=module_input_bc,
                                                             schema_bc=schema_bc,
                                                             schema_metadata_bc=schema_metadata_bc
                                                             ),
        ArrayType(StructType(configurator.get_spark_schema()))
    )

    # Create the dataframe
    df = spark.read.format('binaryFile').load(DATA_DIR)
    df = df.select(parse_udf('content', 'path').alias('parsed'))
    df = df.select(explode('parsed').alias('parsed'))
    df = df.select('parsed.*').cache()

    # Assert correct output
    assertDataFrameEqual(df, expected_df)
