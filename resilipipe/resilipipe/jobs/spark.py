import os
from pyspark import SparkConf, SparkFiles
from pyspark.sql import SparkSession
from pyspark.broadcast import Broadcast
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import StructType, ArrayType
from pyspark.storagelevel import StorageLevel
import time

from resilipipe import RESILIPIPE_DIR, RESOURCES_DIR
from resilipipe.conf.config import Configurator
from resilipipe.log.info_logging import create_logger
from resilipipe.log.statistics import calculate_df_stats, create_filename_from_object_path
from resilipipe.parse import warc_preprocessing as wpp


def parse_warc_partition(warc_content: bytes, warc_path: str, modules_bc: Broadcast, module_input_bc: Broadcast,
                         schema_bc: Broadcast, schema_metadata_bc: Broadcast,
                         logger_name: str = None, logging_dir: str = None):
    """
    Parse a single WARC file as bytes, create dictionaries for each record
    :param warc_content:                Bytes of a WARC file
    :param warc_path:                   Path to the WARC file on S3
    :param modules_bc:                  Broadcast instance of the list of modules
    :param module_input_bc:             Broadcast instance of the dictionary of module inputs
    :param schema_bc:                   Broadcast instance of the spark schema
    :param schema_metadata_bc:          Broadcast instance of the schema metadata (e.g. for versioning)
    :param logger_name:                 (Optional) Name of the logger
    :param logging_dir:                 (Optional) Directory for the logging file
    Returns a list of parsed records
    """
    # Create a logger for each worker; If a SPARK_LOG_DIR on the node is available, the file is created there
    if logger_name and logging_dir:
        spark_log_dir = os.getenv("SPARK_LOG_DIR")
        if spark_log_dir:
            logging_dir = spark_log_dir
        logger = create_logger(logger_name=logger_name, output_file=logging_dir + "/resilipipe.log")
    try:
        return [
            tuple(
                result.get(field.name) for field in schema_bc.value
            )
            for result in wpp.parse_warc(
                warc_input=warc_content,
                modules=modules_bc.value,
                module_input=module_input_bc.value,
                logger_name=logger_name,
                warc_path=warc_path,
                schema_metadata=schema_metadata_bc.value
            )
        ]
    except Exception as e:
        if logger_name:
            logger.info(f'Failed parsing {warc_path} with exception: {e}')
        else:
            print(f'Failed parsing {warc_path} with exception: {e}')
        return []


def run(object_path: str = None, target_path: str = None, test: bool = False):
    # 1. Prepare the logging
    spark_log_dir = os.getenv("SPARK_LOG_DIR")
    if spark_log_dir:
        logging_dir = spark_log_dir
    else:
        logging_dir = f"{target_path}_logs".replace("file://", "")
        os.makedirs(logging_dir, exist_ok=True)
    logger_name = "Resilipipe logger"
    logger = create_logger(logger_name=logger_name, output_file=logging_dir + "/resilipipe.log")

    # 2. Create spark context and session
    conf = SparkConf()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext

    # 3. Create the configurator and load the variables for broadcasting
    if test:
        configurator = Configurator(modules_yaml=RESILIPIPE_DIR / 'conf' / 'modules.yaml')
    else:
        configurator = Configurator(modules_yaml=SparkFiles.get('modules.yaml'))

    modules = configurator.get_modules()
    modules_bc = sc.broadcast(modules)
    module_input_bc = sc.broadcast(
        {module_tuple[0]: module_tuple[1].load_input(RESOURCES_DIR) for module_tuple in modules})
    schema_bc = sc.broadcast(configurator.get_spark_schema())
    schema_metadata_bc = sc.broadcast(configurator.get_schema_metadata())
    logger.info(f"Loaded the following modules {[x[0] for x in modules]}")

    # 4. Prepare the UDF for parsing the records
    parse_udf = udf(
        lambda warc_content, warc_path: parse_warc_partition(warc_content=bytes(warc_content),
                                                             warc_path=warc_path,
                                                             modules_bc=modules_bc,
                                                             module_input_bc=module_input_bc,
                                                             schema_bc=schema_bc,
                                                             schema_metadata_bc=schema_metadata_bc,
                                                             logger_name=logger_name,
                                                             logging_dir=logging_dir
                                                             ),
        ArrayType(StructType(configurator.get_spark_schema()))
    )

    # 5. Parse the WARC files
    logger.info(f'Started parsing WARC files in {object_path}')
    ts_start = time.time()
    df = spark.read.format('binaryFile').load(object_path)
    df = df.select(parse_udf('content', 'path').alias('parsed'))
    df = df.select(explode('parsed').alias('parsed'))
    df = df.select('parsed.*')

    # Save the dataframe and ensure we don't have to re-run the entire pipeline when computing the stats
    if test:
        df.cache()
        df.write.mode('overwrite').parquet(target_path)
    else:
        df.persist(StorageLevel.DISK_ONLY)
        df.write.parquet(target_path)
    ts_end = time.time()

    # 5. Calculate statistics for the job and write them to a parquet file
    stat_df = calculate_df_stats(df=df, spark=spark, ts_start=ts_start, ts_end=ts_end)
    if object_path.startswith('s3a://'):
        bucket_name = os.getenv("MINIO_BUCKET_NAME")
        datestr = create_filename_from_object_path(object_path=object_path)
        if datestr == '1000-1-1':
            logger.info(f"The object_path {object_path} does not contain the expected format of "
                        "year=YYYY/month=M/day=D. Stats will be saved to '1000-1-1_stats.parquet'")
        stats_path = f's3a://{bucket_name}/logs/preprocessed/{datestr}_stats.parquet'
        stat_df.write.mode('overwrite').parquet(stats_path)
    else:
        stat_df.write.mode('overwrite').parquet(f"{logging_dir}/stats.parquet")

    if not test:
        sc.stop()

    logger.info(f'Completed parsing WARC files in {object_path}/')


if __name__ == "__main__":
    # Get object_path from commandline arguments
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('--object_path', type=str)
    parser.add_argument('--target_path', type=str)
    args = parser.parse_args()

    run(object_path=args.object_path,
        target_path=args.target_path)