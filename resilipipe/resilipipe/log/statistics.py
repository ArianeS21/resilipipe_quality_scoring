import pandas as pd
from pyspark.sql import SparkSession, Window, WindowSpec, DataFrame as PysparkDataFrame, functions as f
from pyspark.sql.types import IntegerType, StructField, StructType


def calculate_df_stats(df: PysparkDataFrame, spark: SparkSession, ts_start: float, ts_end: float) -> pd.DataFrame:
    """
    Take the pyspark DataFrame produced by one job and use it to calculate statistics about the job
    :param df:          Pyspark DataFrame with parsing results
    :param spark:       Spark session
    :param ts_start:    Timestamp when the job started
    :param ts_end:      Timestamp when the job finished
    """
    df_grouped = group_df_by_warc_file(df=df, spark=spark, ts_start=ts_start, ts_end=ts_end)

    # Standardize the timestamp columns by turning them into integers
    stat_df = (df_grouped
               .withColumn("creation_ts", f.col("creation_ts").cast(IntegerType()))
               .withColumn("min_ts", f.col("min_ts").cast(IntegerType()))
               .withColumn("max_ts", f.col("max_ts").cast(IntegerType()))
               )
    return stat_df


def group_df_by_warc_file(df: PysparkDataFrame, spark: SparkSession, ts_start: float, ts_end: float) \
        -> PysparkDataFrame:
    """
    Group a pyspark DataFrame of records by the warc files they were taken from.
    Calculate statistics for each warc file:
        - Number of records
        - Number of records per language
        - 20 most common domains
    Parameters:
    :param df:          Pyspark DataFrame with parsing results
    :param spark:       Spark session
    :param ts_start:    Timestamp when the job started
    :param ts_end:      Timestamp when the job finished
    """
    # 1. Add domains, number of records, and minimum/maximum timestamp per warc_file
    warc_window = Window.partitionBy("warc_file")
    df = (df
          .withColumn("num_records", f.count("record_id").over(warc_window))
          .withColumn("domain", f.concat(f.col("url_domain"), f.lit("."), f.col("url_suffix")))
          .withColumn("warc_ts", f.to_timestamp(f.col("warc_date")))
          .withColumn("min_ts", f.min("warc_ts").over(warc_window))
          .withColumn("max_ts", f.max("warc_ts").over(warc_window))
          )

    # 2. For each warc_file, record counts for the 20 most common domains and all languages
    df_domain = _aggregate_counts(df, "domain", warc_window, top_k=20)
    df_lang = _aggregate_counts(df, "language", warc_window)

    # Create individual columns for each of the top 20 domains and turn the language counts into a single dictionary
    df_domain = _process_aggregations(df_domain, "domain", mode="explode")
    df_lang = _process_aggregations(df_lang, "language", mode="group")

    # 3. Group the original dataframe and join the other dataframes
    df_grouped = df.groupby("warc_file").agg(
        f.first("num_records").alias("num_records"),
        f.first("min_ts").alias("min_ts"),
        f.first("max_ts").alias("max_ts")
    )

    # Create estimated creation timestamps based on avg parsing time
    total_parse_time = ts_end - ts_start
    num_warcs = df_grouped.count()

    # Create an empty df if no warc files are available
    if num_warcs == 0:
        creation_ts_df = spark.createDataFrame(
            data=spark.sparkContext.emptyRDD(), schema=StructType([StructField("creation_ts", IntegerType(), True)])
        )
    else:
        avg_parse_time = total_parse_time / num_warcs
        estimated_ts = [ts_start + i*avg_parse_time for i in range(num_warcs)]
        creation_ts_df = spark.createDataFrame([(ts,) for ts in estimated_ts], ['creation_ts'])

    # Prepare row_idx to join the creation times
    df_grouped = (df_grouped.withColumn("row_idx",
                                        f.row_number().over(Window.orderBy(f.monotonically_increasing_id()))
                                        ))
    creation_ts_df = (creation_ts_df.withColumn("row_idx",
                                                f.row_number().over(Window.orderBy(f.monotonically_increasing_id()))
                                                ))

    df_grouped = (df_grouped
                  .join(df_domain, on="warc_file")
                  .join(df_lang, on="warc_file")
                  .join(creation_ts_df, on="row_idx")
                  ).drop("row_idx")
    return df_grouped


def _aggregate_counts(df: PysparkDataFrame, column: str, warc_window: WindowSpec, top_k: int = None) \
        -> PysparkDataFrame:
    """
    Helper function to aggregate counts across multiple warc files.
    """
    # Create a window that partitions by the warc_file and the aggregation column
    col_window = Window.partitionBy(f.col("warc_file"), f.col(column))

    # Create the aggregated dataframe by
    # 1. Counting how often a value in the column occurs
    # 2. Keeping only unique values
    # 3. Ranking the values by their counts (descending)
    df_agg = (df
              .where(f.col(column).isNotNull())
              .withColumn(f"{column}_count", f.count(f.col(column)).over(col_window))
              .withColumn("row", f.row_number().over(col_window.orderBy(f"{column}_count")))
              .filter(f.col("row") == 1).drop("row")
              .withColumn(f"{column}_rank", f.row_number().over(warc_window.orderBy(f.col(f"{column}_count").desc())))
              ).select("warc_file", column, f"{column}_count", f"{column}_rank")

    # If specified, keep only the top k ranks
    if top_k:
        df_agg = df_agg.filter(f.col(f"{column}_rank") <= top_k)
    return df_agg


def _process_aggregations(df_agg, column, mode: str = "group"):
    """
    Postprocess the aggregations by the _aggregate_counts function.
    """
    valid = ["group", "explode"]
    assert mode in valid, print(f"Please provide a valid mode: {', '.join(valid)}. Not {mode}")

    # Turn the arrays into a single column with a dictionary of {column value: count}
    if mode == "group":
        df_agg = df_agg.groupBy("warc_file").agg(
            f.map_from_arrays(
                f.collect_list(column),
                f.collect_list(f"{column}_count")
            ).alias(f"{column}s")
        )

    # Turn the arrays into separate columns with tuples of (column value, count) based on their rank
    elif mode == "explode":
        df_agg = df_agg.groupBy("warc_file").agg(
            f.map_from_arrays(
                f.collect_list(f"{column}_rank"),
                f.arrays_zip(
                    f.collect_list(column),
                    f.collect_list(f"{column}_count")
                )
            ).alias("rank_map")
        )
        keys = (df_agg
                .select(f.explode("rank_map"))
                .select("key")
                .distinct()
                .rdd.flatMap(lambda x: x)
                .collect())
        new_cols = [f.col("rank_map").getItem(k).alias(f"{column}_{k}") for k in sorted(keys)]
        df_agg = df_agg.select(["warc_file", *new_cols])

    return df_agg


def create_filename_from_object_path(object_path: str):
    # Create default values
    dict_ = {"year": '1000', "month": '1', "day": '1'}
    for s in dict_.keys():
        # If the string is not found, skip it
        idx_start = object_path.find(f"{s}=") + len(f"{s}=")
        if idx_start == len(s):
            continue

        # If the object_path ends after the string, use the full length
        idx_end = object_path[idx_start:].find("/") + idx_start
        if idx_end == idx_start - 1:
            idx_end = len(object_path)
        dict_[s] = object_path[idx_start:idx_end]

    return f"{dict_['year']}-{dict_['month']}-{dict_['day']}"
