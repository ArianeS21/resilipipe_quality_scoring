# Logging
While running the pipeline, two types of logs will be created.
- A parquet file summarizes statistics about the job by giving a high level overview on each WARC file processed in the job
- A log file reports any errors or warnings that occurred while running the job

## 1. Statistics
The statistics file is saved in the S3 instance in the same bucket as the WARC files under the object path `logs/preprocessed/{DATESTRING}_stats.parquet`
The `DATESTR` is derived from the object path provided for the WARC files, for example:
```
object_path = "year=2024/month=1/day=1"
DATESTR = "2024-01-01"
```

The parquet file contains the following information:

| Column               | Description                                                          | 
|----------------------|----------------------------------------------------------------------|
| warc_file            | Path of the WARC file                                                |
| num_records          | Number of records parsed / contained in the WARC file                | 
| min_ts               | Earliest timestamp of a record in the WARC                           | 
| max_ts               | Latest timestamp of a record in the WARC                             | 
| domain_1 - domain_20 | Twenty most frequent domains in the WARC file with number of records | 
| language             | Dictionary of languages in the WARC file with number of records      | 
| creation_ts          | Timestamp of the creation of the parquet file                        |

The corresponding code can be found in [statistics.py](statistics.py).

## 2. Basic logging
The log file with messages that occurred during the job is saved to a directory defined by 
the environment variable `SPARK_LOG_DIR` (which is set by the 
[spark-deployment](https://opencode.it4i.eu/openwebsearcheu-public/spark-deployment) workflow).
If no value is set for `SPARK_LOG_DIR`, the log file is saved to the directory provided as `--target_path` to [spark.py](../jobs/spark.py).

Regardless of the directory, the log file is called `resilipipe.log`. 
