# Resilipipe (with Quality Scoring)
**Note**: This README is adapted from the [original Resilipipe README](https://opencode.it4i.eu/openwebsearcheu-public/preprocessing-pipeline/-/tree/main?ref_type=heads) (Accessed on 22/01/2025)

Resilipipe is an open source software framework that implements a scalable 	cluster-based web content analysis pipeline for web archive data based on [Resiliparse](https://resiliparse.chatnoir.eu/).
It can be run on HPC clusters using [Apache Spark](https://spark.apache.org/) and [Magpie](https://github.com/LLNL/magpie/).
Users can expand the content analysis by implementing their own modules following our [interface](resilipipe/resilipipe/parse/modules/abstract.py).

Necessary configurations are made in the YAML files in [conf](resilipipe/resilipipe/conf).

## Schema
The Parquet files produced by this pipeline will contain the following columns:

### Fixed columns
<details>
  <summary><b>Schema Version 0.1.0</b></summary>

| Column                  | Description                                                                                                                        | Pyspark Datatype                      |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| id                      | Unique ID based on hash of the URL and crawling time                                                                               | `StringType()`                        |
| record_id               | UUID of the WARC record                                                                                                            | `StringType()`                        |
| title                   | Title from the HTML                                                                                                                | `StringType()`                        |
| plain_text              | Cleaned text from the HTML                                                                                                         | `StringType()`                        |
| json-ld                 | String list of JSON-LD (https://www.w3.org/TR/json-ld/#embedding-json-ld-in-html-documents)                                        | `StringType()`                        |
| microdata               | String list of HTML Microdata (http://www.w3.org/TR/microdata/#json)                                                               | `StringType()`                        |
| warc_date               | Date from the WARC header                                                                                                          | `StringType()`                        |
| warc_ip                 | IP Address from the WARC header                                                                                                    | `StringType()`                        |
| url                     | Full URL                                                                                                                           | `StringType()`                        |
| url_scheme              | URL scheme specifier                                                                                                               | `StringType()`                        |
| url_path                | Hierarchical path after TLD                                                                                                        | `StringType()`                        |
| url_params              | Parameters for last path element                                                                                                   | `StringType()`                        |
| url_query               | Query component                                                                                                                    | `StringType()`                        |
| url_fragment            | Fragment identifier                                                                                                                | `StringType()`                        |
| url_subdomain           | Subdomain of the network location                                                                                                  | `StringType()`                        |
| url_domain              | Domain of the network location                                                                                                     | `StringType()`                        |
| url_suffix              | Suffix according to the [Public Suffix List](https://publicsuffix.org/)                                                            | `StringType()`                        |
| url_is_private          | If the URL has a private suffix                                                                                                    | `BooleanType()`                       |
| mime_type               | MIME-Type from the HTTP Header                                                                                                     | `StringType()`                        |
| charset                 | charset from the HTTP Header                                                                                                       | `StringType()`                        |
| content_type_other      | List of key, value pairs from the content type that could not be parsed into MIME-type or charset                                  | `MapType(StringType(), StringType())` |
| http_server             | Server from the from the HTTP Header                                                                                               | `StringType()`                        |
| language                | Language as identified by [language.py](preprocessing/parse/language.py); Code according to ISO-639 Part 3                         | `StringType()`                        |
| valid                   | `True`: The record is valid; `False`: The record is no longer valid and should not be processed.                                   | `BooleanType()`                       |
| warc_file               | Name of the original WARC-file that contained record                                                                               | `StringType()`                        |
| ows_canonical           | The canonical link if it exists                                                                                                    | `StringType()`                        |
| ows_resource_type       | Crawl from which the WARC-file originated; Files crawled by the University of Passau are labeled with "Owler"                      | `StringType()`                        |
| ows_curlielabel         | One of the 15 Curlie top level labels                                                                                              | `StringType()`                        |
| ows_index               | `True`: The content is allowed to be used for the purposes of web indexing/web search; `False`: The content cannot be used         | `BooleanType()`                       |
| ows_genai               | `True`: The content is allowed to be used for the purposes of developing Generative AI models; `False`: The content cannot be used | `BooleanType()`                       |
| ows_genai_details       | If `ows_genai=False`, this provides additional context                                                                             | `StringType()`                        |
| ows_fetch_response_time | Fetch time in ms                                                                                                                   | `IntegerType()`                       |
| ows_fetch_num_errors    | Number of errors while fetching (Timeout is the most prominent fetch error)                                                        | `StringType()`                        |
| schema_metadata         | List of key, value pairs that contain global settings like the `schema_version`                                                    | `MapType(StringType(), StringType())` |

</details>

### Columns from [modules](preprocessing/parse/html_modules)
Additional columns can be added by providing modules as outlined in the respective [README](resilipipe/resilipipe/parse/README.md). 
One example is detecting outgoing links.

| Column             | Description                                                                                                                                                                    | Pyspark Datatype                                                                             |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| outgoing_links     | List of all hyperlinks in the HTML that start with 'http'                                                                                                                      | `ArrayType(StringType())`                                                                    |
| image_links        | List of all links to images in the HTML that start with 'http'                                                                                                                 | See `get_spark_schema` in [links.py](resilipipe/resilipipe/parse/modules/links.py)           |
| video_links        | List of all links to videos in the HTML that start with 'http' or iframes with a video                                                                                         | See `get_spark_schema` in [links.py](resilipipe/resilipipe/parse/modules/links.py)           |
| iframes            | List of tuples for nodes that contain an iframe (and are not a video)                                                                                                          | See `get_spark_schema` in [links.py](resilipipe/resilipipe/parse/modules/links.py)           |
| curlielabels       | List of language specific domain labels according to [Curlie.org](https://curlie.org/)                                                                                         | `ArrayType(StringType())`                                                                    |
| curlielabels_en    | List of English domain labels according to [Curlie.org](https://curlie.org/). Mapping by [Lugeon, Sylvain; Piccardi, Tiziano](https://doi.org/10.6084/m9.figshare.19406693.v5) | `ArrayType(StringType())`                                                                    |
| address            | List of dictionaries containing extracted location and coordinates                                                                                                             | See `get_spark_schema` in [geoparsing.py](resilipipe/resilipipe/parse/modules/geoparsing.py) | 
| collection_indices | List of collection indices that a record belongs to. Are defined via `yaml` files on the S3 instance                                                                           | `ArrayType(StringType())                                                                     |                        
| **quality_score**  | A quality score for the webpage based on its plaintext (given as a log probability for the page being relevant to any query) | FloatType()

## Quality Scoring Component
The [quality scoring module](resilipipe/resilipipe/parse/modules/quality_scoring.py) is based on the T5-small based quality scorer (QT5-small) trained by [Chang et al](https://github.com/terrierteam/pyterrier-quality/tree/main).
The model is applied in a [HTMLModule](resilipipe/resilipipe/parse/modules/abstract.py) to the input document's plaintext and outputs the document's estimated quality as a log probability of the document being relevant to *any* query.
An example output for the included [sample file](tests/data/sample.warc.gz) can be found under [tests/data/metadata.parquet](tests/data/metadata.parquet).

## Setup
After cloning this repository, make sure to complete the following steps.

### Configure the pipeline
If you want to adapt which [modules](resilipipe/resilipipe/parse/modules) are used for html parsing, you can add/remove them in the ([modules.yaml](resilipipe/resilipipe/conf/modules.yaml)) by adding the name of the python script (without `.py`) in [modules](resilipipe/resilipipe/parse/modules)

### Installation using `make prepare`
After configuration, the next step consists of installing necessary packages in a virtual environment.
This is done using the following command 
```
make prepare
```

### Standalone Minio Instance
If you don't have access to an S3 instance, you can run a minio standalone server to experiment locally.
A description can be found [here](https://hub.docker.com/r/minio/minio).

Please add the following environment variables to your `/.bashrc` (Adapt the values if necessary):
```
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin"
export MINIO_BUCKET_NAME="bucket"
```

### Apache Spark
As the jobs use Apache Spark for parallel processing, the machine executing the jobs needs to have access to a cluster or a local instance. Please refer to the respective documentation.

### Slurm deployment
To deploy the preprocessing pipeline in an HPC-cluster, we make use of [Magpie](https://github.com/LLNL/magpie/). 
The spark deployment can be found [here](https://opencode.it4i.eu/openwebsearcheu-public/spark-deployment)
Magpie ensures Hadoop/HDFS and Spark are set up correctly within a Slurm batch allocation. We have set up a few scripts and configuration files to simplify deployment at a Slurm-powered cluster.

### Additional steps to avoid common issues
- Ensure that the cluster nodes can access the [resources](resources)-directory by cloning the repository to shared directories
- Add module loading to `~/.bashrc` if the environment is not transferred to cluster nodes

## Docker image
In order to test the pipeline locally, you can use the [Dockerfile](Dockerfile). 
Build the image using the command:
```bash
docker build -t <username>/resilipipe_quality_scoring:latest .
```
or use the pre-built image ```arianes21/resilipipe_quality_scoring:latest```.
To run the pipeline, simply mount a local directory (`tests/data` in the example below) that contains a WARC-file that you want to process and run the following command:
```
docker run \
    --mount type=bind,src=absolute/path/tests/data/",dst=/opt/ows_preprocessing/tests/data/ \
    arianes21/resilipipe_quality_scoring:latest \
    tests/data/sample.warc.gz \
    tests/data/metadata1.parquet
```