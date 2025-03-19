#!/bin/bash

job=$1
object_path=$2
target_path=$3

hadoop_aws=($HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-*)
aws_sdk=($HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-*)
extra_jars="${hadoop_aws},${aws_sdk}"

$SPARK_HOME/bin/spark-submit \
    --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY}" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
    --files ${RESILIPIPE_DIR}/conf/modules.yaml \
    --jars "${extra_jars}" \
    "${RESILIPIPE_DIR}/jobs/${job}" \
    --object_path "${object_path}" \
    --target_path "${target_path}"
