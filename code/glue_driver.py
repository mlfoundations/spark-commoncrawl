

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import argparse
import json
from loguru import logger
import os
import re

from io import BytesIO
from tempfile import SpooledTemporaryFile, TemporaryFile

import time
import boto3
import botocore
import requests
from parse_wat import read_s3_extract_images_and_write_warc
import fsspec
import uuid


if __name__ == "__main__":
    s3_path = "s3://commoncrawl/crawl-data/CC-MAIN-2022-33/wat.paths.gz"
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    input_data_local = sc.textFile(s3_path).collect()
    input_data = sc.parallelize(input_data_local[:100])
    job_id = uuid.uuid4()
    logger.info(f"JOB ID: {job_id}")
    output = input_data.mapPartitionsWithIndex(lambda i,x: read_s3_extract_images_and_write_warc(i, x, base_s3_path=f"s3://tngglue/outputs/{job_id}"))
    print("reading this file:", input_data_local[:100])
    s = time.time()
    res = output.collect()
    e = time.time()
    with fsspec.open("s3://tngglue/outputs/test_output", "w") as f:
        f.write(json.dumps(res))
    print("Took ", e - s, "Seconds")
    print(res)
