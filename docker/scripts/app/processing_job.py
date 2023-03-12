"""
Processing JOB
"""

import sys
import pyspark.sql.functions as f
sys.path.insert(0, './helpers')
from helpers.spark import get_spark_session
from helpers.parser import parse_file_names
sys.path.insert(0, './config')
from config.aws import LANDING_BUCKET, PROCESSING_BUCKET, ENADE_FOLDER
from pyspark.sql import (
    SparkSession, 
    DataFrame
)
import re

 
def read_csv(spark: SparkSession, bucket: str, file_name: str) -> DataFrame:
    """Read CSV files from S3 bucket"""
    print("Reading CSV for file", file_name)

    df = (
        spark
        .read
        .format("csv")
        .options(header='true', inferSchema='true', delimiter=';')
        .load(f"s3a://{bucket}/enade2017/{file_name}")
    )
    return df


def write_parquet(bucket: str, df: DataFrame, directory_name: str) -> DataFrame:
    """Write PARQUET files to S3 bucket"""
    print("Saving parquet for file", directory_name)
    df_path = f"s3a://{bucket}/enade2017/{directory_name}"
    print(f"Path: {df_path}")
    (
        df
        .write
        .format("parquet")
        .mode("overwrite")
        .save(df_path)
    )
    return True


def parse_directory_name(fl_name: str) -> str:
    return fl_name.split("microdados2017_")[1].split(".txt")[0]


if __name__ == "__main__":
    """Main ETL script definition."""
    spark = get_spark_session()

    print("Processing data from landing to processing now. ")
    
    for fl_name in parse_file_names(ENADE_FOLDER):
        df = read_csv(spark, LANDING_BUCKET, fl_name)
        print("Done ----->", fl_name)
        print("Writing Dataframe!")
        write_parquet(PROCESSING_BUCKET, df, parse_directory_name(fl_name))
        print("Done!")
    spark.stop()