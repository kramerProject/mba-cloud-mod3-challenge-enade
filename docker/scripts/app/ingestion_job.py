import zipfile
import requests
from io import BytesIO
import os
import sys

import boto3

sys.path.insert(0, './config')
sys.path.insert(0, './helpers')
from config.aws import LANDING_BUCKET, ENADE_FOLDER, DOWNLOAD_URL
from helpers.parser import parse_file_names

def downloader(url):
    try:
        os.makedirs(ENADE_FOLDER, exist_ok=True)
        file_bytes = BytesIO(
            requests.get(DOWNLOAD_URL, verify=False).content
        )
        myzip = zipfile.ZipFile(file_bytes)
        myzip.extractall(ENADE_FOLDER)
        return True
    except:
        return False

def s3_upload(bucket_name, source_file_path, destination_file):

    s3_client = boto3.client('s3')
    s3_client.upload_file(source_file_path, bucket_name, destination_file)
    print(f"file: {destination_file} uploaded to bucket: {bucket_name} successfully")


if __name__ == "__main__":
    print("Starting Job")
    print(f"DOWNLOADING FILES....")
    downloader(DOWNLOAD_URL)
    print("Sending to S3")
    for file in parse_file_names(ENADE_FOLDER):
        print("UPLOADING----->", file)
        src_path = f"./enade2017/microdados_Enade_2017_LGPD/2.DADOS/{file}"
        s3_upload(LANDING_BUCKET, src_path, f"enade2017/{file}")

    print("Done!")