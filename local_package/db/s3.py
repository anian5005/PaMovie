# Standard library imports
import boto3
import os

# Local application imports
from dotenv import load_dotenv


load_dotenv()
S3_REGION = os.getenv('S3_REGION')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

def init_s3():

    s3 = boto3.resource('s3',
                        aws_access_key_id=S3_ACCESS_KEY,
                        aws_secret_access_key=S3_SECRET_KEY,
                        region_name=S3_REGION)
    return s3


def put_photo_to_s3(photo_name=None, photo_stream=None):
    s3 = init_s3()
    bucket_name = S3_BUCKET_NAME
    bucket = s3.Bucket(bucket_name)
    tags = "public=yes"

    s3_folder = 'movie/poster/' + photo_name
    bucket.put_object(Key=s3_folder, Body=photo_stream, Tagging=tags)
