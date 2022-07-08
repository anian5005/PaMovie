import os
import boto3
from db_setting import connect_set

connex = connect_set.s3.set

def init_s3(connex):

    region = connex['region']
    access_key = connex['access_key']
    secret_key = connex['secret_key']

    s3 = boto3.resource('s3',
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                        region_name=region)
    return s3


def put_photo_to_s3(photo_name=None, photo_stream=None):
    s3 = init_s3(connex)
    bucket_name = connex['bucket_name']
    bucket = s3.Bucket(bucket_name)
    tags = "public=yes"

    # with open(os.path.join('../static/img/poster', photo_name), 'rb') as f:
    #     photo_stream = f.read()
    s3_folder = 'movie/' + photo_name
    bucket.put_object(Key=s3_folder, Body=photo_stream, Tagging=tags)


# put_photo_to_s3(s3=init_s3(connex), photo_name='testV4.jpg')


