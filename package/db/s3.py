import boto3
from db_setting import connect_set
from package.db.sql import mark_column
from package.db.sql import create_conn_pool, get_connection, sql_select_column
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

    s3_folder = 'movie/poster/' + photo_name
    bucket.put_object(Key=s3_folder, Body=photo_stream, Tagging=tags)


# put_photo_to_s3(s3=init_s3(connex), photo_name='testV4.jpg')

def s3_photo_filter():
    sql_conn_pool = create_conn_pool()
    sql_conn = get_connection(sql_conn_pool)
    sql_tb_movie_id_mapping = sql_select_column('movie_id_mapping', ['douban_id', 'imdb_id'], sql_conn, struct='dict', condition='')
    douban_mapping_imdb = {i['douban_id']: i['imdb_id'] for i in sql_tb_movie_id_mapping}
    # print('douban_mapping_imdb', douban_mapping_imdb )
    s3 = init_s3(connex)
    bucket_name = connex['bucket_name']
    my_bucket = s3.Bucket(bucket_name)
    path = 'movie/poster/'
    photos = my_bucket.objects.filter(Prefix=path)

    total_count = 0
    empty_count = 0
    for photo in photos:
        total_count = total_count + 1
        if photo.size == 0:

            movie_id = photo.key.replace(path, '').replace('_main.webp', '')
            if 'tt' not in movie_id:
                douban_id = movie_id
                try:
                    imdb_id = douban_mapping_imdb[douban_id]
                    print(douban_id, imdb_id)
                    empty_count = empty_count + 1
                    mark_column(sql_conn, 'imdb_movie_id', 'douban_img', 2, 'imdb_id', imdb_id)
                except:
                    pass

    print('total_count', total_count)
    print('empty_count', empty_count)
    sql_conn.close()
    sql_conn_pool.dispose()

# s3_photo_filter()
