import boto
from boto.s3.connection import S3Connection
import boto3, botocore
from mySQL_connect import load_connection_info

s3_info = load_connection_info('./login/.aws', [])
AWS_ACCESS_KEY_ID = s3_info['access_key']
AWS_SECRET_ACCESS_KEY = s3_info['secret_key']

def boto3_connection():
    s3_info = load_connection_info('./login/.aws', [])
    AWS_ACCESS_KEY_ID = s3_info['access_key']
    AWS_SECRET_ACCESS_KEY = s3_info['secret_key']
    try:
        s3_sess = boto3.resource('s3')\
            # , aws_access_key_id = s3_info['access_key'],
            #                      aws_secret_access_key = s3_info['secret_key'])
        print('session created successfully')
    except Exception as e:
        print(e)
    # try:
    #     s3_cl = boto3.client('s3')\
    #         # , aws_access_key_id = s3_info['access_key'],
    #         #                      aws_secret_access_key = s3_info['secret_key'])
    #     response = s3_cl.list_buckets()
    #     print(response['Buckets'])
    except Exception as e:
        print(e)



def connect_to_s3(s3_info):
    """Inputs: dictionary with AWS primary key and access key"""
    try:
        s3con = S3Connection(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
        print('Connection successful')
        return s3con
    except Exception as e:
        print(e)


def list_buckets(s3con):
    listbuckets = s3con.get_all_buckets()
    print(listbuckets)


def list_bucket_contents(s3con, bucket):
    bucket = s3con.get_bucket(bucket)
    contents = bucket.list()
    for item in contents:
        print(item)

def get_file(s3con, bucketname, folder, filename):
    bucket = s3con.get_bucket(bucketname)
    # path = '/' + folder + '/' + filename
    # print(path)
    key = bucket.get_key('/' + folder + '/' + filename)
    key.get_contents_to_filename(filename)


def close_s3_connection(con):
    con.close()
    print('Connection closed')


def __main__():
    s3_info = load_connection_info('./login/.aws', [])
    s3con = connect_to_s3(s3_info)
    list_bucket_contents(s3con, 'nt-augmedix-demo')
    get_file(s3con, 'nt-augmedix-demo', 'new-folder', 'ee_log.csv')
    close_s3_connection(s3con)

if __name__ == '__main__':
    # boto3_connection()
    __main__()

    # bucket = con.get_bucket('nt-augmedix-demo')
    # try:
    #     listbuckets = con.get_all_buckets()
    #     print(listbuckets)
    # finally:
    #     close_s3_connection(con)
    # print(bucket.list())

