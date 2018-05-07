import boto
import boto3, botocore
from connect_MySQL import load_connection_info

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
    try:
        s3_cl = boto3.client('s3')\
            # , aws_access_key_id = s3_info['access_key'],
            #                      aws_secret_access_key = s3_info['secret_key'])
        response = s3_cl.list_buckets()
        print(response['Buckets'])
    except Exception as e:
        print(e)



def connect_to_s3(s3_info):
    """Inputs: dictionary with AWS primary key and access key"""
    try:
        con = S3Connection(s3_info['access_key'],s3_info['secret_key'])
        print('Connection successful')
        return con
    except Exception as e:
        print(e)


def close_s3_connection(con):
    con.close()
    print('Connection closed')


if __name__ == '__main__':
    boto3_connection()
    # s3_info = load_connection_info('./login/.aws', [])
    # con = connect_to_s3(s3_info)

    # bucket = con.get_bucket('nt-augmedix-demo')
    # try:
    #     listbuckets = con.get_all_buckets()
    #     print(listbuckets)
    # finally:
    #     close_s3_connection(con)
    # print(bucket.list())

