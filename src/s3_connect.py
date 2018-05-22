from boto.s3.connection import S3Connection
from argparse import ArgumentParser
from mySQL_connect import load_connection_info

LOGIN_PATH = '../login/.aws'


def connect_to_s3(access_key, secret_key):
    """Creates a connection to S3.
    Inputs: AWS primary key and access key"""
    try:
        s3con = S3Connection(access_key, secret_key)
        print('Connection successful')
        return s3con
    except Exception as e:
        print(e)


def list_buckets(s3con):
    """Lists all of the buckets in an S3 connection"""
    listbuckets = s3con.get_all_buckets()
    print(listbuckets)


def list_bucket_contents(s3con, bucket):
    """Lists all of the contents of an S3 bucket"""
    bucket = s3con.get_bucket(bucket)
    contents = bucket.list()
    for item in contents:
        print(item)


def get_file(s3con, bucket, folder, filename):
    """Downloads a file from an S3 bucket into the local directory
    Inputs: connection, bucket name, folder (full path) in the bucket, filename"""
    bucket_obj = s3con.get_bucket(bucket)
    key = bucket_obj.get_key('/' + folder + '/' + filename)
    key.get_contents_to_filename(filename)


def close_s3_connection(con):
    con.close()
    print('Connection closed')


def __main__():
    # Connect to S3
    s3_info = load_connection_info(LOGIN_PATH, [])
    s3con = connect_to_s3(s3_info['access_key'], s3_info['secret_key'])

    # Use ArgumentParser to parse command line arguments
    parser = ArgumentParser(description='Augmedix project S3 functions CLI')
    subparser_base = parser.add_subparsers(title='actions', description='Choose an action')

    sp = subparser_base.add_parser('list_buckets')
    sp.set_defaults(which='list_buckets')

    sp = subparser_base.add_parser('bucket_contents')
    sp.set_defaults(which='bucket_contents')
    sp.add_argument('-b', '--bucket', default='nt-augmedix-demo', help='bucket to list contents for')

    sp = subparser_base.add_parser('get_file')
    sp.set_defaults(which='get_file')
    sp.add_argument('-b', '--bucket', default='nt-augmedix-demo', help='bucket to download file from')
    sp.add_argument('-f', '--folder', default='new-folder', help='full path of folder where file lies')
    sp.add_argument('-n', '--filename', default='doctor.csv', help='name of file to be downloaded')

    args = vars(parser.parse_args())
    args['s3con'] = s3con
    action = args['which']
    del args['which']

    if action == 'list_buckets':
        list_buckets(**args)
    elif action == 'bucket_contents':
        list_bucket_contents(**args)
    elif action =='get_file':
        get_file(**args)

    close_s3_connection(s3con)


if __name__ == '__main__':
    __main__()
