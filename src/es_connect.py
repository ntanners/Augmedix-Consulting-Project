"""
This module includes functions for connecting to an Elasticsearch cluster, creating mappings for indexing
documents, using the Elasticsearch Bulk API to move tables from mySQL to Elasticsearch (both with a single
API call process and with parallel processes) and running benchmark tests on batch size and parallel
process workers.

Functions:
    submit_single_bulk_api: indexes records by submitting a single call to the Elasticsearch Bulk API
    submit_parallel_es_requests: indexes records by submitting parallel calls to the Elasticsearch Bulk API
    migrate_table: migrates a table from MySQL to Elasticsearch using either a single API call or parallel calls
    create_index: creates a new index in Elasticsearch
    generate_json: generates actions to be used in parallel Elasticsearch Bulk API calls
    generate_bulk_actions_list: generates actions to be used in a single Elasticsearch Bulk API call
    generate_mapping: generates mapping for a table to be indexed in Elasticsearch
    benchmark_import_size: runs benchmark tests to determine the optimal batch size for the Elasticsearch bulk API
    benchmark_workers: runs benchmark tests to determine the optimal number of workers for Elasticsearch bulk API calls
    main: uses argparse to set up a command line interface for this module
"""


from elasticsearch import Elasticsearch, helpers
from mySQL_connect import load_connection_info, rds_mysql_connection, close_connection, get_colnames, interval_query, \
    read_schema_from_db
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import requests
import matplotlib.pyplot as plt
import pdb
import json
from argparse import ArgumentParser
from itertools import cycle

CONNECTION_IP = {'standalone': ['ec2-54-236-6-174.compute-1.amazonaws.com'],
                 'cluster': ['ec2-174-129-84-154.compute-1.amazonaws.com',
                             'ec2-54-210-157-133.compute-1.amazonaws.com',
                             'ec2-54-164-6-26.compute-1.amazonaws.com',
                             'ec2-54-234-235-235.compute-1.amazonaws.com'],
                 'cluster1': ['ec2-174-129-84-154.compute-1.amazonaws.com'],
                 'cluster2': ['ec2-54-210-157-133.compute-1.amazonaws.com'],
                 'cluster3': ['ec2-54-164-6-26.compute-1.amazonaws.com'],
                 'cluster4': ['ec2-54-234-235-235.compute-1.amazonaws.com']}
PORT = 9200


def submit_single_bulk_api(connection, workers, actions_list):
    """ Imports records from a table into Elasticsearch by submitting a single call to the Elasticsearch Bulk API.
        Args:
            connection (string): name of the Elasticsearch connection to be used
            workers (integer): not used in this function.  Included so that migrate_table can call either
                this function or submit_parallel_es_requests
            actions_list (list of json objects): list of actions to send to the Elasticsearch Bulk API
        Returns:
            list containing the response returned from the Bulk API call
    """
    es = Elasticsearch(CONNECTION_IP[connection], port=PORT)
    response = helpers.bulk(es, actions_list)
    return [response]


def submit_parallel_es_requests(connection, workers, actions_list):
    """ Imports a table into Elasticsearch by submitting parallel calls to the Elasticsearch Bulk API.
        Args:
            connection (string): name of the Elasticsearch connection to be used
            workers (integer): number of parallel workers to make Bulk API calls with
            actions_list (list of json objects): list of actions to send to the Elasticsearch Bulk API
        Returns:
            list of warnings returned if too many API call have been submitted (empty list otherwise)
    """
    flags = []
    # Initialize variables used for concurrent futures
    index = actions_list[0]['index']['_index']
    ip_list = ['http://' + ip + ':9200/' + index + '/_bulk' for ip in
               CONNECTION_IP[connection]]
    tpool = ThreadPoolExecutor(max_workers=workers)
    func = requests.post
    futures = []
    # Set up for workers loop
    worker_start = 0
    worker_end = int(round(len(actions_list) / workers))
    # Cycle through the different Elasticsearch nodes in the cluster
    ip_cycle = cycle(ip_list)
    for i in range(workers):
        # get a slice of actions for each worker
        worker_actions_list = actions_list[worker_start:worker_end]
        # convert the list of json objects to a single '\n'-delimited json object
        worker_actions = "\n".join([json.dumps(x) for x in worker_actions_list]) + "\n"
        worker_start = worker_end
        worker_end = min(worker_end + int(round(len(actions_list) / workers)), len(actions_list))
        ip = next(ip_cycle)
        # add a bulk API call to the thread pool, using the 'headers' keyword argument to specify the type of
        # json document being used.
        futures.append(tpool.submit(func, ip, data=worker_actions, headers={"Content-type": "application/x-ndjson"}))
    for f in as_completed(futures):
        try:
            result = f.result()
            # Check to see if too many API calls have been submitted
            if result.status_code == 429:
                flags.append(result)
        except Exception as exc:
            print('exception: {}'.format(exc))
            pdb.set_trace()
    return flags


def migrate_table(connection, cur, table, workers, batch_size, limit, actions_func, api_func):
    """ Migrates a table into Elasticsearch using the Elasticsearch Bulk API.  It can use a single worker
        making API calls or multiple workers making API calls in parallel, based on the value of api_func.
        Args:
            connection (string): name of the Elasticsearch connection to be used
            cur (cursor object): MySQL cursor object that is connected to the MySQL database
                where records will be pulled from
            table (string): the name of the table to be indexed in Elasticsearch
            workers (integer): number of parallel workers to use in Bulk API calls (only used in the parallel
                API calls approach)
            batch_size (integer): number of records to send to Elasticsearch in each Bulk API call
            limit (integer): total number of records to migrate from MySQL to Elasticsearch
            actions_func (function): function to use to generate actions for the Bulk API call.  Can be either
                generate_json (for the parallel API calls approach) or generate_bulk_actions_list (for the
                non-parallel approach)
            api_func (function): function to use to make calls to the Elasticsearch Bulk API.  Can be either
                submit_parallel_es_requests (for the parallel API calls approach) or submit_single_bulk_API
                (for the non-parallel approach)
        Returns:
            tuple: contains time required to set up the migration process (setup_time), time required to
                query data from the table in the MySQL database (sql_time), time required to generate
                actions for all Bulk API calls (actions_time), time required for Elasticsearch to index
                records (es_time), and the total time for the whole process.
    """

    # Setup steps: create index, get column names (for action generation), initialize variables
    t0 = time.time()
    create_index(connection, cur, table)
    col_names = get_colnames(cur, table)
    t1 = time.time()
    setup_time = t1 - t0
    start = 0
    num_rows = limit
    sql_time = 0
    actions_time = 0
    es_time = 0
    batch_size = batch_size * workers  # make sure that each worker has the optimal batch_size
    result = []
    index_name = table + '_index'
    doc_type_name = 'record'
    # Loop through the table in batches.  For each loop, create actions for the Elasticsearch bulk API and
    # submit those actions to the API.
    while num_rows > 0:
        t2 = time.time()
        num_results, cur = interval_query(cur, table, start, batch_size)
        t3 = time.time()
        sql_time += t3 - t2
        if num_results == 0:
            break
        actions_list = actions_func(num_results, cur, col_names, index_name, doc_type_name)
        t4 = time.time()
        actions_time += t4 - t3
        result += api_func(connection, workers, actions_list)
        t5 = time.time()
        es_time += t5 - t4
        num_rows -= num_results
        start += num_results
    return (setup_time, sql_time, actions_time, es_time, sum([setup_time, sql_time, actions_time, es_time]))


def create_index(connection, cur, table):
    """ Creates a new index in Elasticsearch.  If the index already exists, this function deletes it
        before creating a new index.
        Args:
            connection (string): name of the Elasticsearch connection to be used
            cur (cursor object): MySQL cursor object that is connected to the MySQL database where records
                will be pulled from
            table (string): name of the table to be indexed in Elasticsearch
    """
    mapping = generate_mapping(cur, table, 'record')
    es = Elasticsearch(CONNECTION_IP[connection], port=PORT)
    index_name = table + "_index"
    if es.indices.exists(index_name):
        es.indices.delete(index_name)
    response = es.indices.create(index=index_name, ignore=400, body=mapping)
    print(response)


def generate_json(num_rows, cur, col_names, index_name, doc_type_name):
    """ Generates a list of json objects to be used by submit_parallel_es_requests(), which uses the Requests
        Python library to submit parallel calls to the Elasticsearch Bulk API.
        Args:
            num_rows (integer): number of rows in the MySQL table to be indexed by Elasticsearch
            cur (cursor object): MySQL cursor object that holds the result of the query pulling records from
                the table in the MySQL database
            col_names (list of strings): names of columns in the table in the MySQL database
            index_name (string): name of the index in Elasticsearch where records will be added
            doc_type_name (string): name of the document type in Elasticsearch where records will be added
        Returns:
            list of json objects (Python dictionaries)
    """
    num_rows = min(num_rows, cur.rowcount)
    body = []
    header = {"index": {"_index": index_name, "_type": doc_type_name}}
    for i in range(num_rows):
        line = cur.fetchone()
        content = {}
        for j, item in enumerate(line):
            content[col_names[j]] = str(item)
        body.append(header)
        body.append(content)
    return body


def generate_bulk_actions_list(num_rows, cur, col_names, index_name, doc_type_name):
    """ Generates a list of actions to be used by submit_single_bulk_api(), which uses the Bulk API
        helpers Python library.
        Args:
            num_rows (integer): number of rows in the MySQL table to be indexed by Elasticsearch
            cur (cursor object): MySQL cursor object that holds the result of the query pulling records from
                the table in the MySQL database
            col_names (list of strings): names of columns in the table in the MySQL database
            index_name (string): name of the index in Elasticsearch where records will be added
            doc_type_name (string): name of the document type in Elasticsearch where records will be added
        Returns:
            list of json objects (Python dictionaries)
    """
    actions = []
    num_rows = min(num_rows, cur.rowcount)
    for i in range(num_rows):
        line = cur.fetchone()
        content = {}
        for j, item in enumerate(line):
            content[col_names[j]] = str(item)
        actions.append({
            "_index": index_name,
            "_type": doc_type_name,
            "_source": content
        })
    return actions


def generate_mapping(cur, table, doc_type_name):
    """ Generates mapping to be used in creating an index in Elasticsearch.
        Args:
            cur (cursor object): MySQL cursor object that is connected to the database where the table to be
                mapped currently is
            table (string): name of the table to be mapped
            doc_type_name (string): name to be used for the doc_type assigned to this table in Elasticsearch
        Returns:
            json object (Python library) with mapping information for Elasticsearch
    """
    schema = read_schema_from_db(cur, table)
    properties = ""
    for i, col in enumerate(schema):
        col_name = '"' + col[0] + '"'
        # sets different data types if the schema indicates that the field is an integer or a date
        if 'int' in col[1]:
            dtype = '"integer"'
        elif col[1] == 'datetime':
            dtype = '"date",\n"format": "yyyy-MM-dd HH:mm:ss"'
        else:
            dtype = '"text"'
        comma = ',' if i < len(schema) - 1 else ''
        properties += (col_name + ': {\n"type": ' + dtype + '\n}' + comma + '\n')
    mapping = '''{"mappings":{\n"''' + doc_type_name + '''":{\n"properties":{\n''' + properties + '}\n}\n}\n}'
    return mapping


def benchmark_import_size(connection, cur, table, low_tests, high_tests):
    """ Performs benchmark tests to determine the optimal batch size to use for Elasticsearch Bulk API calls.
        For benchmarking purposes, it is recommended to connect to a single-node Elasticsearch cluster.

        Args:
            connection (string): name of the Elasticsearch connection to be used
            cur (cursor object): MySQL cursor object for accessing the table to be migrated
            table (string): name of the table to be migrated
            low_tests (integer): starting point of the range to use for batch size (2 is raised to this power)
            high_tests (integer): ending point of the range to use for batch size (2 is raised to this power)
        Returns:
            shows a bar graph plotting the total indexing speed against the batch size used for API calls.
    """
    import_times = []
    total_speeds = []
    sizes = []
    # Loop to conduct benchmark tests on different batch sizes
    for i in range(low_tests, high_tests + 1):
        batch_size = 100 * (2 ** i)
        print('beginning import of {} records'.format(batch_size))
        times = migrate_table(connection, cur, table, 1, batch_size, batch_size, generate_bulk_actions_list,
                              submit_single_bulk_api)
        import_times.append(times[4])
        sizes.append(batch_size)
        total_speeds.append(batch_size / times[4])
    # Generate bar chart of benchmark results
    sizecats = range(len(sizes))
    plt.bar(sizecats, total_speeds, align='center', width=0.4, color='orangered', label='Elasticsearch Import Speeds')
    plt.xlabel('Batch Size (# documents)', size='large')
    plt.ylabel('Records per Second', size='large')
    plt.ylim((0, round(max(total_speeds), -3) * 1.2))
    plt.xticks(sizecats, sizes)
    plt.legend(loc=2)
    plt.show()


def benchmark_workers(connection, cur, table, low_tests, high_tests, batch_size, limit):
    """ Performs benchmark tests to determine the optimal number of workers to use in making parallel Elasticsearch
        Bulk API calls.
        Args:
            connection (string): name of the Elasticsearch connection to be used
            cur (cursor object): MySQL cursor object for accessing the table to be migrated
            table (string): name of the table to be migrated
            low_tests (integer): starting point of the range to use for the number of workers
            high_tests (integer): ending point of the range to use for the number of workers
            batch_size (integer): batch size to be used in each test
            limit: total number of records to be imported during each test
        Returns:
            shows a bar graph plotting the total indexing speed against the number of workers used for parallel
            API calls
    """
    # Runs benchmark tests on different numbers of parallel workers making Elasticsearch Bulk API calls.
    # Produces a bar chart plotting impot speeds against numbers of workers.
    import_times = []
    num_workers = []
    total_speeds = []
    # Loop to conduct benchmark tests on different numbers of parallel workers
    for i in range(low_tests, high_tests + 1):
        print('beginning import of {} records with {} workers'.format(limit, i))
        times = migrate_table(connection, cur, table, i, batch_size, limit, generate_json, submit_parallel_es_requests)
        import_times.append(times[4])
        num_workers.append(i)
        total_speeds.append(round(limit / (times[4]), 2))
    # Generate bar chart of benchmark results
    worker_categories = range(len(num_workers))
    plt.bar(worker_categories, total_speeds, align='center', width=0.4, color='orangered',
            label='Elasticsearch Import Speeds')
    plt.xlabel('Number of Import Processes', size='large')
    plt.ylabel('Records per Second', size='large')
    plt.ylim((0, round(max(total_speeds), -3) * 1.2))
    plt.xticks(worker_categories, num_workers)
    plt.legend(loc=2)
    plt.show()


def main():
    """
    Uses argparser to implement a command line interface for the functions in this module.
    """
    # Connect to RDS
    rds_info = load_connection_info('./login/.rds', ['port'])
    con, cur = rds_mysql_connection(rds_info)
    cur.execute("""SHOW TABLES""")
    table_list = [i[0] for i in cur.fetchall()]

    # Use argparser to parse command line arguments
    parser = ArgumentParser(description='Augmedix Elasticsearch Project CLI')
    parser.add_argument('-c', '--connection', default='cluster', choices=['standalone', 'cluster', 'cluster1',
                                                                          'cluster2', 'cluster3', 'cluster4'],
                        help='Select the Elasticsearch instance to connect to')
    parser.add_argument('-t', '--table', default='ee_audit_events', choices=table_list, help='Select the table to '
                                                                                             'import to Elasticsearch')

    subparser_base = parser.add_subparsers(title='actions', description='Choose an action')
    sp = subparser_base.add_parser('workertest')
    sp.set_defaults(which='workertest')
    sp.add_argument('-b', '--batch_size', type=int, default=5000, help="number of documents to be imported to "
                                                                       "Elasticsearch in each batch")
    sp.add_argument('-l', '--limit', type=int, default=20000, help="total number of rows to be moved from MySQL to "
                                                                   "Elasticsearch")
    sp.add_argument('-lt', '--low_tests', type=int, default=1, help='starting number to test with for benchmarking')
    sp.add_argument('-ht', '--high_tests', type=int, default=4, help='stopping number to test with for benchmarking')

    sp = subparser_base.add_parser('sizetest')
    sp.set_defaults(which='sizetest')
    sp.add_argument('-lt', '--low_tests', type=int, default=1, help='starting number to test with for benchmarking')
    sp.add_argument('-ht', '--high_tests', type=int, default=3, help='stopping number to test with for benchmarking')

    sp = subparser_base.add_parser('parallel')
    sp.set_defaults(which='parallel')
    sp.add_argument('-b', '--batch_size', type=int, default=5000, help="number of documents to be imported to "
                                                                       "Elasticsearch in each batch")
    sp.add_argument('-l', '--limit', type=int, default=20000, help="total number of rows to be moved from MySQL to "
                                                                   "Elasticsearch")
    sp.add_argument('-w', '--workers', type=int, default=4, help="number of workers that send index requests"
                                                                 "to the Elasticsearch bulk API")

    sp = subparser_base.add_parser('migrate')
    sp.set_defaults(which='migrate')
    sp.add_argument('-b', '--batch_size', type=int, default=5000, help="number of documents to be imported to "
                                                                       "Elasticsearch in each batch")
    sp.add_argument('-l', '--limit', type=int, default=20000, help="total number of rows to be moved from MySQL to "
                                                                   "Elasticsearch")
    sp.add_argument('-w', '--workers', type=int, default=1, help="number of workers that send index requests "
                                                                 "to the Elasticsearch bulk API")

    args = vars(parser.parse_args())
    args['cur'] = cur
    action = args['which']
    del args['which']

    if action == 'sizetest':
        # Performs a benchmarking test on the bulk API on the standalone elasticseach cluster using
        # different batch sizes
        benchmark_import_size(**args)
    elif action == 'workertest':
        # Performs a benchmarking test on the parallel import approach using different numbers of workers
        benchmark_workers(**args)
    elif action == 'migrate':
        # Migrates a table (up to <limit>) using the Elasticsearch Bulk API
        args['actions_func'] = generate_bulk_actions_list
        args['api_func'] = submit_single_bulk_api
        times = migrate_table(**args)
        print('Setup time: {:.2f} s, SQL query time: {:.2f}s , actions prep time: {:.2f} s, ES API time: {:.2f} s, '
              'total time: {:.2f} s'.format(*times))
    elif action == 'parallel':
        # Runs a faster table migration by implementing a number of parallel API calls
        args['actions_func'] = generate_json
        args['api_func'] = submit_parallel_es_requests
        times = migrate_table(**args)
        print('Setup time: {:.2f} s, SQL query time: {:.2f}s , actions prep time: {:.2f} s, ES API time: {:.2f} s, '
              'total time: {:.2f} s'.format(*times))

    close_connection(con, cur)


if __name__ == '__main__':
    main()
