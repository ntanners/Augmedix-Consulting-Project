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
    # workers parameter is not used but is included in order to allow migrate_table to call either api-calling function
    es = Elasticsearch(CONNECTION_IP[connection], port=PORT)
    response = helpers.bulk(es, actions_list)
    return [response]


def submit_parallel_es_requests(connection, workers, actions_list):
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
    worker_end = int(round(len(actions_list) / workers))  # each document in actions_list has 2 rows
    ip_cycle = cycle(ip_list)
    for i in range(workers):
        worker_actions_list = actions_list[worker_start:worker_end]
        worker_actions = "\n".join([json.dumps(x) for x in worker_actions_list]) + "\n"
        worker_start = worker_end
        worker_end = min(worker_end + int(round(len(actions_list) / workers)), len(actions_list))
        ip = next(ip_cycle)
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
    # Setup steps: create index, get column names (for action generation), initialize variables
    t0 = time.time()
    create_index(connection, cur, table)
    col_names = get_colnames(cur, table)
    t1 = time.time()
    setup_time = t1 - t0
    start = 0
    nrows = limit
    sql_time = 0
    actions_time = 0
    es_time = 0
    batch_size = batch_size * workers  # make sure that each worker has the optimal batch_size
    result = []
    index_name = table + '_index'
    doc_type_name = 'record'
    # Loop through the table in batches.  For each loop, create actions for the Elasticsearch bulk API and
    # submit those actions to the API.
    while nrows > 0:
        t2 = time.time()
        nresults, cur = interval_query(cur, table, start, batch_size)
        t3 = time.time()
        sql_time += t3 - t2
        if nresults == 0:
            break
        actions_list = actions_func(nresults, cur, col_names, index_name, doc_type_name)
        t4 = time.time()
        actions_time += t4 - t3
        result = result + api_func(connection, workers, actions_list)
        t5 = time.time()
        es_time += t5 - t4
        nrows -= nresults
        start += nresults
    return (setup_time, sql_time, actions_time, es_time, sum([setup_time, sql_time, actions_time, es_time]))


def create_index(connection, cur, table):
    mapping = generate_mapping(cur, table, 'record')
    es = Elasticsearch(CONNECTION_IP[connection], port=PORT)
    index_name = table + "_index"
    if es.indices.exists(index_name):
        es.indices.delete(index_name)
    response = es.indices.create(index=index_name, ignore=400, body=mapping)
    print(response)


def generate_json(num_rows, cur, colnames, index_name, doc_type_name):
    num_rows = min(num_rows, cur.rowcount)
    body = []
    header = {"index": {"_index": index_name, "_type": doc_type_name}}
    for i in range(num_rows):
        line = cur.fetchone()
        content = {}
        for j, item in enumerate(line):
            content[colnames[j]] = str(item)
        body.append(header)
        body.append(content)
    return body


def generate_bulk_actions_list(num_rows, cur, col_names, index_name, doc_type_name):
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
    schema = read_schema_from_db(cur, table)
    properties = ""
    for i, col in enumerate(schema):
        col_name = '"' + col[0] + '"'
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
    # Generate graph of benchmark results
    sizecats = range(len(sizes))
    plt.bar(sizecats, total_speeds, align='center', width=0.4, color='orangered', label='Elasticsearch Import Speeds')
    plt.xlabel('Batch Size (# documents)', size='large')
    plt.ylabel('Records per Second', size='large')
    plt.ylim((0, round(max(total_speeds), -3) * 1.2))
    plt.xticks(sizecats, sizes)
    plt.legend(loc=2)
    plt.show()


def benchmark_workers(connection, cur, table, low_tests, high_tests, batch_size, limit):
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
    # Generate graph of benchmark results
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
