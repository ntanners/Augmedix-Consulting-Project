from datetime import datetime

from elasticsearch import Elasticsearch, helpers
from mySQL_connect import load_connection_info, rds_mySQL_connection, close_connection, get_colnames, interval_query, \
    read_schema_from_db
from concurrent.futures import ThreadPoolExecutor, as_completed
import time, sys, requests
import matplotlib.pyplot as plt
import pdb
import json, codecs

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
TABLE = 'ee_audit_events_orig'
INDEX = TABLE
TYPE = 'record'
BATCHSIZE = 200000
ES_IMPORT_TIMES = []

def es_connection(ip):
    es = Elasticsearch(ip, port=PORT)
    return es


def delete_index(es, index):
    return es.indices.delete(index=index)


def sizetest_import(es, cur, table, testsize):
    colnames = get_colnames(cur, table)
    mySQL_start_time = time.time()
    cur.execute("""SELECT * FROM {} LIMIT {}""".format(table, testsize))
    mySQL_end_time = time.time()
    mySQL_query_time = mySQL_end_time-mySQL_start_time
    actions = generate_bulk_actions(testsize, cur, colnames, table+'_index', 'test_type')
    list_actions = list(actions)
    es_start_time = time.time()
    insert_response = bulk_insert(es, list_actions)
    es_end_time = time.time()
    memsize = sys.getsizeof(list_actions)
    es_import_time = es_end_time-es_start_time
    print('ES import time = {:.2f} seconds'.format(es_import_time))
    print('ES import speed = {:.0f} records per second'.format(testsize/(es_import_time)))
    print(insert_response)
    return [testsize, mySQL_query_time, testsize/mySQL_query_time, es_import_time, testsize/es_import_time, memsize]


def migrate_table(es, cur, table, batchsize, limit, benchmark=False):
    colnames = get_colnames(cur, table)
    cur.execute("""SELECT COUNT(*) FROM {}""".format(table))
    nrows = min(limit, cur.fetchone()[0])
    start_point = 0
    import_times = []
    while nrows > 0:
        # print('retrieving records {} to {}'.format(start_point, start_point + min(nrows, batchsize)))
        sql_start = time.time()
        nresults = cur.execute(
            """SELECT * FROM {} LIMIT {},{}""".format(table, start_point, min(nrows, batchsize)))
        sql_end = time.time()
        actions_start = time.time()
        actions = generate_bulk_actions_list(int(nresults), cur, colnames, table + "_index", TYPE)
        actions_end = time.time()
        es_start = time.time()
        insert_response = bulk_insert(es, actions)
        es_end = time.time()
        import_times.append((sql_end-sql_start, actions_end-actions_start, es_end-es_start))
            # print("ES insert time: {:.2f} s. ES insert speed: {:.6f} records/s".format(es_end-es_start,
            #                                                                           nresults/(es_end-es_start)))
        nrows -= batchsize
        start_point += batchsize
    return [sum(x) for x in zip(*import_times)]


def parallel_requests_migrate(ip_list, workers, json_list, batchsize):
    tpool = ThreadPoolExecutor(max_workers=workers)
    func = requests.post
    futures = []
    worker_start = 0
    worker_end = int(round(batchsize / workers) * 2)
    flags = []
    for i in range(workers):
        worker_json_list = json_list[worker_start:worker_end]
        try:
            json_start = time.time()
            worker_json = "\n".join([json.dumps(x, codecs.getwriter('utf-8')) for x in worker_json_list]) + "\n"
            json_end = time.time()
            #print(len(worker_json_list)/(json_end-json_start))
        except:
            pdb.set_trace()
        worker_start = worker_end
        worker_end = min(worker_end + int(round(batchsize / workers) * 2), batchsize*2)
        ip = ip_list[i%len(ip_list)]
        futures.append(tpool.submit(func, ip, data=worker_json, headers={"Content-type": "application/x-ndjson"}))
    for f in as_completed(futures):
        try:
            result = f.result()
            if result.status_code == 429: flags.append(result)
        except Exception as exc:
            print('exception: {}'.format(exc))
            pdb.set_trace()
    return flags

def parallel_batch_requests_migrate(ip_list, cur, table, workers, batchsize, limit, benchmark=False):
    colnames = get_colnames(cur, table)
    cur.execute("""SELECT COUNT(*) FROM {}""".format(table))
    nrows = min(limit, cur.fetchone()[0])
    start = 0
    import_times = []
    flags = []
    while nrows > 0:
        # print('executing parallel batch of {} records, starting at {}'.format(min(nrows,batchsize), start))
        sql_start = time.time()
        nresults, cur = interval_query(cur, table, start, batchsize)
        sql_end = time.time()
        json_start = time.time()
        json_list = generate_json(nresults, cur, colnames, table + '_index', 'record')
        json_end = time.time()
        es_start = time.time()
        flags = flags + parallel_requests_migrate(ip_list, workers, json_list, nresults)
        es_end = time.time()
        if benchmark:
            import_times.append((sql_end-sql_start, json_end-json_start, es_end-es_start))
            # print('ES insert time: {:.2f} s. ES insert speed: {:.6f} records/s'.format(es_end - es_start,
            #             min(nrows,batchsize)/(es_end - es_start)))
        nrows -= batchsize
        start += batchsize
    if benchmark:
        return [sum(x) for x in zip(*import_times)], flags


def generate_json(nrows, cur, colnames, index_name, doc_type_name):
    nrows = min(nrows, cur.rowcount)
    body = []
    header = {"index": {"_index": index_name, "_type": doc_type_name}}
    for i in range(nrows):
        line = cur.fetchone()
        content = {}
        for j, item in enumerate(line):
            # if type(item) is str: # remove unicode characters
            #     item = ''.join(i for i in str(item) if ord(i)<128)
            content[colnames[j]] = str(item)
        body.append(header)
        body.append(content)
    return body

def json_test(cur, table, start, batchsize):
    colnames = get_colnames(cur, table)
    nresults, cur = interval_query(cur, table, start, batchsize)
    print(nresults)
    for i in range(nresults):
        line = cur.fetchone()
        content = {}
        for j, item in enumerate(line):
            # if type(item) is str:
            #     item = ''.join(i for i in str(item) if ord(i)<128)
            body = []
            content[colnames[j]] = str(item)
            body.append(content)
            try:
                test = "\n".join(map(json.dumps, body)) + "\n"
            except:
                print("something didn't work here: {}".format(body))


def generate_bulk_actions(nrows, cur, colnames, index_name, doc_type_name):
    nrows = min(nrows, cur.rowcount)
    for i in range(nrows):
        line = cur.fetchone()
        content = {}
        for j, item in enumerate(line):
            # if colnames[j] == 'doctorDate': continue
            content[colnames[j]] = str(item)
        yield {
            "_index": index_name,
            "_type": doc_type_name,
            "_source": content
        }


def generate_bulk_actions_list(nrows, cur, colnames, index_name, doc_type_name):
    actions = []
    nrows = min(nrows, cur.rowcount)
    for i in range(nrows):
        line = cur.fetchone()
        content = {}
        for j, item in enumerate(line):
            # if colnames[j] == 'doctorDate': continue
            content[colnames[j]] = str(item)
        actions.append({
            "_index": index_name,
            "_type": doc_type_name,
            "_source": content
        })
    return actions


def generate_mapping(cur, table, doc_type_name):
    schema = read_schema_from_db(cur, table)
    properties = ""
    for i,col in enumerate(schema):
        colname = '"'+col[0]+'"'
        if 'int' in col[1]:
            dtype = '"integer"'
        elif 'varchar' in col[1] or 'text' in col[1]:
            dtype = '"text"'
        elif col[1] == 'datetime':
            dtype = '"date",\n"format": "yyyy-MM-dd HH:mm:ss"'
        comma = ',' if i < len(schema)-1 else ''
        properties += (colname + ': {\n"type": ' + dtype +'\n}' + comma + '\n')
    mapping = '''{"mappings":{\n"''' + doc_type_name + '''":{\n"properties":{\n''' + properties +'}\n}\n}\n}'
    return mapping


def new_index(es, index_name, mapping):
    response = es.indices.create(index=index_name, ignore=400, body=mapping)
    return response


def benchmark_import_size(es, cur, table, num_tests):
    index = table + "_index"
    if es.indices.exists(index):
        delete_index(es, index)
    test_results = []
    total_speeds = []
    for i in range(3, num_tests):
        start = time.time()
        batchsize = 100*(2**i)
        result = sizetest_import(es, cur, table, batchsize)
        end = time.time()
        test_results.append(result)
        delete_index(es, index)
        total_speeds.append(batchsize/(end-start))
    sizes, mySql_speeds, es_speeds, memsizes = [], [], [], []
    for result in test_results:
        sizes.append(result[0])
        mySql_speeds.append(result[2])
        es_speeds.append(result[4])
        memsizes.append(result[5])
    print(memsizes, es_speeds, total_speeds)
    sizecats = range(len(sizes))
    plt.bar(sizecats, total_speeds, align='center', width=0.4, color='orangered', label='Elasticsearch Import Speeds')
    plt.xlabel('Batch Size (# documents)', size='large')
    plt.ylabel('Records per Second', size='large')
    plt.ylim((0, round(max(total_speeds), -3)*1.2))
    plt.xticks(sizecats, sizes)
    plt.legend(loc=2)
    plt.show()


def benchmark_workers(ip_list, es, cur, table, num_tests, batchsize, limit):
    index = table+"_index"
    if es.indices.exists(index):
        delete_index(es, index)
    import_times=[]
    num_workers = []
    total_speeds = []
    for i in range(1,num_tests):
        print('beginning import of {} records with {} workers'.format(limit, i))
        start = time.time()
        import_times, flags = parallel_batch_requests_migrate(ip_list, cur, table, i, batchsize*i, limit,
                                                              benchmark=True)
        end = time.time()
        if len(flags) > 0: print(flags)
        es_time = import_times[2]
        delete_index(es, index)
        num_workers.append(i)
        total_speeds.append(round(limit/(end-start), 2))
        print('import took {:.2f} seconds at an average speed of {:.0f} records/s'.format(end-start,
                                                                                          limit/(end-start)))
    worker_categories = range(len(num_workers))
    plt.bar(worker_categories, total_speeds, align='center', width=0.4, color='orangered',
            label='Elasticsearch Import Speeds')
    plt.xlabel('Number of Import Processes', size='large')
    plt.ylabel('Records per Second', size='large')
    plt.ylim((0, round(max(total_speeds), -3)*1.2))
    plt.xticks(worker_categories, num_workers)
    plt.legend(loc=2)
    plt.show()


def bulk_insert(es, actions):
    insert_response = helpers.bulk(es, actions)
    return (insert_response)


def main():
    # Connect to RDS
    rds_info = load_connection_info('./login/.rds', ['port'])
    con, cur = rds_mySQL_connection(rds_info)

    # Retrieve arguments from shell used across multiple selections
    selection = sys.argv[1] # menu selection entered from shell
    connection = sys.argv[2] # elasticsearch instance entered from shell.  Can be 'standalone,' 'cluster' or
    # 'cluster1' through 'cluster4'
    table = sys.argv[3] # mySQL table entered from shell.

    limit = 6000
    batch = 200

    if selection == 'sizetest':
        # Performs a benchmarking test on the bulk API on the standalone elasticseach cluster using
        # different batch sizes
        # Arguments: sizetest standalone <table> <maxbatchsize>
        es = es_connection(CONNECTION_IP[connection])
        benchmark_import_size(es, cur, table, int(sys.argv[4]))
    elif selection == 'workertest':
        # Performs a benchmarking test on the requests import approach using different numbers of workers
        # Arguments: workertest <connection> <table> <maxnumworkers> <batchsize> <tablesize>
        ip_list = ['http://' + connection + ':9200/' + table + "_index/_bulk" for connection in
                   CONNECTION_IP['cluster']]
        es = es_connection(CONNECTION_IP['cluster1'])
        benchmark_workers(ip_list, es, cur, table, int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))
    elif selection == 'delete':
        # Deletes an index, with the naming convention "<table>_index"
        es = es_connection(CONNECTION_IP[sys.argv[2]])
        table = sys.argv[3]
        delete_index(es, table+'_index')
    elif selection == 'migrate':
        # Runs a migration to a single elasticsearch node using the bulk API.
        # Arguments: migrate <connection> <table>
        es = es_connection(CONNECTION_IP[connection])
        index = table+'_index'
        if es.indices.exists(index):
            delete_index(es, index)
        response = new_index(es, index, generate_mapping(cur, table, 'record'))
        # print(response)
        batchsize = int(sys.argv[4])
        limit = int(sys.argv[5])
        start = time.time()
        import_times = migrate_table(es, cur, table, batchsize, limit, benchmark=True)
        end = time.time()
        print('SQL query time: {:.2f}, actions generation time: {:.2f}, ES import time: {:.2f}, total import '
              'time: {:.2f}'.format(import_times[0], import_times[1], import_times[2], end - start))
    elif selection == 'requests':
        # Uses the requests library to run parallel bulk API imports into elasticsearch.
        # Arguments: requests <connection> <table> <workers> <batch> <limit>
        ip_list = ['http://' + connection + ':9200/' + table+"_index/_bulk"
                   for connection in CONNECTION_IP[sys.argv[2]]]
        es_list = [es_connection([connection]) for connection in CONNECTION_IP[sys.argv[2]]]
        index = table + '_index'
        workers = int(sys.argv[4])
        batch = int(sys.argv[5])
        limit = int(sys.argv[6])
        if es_list[0].indices.exists(index):
            delete_index(es_list[0], index)
        response = new_index(es_list[0], index, generate_mapping(cur, table, 'record'))
        # print(response)
        start = time.time()
        import_times, flags = parallel_batch_requests_migrate(ip_list, cur, table, workers, workers*batch,
                                                              limit, benchmark=True)
        end = time.time()
        print('SQL query time: {:.2f}, json generation time: {:.2f}, ES import time: {:.2f}, total import '
              'time: {:.2f}'.format(import_times[0], import_times[1], import_times[2], end-start))
    elif selection == 'mapping':
        mapping = generate_mapping(cur,sys.argv[2],"record")
        print(mapping)
    elif selection == 'new_index':
        es = es_connection(CONNECTION_IP[sys.argv[2]])
        table = sys.argv[3]
        index = table+'_new_index'
        print(index)
        response = new_index(es, table+'_new_index', generate_mapping(cur, table, 'record'))
        print(response)
    elif selection == 'json_test':
        json_test(cur, 'ee_log', 0, 12000)



    # parallel_batch_migrate(cur, 'ee_audit_events_orig', 4, 1000, 10000)
    # print(ES_IMPORT_SPEEDS)
    # es_parallel = [Elasticsearch(ip, port=PORT) for ip in CONNECTION_IP['cluster']]
    # print(es_parallel)
    # for i in range(10):
    #     benchmark_migrate(es, cur, 'ee_audit_events', 100*(2**i))

    # migrate_table(es, cur, TABLE, BATCHSIZE)
    # query = """DESCRIBE doctor"""
    # # """SELECT * FROM doctor"""
    # cur.execute(query)
    # cols = cur.fetchall()
    # colnames = []
    # for col in cols:
    #     colnames.append(col[0])
    # query = """SELECT * FROM doctor LIMIT 5"""
    # numrows = cur.execute(query)
    # delete_response = delete_index(es, 'new_doctors')
    # print(delete_response)
    # actions = generate_bulk_actions(numrows, cur, colnames, INDEX, 'record')
    # insert_response = bulk_insert(es, actions)
    # print(insert_response)
    # delete_response = delete_index(es, 'new_doctors')
    # print(delete_response)
    # for item in test:
    #     print(test)

    # query_result = cur.fetchall()
    # docs = []
    # for doc in query_result:
    #     doc_json = {}
    #     for i,col in enumerate(doc):
    #         doc_json[colnames[i]] = str(col)
    #     docs.append(json.dumps(doc_json))
    # response = requests.post(CONNECTION_IP + ":" + str(PORT) + "/doctors/test", data = docs[0])
    # print(docs)
    # for j, doc_json in enumerate(docs):
    #     es.index(index='doctors', doc_type='records', id=j, body=doc_json)
    close_connection(con, cur)


if __name__ == '__main__':
    main()

# doc = {
#     'author': 'kimchy',
#     'text': 'Elasticsearch: cool. bonsai cool.',
#     'timestamp': datetime.now(),
# }
# res = es.index(index="test-index", doc_type='tweet', id=1, body=doc)
# print(res['result'])
#
# res = es.get(index="test-index", doc_type='tweet', id=1)
# print(res['_source'])
#
# es.indices.refresh(index="test-index")
#
# res = es.search(index="test-index", body={"query": {"match_all": {}}})
# print("Got %d Hits:" % res['hits']['total'])
# for hit in res['hits']['hits']:
#     print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
