from datetime import datetime

from elasticsearch import Elasticsearch, helpers
from mySQL_connect import load_connection_info, rds_mySQL_connection, close_connection, get_colnames, interval_query, \
    read_schema_from_db
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import sys
import requests
import pdb
import json

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


def benchmark_migrate(es, cur, table, testsize):
    colnames = get_colnames(cur, table)
    print('migrating batch of size {}'.format(testsize))
    mySQL_start_time = time.time()
    cur.execute("""SELECT * FROM {} LIMIT {}""".format(table, testsize))
    mySQL_end_time = time.time()
    print('mySQL query time = {:.6f} seconds per record'.format((mySQL_end_time-mySQL_start_time)/testsize))
    actions = generate_bulk_actions(testsize, cur, colnames, table+'_index', 'test_type')
    es_start_time = time.time()
    insert_response = bulk_insert(es, actions)
    es_end_time = time.time()
    print('ES import time = {:.2f} seconds'.format(es_end_time-es_start_time))
    print('ES import speed = {:.6f} seconds per record'.format((es_end_time-es_start_time)/testsize))
    print(insert_response)
    # delete_index(es, 'test_index')


def parallel_migrate(es_list, workers, actions, batchsize): #es
    tpool = ThreadPoolExecutor(max_workers=workers)
    func = helpers.bulk
    futures = []
    worker_start = 0
    worker_end = int(round(batchsize / workers))

    for i in range(workers):
        worker_actions = actions[worker_start:worker_end]
        worker_start = worker_end
        worker_end = min(worker_end + int(round(batchsize/workers)), batchsize)
        es = es_list[i%len(es_list)]
        futures.append(tpool.submit(func, es, worker_actions)) #es_parallel[i]

    for f in as_completed(futures):
        try:
            result = f.result()
        except Exception as exc:
            print('exception: {}'.format(exc))


def parallel_batch_migrate(es_list, cur, table, workers, batchsize, limit, benchmark=False):
    colnames = get_colnames(cur, table)
    cur.execute("""SELECT COUNT(*) FROM {}""".format(table))
    nrows = min(limit, cur.fetchone()[0])
    start = 0
    while nrows > 0:
        print('executing parallel batch of {} records, starting at {}'.format(min(batchsize, nrows), start))
        mysql_start = time.time()
        nresults, cur = interval_query(cur, table, start, batchsize)
        mysql_end = time.time()
        # print('MySQL Query: {} records at {:.6f} records/s'.format(nresults, (mysql_end-mysql_start)/nresults))
        actions_start = time.time()
        actions = list(generate_bulk_actions(nresults, cur, colnames, table+'_index', 'record'))
        actions_end = time.time()
        # if benchmark:
        #     print('Actions generation time: {:.2f} s. Actions generation speed: {:.6f} s/record'.format(
        #         actions_end-actions_start, (actions_end-actions_start)/nresults))
        es_start = time.time()
        parallel_migrate(es_list, workers, actions, nresults) #es_parallel
        es_end = time.time()
        ES_IMPORT_TIMES.append((es_end-es_start))
        if benchmark:
            # print('ES insert time: {:.2f} s. ES insert speed: {:.6f} s/record'.format(es_end-es_start,
            #                                                                           (es_end-es_start)/nresults))
            ES_IMPORT_TIMES.append(es_end-es_start)
        nrows -= batchsize
        start += batchsize
    print(sum(ES_IMPORT_TIMES))


def migrate_table(es, cur, table, batchsize, limit, benchmark=False):
    colnames = get_colnames(cur, table)
    cur.execute("""SELECT COUNT(*) FROM {}""".format(table))
    nrows = min(limit, cur.fetchone()[0])
    start_point = 0
    results = []
    while nrows > 0:
        # print('retrieving records {} to {}'.format(start_point, start_point + min(nrows, batchsize)))
        nresults = cur.execute(
            """SELECT * FROM {} LIMIT {},{}""".format(table, start_point, min(nrows, batchsize)))
        # print('records retrieved')
        actions = generate_bulk_actions(int(nresults), cur, colnames, table + "_index", TYPE)
        if benchmark:
            es_start = time.time()
        insert_response = bulk_insert(es, actions)
        if benchmark:
            es_end = time.time()
            ES_IMPORT_TIMES.append(es_end-es_start)
            # print("ES insert time: {:.2f} s. ES insert speed: {:.6f} s/record".format(es_end-es_start,
            #                                                                           (es_end-es_start)/nresults))
        nrows -= batchsize
        start_point += batchsize
    print(sum(ES_IMPORT_TIMES))

def parallel_requests_migrate(ip_list, workers, json_list, batchsize):
    tpool = ThreadPoolExecutor(max_workers=workers)
    func = requests.post
    futures = []
    worker_start = 0
    worker_end = int(round(batchsize / workers) * 2)
    for i in range(workers):
        worker_json_list = json_list[worker_start:worker_end]
        worker_json = "\n".join(map(json.dumps, worker_json_list)) + "\n"
        worker_start = worker_end
        worker_end = min(worker_end + int(round(batchsize / workers) * 2), batchsize*2)
        ip = ip_list[i%len(ip_list)]
        futures.append(tpool.submit(func, ip, data=worker_json, headers={"Content-type": "application/x-ndjson"}))
    for f in as_completed(futures):
        try:
            result = f.result()
            # print(result)
            # pdb.set_trace()
        except Exception as exc:
            print('exception: {}'.format(exc))


def parallel_batch_requests_migrate(ip_list, cur, table, workers, batchsize, limit, benchmark=False):
    colnames = get_colnames(cur, table)
    cur.execute("""SELECT COUNT(*) FROM {}""".format(table))
    nrows = min(limit, cur.fetchone()[0])
    start = 0
    while nrows > 0:
        print('executing parallel batch of {} records, starting at {}'.format(min(nrows,batchsize), start))
        nresults, cur = interval_query(cur, table, start, batchsize)
        # print('MySQL Query: {} records at {:.6f} records/s'.format(nresults, (mysql_end-mysql_start)/nresults))
        actions_start = time.time()
        json_list = generate_json(nresults, cur, colnames, table + '_index', 'record')
        actions_end = time.time()
        # if benchmark:
        #     print('Actions generation time: {:.2f} s. Actions generation speed: {:.6f} s/record'.format(
        #         actions_end-actions_start, (actions_end-actions_start)/nresults))
        es_start = time.time()
        parallel_requests_migrate(ip_list, workers, json_list, nresults)
        es_end = time.time()
        if benchmark:
            ES_IMPORT_TIMES.append(es_end-es_start)
            # print('ES insert time: {:.2f} s. ES insert speed: {:.6f} s/record'.format(es_end - es_start, (
            #             es_end - es_start) / nresults))
        nrows -= batchsize
        start += batchsize
    print(sum(ES_IMPORT_TIMES))

def generate_json(nrows, cur, colnames, index_name, doc_type_name):
    nrows = min(nrows, cur.rowcount)
    body = []
    header = {"index": {"_index": index_name, "_type": doc_type_name}}
    for i in range(nrows):
        line = cur.fetchone()
        content = {}
        for j, item in enumerate(line):
            content[colnames[j]] = str(item)
        body.append(header)
        body.append(content)
    return body


def generate_bulk_actions(nrows, cur, colnames, index_name, doc_type_name):
    # actions = []
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
    # return actions


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


def benchmark_import_size(cur, table, cluster):
    es = es_connection(CONNECTION_IP[cluster])
    for i in range(10):
        benchmark_migrate(es, cur, table, 100*(2**i))

def benchmark_workers(cur, table, cluster):
    es = es_connection(CONNECTION_IP[cluster])
    for i in range(1,10):
        parallel_batch_migrate(es, cur, table, i, 10000, 30000)
        print('with {} workers, total import time: {:.2f}'.format(i, sum(ES_IMPORT_TIMES)))
        del ES_IMPORT_TIMES[:]

def bulk_insert(es, actions):
    insert_response = helpers.bulk(es, actions)
    return (insert_response)


def main():
    # Connect to RDS
    rds_info = load_connection_info('./login/.rds', ['port'])
    con, cur = rds_mySQL_connection(rds_info)
    # Collect information about MySQL table

    # colnames = get_colnames(cur, TABLE)
    # numrows, cur = run_fetch_query(cur, 'contents', TABLE)


    table = 'ee_audit_events'
    workers = 4
    limit = 100000
    batch = 10000


    # es = es_connection(CONNECTION_IP['cluster'])

    if sys.argv[1] == 'benchmark':
        benchmark_import_size(cur, table, sys.argv[2])
    elif sys.argv[1] == 'workertest':
        benchmark_workers(cur, table, sys.argv[2])
    elif sys.argv[1] == 'delete':
        es = es_connection(CONNECTION_IP[sys.argv[2]])
        table = sys.argv[3]
        delete_index(es, table+'_index')
    elif sys.argv[1] == 'migrate':
        es = es_connection(CONNECTION_IP[sys.argv[2]])
        table = sys.argv[3]
        index = table+'_index'
        if es.indices.exists(index):
            delete_index(es, index)
        response = new_index(es, index, generate_mapping(cur, table, 'record'))
        print(response)
        migrate_table(es, cur, table, 10000, limit, benchmark=True)
    elif sys.argv[1] == 'parallel':
        es_list = [es_connection([connection]) for connection in CONNECTION_IP[sys.argv[2]]]
        table = sys.argv[4]
        index = table+'_index'
        workers = int(sys.argv[3])
        if es_list[0].indices.exists(index):
            delete_index(es_list[0], index)
        response = new_index(es_list[0], index, generate_mapping(cur, table, 'record'))
        print(response)
        parallel_batch_migrate(es_list, cur, table, workers, workers*batch, limit, benchmark=True)
    elif sys.argv[1] == 'requests':
        ip_list = ['http://' + connection + ':9200/' + table+"_index/_bulk" for connection in CONNECTION_IP[sys.argv[2]]]
        es_list = [es_connection([connection]) for connection in CONNECTION_IP[sys.argv[2]]]
        table = sys.argv[4]
        index = table + '_index'
        workers = int(sys.argv[3])
        if es_list[0].indices.exists(index):
            delete_index(es_list[0], index)
        response = new_index(es_list[0], index, generate_mapping(cur, table, 'record'))
        print(response)
        parallel_batch_requests_migrate(ip_list, cur, table, workers, workers*batch, limit, benchmark=True)
    elif sys.argv[1] == 'mapping':
        mapping = generate_mapping(cur,sys.argv[2],"record")
        print(mapping)
    elif sys.argv[1] == 'new_index':
        es = es_connection(CONNECTION_IP[sys.argv[2]])
        table = sys.argv[3]
        index = table+'_new_index'
        print(index)
        response = new_index(es, table+'_new_index', generate_mapping(cur, table, 'record'))
        print(response)



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
