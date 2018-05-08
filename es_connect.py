from datetime import datetime

from elasticsearch import Elasticsearch, helpers
from mySQL_connect import load_connection_info, rds_mySQL_connection, close_connection, get_colnames, run_fetch_query

import json

CONNECTION_IP = 'ec2-54-236-6-174.compute-1.amazonaws.com'
PORT = 9200
TABLE = 'ee_audit_events_orig'
INDEX = TABLE
TYPE = 'records'
BATCHSIZE = 200000


def es_connection():
    es = Elasticsearch([CONNECTION_IP], port=PORT)
    return es


def delete_index(es, index):
    return es.indices.delete(index=index)


def migrate_table(es, cur, table, batchsize):
    colnames = get_colnames(cur, table)
    cur.execute("""SELECT COUNT(*) FROM {}""".format(table))
    nrows = cur.fetchone()[0]
    start_point = 0
    results = []
    while nrows > 0:
        print('retrieving records {} to {}'.format(start_point, start_point + min(nrows, batchsize)))

        nresults = cur.execute(
            """SELECT * FROM {} ORDER BY 1 LIMIT {},{}""".format(table, start_point, min(nrows, batchsize)))
        actions = generate_bulk_actions(nresults, cur, colnames, INDEX, TYPE)
        insert_response = bulk_insert(es, actions)
        print(insert_response)
        nrows -= batchsize
        start_point += batchsize


def generate_bulk_actions(nrows, cur, colnames, index_name, doc_type_name):
    # actions = []
    for i in range(nrows):
        line = cur.fetchone()
        content = {}
        for j, item in enumerate(line):
            # if colnames[j] == 'doctorDate': continue
            content[colnames[j]] = str(item)
        # actions.append({
        yield {
            "_index": index_name,
            "_type": doc_type_name,
            "_source": content
        }
    # return actions


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

    es = es_connection()

    migrate_table(es, cur, TABLE, BATCHSIZE)
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
    # actions = generate_bulk_actions(numrows, cur, colnames, INDEX, 'records')
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
