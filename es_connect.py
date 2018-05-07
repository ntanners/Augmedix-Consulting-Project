from datetime import datetime
from typing import Dict, Any

from elasticsearch import Elasticsearch
from mySQL_connect import load_connection_info, rds_mySQL_connection, close_connection

CONNECTION_IP = 'ec2-54-236-6-174.compute-1.amazonaws.com'
PORT = 9200

def es_connection():
    es = Elasticsearch([CONNECTION_IP],port=PORT)
    return es

def main():
    rds_info = load_connection_info('./login/.rds', ['port'])
    con, cur = rds_mySQL_connection(rds_info)
    es = es_connection()
    query = """DESCRIBE doctor"""
    # """SELECT * FROM doctor"""
    cur.execute(query)
    cols = cur.fetchall()
    colnames = []
    for col in cols:
        colnames.append(col[0])
    query = """SELECT * FROM doctor LIMIT 3"""
    cur.execute(query)
    query_result = cur.fetchall()
    docs = []
    for doc in query_result:
        doc_json = {}
        for i,col in enumerate(doc):
            doc_json[colnames[i]] = col
        docs.append(doc_json)
    for j, doc_json in enumerate(docs):
        es.index(index='doctors', doc_type='records', id=j, body=doc_json)
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