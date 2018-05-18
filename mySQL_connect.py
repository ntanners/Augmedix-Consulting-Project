import MySQLdb
import sys
import time
import csv
from datetime import datetime

QUERIES = {
    'describe': """DESCRIBE {}""",
    'count': """SELECT COUNT(*) FROM {}""",
    'table_list': """SHOW TABLES""",
    'contents': """SELECT * FROM {}""",
    'contents_limit': """SELECT * FROM {} LIMIT {}"""
}


def load_connection_info(path, intvars):
    """ Takes two inputs:
        path (string): path where connection informaton is stored
        intvars (list): variables that should be converted to int values"""
    with open(path) as conn_file:
        conn_info = {}
        for line in conn_file:
            line = line.split()
            conn_info[line[0]] = line[1]
        for var in intvars:
            conn_info[var] = int(conn_info[var])
    return conn_info

def rds_mySQL_connection(rds_info):
    try:
        con = MySQLdb.Connection(**rds_info)
        cur = con.cursor()
        print("Connection successful")
        return con, cur
    except Exception as e:
        print(e)

def close_connection(con, cur):
    cur.close()
    con.close()
    print("Connection closed")

def delete_table_contents(cur, table):
    cur.execute("DELETE FROM {}".format(table))

def run_fetch_query(cur, keyword, table=None, limit=None):
    if keyword == 'contents_limit':
        query = QUERIES[keyword].format(table, limit)
    elif keyword == 'count' or keyword == 'describe' or keyword == 'contents':
        query = QUERIES[keyword].format(table)
    elif keyword == 'list':
        query = QUERIES['table_list']
    else:
        raise "not a valid query"
    numrows = cur.execute(query)
    return numrows, cur

def run_select_query(cur, table, batchsize):
    cur.execute("""SELECT COUNT(*) FROM {}""".format(table))
    nrows = cur.fetchone()[0]
    start_point = 0
    results = []
    while nrows > 0:
        print('retrieving records {} to {}'.format(start_point, start_point + min(nrows, batchsize)))
        cur.execute("""SELECT * FROM {} ORDER BY 1 LIMIT {},{}""".format(table,start_point,min(nrows,batchsize)))
        # results = results + list(cur.fetchone())
        print(cur.fetchone()[0])
        nrows -= batchsize
        start_point += batchsize
    # print(len(results))

def run_query(cur, query, benchmark = False):
    start_time = time.time()
    nrows = cur.execute(query)
    end_time = time.time()
    if benchmark:
        return nrows, cur, end_time - start_time
    else:
        return nrows, cur


def import_schemas_from_file():
    with open('./tblSchemas') as schemas_file:
        schemas = {}
        for line in schemas_file:
            line = line.split()
            if len(line) == 0: continue
            if line[0] == 'tblname':
                tbl_name = line[1]
                schemas[tbl_name] = []
            else:
                schemas[tbl_name].append(line)
    return schemas


def read_schema_from_db(cur, table):
    nrows = cur.execute("""DESCRIBE {}""".format(table))
    tbl_schema = []
    for i in range(nrows):
        row = cur.fetchone()
        tbl_schema.append([row[0], row[1]])
    return tbl_schema

def create_table(cur, tbl_name, tbl_schema):
    query = """CREATE TABLE IF NOT EXISTS """ + tbl_name + " (" + \
            (", ".join(" ".join(row) for row in tbl_schema)) + ")"
    cur.execute(query)


def schema_process(tbl_schema, j, item):
    if tbl_schema[j][1] == 'DATETIME' and item != 'NULL':
        try:
            return datetime.strptime(item, "%Y-%m-%d %H:%M:%S")
        except:
            print(item)
            return 'x'
    elif 'INT' in tbl_schema[j][1]:
        return int(item)
    else:
        return item

def import_table_data(con, cur, tbl_name, file_name, tbl_schema):
    file_records = []
    create_query_str = """INSERT INTO {} VALUES {}""".format(tbl_name, '(' + ','.join(['%s'] * len(tbl_schema)) + ')')

    with open(file_name) as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        for i, line in enumerate(reader):
            # if i == 2: break
            record = [schema_process(tbl_schema, j, item) for j, item in enumerate(line)]
            file_records.append(record)
            if i % 1000 == 0:
                print('inserting 1000 rows')
                cur.executemany(create_query_str, file_records)
                con.commit()
                file_records = []
        print('inserting {} rows'.format(len(file_records)))
        cur.executemany(create_query_str, file_records)
        con.commit()


def interval_query(cur, table, start, nrows):
    nresults = cur.execute("""SELECT * FROM {} LIMIT {},{}""".format(table, start, nrows))
    return nresults, cur

def get_colnames(cur, table):
    cur.execute("""DESCRIBE {}""".format(table))
    cols = cur.fetchall()
    return [col[0] for col in cols]

def time_query(cur, query):
    start_time = time.time()
    n, cur = run_query(cur, query)
    end_time = time.time()
    return n, cur, end_time-start_time


def main():
    rds_info = load_connection_info('./login/.rds', ['port'])
    con, cur = rds_mySQL_connection(rds_info)
    if sys.argv[1] == 'tables':
        nrows, cur = run_query(cur, """SHOW TABLES""")
        print(cur.fetchall())
    elif sys.argv[1] == 'query':
        nrows, cur = run_query(cur, sys.argv[2])
        print('number of results: {}'.format(nrows))
        if raw_input('display all results (y/n)? ') == 'y':
            print(cur.fetchall())
    elif sys.argv[1] == 'tquery':
        nrows, cur, query_time = run_query(cur, sys.argv[2], benchmark=True)
        print('The query took {:.2f} seconds and produced {} rows.'.format(query_time, nrows))
    elif sys.argv[1] == 'count':
        nrows, cur = run_query(cur, """SELECT COUNT(*) FROM {}""".format(sys.argv[2]))
        print(cur.fetchall())
    elif sys.argv[1] == 'schema':
        schema = read_schema_from_db(cur, sys.argv[2])
        print(schema)
    elif sys.argv[1] == 'import':
        table=sys.argv[2]
        schemas = import_schemas_from_file()
        schema = schemas[table]
        print(schema)
        import_table_data(con, cur, table, table+'.csv', schema)
    # tbl_name = sys.argv[1]
    # tbl_schema = schemas[tbl_name]
    # file_name = sys.argv[2]
    # delete_table_contents(cur, tbl_name)
    # create_table(cur, tbl_name, tbl_schema)
    # import_table_data(con, cur, tbl_name, file_name, tbl_schema)
    # cur = run_query("""SELECT COUNT(*) FROM {}""".format(tbl_name))
    # print(cur.fetchall())
    # print(import_data[0])


    # create_table(cur, 'doctor_new', tbl_schema)

    # cur.execute("""SET net_read_timeout=28800""")
    # cur.execute("""SET GLOBAL connect_timeout=600""")

    # n, cur, time = time_query(cur, sys.argv[1])
    # print(n, cur.fetchone(), time)
    # run_select_query(cur, 'ee_audit_events_orig', 200000)
    # keyword = sys.argv[1]
    # table, limit = None, None
    # if len(sys.argv)>2:
    #     table = sys.argv[2]
    # if len(sys.argv)>3:
    #     limit = sys.argv[3]
    # n, cur, runtime  = time_query(cur, keyword, table, limit)
    # print('the query took {:.2f} seconds, and the result was:'.format(runtime))
    # if n > 100:
    #     print(n, 'rows')
    # else:
    #     print(cur.fetchall())

    # cur.execute("""SHOW VARIABLES LIKE '%TIME%'""")
    # cur.execute("""SELECT @@MAX_ALLOWED_PACKET""")
    # print(cur.fetchall())
    # if sys.argv[1] == 'cols':
    #     result = get_colnames(cur, sys.argv[2])
    # else:
    #     _, result = run_fetch_query(cur, sys.argv[1])
    # print(result.fetchall())
    close_connection(con, cur)

if __name__ == '__main__':
    main()