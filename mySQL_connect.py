import MySQLdb
import sys
import time

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

def import_schemas():
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

def create_table(cur, tbl_name, tbl_schema):
    query = """CREATE TABLE IF NOT EXISTS """ + tbl_name + " (" + \
            (", ".join(" ".join(row) for row in tbl_schema)) + ")"
    cur.execute(query)

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

def run_query(cur, query):
    nrows = cur.execute(query)
    return nrows, cur

def get_colnames(cur, table):
    n, cur = run_fetch_query(cur, 'describe', table)
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
    # schemas = import_schemas()
    # tbl_name = 'doctor'
    # tbl_schema = schemas[tbl_name]
    # create_table(cur, 'doctor_new', tbl_schema)

    # cur.execute("""SET net_read_timeout=28800""")
    # cur.execute("""SET GLOBAL connect_timeout=600""")

    n, cur, time = time_query(cur, sys.argv[1])
    print(n, cur.fetchone(), time)
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