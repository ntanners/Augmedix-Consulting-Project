import MySQLdb
import sys
import time

QUERIES = {
    'describe': """DESCRIBE {}""",
    'count': """SELECT COUNT(*) FROM {}""",
    'table_list': """SHOW TABLES"""
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

def connect_to_rds_MySQL(rds_info):
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

def run_fetch_query(cur, keyword):
    if keyword == 'count' or keyword == 'describe':
        query = QUERIES[keyword].format(sys.argv[2])
    elif keyword == 'list':
        query = QUERIES['table_list']
    else:
        raise "not a valid query"
    cur.execute(query)
    return cur

def time_query(cur, keyword):
    start_time = time.time()
    cur = run_fetch_query(cur, keyword)
    end_time = time.time()
    return cur, end_time-start_time


def main():
    rds_info = load_connection_info('./login/.rds', ['port'])
    con, cur = connect_to_rds_MySQL(rds_info)
    # schemas = import_schemas()
    # tbl_name = 'doctor'
    # tbl_schema = schemas[tbl_name]
    # create_table(cur, 'doctor_new', tbl_schema)
    cur, runtime  = time_query(cur, sys.argv[1])
    print('the query took {:.2f} seconds, and the result was:'.format(runtime))
    print(cur.fetchall())
    close_connection(con, cur)

if __name__ == '__main__':
    main()