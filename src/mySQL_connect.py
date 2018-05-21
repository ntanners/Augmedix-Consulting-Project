import MySQLdb
import sys
import time
import csv
from datetime import datetime
from argparse import ArgumentParser

CONNECTION_PATH = '../login/.rds'

CSV_PATH = '../table_csv_files/'

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


def rds_mysql_connection(rds_info):
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


def run_query(cur, query, show_results=False):
    nrows = cur.execute(query)
    print('the query returned {} rows'.format(nrows))
    if show_results:
        for row in cur.fetchall():
            print(row)


def import_schemas_from_file():
    with open('../tblSchemas') as schemas_file:
        schemas = {}
        tbl_name = None
        for line in schemas_file:
            line = line.split()
            if len(line) == 0:
                continue
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
    # Processes a table's csv file contents and converts strings to datetime or integer objects,
    # according to the table's schema.
    if tbl_schema[j][1] == 'DATETIME' and item != 'NULL':
        return datetime.strptime(item, "%Y-%m-%d %H:%M:%S")
    elif 'INT' in tbl_schema[j][1]:
        return int(item)
    else:
        return item


def import_table_data(con, cur, table):
    # Imports a table into the MySQL database.
    # Prerequisite: a CSV with the name <table_name>.csv needs to be saved in the CSV_PATH directory

    # Read schema from external file and create table according to schema
    schemas = import_schemas_from_file()
    tbl_schema = schemas[table]
    create_table(cur, table, tbl_schema)

    # Loop through CSV file and prepare data for import
    file_records = []
    create_query_str = """INSERT INTO {} VALUES {}""".format(table, '(' + ','.join(['%s'] * len(tbl_schema)) + ')')
    table_csv_path = CSV_PATH + table + '.csv'

    with open(table_csv_path) as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        for i, line in enumerate(reader):
            record = [schema_process(tbl_schema, j, item) for j, item in enumerate(line)]
            file_records.append(record)
            # Import records into the MySQL database table, 1,000 records at a time.
            if i % 1000 == 0:
                print('inserting 1000 rows')
                cur.executemany(create_query_str, file_records)
                con.commit()
                file_records = []
        # Insert any remaining records.
        print('inserting {} rows'.format(len(file_records)))
        cur.executemany(create_query_str, file_records)
        con.commit()


def interval_query(cur, table, start, nrows):
    # Run a select query from a given starting point and with a given number of rows
    nresults = cur.execute("""SELECT * FROM {} LIMIT {},{}""".format(table, start, nrows))
    return nresults, cur


def get_colnames(cur, table):
    # Generate a list of column names for a table in the database
    cur.execute("""DESCRIBE {}""".format(table))
    cols = cur.fetchall()
    return [col[0] for col in cols]


def time_query(cur, query, show_results=False):
    # Run a query and print the time that it took the query to run
    start_time = time.time()
    run_query(cur, query, show_results)
    end_time = time.time()
    print('the query took {:.2f} seconds'.format(end_time-start_time))


def main():
    # Connect to RDS
    rds_info = load_connection_info('../login/.rds', ['port']) # path points to the file with my login information
    con, cur = rds_mysql_connection(rds_info)
    cur.execute("""SHOW TABLES""")
    table_list = [i[0] for i in cur.fetchall()]

    # Use argparser to parse command line arguments
    parser = ArgumentParser(description = 'Augmedix project mySQL functions CLI')
    subparser_base = parser.add_subparsers(title='actions', description='Choose an action')

    sp = subparser_base.add_parser('tables')
    sp.set_defaults(which='tables')

    sp = subparser_base.add_parser('query')
    sp.set_defaults(which='query')
    sp.add_argument('-q', '--query', help="mySQL query string to execute")
    sp.add_argument('-s', '--show_results', help="show all results of query (instead of just one row)",
                    action='store_true')

    sp = subparser_base.add_parser('tquery')
    sp.set_defaults(which='tquery')
    sp.add_argument('-q', '--query', help="mySQL query string to execute")
    sp.add_argument('-s', '--show_results', help="show all results of query (instead of just one row)",
                    action='store_true')

    sp = subparser_base.add_parser('record_count')
    sp.set_defaults(which='record_count')
    sp.add_argument('-t', '--table', choices=table_list, help="table to count records for")

    sp = subparser_base.add_parser('schema')
    sp.set_defaults(which='schema')
    sp.add_argument('-t', '--table', choices=table_list, help="table to find schema for")

    sp = subparser_base.add_parser('import')
    sp.set_defaults(which='import')
    sp.add_argument('-t', '--table', help="table to import into the database")


    args = vars(parser.parse_args())
    args['cur'] = cur
    action = args['which']
    del args['which']

    if action == 'tables':
        cur.execute("""SHOW TABLES""")
        print(cur.fetchall())
    if action == 'query':
        run_query(**args)
    if action == 'tquery':
        time_query(**args)
    if action == 'record_count':
        cur.execute("""SELECT COUNT(*) FROM {}""".format(args['table']))
        print(cur.fetchall())
    if action == 'schema':
        schema = read_schema_from_db(**args)
        print(schema)
    if action == 'import':
        args['con'] = con
        import_table_data(**args)

    close_connection(con, cur)


if __name__ == '__main__':
    main()
