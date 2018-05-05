import MySQLdb

QUERIES = {
    'describe': """DESCRIBE {}""",
    'count': """SELECT COUNT(*) FROM {}""",
    'table_list': """SHOW TABLES"""
}


def load_connection_info():
    with open('./login/.rds') as rdsfile:
        rds_info = {}
        for line in rdsfile:
            line = line.split()
            rds_info[line[0]] = line[1]
        rds_info['port'] = int(rds_info['port'])
    return rds_info

#print(rds_info)

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

def run_fetch_query(cur, query):
    cur.execute(query)
    return cur

def main():
    rds_info = load_connection_info()
    con, cur = connect_to_rds_MySQL(rds_info)
    schemas = import_schemas()
    tbl_name = 'doctor'
    tbl_schema = schemas[tbl_name]
    create_table(cur, 'doctor_new', tbl_schema)
    cur = run_fetch_query(cur, QUERIES['table_list'])
    print(cur.fetchall())
    close_connection(con, cur)

if __name__ == '__main__':
    main()