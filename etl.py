import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, table_list, count_rows_query
from expectedresults import expectedresults


def load_staging_tables(cur, conn, arn):
    for query in copy_table_queries:
        print(query.format(arn))
        cur.execute(query.format(arn))
        conn.commit()


def insert_tables(cur, conn, arn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        

def count_rows_results(cur, conn, arn):
    print("Comparing rowcounts with expected results")
    print("{0: <18}{1: <10}{2: <10}{3: <15}".format("Table","Actual","Expected", "Test"))
    print('-'*40)
    for table in table_list:
        cur.execute(count_rows_query.format(table))
        conn.commit()
        for row in cur.fetchall():
            #print(type(row))
            print("{0: <18}{1: <10}{2: <10}{3: <15}".format(table, row[0], expectedresults[table], 'OK' if row[0]==expectedresults[table] else 'Failed' ))


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn, *config['IAM_ROLE'].values())
    insert_tables(cur, conn, *config['IAM_ROLE'].values())
    #count_rows_results(cur, conn, *config['IAM_ROLE'].values())

    conn.close()


if __name__ == "__main__":
    main()