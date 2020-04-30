import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, table_list, count_rows_query



def load_staging_tables(cur, conn, arn):
    """Calls SQL Scripts to load the staging tables
    
    Args: 
        cur: Curser Object of psycopg2 Database Connection
        conn: Connection Object
        arn: ARN for AWS IAM Role with S3 Read Only Access
        
    Returns: None
    """
    
    for query in copy_table_queries:
        print(query.format(arn))
        cur.execute(query.format(arn))
        conn.commit()


def insert_tables(cur, conn):
    """Calls SQL Scripts to load fact and dimension tables for star schema
    
    Args: 
        cur: Curser Object of psycopg2 Database Connection
        conn: Connection Object
        
    Returns: None
    """
    
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        

def count_rows_results(cur, conn):
    """Prints rowcount of final tables to console
    
    Args: 
        cur: Curser Object of psycopg2 Database Connection
        conn: Connection Object
        
    Returns: None
    """
    
    print("{0: <18}{1: <10}".format("Table","Rowcount"))
    print('-'*30)
    for table in table_list:
        cur.execute(count_rows_query.format(table))
        conn.commit()
        for row in cur.fetchall():
            print("{0: <18}{1: <10}".format(table, row[0]))


def main():
    """Main Program: Parses config file, connects to Database and 
    calls load_staging_tables, insert_tables and count_rows_results functions
    at the end the connection is closed
    
    Args: None
    Returns: None
    
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn, *config['IAM_ROLE'].values())
    insert_tables(cur, conn)
    count_rows_results(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()