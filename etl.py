import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, preprocess_queries, analytical_queries


def load_staging_tables(cur, conn):
    '''Performs all queries the load the data from S3 to staging tables in Redshift'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def preprocess_data(cur, conn):
    '''Preprocess data in staging tables for future insertion'''
    for query in preprocess_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    '''Performs all queries that insert data from staging tables to star schema'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def investigate(cur):
    '''Performs sample analytical queries and outputs column names and results'''
    for query in analytical_queries:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        print(columns)
        for row in rows:
            print(row)


def main():
    config = configparser.ConfigParser()
    config.read('Data_Warehouse_Project_Template/dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    preprocess_data(cur, conn)
    insert_tables(cur, conn)
    investigate(cur)

    conn.close()


if __name__ == "__main__":
    main()