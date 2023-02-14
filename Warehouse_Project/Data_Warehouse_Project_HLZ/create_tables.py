import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):

    '''
    Function drops all tables if already created.

    Parameters:
     cur - cursor object for executing SQL queries
     conn - psycopg2 connection to Redshift cluster.

     Output: 
     None
    
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
     Function creates staging tables and final database tables

     Parameters:
     cur - cursor object for executing SQL queries
     conn - psycopg2 connection to Redshift cluster.

     Output: 
     None
    
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Main function that runs drop_tables, create_tables and creates connection to Redshift    '''

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()