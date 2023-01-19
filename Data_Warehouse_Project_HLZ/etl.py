import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, final_tables


def load_staging_tables(cur, conn):
    '''
     Function loads raw data from S3 files into staging tables
     Parameters:
     cur - cursor object for executing SQL queries
     conn - psycopg2 connection to Redshift cluster.

     Output: 
     None
    
    '''
    for query in copy_table_queries:
        print('Executing query:{} '.format(query)) #Print query for debugging and tracking purposes
        cur.execute(query)
        conn.commit()
        print('Succesfuly commited query: {}'.format(query)) #Print query for debugging and tracking purposes


def insert_tables(cur, conn):
    '''
     Function loads raw data from S3 files into staging tables
     Parameters:
     cur - cursor object for executing SQL queries
     conn - psycopg2 connection to Redshift cluster.

     Output: 
     None
    
    '''

   
    for query in insert_table_queries: #For loop to insert data into tables
        print('Inserting query: \n {}'.format(query))
        cur.execute(query)
        conn.commit()
        print("Successfuly inserted {}".format(query))


        
     #Printing first 5 rows of all tables
    for table in final_tables: 
        print('Printing first 5 rows from {}.'.format(table))
        
        cur.execute("SELECT * FROM {}".format(table))
        row = cur.fetchone()
        i = 0
        while i < 6 :
                print(row)
                row = cur.fetchone()
                i+= 1
    
        

        


    


def main():
    '''
    Main function that runs drop_tables, create_tables and creates connection to Redshift    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)


    conn.close()


if __name__ == "__main__":
    main()