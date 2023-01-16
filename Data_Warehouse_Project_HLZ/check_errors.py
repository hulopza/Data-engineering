#This code is just to check the stl_load_errors table
import configparser
import psycopg2


config = configparser.ConfigParser()
config.read('dwh.cfg')

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

cur = conn.cursor()

cur.execute("SELECT * FROM stl_load_errors")
row = cur.fetchone()
while row !=None:
    print(row)
    row = cur.fetchone()

