import time
from io import open
import pandas as pd
import psycopg2

dbname = 'DBBank'
host = 'localhost'
user = 'postgres'
password = 'postgres'

conn = psycopg2.connect(
    host=host,
    database=dbname,
    user=user,
    password=password,
)

def sql_query(query):
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
