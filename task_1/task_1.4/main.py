import psycopg2
import time
import datetime
from datetime import date

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


def sql_query(query, parameters=''):
    cur = conn.cursor()
    if parameters == '':
        cur.execute(query)
    else:
        cur.execute(query, parameters)
    conn.commit()
    cur.close()


if __name__ == '__main__':
    print("Введите начальную дату")
    p_year = input("\nГод ")
    p_month = input("\nМесяц ")
    p_day = input("\nДень ")

    print("\nВведите конечную дату")
    p1_year = input("\nГод ")
    p1_month = input("\nМесяц ")
    p1_day = input("\nДень ")

    date_s = date(int(p_year), int(p_month), int(p_day))
    date_n = date_s

    with open('/home/posting_file.csv', "w+") as f:
        sql_query(f'''COPY (select * from debit_and_credit_posting(to_date('{date_n}','yyyy-mm-dd'))) TO PROGRAM  'cat >>/home/posting_file.csv' DELIMITER ';' CSV HEADER NULL '';''')
        print("\nВыгружаются данные в csv-файл")
        date_n = date_n + datetime.timedelta(days=1)

        while date_n <= date(int(p1_year), int(p1_month), int(p1_day)):
            sql_query( f'''COPY (select * from debit_and_credit_posting(to_date('{date_n}','yyyy-mm-dd'))) TO PROGRAM  'cat >>/home/posting_file.csv' DELIMITER ';' NULL '';''')
            date_n = date_n + datetime.timedelta(days=1)

    conn.close()