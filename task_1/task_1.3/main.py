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

def data_handling():
    number = input("1 - выгрузить данные из витрины «dm.dm_f101_round_f» в csv-файл\n"
                   "2 - загрузить данные в копию таблицы 101-формы «dm.dm_f101_round_f_v2\n"
                   "Введите номер действия: "
                   )

    match number:
        case "1":
            print("\nОчищается csv-файл перед загрузкой данных")
            with open('/home/dm_f101_round_f.csv', "w+") as f:
               sql_query('''COPY dm.dm_f101_round_f TO '/home/dm_f101_round_f.csv' DELIMITER ';' CSV HEADER NULL '';''')
               print("\nВыгружаются данные в csv-файл")

        case "2":
            dm_f101_round_f_file_name = "/home/dm_f101_round_f.csv"

            print("\nЗагружаются данные в таблицу")
            sql_query('''DROP TABLE IF EXISTS dm.dm_f101_round_f_v2;''')
            sql_query(
                '''CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2 AS (SELECT * FROM dm.dm_f101_round_f) with no data;''')
            sql_query(
                '''insert into LOGS.log_table (log_data, log_time) values ('Начало вставки в таблицу dm_f101_round_f_v2',  LOCALTIMESTAMP(0));''')
            sql_query('''SET search_path TO dm, public;''')

            cur = conn.cursor()
            with open(dm_f101_round_f_file_name) as f:
                next(f)
                tbl = 'dm_f101_round_f_v2'
                sep = ';'
                cur.copy_from(f, tbl, sep, null='')
            sql_query(
                '''insert into LOGS.log_table (log_data, log_time) values ('Окончена вставка в таблицу dm_f101_round_f_v2',  LOCALTIMESTAMP(0));''')

        case _:
            print("Такого действия нет")


if __name__ == '__main__':
    data_handling()
    answer = "1"
    while answer == "1":
        answer = input("\nХотите продолжить? (1 - да; 2 - нет) ")
        match answer:
            case "1":
                data_handling()
            case _:
                print("\nРабота программы завершена")

    conn.close()
