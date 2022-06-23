import psycopg2
import logging
from datetime import datetime
import csv


def create_connection(db_name="police_call_db", db_user="postgres",
                      db_password="qpmz1928", db_host="127.0.0.1", db_port="5432"):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
        logging.info("%s server connect" % (datetime.now()))

    except psycopg2.OperationalError as e:
        print(f"The error '{e}' occurred")
        logging.exception(f"The error '{e}' occurred")
    return connection


def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        logging.info("Query executed successfully")
    except psycopg2.OperationalError as e:
        logging.exception(f"The error '{e}' occurred")


def create_output_id_list(date_from="2018-01-01 01:01:01", date_to="2019-01-01 01:01:01"):
    connection = create_connection()
    cursor = connection.cursor()
    start_time, stop_time = datetime.strptime(date_from, '%Y-%m-%d %H:%M:%S'), \
                            datetime.strptime(date_to, '%Y-%m-%d %H:%M:%S')
    cursor.execute(f"SELECT id, call_date_time from time_table")
    logging.info('Start of creation id_list')
    id_output_list = []
    for list_stolb in cursor.fetchall():
        time_stolb_list = datetime.strptime(list_stolb[1], "%Y-%m-%dT%H:%M:%S")
        if start_time <= time_stolb_list <= stop_time:
            id_output_list.append(list_stolb[0])
    logging.info('End of creation id_list')
    return id_output_list


def output_info_dict(id_list, start_num, step):
    connection = create_connection()
    cursor = connection.cursor()
    '''cursor.execute(f"SELECT id, call_date_time from time_table")
    id_list = create_output_id_list(cursor.fetchall(), '2018-01-01 00:00:01', '2019-01-01 01:01:01')'''

    res_dict = {}
    cursor.execute(f"SELECT * from main_table WHERE Id BETWEEN {id_list[start_num]} AND {id_list[start_num + step]}")
    for main_string in cursor.fetchall():
        if main_string[0] in id_list:
            res_dict[main_string[0]] = [main_string[1:]]

    cursor.execute(f"SELECT * from time_table WHERE Id BETWEEN {id_list[start_num]} AND {id_list[start_num + step]}")
    for time_string in cursor.fetchall():
        if time_string[0] in id_list:
            res_dict[time_string[0]].extend(time_string[1] + "call_date" + time_string[1] \
                                            + "offense_date" + time_string[1] + "call_time" \
                                            + time_string[-1].split("T")[-1] + "call_date_time" + time_string[-1])

    cursor.execute(f"SELECT * from place_table WHERE Id BETWEEN {id_list[start_num]} AND {id_list[start_num + step]}")
    for place_string in cursor.fetchall():
        if place_string[0] in id_list:
            res_dict[place_string[0]].extend(place_string[1:])

    return res_dict
