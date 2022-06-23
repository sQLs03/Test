"""Заполнение таблиц"""

import logging
import function
from datetime import datetime
import csv


def completion_db():
    connection = function.create_connection()
    cursor = connection.cursor()

    with open('police-department-calls-for-service.csv', 'r') as df:
        info = csv.reader(df)
        for index, string in enumerate(info):
            if index != 0:

                user_records = ", ".join(["%s"] * 3)
                insert_to_server = (
                    f"INSERT INTO main_table (crime_id, original_crime_type_name, address) VALUES ({user_records})"
                )
                info_list_tuple = (string[0], string[1], string[8],)
                cursor.execute(insert_to_server, info_list_tuple)

                user_records = ", ".join(["%s"] * 2)
                insert_to_server = (
                    f"INSERT INTO time_table (report_date, call_date_time) VALUES ({user_records})"
                )
                info_list_tuple = (string[2], string[6],)
                cursor.execute(insert_to_server, info_list_tuple)

                user_records = ", ".join(["%s"] * 6)
                insert_to_server = (
                    f"INSERT INTO place_table (disposition, address_type, state, city, agency_id, common_location) "
                    f"VALUES ({user_records}) "
                )
                info_list_tuple = (string[7], string[12], string[10], string[9], string[11], string[13],)
                cursor.execute(insert_to_server, info_list_tuple)

                if index % 100 == 0:
                    connection.commit()
                    if index % 100000 == 0:
                        print(index)

    connection.close()
    print('Server disconnect')
    logging.info(f"%s server disconnect" % (datetime.now()))
    logging.info(f"%s lines added" % (index * 3))


completion_db()
