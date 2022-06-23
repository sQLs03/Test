"""Создание таблиц в базе данных"""

import logging
from datetime import datetime
import function


def create_table_in_bd():
    connection = function.create_connection()

    create_main_table = """
    CREATE TABLE main_table (
      id SERIAL PRIMARY KEY,
      crime_id serial NOT NULL, 
      original_crime_type_name TEXT,
      address TEXT
    );
    """
    function.execute_query(connection, create_main_table)
    logging.info(f"Create main_table in police_call_db")

    create_time_table = """
    CREATE TABLE time_table (
      id SERIAL PRIMARY KEY,
      report_date TEXT, 
      call_date_time TEXT 
    );
    """
    function.execute_query(connection, create_time_table)
    logging.info(f"Create time_table in police_call_db")

    create_place_table = """
    CREATE TABLE place_table (
      id SERIAL PRIMARY KEY,
      disposition TEXT, 
      address_type TEXT,
      state TEXT, 
      city TEXT, 
      agency_id SMALLSERIAL, 
      common_location TEXT
    );
    """
    function.execute_query(connection, create_place_table)
    logging.info(f"Create place_table in police_call_db")

    connection.close()
    print('Server disconnect')
    logging.info(f"%s server disconnect" % (datetime.now()))


create_table_in_bd()
