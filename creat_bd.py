'''Скрипт для создания и заболения бд'''


import chek_df
import pyodbc
import pandas as pd
import time
import datetime
import logging
from tqdm import tqdm


class Sql:
    logging.basicConfig(filename='log.log', level=logging.DEBUG)
    startTime, df, rowcounter, numcreat = time.time(), pd.read_csv('police-department-calls-for-service.csv') \
        .fillna("Значение не изветно"), 0, 0

    def __init__(self, database, server="MYCOMP2003"):
        self.cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                   "Server=" + server + ";"
                                                        "Database=" + database + ";"
                                                                                 "Trusted_Connection=yes;")
        self.cur = self.cnxn.cursor()
        logging.info("%s server connect" % (datetime.datetime.now()))

    def start_take_info(self, stolb, table_name):
        start_time_take = time.time()
        df, cur = self.df, self.cur
        cur.fast_executemany = True

        create_table_str = f"create table dbo.{table_name} ({chek_df.outList(stolb)} );"
        cur.execute(create_table_str)
        self.cnxn.commit()
        logging.info(f"table {table_name} create")
        self.numcreat += 1
        print(f"--table {table_name} {self.numcreat} form 9--")

        stolb_name = ["_".join(i.split()) for i in stolb]
        insert_to_server = f"INSERT INTO dbo.{table_name} ({', '.join(stolb_name)})VALUES ({(len(stolb_name) * '?,')[:-1]})"
        if len(stolb_name) == 1:
            info = [[i] for i in df[stolb[0]].unique().tolist()]
        else:
            info = df[stolb].values.tolist()
        print(f"insert info into {table_name}")
        cur.executemany(insert_to_server, info)
        self.cnxn.commit()

        end_time_take = time.time()
        self.rowcounter += len(info)
        logging.info(f"insert {len(info)} row in {table_name}")
        logging.info(f" %s seconds start_take_info" % (end_time_take - start_time_take))

    def end_creat_table(self, table_name, table_list):
        start_creat_time = time.time()
        cur, df = self.cur, self.df
        self.numcreat += 1

        res_stolbname = []
        for name in table_list:
            for i in name.split("1")[:-1]:
                i = " ".join(i.split("_"))
                res_stolbname.append(i)

        create_table_str = f"create table dbo.{table_name} (Id int IDENTITY(1,1) PRIMARY KEY," \
                           f" {' varchar(100) NOT NULL, '.join(table_list)} varchar(100) NOT NULL);"
        cur.execute(create_table_str)
        self.cnxn.commit()
        logging.info(f"table {table_name} create")
        print(f"--table {table_name} {self.numcreat} form 9--")

        cur.fast_executemany = True
        for indexname, name in enumerate(table_list):
            cur.execute(f'SELECT * FROM {name}')
            for row in cur:
                df[res_stolbname[indexname]] = df[res_stolbname[indexname]].replace(row[1], row[0])
                print(row[0], row[1])
        print(f"insert info into {table_name}")
        insert_to_server = f"INSERT INTO dbo.{table_name} ({', '.join(table_list)})VALUES ({(len(table_list) * '?,')[:-1]})"
        cur.executemany(insert_to_server, df[res_stolbname].values.tolist())
        self.rowcounter += len(df[res_stolbname].values.tolist())
        self.cnxn.commit()
        end_create_time = time.time()
        logging.info(f"insert {len(df[res_stolbname].values.tolist())} row in {table_name}")
        logging.info("%s seconds end_creat_table" % (end_create_time - start_creat_time))
        endTime, startTime = time.time(), self.startTime
        logging.info(f"%s server disconnect, insert {self.rowcounter} - row " % (datetime.datetime.now()))
        logging.info("%s seconds insert " % (endTime - startTime))


    def __del__(self):
        self.cnxn.close()


sql = Sql("game_space")
sql.start_take_info(["Crime Id", "Original Crime Type Name", "Address"], "Main1Table")
sql.start_take_info(["Address Type"], "Address_Type1Table")
sql.start_take_info(["Call Date Time", "Report Date"], "Time")
sql.start_take_info(["Disposition"], "Disposition1Table")
sql.start_take_info(["Common Location"], "Common_Location1Table")
sql.start_take_info(["City"], "City1Table")
sql.start_take_info(["State"], "State1Table")
sql.start_take_info(["Agency Id"], "Agency_Id1Table")
sql.end_creat_table("Place", ["Disposition1Table", "Address_Type1Table","State1Table", "City1Table", "Agency_Id1Table", "Common_Location1Table"])
