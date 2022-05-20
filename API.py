'''Скрипт для вывода данных в take_info.txt'''


import pandas as pd
import time
import pyodbc
import datetime
import logging


class Sql:
    startTime = time.time()

    def __init__(self, database, server="MYCOMP2003"):
        self.cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                   "Server=" + server + ";"
                                   "Database=" + database + ";"
                                   "Trusted_Connection=yes;")
        self.cur = self.cnxn.cursor()
        logging.info(f"---- %s server connect ----" % (datetime.datetime.now()))

    def creat_time_dict(self, date_from="0000-00-00 00:00:00", date_to="2019-00-00 00:00:00"):
        cur, dat_date_to, dat_date_from = self.cur, \
                                          datetime.datetime.strptime(date_to, "%Y-%m-%d %H:%M:%S"), \
                                          datetime.datetime.strptime(date_from, "%Y-%m-%d %H:%M:%S")
        cur.execute(f'SELECT * FROM Time')

        dist_index_time, timelist = {}, list(cur)
        for index in range(0, len(timelist)):
            value = " ".join(timelist[index][1].split("T"))
            if dat_date_from <= datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S") <= dat_date_to:
                dist_index_time[index] = (value.split()[0] + " ") * 3 + value.split()[1] + " " + value
        return dist_index_time

    def info_out(self, date_from="0000-00-00 00:00:00", date_to="0000-00-00 00:00:00"):
        dict_index_time, cur, __page, = sql.creat_time_dict(date_from, date_to), self.cur, 20
        id_list, start, nameplacestolb = list(dict_index_time.keys()), list(dict_index_time.keys())[0], \
                                         ["Disposition1Table", "Address_Type1Table", "State1Table", "City1Table",
                                          "Agency_Id1Table", "Common_Location1Table"]
        while input("Вывести info - 2, выйти - 1:  ") != "1":
            out_list, count = [], 0
            for value in list(cur.execute(f'SELECT * FROM dbo.Main1Table WHERE'
                                          f' Id BETWEEN {start} AND {start + 100}')):
                if value[0] in id_list:
                    if len(out_list) >= __page:
                        break
                    out_list.append([value[1:], dict_index_time[value[0]]])
                count += 1

            place_list = list(cur.execute(f'SELECT * FROM Place WHERE'
                                          f' Id BETWEEN {start} AND {start + 100}'))
            #print(place_list)
            for rownum, value_place in enumerate(place_list):
                #print(f"{rownum} - {value_place}")
                if value_place[0] in id_list:
                    if rownum >= __page - 1:
                        break
                    for index, i in enumerate(nameplacestolb, start=1):
                        for chek in cur.execute(f'SELECT * FROM {i}'):
                            #print(f"{int(value_place[index])} - {chek[0]}")
                            if int(value_place[index]) == chek[0]:
                                #print(f"{chek[1]} - {rownum - 1}")
                                out_list[rownum - 1].append(chek[1])
                else:
                    rownum -= 1

            start += count
            #print(out_list)
            with open("take_info.txt", "a+", encoding="Windows-1251") as file:
                for i in out_list:
                    print(*i, file=file)
            logging.info(f" %s info to file " % (datetime.datetime.now()))

        logging.info(f"---- %s server disconnect ----" % (datetime.datetime.now()))
        endTime, startTime = time.time(), self.startTime
        logging.info("--- %s seconds ---" % (endTime - startTime))

    def __del__(self):
        self.cnxn.close()


if __name__ == '__main__':
    logging.basicConfig(filename='log.log', level=logging.DEBUG)
    sql = Sql("game_space")
    sql.info_out(input("Введите начальную дату: "), input("Конечную начальную дату: "))
# 2016-06-03 14:29:00 2016-06-07 00:00:00
