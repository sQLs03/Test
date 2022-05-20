import pandas as pd
from itertools import cycle
from time import sleep

str_col_type = ['Original_Crime_Type_Name', 'Report_Date', 'Call_Date',
                'Offense_Date', 'Call_Time', 'Call_Date_Time', 'Disposition',
                'Address', 'City', 'State', 'Address_Type', 'Common_Location']
int_col_type = ['Agency_Id']


def outList(stolb_list: list) -> str:
    res_str = "Id int IDENTITY(1,1) PRIMARY KEY, \n\t"

    for index, i in enumerate(stolb_list):
        i = "_".join(i.split())

        if index == len(stolb_list) - 1:
            if i in int_col_type:
                res_str += f"{i} int NULL \n\t"
            elif i in str_col_type:
                res_str += f"{i} varchar(100) NULL \n\t"
            else:
                res_str += f"Crime_Id int NOT NULL \n\t"

        else:
            if i in int_col_type:
                res_str += f"{i} int NULL,\n\t"
            elif i in str_col_type:
                res_str += f"{i} varchar(100) NULL,\n\t"
            else:
                res_str += f"Crime_Id int NOT NULL,\n\t"

    return res_str

