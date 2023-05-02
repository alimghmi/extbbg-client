import urllib
import warnings

import urllib
import pyodbc
import numpy as np
import pandas as pd
from decouple import config
from fast_to_sql import fast_to_sql
from sqlalchemy import create_engine

warnings.filterwarnings("ignore")


class MSSQLDatabase(object):

    SERVER = config("MSSQL_SERVER", cast=str)
    DATABASE = config("MSSQL_DATABASE", cast=str)
    USERNAME = config("MSSQL_USERNAME", cast=str)
    PASSWORD = config("MSSQL_PASSWORD", cast=str)

    CNX_STRING = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
    )

    PARSED_CNX_URL = urllib.parse.quote_plus(CNX_STRING)

    def __init__(self):
        self.engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={self.PARSED_CNX_URL}"
        )
        self.cnx = pyodbc.connect(self.CNX_STRING)

    def select_table(self, table_name, columns=[]):
        if columns:
            fcolumns = ','.join(columns)
        else:
            fcolumns = '*'

        return pd.read_sql(f"SELECT {fcolumns} FROM {table_name}", self.cnx)


    # def __df2db__(self, df, table_name, schema, if_exists='append'):
    #     if if_exists == 'append':
    #         query = f"""
            
    #         DELETE FROM {schema}.{table_name}
            
    #         """
    #         cursor = self.cnx.cursor()
    #         cursor.execute(query)

    #     custom = {}

    #     for column in df.columns.tolist():

    #         if 'timestamp' in column.lower():
    #             continue
    #             # custom[column] = 'datetime'

    #         # elif 'last_update' in column.lower():
    #         #     continue

    #         # elif 'date' in column.lower() or 'dt' in column.lower():
    #         #     continue

    #         elif df.dtypes[column] != np.int64 and df.dtypes[column] != np.float64:
    #             custom[column] = 'varchar(100)'
        
    #     fast_to_sql.fast_to_sql(df=df, name=f'{schema}.{table_name}', conn=self.cnx, if_exists=if_exists, custom=custom)
    #     self.cnx.commit()
    #     self.cnx.close()
    #     return