import logging
import urllib
import warnings
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
        self.cnx = None

    def select_table(self, table_name, columns=None):
        """
        Select data from the specified table with optional columns.

        :param table_name: str, name of the table to select data from.
        :param columns: list of str, columns to include in the result, default is None (all columns).
        :return: DataFrame, containing the selected data.
        """
        self.reopen_connection()
        if columns:
            fcolumns = ",".join(columns)
        else:
            fcolumns = "*"

        df = pd.read_sql(f"SELECT {fcolumns} FROM {table_name}", self.cnx)
        logging.info(f"Selected {len(df)} rows from {table_name} table")
        self.cnx.close()
        return df

    def insert_table(self, df, table_name, if_exists="append"):
        """
        Insert a DataFrame into a database table, with optional behavior if the table exists.

        :param df: DataFrame, containing data to insert into the table.
        :param table_name: str, name of the table to insert data into.
        :param if_exists: str, behavior if the table exists, default is 'append'.

        """
        self.reopen_connection()
        if if_exists == "append":
            query = f"DELETE FROM {table_name}"
            cursor = self.cnx.cursor()
            cursor.execute(query)

        custom = {}

        for column in df.columns.tolist():
            if "timestamp" in column.lower():
                continue

            elif df.dtypes[column] != np.int64 and df.dtypes[column] != np.float64:
                custom[column] = "varchar(100)"

        fast_to_sql.fast_to_sql(
            df=df, name=table_name, conn=self.cnx, if_exists=if_exists, custom=custom
        )
        logging.info(f"Inserted {len(df)} rows into {table_name} table")
        self.cnx.commit()
        self.cnx.close()
        return

    def reopen_connection(self):
        """
        Reopen the connection to the database if it is closed.
        """
        if not self.cnx or self.cnx.connected:
            self.cnx = pyodbc.connect(self.CNX_STRING)
