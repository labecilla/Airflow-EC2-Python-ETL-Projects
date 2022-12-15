import pandas as pd
import pyodbc
import sqlalchemy
import os

#define constants
USERNAME = os.environ.get('SQLUSERNAME')
PASSWORD = os.environ.get('SQLPASSWORD')
DRIVER = '{SQL Server}'
SERVER = 'DESKTOP-R5Q6CSR\SQLSERVER2019'
SOURCE_DATABASE = 'AdventureWorksDW2019'
TARGET_DATABASE = 'AdventureWorks'

#extract data from source
def extract():
    #connect to SQL Server
    try:
        engine = sqlalchemy.create_engine(f'mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{SOURCE_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server')
        print("Connection successful!\n")

        #query the source database
        query = """
        SELECT  T.name as table_name
        FROM sys.tables T
        WHERE T.name in ('DimProductSubcategory','DimProductCategory','FactInternetSales')
        """
        source_tables = pd.read_sql_query(query, engine).values.tolist()
        print(source_tables)
        #load to target database
        for table in source_tables:
            df = pd.read_sql_query(f"SELECT * FROM {table[0]}", engine)
            load(df, table[0])
    except Exception as e:
        print(e)

def load(df, table):
    try:
        rows_imported = 0
        engine = sqlalchemy.create_engine(f'mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{TARGET_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server')
        print(f'Importing rows {rows_imported} to {len(df)} for table {table}.')
        df.to_sql(f'Staging_{table}', con=engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print(f'Imported {rows_imported} out of {len(df)} records.')
    except Exception as e:
        print(e)

#call extract function
try:
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))
        




