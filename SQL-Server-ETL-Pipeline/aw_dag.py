import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.hooks import MsSqlHook
from airflow.hooks.base import BaseHook
import pandas as pd
from sqlalchemy import create_engine

@task()
def get_source_tables():
    hook = MsSqlHook(mssql_conn_id="sql_server")
    query = """
    SELECT  T.name as table_name
    FROM sys.tables T
    WHERE T.name in ('DimProductSubcategory','DimProductCategory','FactInternetSales')
    """
    df = hook.get_pandas_df(query)
    print(df)
    table_dict = df.to_dict('dict')
    return table_dict

@task()
def load_source_data(table_dict: dict):
    conn = BaseHook.get_connection('sql_server')
    engine = create_engine(f'mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server')
    all_table_name = []
    start_time = time.time()
    #access the table_name element in dictionaries
    for k, v in table_dict['table_name'].items():
        #print(v)
        all_table_name.append(v)
        rows_imported = 0
        sql = f'SELECT * FROM {v}'
        hook = MsSqlHook(mssql_conn_id="sql_server")
        df = hook.get_pandas_df(sql)
        print(f'importing rows {rows_imported} to {len(df)} for table {v} ')
        df.to_sql(f'Source_{v}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print(f'Imported {rows_imported} out of {len(df)} records.')
        print(f'Done. {str(round(time.time() - start_time, 2))} total seconds elapsed.')
    print("Data imported successful")
    return all_table_name

@task()
def transform_source_product_subcategory():
    conn = BaseHook.get_connection('sql_server')
    engine = create_engine(f'mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server')
    df = pd.read_sql_query('SELECT * FROM Source_DimProductSubcategory', engine)
    #drop columns
    revised = df[['ProductSubcategoryKey','EnglishProductSubcategoryName', 'ProductSubcategoryAlternateKey','EnglishProductSubcategoryName', 'ProductCategoryKey']]
    # Rename columns with rename function
    revised = revised.rename(columns={"EnglishProductSubcategoryName": "ProductSubcategoryName"})
    revised.to_sql(f'Staging_DimProductSubcategory', engine, if_exists='replace', index=False)
    print("ProductSubcategory table transform and load succeeded.")

@task()
def transform_source_product_category():
    conn = BaseHook.get_connection('sql_server')
    engine = create_engine(f'mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server')
    df = pd.read_sql_query('SELECT * FROM Source_DimProductCategory ', engine)
    #drop columns
    revised = df[['ProductCategoryKey', 'ProductCategoryAlternateKey','EnglishProductCategoryName']]
    # Rename columns with rename function
    revised = revised.rename(columns={"EnglishProductCategoryName": "ProductCategoryName"})
    revised.to_sql(f'Staging_DimProductCategory', engine, if_exists='replace', index=False)
    print("ProductCategory table transform and load succeeded.")

@task()
def normalize_product_category():
    conn = BaseHook.get_connection('sql_server')
    engine = create_engine(f'mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server')
    product_category = pd.read_sql_query('SELECT * FROM Staging_DimProductCategory', engine)
    product_subcategory = pd.read_sql_query('SELECT * FROM Staging_DimProductSubCategory', engine)
    #join all three
    merged = product_category.merge(product_subcategory, on='ProductCategoryKey')
    merged.to_sql(f'Production_DimProductCategory', engine, if_exists='replace', index=False)
    print("Production_ProductCategory table transform and load succeeded.")

with DAG(dag_id="aw_dag",schedule_interval="0 9 * * *", start_date=datetime(2022, 3, 5), catchup=False, tags=["product_model"]) as dag:

    with TaskGroup("source_extract_load", tooltip="Extract and load source data.") as source_extract_load:
        source_table_names = get_source_tables()
        load_source_tables = load_source_data(source_table_names)
        #define order
        source_table_names >> load_source_tables

    with TaskGroup("staging_transform_load", tooltip="Transform and load data to staging.") as staging_transform_load:
        transform_source_product_subcategory = transform_source_product_subcategory()
        transform_source_product_category = transform_source_product_category()
        #define task order
        [transform_source_product_subcategory, transform_source_product_subcategory]

    with TaskGroup("production_load", tooltip="Load data to production.") as production_load:
        normalize_product_category = normalize_product_category()
        #define order
        normalize_product_category

    source_extract_load >> staging_transform_load >> production_load
