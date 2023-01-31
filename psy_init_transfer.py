import sys
import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine

sys.path.append( './dags/migration/' )
sys.path.append( './dags/migration/query/' )


from migration.create_proc import create_proc

#get t_* tables
#select * from INFORMATION_SCHEMA.TABLES where substring(TABLE_NAME,1,2)='t_'
@task()
def get_src_tables():
    hook = MsSqlHook(mssql_conn_id="t440s")
    sql = """ select  LOWER(table_name) as table_name from INFORMATION_SCHEMA.TABLES where substring(TABLE_NAME,1,2)='t_' """
    df = hook.get_pandas_df(sql)
    print(df)
    tbl_dict = df.to_dict('dict')
    print(tbl_dict)
    return tbl_dict
#
@task()
def load_src_data(tbl_dict: dict):
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    all_tbl_name = []
    start_time = time.time()
    #access the table_name element in dictionaries
    for k, v in tbl_dict['table_name'].items():
        #print(v)
        all_tbl_name.append(v)
        rows_imported = 0
        sql = f'select * FROM {v}'
        hook = MsSqlHook(mssql_conn_id="t440s")
        df = hook.get_pandas_df(sql)
        df.columns = df.columns.str.lower()
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {v} ')
        df.to_sql(v, engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print(f'Done. {str(round(time.time() - start_time, 2))} total seconds elapsed')
    print("Data imported successful")
    return all_tbl_name

@task()
def prepare_proc():
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    c=create_proc()
    ret=c.get_all_proc()


    for proc in ret:
        exec_str=''
        print(proc['iri'])    
        stmt_ret=c.get_statements(proc['iri'])
        for stmt in stmt_ret:        
            exec_str=exec_str+'\n'+stmt['text']['value']
            print(stmt['text']['value'])
        if exec_str!='' :
            print(exec_str)
            engine.execute(exec_str)


with DAG(dag_id="psy_etl_dag",schedule_interval="0 9 * * *", start_date=datetime(2022, 3, 5),catchup=False,  tags=["psy_init"]) as dag:

    with TaskGroup("extract_psy_load", tooltip="Extract and load source data") as extract_load_src:
        #src_product_tbls = get_src_tables()
        #load_dimProducts = load_src_data(src_product_tbls)
        pg_prepare_proc= prepare_proc()
        #define order
        #src_product_tbls >> load_dimProducts >> 
        pg_prepare_proc
    extract_load_src