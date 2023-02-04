import sys
import os
import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable

sys.path.append( './dags/migration/' )
sys.path.append( './dags/migration/query/' )


from migration.create_proc import create_proc
from migration.get_proc_plans import mssql_to_postgres
from migration.convert_json_to_ontology import  json_to_ontology
import migration.query.runSparqlWrapper as sparql_service
from migration.process_declare import process_declare

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
    taksIri=c.log_task_start()
    c.ttl_serice.log_proc_create(taksIri,proc['iri'])
    c.iterate_proc()
    ret=c.get_all_proc()
    for proc in ret:
        exec_str=''
        print(proc['iri'])    
        c.ttl_serice.log_proc_create(taksIri,proc['iri'])
        stmt_ret=c.get_statements(proc['iri'])

        for stmt in stmt_ret:        
            exec_str=exec_str+'\n'+stmt['text']['value']
            print(stmt['text']['value'])
        if exec_str!='' :
            try: 
                print(exec_str)
                engine.execute(exec_str)
            except Exception as e:
                print(e)    
                c.ttl_serice.log_proc_error(taksIri,proc['iri'],str(e))
@task
def get_proc_plan():    
    ms_to_pq=mssql_to_postgres(con_server=Variable.get('mssql_serv'), con_passw=Variable.get('mssql_pass'))
    ms_to_pq.exec()  

@task 
def convert_to_ontology():
    conv=json_to_ontology('./dags/json_data/')    
    conv.processJsonDir()

@task 
def run_insert_sparql():
    data = open('./dags/sparql/insert.sparql').read()
    service=sparql_service.runSparqlWrapper()
    service.insert(data)

@task
def add_proc_declare():      
    c=process_declare()
    c.iterate_declare()      

with DAG(dag_id="psy_etl_dag",schedule_interval="0 9 * * *", start_date=datetime(2022, 3, 5),catchup=False,  tags=["psy_init"]) as dag:

    with TaskGroup("mssql_proc_to_pgsql", tooltip="ms procedure to pgsql procedure") as extract_load_src:
        src_product_tbls = get_src_tables()
        load_dimProducts = load_src_data(src_product_tbls)
        pg_create_proc= prepare_proc()
        proc_plan=get_proc_plan()
        convert_json_to_rdf=convert_to_ontology()
        run_initial_insert=run_insert_sparql()
        procedure_declare=add_proc_declare()
        #define order
        src_product_tbls >> load_dimProducts >> proc_plan >> convert_json_to_rdf >> run_initial_insert >> procedure_declare >> pg_create_proc
    extract_load_src