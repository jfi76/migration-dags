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
from migration.load_init_rdf_json import load_init_rdf_json

# get t_* tables
# use own filter to get needed tables
@task()
def get_src_tables():
    hook = MsSqlHook(mssql_conn_id="mssql")    
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
    for k, v in tbl_dict['table_name'].items():
        all_tbl_name.append(v)
        rows_imported = 0
        sql = f'select * FROM {v}'
        hook = MsSqlHook(mssql_conn_id="mssql")
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
    c.log_task_start()
    c.iterate_proc()
    c.ttl_serice.emptyGraph()    

    for proc in c.procedures:
        print(proc['iri'])    
        try: 
            exec_str=''
            c.ttl_serice.log_proc_create(c.taskIri,proc['iri'],proc['name'])
            stmt_ret=c.get_statements(proc['iri'])
            for stmt in stmt_ret:        
                exec_str=exec_str+'\n'+stmt['text']['value']
                print(stmt['text']['value'])
            if exec_str!='' :
                engine.execute(exec_str)
        except Exception as e:
            print('taskIri:'+c.taskIri + ';proc:' + proc['iri'] + ';exception:'+str(e))    
            c.ttl_serice.log_proc_error(c.taskIri, proc['iri'], str(e))
    c.ttl_serice.graph.serialize(c.fileoutput, 'turtle')        
    c.queryService.load_ttl(c.fileoutput)        
@task
def get_proc_plan():    
    ms_to_pq=mssql_to_postgres(con_server=Variable.get('mssql_serv'), con_passw=Variable.get('mssql_pass'), con_db=Variable.get('mssql_db'))
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

@task
def init_rdf_json():
    load_init_rdf_json('./dags/init_rdf_json/','./dags/output/')
with DAG(dag_id="proc_etl_migr_dag",schedule_interval=None , start_date=datetime(2023, 2, 1),catchup=False,  tags=["proc_migr"]) as dag:

    with TaskGroup("mssql_proc_to_pgsql", tooltip="ms procedure to pgsql procedure") as extract_load_src:
        src_product_tbls = get_src_tables()
        load_dimProducts = load_src_data(src_product_tbls)
        pg_create_proc= prepare_proc()
        proc_plan=get_proc_plan()
        convert_json_to_rdf=convert_to_ontology()
        run_initial_insert=run_insert_sparql()
        procedure_declare=add_proc_declare()
        load_init=init_rdf_json()
        #define order
        src_product_tbls >> load_dimProducts >> load_init >> proc_plan >>  convert_json_to_rdf >> run_initial_insert >> procedure_declare >> pg_create_proc
    extract_load_src