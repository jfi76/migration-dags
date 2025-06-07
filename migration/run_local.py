from create_proc import create_proc
from get_proc_plans import mssql_to_postgres
from convert_json_to_ontology import  json_to_ontology
import query.runSparqlWrapper as sparql_service
from process_declare import process_declare
from load_init_rdf_json import load_init_rdf_json
import sqlalchemy 
from sqlalchemy import create_engine, MetaData, text
import os

def drop_proc(procedures, engine):
    with engine.connect() as connection:
        for proc in procedures:
            
            try: 
                connection.begin()
                connection.execute(text(f'''drop procedure if exists {proc['name']}; '''))
                connection.commit()
            except Exception as e:
                connection.rollback()
                print(e)    

def prepare_proc():
    engine = sqlalchemy.create_engine(f'''postgresql://postgres:mysecretpassword@localhost:5432/mig15''')    
    c=create_proc()
    c.fileoutput='../output\/proc.ttl'
    c.log_task_start()
    c.iterate_proc()    
    c.ttl_serice.emptyGraph()    
    drop_proc(c.procedures,engine)
    
    with engine.connect() as connection:
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
                    connection.begin()
                    connection.execute(text(exec_str))
                    connection.commit()
            except Exception as e:
                connection.rollback()
                print('taskIri:'+c.taskIri + ';proc:' + proc['iri'] + ';exception:'+str(e))    
                c.ttl_serice.log_proc_error(c.taskIri, proc['iri'], str(e))
    c.ttl_serice.graph.serialize(c.fileoutput, 'turtle')        
    c.queryService.load_ttl(c.fileoutput)

if __name__ == "__main__":
   ####init_rdf_json####
#    cl=load_init_rdf_json('../init_rdf_json/','../output/')
   ###get_proc_plan####
#    ms_to_pq=mssql_to_postgres(con_server=os.environ['mssql_serv'], con_passw=os.environ['mssql_pass'], con_db='PS_TEST_1')
#    ms_to_pq.xml_data='../xml_data/'
#    ms_to_pq.json_data='../json_data/'
#    ms_to_pq.exec()
# #    ####convert_to_ontology####
#    conv=json_to_ontology('../json_data/')
#    conv.rdf_parsed='../rdf_parsed/'
#    conv.processJsonDir()
#    ###run_insert_sparql####
#    data = open('../sparql/insert.sparql').read()
#    service=sparql_service.runSparqlWrapper()
#    service.insert(data)
     ##
  d=process_declare()
  d.filepath='../output/vars.ttl'
  d.iterate_declare()      
   #prepare_proc####   
  prepare_proc() 