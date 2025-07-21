
import sys

from pbi_superset.superset_api import form_send_api 
sys.path.append( './query/' )
sys.path.append( './pbi_superset/' )
sys.path.append( './migration/query/' )
sys.path.append( './migration/pbi_superset/' )


import json
import datetime
import runSparqlWrapper as sparql_service
import statements as stmt
from rdf_ttl_service import rdfTTLService


class superset_migr:
    def __init__(self, dir_to_save):
        self.queryService=sparql_service.runSparqlWrapper()
        self.dir_to_save=dir_to_save
        self.ttl_service=rdfTTLService()
        self.form_send_api=form_send_api(dir_to_save)

    def iterate_marts(self):     
        self.queryService.insert('delete {?dashmart mig:hasSqlDataset ?o} where { ?dashmart rdf:type mig:dashmart .?dashmart mig:hasSqlDataset ?o .} ')           
        self.queryService.insert('delete {?dashmart mig:hasSqlDataset ?o} where { ?dashmart rdf:type mig:DashColumn .?dashmart mig:hasSqlDataset ?o .} ')           
        ret=self.queryService.query(stmt.stmt_to_get_marts)
        for table_expr_stmt in ret:   
            #print(table_expr_stmt['hasMainSqlName']['value'])
            self.prepare_dataset_sql(table_expr_stmt['iri']['value'], table_expr_stmt['hasMainSqlName']['value'])
        filepath=self.dir_to_save+'create_dataset_sql.ttl'  
        self.ttl_service.graph.serialize(filepath, 'turtle') 
        self.queryService.load_ttl(filepath)

    def create_superset(self):        
        ret=self.queryService.query(stmt.stmt_to_get_marts)
        for table_expr_stmt in ret:   
            self.create_superset_from_mart(table_expr_stmt)
            #table_expr_stmt['iri']['value'],table_expr_stmt['sql']['value']

    def json_dump_recordset(self,recordset, filepath):
        
        fjson = open(filepath, "w", encoding="utf-8")
        fjson.write(json.dumps(recordset, ensure_ascii=False))            
        fjson.close()

    def create_superset_from_mart(self,recordset):        
        #self.queryService.query(stmt.stmt_to_get_marts)
        self.json_dump_recordset(recordset, self.dir_to_save+'dataset.json')
        self.form_send_api.post_dataset(recordset['dash_prefix']['value'] + ' ' + str(datetime.datetime.now()))
        self.form_send_api.post_dashboard(recordset['dash_prefix']['value'] + ' ' + str(datetime.datetime.now()))
        self.iterate_sections(recordset['dash_iri']['value'])
    def prepare_dataset_sql(self,mart_iri, mainsql):
# ?sqlNameTable ?col_name  ?dataType ?conv_dataType ?exportSqlName (concat(' Nullable (',?conv_dataType,')') as ?clickDt) 
# ?run_sql_click         
        ret=self.queryService.query(stmt.stmt_dataset_sql.replace('?param?',"'"+mart_iri+"'"))
        select_line=''
        for table_expr_stmt in ret:
            col_line=''
            if table_expr_stmt['calc_line']['value']!='':                  
                col_line = table_expr_stmt['calc_line']['value'] + ' as ' 
            col_line= col_line + table_expr_stmt['exportSqlName']['value']
            #print(col_line)
            
            self.ttl_service.mart_dataset_column_sql(table_expr_stmt['col']['value'], col_line )
            if select_line!='':
                select_line=select_line+ """,
            """ 
            select_line=select_line+ col_line                
        #print(select_line + ' from ' + mainsql)
        self.ttl_service.mart_dataset_sql(mart_iri,'select ' + select_line + ' from ' + mainsql)

    def prepare_pivot_tableex(self,recordset:dict, type:str,index:int):
        
        self.json_dump_recordset(recordset, self.dir_to_save+f'{type}{index}.json')
        ret=self.queryService.query(stmt.stmt_get_layout_table.replace('?param?',"'"+recordset['vc']['value'] +"'"))
        self.json_dump_recordset(ret, self.dir_to_save+f'{type}{index}_columns.json')
        
        self.form_send_api.form_chart_wrapper(type)

    def iterate_sections(self, dash_iri):
            #self.queryService.insert('delete {?iri ?p ?o} where {?iri rdf:type mig:dashrenamedcolumn . ?iri ?p ?o}')
            ret=self.queryService.query(stmt.stmt_pbi_section_containers.replace("?param?", '"'+ dash_iri +'"' ))
            i=0
            for table_expr_stmt in ret :   
                if table_expr_stmt['vctype']['value'] in ['pivotTable','tableEx']:
                    self.prepare_pivot_tableex(table_expr_stmt,table_expr_stmt['vctype']['value'],i)
                    i=i+1
            ret=self.queryService.query(stmt.stmt_get_layout_table_dash.replace("?param?", '"'+ dash_iri +'"' ))                    
            self.json_dump_recordset(ret, self.dir_to_save+f'all_columns.json')
if __name__ == "__main__":
    c=superset_migr('../playground_ai/')
    #c.iterate_marts()
    c.create_superset()