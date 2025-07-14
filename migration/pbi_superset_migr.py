
import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )


import json
import runSparqlWrapper as sparql_service
import statements as stmt
from rdf_ttl_service import rdfTTLService

class superset_migr:
    def __init__(self, dir_to_save):
        self.queryService=sparql_service.runSparqlWrapper()
        self.dir_to_save=dir_to_save
        self.ttl_service=rdfTTLService()

    def iterate_marts(self):        
        ret=self.queryService.query(stmt.stmt_to_get_marts)
        for table_expr_stmt in ret:   
            self.prepare_dataset_sql(table_expr_stmt['iri']['value'], table_expr_stmt['hasMainSqlName']['value'])
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
            select_line=select_line+ """,
            """ + col_line
        #print(select_line + ' from ' + mainsql)
        self.ttl_service.mart_dataset_sql(mart_iri, select_line + ' from ' + mainsql)
        filepath=self.dir_to_save+'create_dataset_sql.ttl'  
        self.ttl_service.graph.serialize(filepath, 'turtle') 
        self.queryService.load_ttl(filepath)


    def iterate_sections(self):
            #self.queryService.insert('delete {?iri ?p ?o} where {?iri rdf:type mig:dashrenamedcolumn . ?iri ?p ?o}')

            ret=self.queryService.query(stmt.stmt_pbi_section_containers)
            for table_expr_stmt in ret:   
               #?vc ?name ?y ?vctype     
                if table_expr_stmt['?vctype']['value'] == 'pivotTable':
                    table_expr_stmt['vc']['value'] 

if __name__ == "__main__":
    c=superset_migr('../playground_ai/')
    c.iterate_marts()