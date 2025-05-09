import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )


import json
import runSparqlWrapper as sparql_service
import statements as stmt
from rdf_ttl_service import rdfTTLService

import ast
import jsbeautifier

#?table ?tabname ?expr ?jsstring 
class process_table_expreesion:
    def __init__(self,  dir_to_save):        
        self.dir_to_save=dir_to_save
        self.queryService=sparql_service.runSparqlWrapper()
        self.ttl_serice=rdfTTLService()

    def proc_schema(self,expr:dict):
        val:str=expr['jsstring']['value']
        start=val.find('{[Schema="')
        val1=val[start+len('{[Schema="'):]
        end=val1.find('"')
        schema=val1[:end]
        start=val.find('Item="')
        val1=val[start+len('Item="'):]
        end=val1.find('"')
        item=val1[:end]        
        self.ttl_serice.add_table_name(expr['table']['value'],schema,item)
    def proc_rename_columns(self,expr:dict):
        val:str=expr['jsstring']['value']
        start=val.find('Table.RenameColumns(')
        val1=val[start+len('Table.RenameColumns('):]
        #Table.RenameColumns(DIM_branch,{{\"branch_short_name\", \"МРФ\"}, {\"branch_pleasant_name\", \"РФ\"}, {\"macrobranch_consolidation\", \"МР\"}}),
        start=val.find('{')
        val1=val[start-1+len('{'):]
        end=val1.rfind(')')
        item=val1[:end].replace('{','[').replace('}',']')
        parsed=ast.literal_eval(item)
        #print(parsed)
        if len(parsed)>0 and isinstance(parsed[0],list)==False:
            self.ttl_serice.add_renamed_col(parsed[0],parsed[1],expr['expr']['value'])
            #print(parsed[0]+'=>' + parsed[1])     
        else:
            for col_renamed in parsed:
                self.ttl_serice.add_renamed_col(col_renamed[0],col_renamed[1],expr['expr']['value'])
                #print(col_renamed[0]+'=>' + col_renamed[1])
        
    def iterate_expr(self):
        stmt_str=stmt.stmt_table_expr
        ret=self.queryService.query(stmt_str)
        for table_expr_stmt in ret:   
            if 'Schema=' in  table_expr_stmt['jsstring']['value'] and 'Item=' in  table_expr_stmt['jsstring']['value']: self.proc_schema(table_expr_stmt)
            if 'Table.RenameColumns' in  table_expr_stmt['jsstring']['value']: self.proc_rename_columns(table_expr_stmt)

        filepath=self.dir_to_save+'table_expessions.ttl'  
        self.ttl_serice.graph.serialize(filepath, 'turtle') 
        self.queryService.load_ttl(filepath)

if __name__ == "__main__":

    c=process_table_expreesion('../playground_parsed_adds/')
    c.iterate_expr()
#
#        
#        
#        