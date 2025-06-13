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
        self.ttl_service=rdfTTLService()
        self.duplicated_column={}

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
        self.ttl_service.add_table_name(expr['table']['value'],schema,item)
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
            init_col=parsed[0]
            if init_col in self.duplicated_column.keys():
                print('matched') 
                init_col=self.duplicated_column[init_col]
                print(init_col)
            self.ttl_service.add_renamed_col(init_col,parsed[1],expr['expr']['value'])
        else:
            for col_renamed in parsed:
                init_col=col_renamed[0]
                if init_col in self.duplicated_column.keys(): 
                    print('matched')
                    init_col=self.duplicated_column[init_col]
                    print(init_col)
                self.ttl_service.add_renamed_col(init_col,col_renamed[1],expr['expr']['value'])
    def proc_add_columns(self,expr:dict):
        val:str=expr['jsstring']['value']
        start=val.find('Table.DuplicateColumn(')
        val1=val[start+len('Table.DuplicateColumn('):]
        #"    #\"Дублированный столбец\" = Table.DuplicateColumn(LOGISTICS_expert_total, \"doc_open\", \"Копия doc_open\"),",
        start=val.find(',')
        val1=val[start+len(','):]
        end=val1.rfind(')')
        item=val1[:end].replace('{','[').replace('}',']')
        print('item:')
        print(item)
        parsed=ast.literal_eval(item)
        
        if len(parsed)>0 and isinstance(parsed[0],list)==False:
            self.ttl_service.add_duplicated_col(parsed[0],parsed[1],expr['expr']['value'])
            self.duplicated_column[parsed[1]]=parsed[0]
        else:
            for col_renamed in parsed:
                self.ttl_service.add_duplicated_col(col_renamed[0],col_renamed[1],expr['expr']['value'])
                self.duplicated_column[col_renamed[1]]=col_renamed[0]
        print(self.duplicated_column)

    def proc_duplicate_columns(self,expr:dict):
        val:str=expr['jsstring']['value']
        start=val.find('Table.DuplicateColumn(')
        val1=val[start+len('Table.DuplicateColumn('):]
        #"    #\"Дублированный столбец\" = Table.DuplicateColumn(LOGISTICS_expert_total, \"doc_open\", \"Копия doc_open\"),",
        start=val.find(',')
        val1=val[start+len(','):]
        end=val1.rfind(')')
        item=val1[:end].replace('{','[').replace('}',']')
        print('item:')
        print(item)
        parsed=ast.literal_eval(item)
        
        if len(parsed)>0 and isinstance(parsed[0],list)==False:
            self.ttl_service.add_duplicated_col(parsed[0],parsed[1],expr['expr']['value'])
            self.duplicated_column[parsed[1]]=parsed[0]
        else:
            for col_renamed in parsed:
                self.ttl_service.add_duplicated_col(col_renamed[0],col_renamed[1],expr['expr']['value'])
                self.duplicated_column[col_renamed[1]]=col_renamed[0]
        print(self.duplicated_column)
    def iterate_expr(self):
        self.queryService.insert('delete {?iri ?p ?o} where {?iri rdf:type mig:dashrenamedcolumn . ?iri ?p ?o}')
        self.queryService.insert('delete {?iri ?p ?o} where {?iri rdf:type mig:dashduplicatedcolumn . ?iri ?p ?o}')
        self.queryService.insert('delete {?iri ?p ?o} where {?iri rdf:type mig:dashaddedcolumn . ?iri ?p ?o}')        

        stmt_str=stmt.stmt_table_expr
        ret=self.queryService.query(stmt_str)
        self.duplicated_column={}
        for table_expr_stmt in ret:   
            if 'Schema=' in  table_expr_stmt['jsstring']['value'] and 'Item=' in  table_expr_stmt['jsstring']['value']: self.proc_schema(table_expr_stmt)
            if 'Table.RenameColumns' in  table_expr_stmt['jsstring']['value']: self.proc_rename_columns(table_expr_stmt)
            if 'Table.DuplicateColumn' in  table_expr_stmt['jsstring']['value']: self.proc_duplicate_columns(table_expr_stmt)
            if 'Table.AddColumn' in  table_expr_stmt['jsstring']['value']: self.proc_add_columns(table_expr_stmt) 
        filepath=self.dir_to_save+'table_expessions.ttl'  
        self.ttl_service.graph.serialize(filepath, 'turtle') 
        self.queryService.load_ttl(filepath)

if __name__ == "__main__":

    c=process_table_expreesion('../playground_parsed_adds/')
    c.iterate_expr()
#
#        
#        
#        