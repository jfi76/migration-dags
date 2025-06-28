import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )


import json
import runSparqlWrapper as sparql_service
import statements as stmt
from rdf_ttl_service import rdfTTLService
import re
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
                init_col=self.duplicated_column[init_col]
            self.ttl_service.add_renamed_col(init_col,parsed[1],expr['expr']['value'])
        else:
            for col_renamed in parsed:
                init_col=col_renamed[0]
                if init_col in self.duplicated_column.keys(): 
                    init_col=self.duplicated_column[init_col]
                self.ttl_service.add_renamed_col(init_col,col_renamed[1],expr['expr']['value'])
    def add_column_each_textcomb(self,expr:dict):
        ##"Вставлено: объединенный столбец" = Table.AddColumn(logistics_hand_dash_fram_exception_doc, "doc_header_id + item+code", each Text.Combine({Text.From([doc_header_id], "ru-RU"), [item_code]}, ""), type text)
        #Text.Combine({Text.From([doc_header_id], "ru-RU"), [item_code]}, "")
        val:str=expr['jsstring']['value']

        start=val.find(',')
        val1=val[start+len(','):]
        end=val1.find(',')
        item1=val1[:end].replace('"','').strip()
        print(item1)

        start=val.find('Table.AddColumn(')
        val1=val[start+len('Table.AddColumn('):]
        start=val.find('Text.Combine({')
        val1=val[start+len('Text.Combine({'):]
        end=val1.rfind('}')
        
        items=val1[:end].split(',')
        item2=''
        for item in items:
            if '[' in item and ']' in item:
                if item2!='': item2= item2 + ' || '
                start=item.find('[')
                end=item.rfind(']')
                attr=item[start+1:end]
                print (attr)
                item2=item2 + attr

        self.ttl_service.add_added_col(item1,item2,expr['expr']['value'])
        self.added_col[item1]=item2

    def add_column_each(self,expr:dict):
        val:str=expr['jsstring']['value']
        start=val.find('Table.AddColumn(')
        val1=val[start+len('Table.AddColumn('):]
         #\"Добавлен пользовательский объект\" = Table.AddColumn(#\"Измененный тип\", \"doc_header_id + item+code\", each [doc_header_id] & [item_code]),",
        
        start=val.find(',')
        val1=val[start+len(','):]
        end=val1.find(',')
        item1=val1[:end].replace('"','').strip()
        #replace('{','[').replace('}',']')
        # print('item1:' )
        # print(item1)


        start=val.find('each')
        val1=val[start+len('each'):]
        end=val1.rfind(')')
        item2=val1[:end].replace('[','').replace(']','').replace('&', '||')
        # print('item2:')
        # print(item2)
         
        #parsed=ast.literal_eval(val)
        
    

        self.ttl_service.add_added_col(item1,item2,expr['expr']['value'])
        self.added_col[item1]=item2

    def proc_add_columns(self,expr:dict):
        if 'each ' in expr['jsstring']['value'] and ' & ' in expr['jsstring']['value']:
                self.add_column_each(expr)
        if 'each ' in expr['jsstring']['value'] and 'Text.Combine' in expr['jsstring']['value']:
                self.add_column_each_textcomb(expr)
    def distinct_table(self,expr:dict):
        val:str=expr['jsstring']['value']
        ##Table.Distinct(#\"Переименованные столбцы\", {\"doc_header_id\"})
        match = re.search(r'\{(.*?)\}', val)

        if match:
            result = match.group(1)
            #print(result)
            self.ttl_service.add_table_distinct_col(result,expr['expr']['value'])



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
        self.queryService.insert('delete {?iri ?p ?o} where {?iri rdf:type mig:tabledistinctcolumn . ?iri ?p ?o}')        

        stmt_str=stmt.stmt_table_expr
        ret=self.queryService.query(stmt_str)
        self.duplicated_column={}
        self.added_col={}
        for table_expr_stmt in ret:   
            try:

                if 'Schema=' in  table_expr_stmt['jsstring']['value'] and 'Item=' in  table_expr_stmt['jsstring']['value']: self.proc_schema(table_expr_stmt)
                if 'Table.RenameColumns' in  table_expr_stmt['jsstring']['value']: self.proc_rename_columns(table_expr_stmt)
                if 'Table.DuplicateColumn' in  table_expr_stmt['jsstring']['value']: self.proc_duplicate_columns(table_expr_stmt)
                if 'Table.AddColumn' in  table_expr_stmt['jsstring']['value']: self.proc_add_columns(table_expr_stmt) 
                if 'Table.Distinct' in table_expr_stmt['jsstring']['value']: self.distinct_table(table_expr_stmt) 
                
            except Exception as e:
                print('error in table expr:')
                print(e)
                print(table_expr_stmt)    

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