import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service
import statements as stmt
from rdf_ttl_service import rdfTTLService

class replace_cols_tabs:
    tables=[]
    columns={}
    dax_for_replace=[]
    def __init__(self, dash_iri, sparql_tables:str, sparql_columns:str, sparql_dax:str, dir_to_save, stmt_dashes ):
        self.queryService=sparql_service.runSparqlWrapper()
        self.ttl_service=rdfTTLService()        
        self.dash_iri=dash_iri
        self.sparql_tables=sparql_tables
        self.sparql_columns=sparql_columns
        self.sparql_dax=sparql_dax
        self.dir_to_save=dir_to_save
        self.stmt_dashes=stmt_dashes
    def do_dashes(self):
        ret=self.queryService.query(self.stmt_dashes)
        #self.queryService.insert('delete {?tab  mig:hasExpressionReplaced ?o} where { ?tab rdf:type mig:msDashTable .?tab mig:hasExpressionReplaced ?o } ')        

        for export_stmt_result in ret:
            self.dash_iri= export_stmt_result['dash']['value']
            self.init_arrays()
            self.replace_cols()
            self.finalize()
        

    def replace_str_by_cols(self, tabname, dax, iri):
        ret=dax
        if  tabname in self.columns.keys():
            print(tabname)
            dax_str=dax            
            for col in self.columns[tabname]:
                if col["tab_col_name"] in dax:
                    print(col["tab_col_name"]+' '+ col["to_replace"])
                    dax_prev=dax_str
                    dax_str=dax_str.replace(col["tab_col_name"],col["to_replace"]) 
                    if dax_prev!=dax_str:    
                        self.ttl_service.add_dependency(col["col_iri"],iri)
            if dax_str!=dax:                 
                ret=dax_str        
                
        return ret 

    def replace_str_by_cols_simple(self, tabname, dax, iri):
        ret=dax
        if  tabname in self.columns.keys():
            dax_str=dax            
            for col in self.columns[tabname]:
                if col["col_name"] in dax:                    
                    print(col["col_name"]+' '+ col["to_replace"])
                    dax_prev=dax_str
                    dax_str=dax_str.replace(col["col_name"],col["to_replace"]) 
                    if dax_prev!=dax_str:    
                        self.ttl_service.add_dependency(col["col_iri"],iri)
            if dax_str!=dax: 
                ret=dax_str        
                
        return ret 

    def replace_cols(self):
        for tab in self.dax_for_replace:
            for table in self.tables:
                if table in tab['replaced']:
                    print(table)
                    tab['replaced']=self.replace_str_by_cols(table, tab['replaced'], tab["iri"])
            if self.check_uncompleted(tab['replaced']):
                print('uncomleted')
                for table1 in self.tables:
                    if table1==tab['tab_name']:
                        tab['replaced']=self.replace_str_by_cols_simple(table1, tab['replaced'], tab["iri"])
                for table2 in self.tables:        
                    tab['replaced']=self.replace_str_by_cols_simple(table2, tab['replaced'], tab["iri"])


    def check_uncompleted(self,s:str):
        char="["
        positions = [i for i, c in enumerate(s) if c == char]
        for posit in positions:
            pos = s.find('[',posit)          
            if pos != -1:  
                if pos == 0:
                    continue 
                elif s[pos - 1] != "'":
                    return True 
        
        return False
        
    def finalize(self):
        for dax in self.dax_for_replace: 
            if dax['replaced']!=dax['dax'] :
                self.ttl_service.table_expression_renamed(dax['iri'],dax['replaced'])
        filepath=self.dir_to_save+'col_table_exp_replaced.ttl'  
        self.ttl_service.graph.serialize(filepath, 'turtle') 
        self.queryService.load_ttl(filepath)
        self.ttl_service.emptyGraph()

                    

    def init_arrays(self):
        q=self.sparql_tables.replace('?param?',f'"{self.dash_iri}"')        
        ret=self.queryService.query(q)               
        for export_stmt_result in ret:         
            self.tables.append(export_stmt_result['obj_name']['value'])
        q=self.sparql_columns.replace('?param?',f'"{self.dash_iri}"')        
        ret=self.queryService.query(q)               
        
        for export_stmt_result in ret:         
            if export_stmt_result['obj_name']['value'] not in self.columns.keys():
                self.columns[export_stmt_result['obj_name']['value']]=[]
            self.columns[export_stmt_result['obj_name']['value']].append(
                {"tab_col_name": f"""'{export_stmt_result['obj_name']['value']}'[{export_stmt_result['colname']['value']}]""" ,
                 "to_replace":f"""'{export_stmt_result['hasExportSqlNameTab']['value']}'[{export_stmt_result['hasExportSqlNameCol']['value']}]""",
                 "col_name":f"""[{export_stmt_result['colname']['value']}]""",
                 "col_iri":f"""{export_stmt_result['obj_iri']['value']}"""
                }
                 )
        q=self.sparql_dax.replace('?param?',f'"{self.dash_iri}"')        
        ret=self.queryService.query(q)             
        for export_stmt_result in ret:         
            self.dax_for_replace.append({"iri" : export_stmt_result['iri']['value'],"obj_name": export_stmt_result['obj_name']['value'],
                                "dax" : export_stmt_result['sourceString']['value'],"replaced":export_stmt_result['sourceString']['value'],
                                "tab_name": export_stmt_result['tab_name']['value']
                                })
            

if __name__ == "__main__":
    c=replace_cols_tabs('',stmt.stmt_tables_source_str,stmt.stmt_tables_cols,stmt.stmt_tables_source_str, '../playground_parsed_adds/', """select ?dash ?fileName  {
    ?dash rdf:type mig:msdash . 
    ?dash etl:hasSourceFile ?fileName . 
    }""")

    

