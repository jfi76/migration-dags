import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )


import json
import runSparqlWrapper as sparql_service
import statements as stmt
from rdf_ttl_service import rdfTTLService
import re

class calculated_columns:
    columns:dict
    def __init__(self,  dir_to_save):        
        self.dir_to_save=dir_to_save
        self.queryService=sparql_service.runSparqlWrapper()
        self.ttl_service=rdfTTLService()

    def replace_expression(self):
        self.ttl_service.emptyGraph()
        self.queryService.insert('delete {?iri mig:hasExpressionReplaced ?o} where {?iri mig:hasExpressionReplaced ?o . ?iri rdf:type mig:DashColumn}')        
        self.queryService.insert('delete {?iri mig:hasExpressionColumn ?o} where {?iri mig:hasExpressionColumn ?o . ?iri rdf:type mig:DashColumn}')        
        
        #colsearch
        ret=self.queryService.query(stmt.stmt_calc_columns_repalce_expr)
        self.columns={}
        self.expr_cols=[]
        for column_expr_stmt in ret:   
            try:                
                if column_expr_stmt['iri']['value'] in self.columns.keys():
                    val=self.columns[column_expr_stmt['iri']['value']]
                else: 
                    val=column_expr_stmt['expression']['value']    
                # print(column_expr_stmt['colsearch']['value'])    
                # print(column_expr_stmt['colrepl']['value'])
                #colsearch=f"""{column_expr_stmt['tableName']['value']}'[{column_expr_stmt['colname2']['value']}]"""
                colsearch=f"""[{column_expr_stmt['colname2']['value']}]"""                    
                # print(colsearch)
                # print(column_expr_stmt['colrepl']['value'])
                replaced=val.replace(colsearch, column_expr_stmt['colrepl']['value'])
                # if replaced!=column_expr_stmt['expression']['value']:
                self.columns[column_expr_stmt['iri']['value']]=replaced
                self.expr_cols.append({"col": column_expr_stmt['iri']['value'], "exp_col": column_expr_stmt['column2']['value']})
                # print(self.columns)    
            except Exception as e:
                print('error in column expr:')
                print(e)
                print(column_expr_stmt)    
        for item in self.columns.keys():
            # print(item)
            # print(self.columns[item])
            self.ttl_service.column_expression_renamed(item,self.columns[item])
        for item in self.expr_cols:
           self.ttl_service.column_expression_column(item['col'],item['exp_col'])     
        filepath=self.dir_to_save+'column_expession_renamed.ttl'  
        self.ttl_service.graph.serialize(filepath, 'turtle') 
        self.queryService.load_ttl(filepath)
            
if __name__ == "__main__":
    c=calculated_columns('../playground_parsed_adds/')
    c.replace_expression()