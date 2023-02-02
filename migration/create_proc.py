import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service
import statements as stmt
from rdf_ttl_service import rdfTTLService

class create_proc:
    def __init__(self):
        self.procedures=[]
        self.statement_types=[]
        self.statement_types_dict:dict={}
        self.queryService=sparql_service.runSparqlWrapper()
        self.ttl_serice=rdfTTLService()
        self.statementId =0
        self.fileoutput='./dags/output/proc.ttl'
    def iterate_proc(self):
        self.set_statement_types()        
        self.get_all_proc()
        for proc in self.procedures:            
            self.statementId =0
            self.process_proc(proc)
        self.ttl_serice.graph.serialize(self.fileoutput, 'turtle')
        self.queryService.load_ttl(self.fileoutput)
        return self.procedures
    
    def process_proc(self,proc):
#create
        self.ttl_serice.add_stmt('create or replace procedure ' + proc['name'] + ' ', self.statementId , proc['iri'], self.statement_types_dict['CREATE PROC'])   

        param_str=self.prep_params(proc['iri']) 
        self.statementId =self.statementId+1    
#parameters
        self.ttl_serice.add_stmt('(' + param_str +')' , self.statementId , proc['iri'], self.statement_types_dict['CREATE PARAM'])   
        self.statementId =self.statementId+1     

        self.ttl_serice.add_stmt("""language plpgsql
as $$""", self.statementId , proc['iri'], self.statement_types_dict['CREATE LANG'])
        self.statementId =self.statementId+1     
#declare section
        declare_section=self.get_proc_variables(proc['iri'])
        self.ttl_serice.add_stmt(declare_section, self.statementId , proc['iri'], self.statement_types_dict['DECLARE'])        
        self.statementId =self.statementId+1             
#body
        self.ttl_serice.add_stmt("""begin""", self.statementId , proc['iri'], self.statement_types_dict['CREATE BEGIN'])        
        self.statementId =self.statementId+1

        self.ttl_serice.add_stmt("""end; $$""", self.statementId , proc['iri'], self.statement_types_dict['CREATE END'])        
        self.statementId =self.statementId+1                     
        
    def prep_params(self,procIri):
        stmt_str=stmt.procedure_params.replace('?param?',f"<{procIri}>")
        ret=self.queryService.query(stmt_str)
        param_str=''
        for param in ret:
            if param_str!='' : param_str=param_str+', '
            if  param['mode']['value']!='' : param_str=param_str+' '+ param['mode']['value']            
            param_str=param_str+' '+ param['name']['value'] +  ' ' + param['datatype']['value'] + ' ' 
            if  param['charlen']['value']!='' : param_str=param_str+' '+ param['charlen']['value']+ ' '
        return param_str    
    def set_statement_types(self):
        ret=self.queryService.query(stmt.statement_types)                   
        for type in ret:
            self.statement_types.append(type['label']['value'])
            self.statement_types_dict[type['label']['value']]=type['iri']['value']
    def get_statements(self,procIri):
        stmt_str=stmt.procedure_statements.replace('?param?',f"<{procIri}>")
        ret=self.queryService.query(stmt_str)        
        return ret
    def get_proc_variables(self,procIri):
        ret_str=''
        stmt_str=stmt.get_proc_variable.replace('?param?',f"<{procIri}>")
        ret=self.queryService.query(stmt_str)        
        for declare_var in ret:
            ret_str=ret_str+declare_var['name']['value']+ ' ' +declare_var['type']['value'] + ' ' +declare_var['type_len']['value'] +'; \n'
        if ret_str!='':
            ret_str='declare \n'+ret_str
        return ret_str
    def get_all_proc(self):
        ret=self.queryService.query(stmt.stmt_get_all_proc)
        for proc in ret:
            print('process:' + proc['name']['value'])    
            self.procedures.append({"iri":proc['iri']['value'], "name":proc['name']['value']})
        return self.procedures
    
if __name__ == "__main__":
    print ('main')
    c=create_proc()
    c.fileoutput='./output/proc.ttl'
    c.iterate_proc()
    #c.get_statements('mig:c4c649e1-834d-45c6-ac7f-03583b3494bd')
