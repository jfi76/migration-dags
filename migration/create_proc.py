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
    def iterate_proc(self):
        self.set_statement_types()        
        self.get_all_proc()
        for proc in self.procedures:            
            self.statementId =0
            self.process_proc(proc)
        self.ttl_serice.graph.serialize('./output/proc.ttl', 'turtle')
        return self.procedures
    
    def process_proc(self,proc):
        self.ttl_serice.add_stmt('create or replace procedure ' + proc['name'] + '() ', self.statementId , proc['iri'], self.statement_types_dict['CREATE PROC'])   
        self.statementId =self.statementId+1     
        self.ttl_serice.add_stmt("""language plpgsql
as $$""", self.statementId , proc['iri'], self.statement_types_dict['CREATE LANG'])
        self.statementId =self.statementId+1     
        self.ttl_serice.add_stmt("""begin""", self.statementId , proc['iri'], self.statement_types_dict['CREATE BEGIN'])        
        self.statementId =self.statementId+1             
        self.ttl_serice.add_stmt("""end; $$""", self.statementId , proc['iri'], self.statement_types_dict['CREATE END'])        
    def prep_params(procIri):
        print(procIri)
        stmt_str=stmt.procedure_params.format(iri=procIri)
        print(stmt_str)
    def set_statement_types(self):
        ret=self.queryService.query(stmt.statement_types)                   
        for type in ret:
            self.statement_types.append(type['label']['value'])
            self.statement_types_dict[type['label']['value']]=type['iri']['value']
    def get_statements(self,procIri):
        stmt_str=stmt.procedure_statements.replace('?param?',f"<{procIri}>")
        ret=self.queryService.query(stmt_str)        
        return ret 
    def get_all_proc(self):
        ret=self.queryService.query(stmt.stmt_get_all_proc)
        for proc in ret:
            print('process:' + proc['name']['value'])    
            self.procedures.append({"iri":proc['iri']['value'], "name":proc['name']['value']})
        return self.procedures
    
if __name__ == "__main__":
    print ('main')
    c=create_proc()
    c.iterate_proc()
    #c.get_statements('mig:c4c649e1-834d-45c6-ac7f-03583b3494bd')
