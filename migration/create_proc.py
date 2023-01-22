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
        ret=self.queryService.query(stmt.stmt_get_all_proc)
        for proc in ret:
            print('process:' + proc['name']['value'])    
            self.procedures.append({"iri":proc['iri']['value'], "name":proc['iri']['value']})
        for proc in self.procedures:
            #self.ttl_serice.add_stmt('create or alter procedure ' + proc.name , self.statementId , proc.iri, '')
            print(proc['iri'])
        #print(self.procedures[0]['iri'])    
#self.ttl_serice            
        return ret
    def set_statement_types(self):
        ret=self.queryService.query(stmt.statement_types)                   
        for type in ret:
            self.statement_types.append(type['label']['value'])
            self.statement_types_dict[type['label']['value']]=type['iri']['value']
        print('item ')    
        print(self.statement_types[0]+'q:'+self.statement_types_dict['SELECT'])    

if __name__ == "__main__":
    print ('main')
    c=create_proc()
    c.iterate_proc()