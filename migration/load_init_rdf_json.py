
import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service
from convert_json_to_ontology import json_to_ontology

class load_init_rdf_json:
    def __init__(self,init_rdf_json, output):

        self.init_rdf_json=init_rdf_json
        self.queryService=sparql_service.runSparqlWrapper()
        conv=json_to_ontology(self.init_rdf_json)
        conv.rdf_parsed=output
        conv.processJsonDir()
        self.queryService.load_ttl(self.init_rdf_json+'etl.ttl')    
        self.queryService.load_ttl(self.init_rdf_json+'json.ttl')    
        self.queryService.load_ttl(self.init_rdf_json+'mig.ttl')            
        self.queryService.load_ttl(self.init_rdf_json+'report.ttl')                    

class load_init_rdf_json_doc:
    def __init__(self,init_rdf_json, output):

        self.init_rdf_json=init_rdf_json
        self.queryService=sparql_service.runSparqlWrapper()
        conv=json_to_ontology(self.init_rdf_json)
        conv.rdf_parsed=output
        conv.processJsonDir()
#        self.queryService.load_ttl(self.init_rdf_json+'etl.ttl')    
        self.queryService.load_ttl(self.init_rdf_json+'json.ttl')    


if __name__ == "__main__":
   c=load_init_rdf_json('./init_rdf_json/','./output/')
   
