import sys 
sys.path.append( '../migration/' )
sys.path.append( '../migration/query/' )

from convert_json_to_ontology import json_to_ontology
import query.runSparqlWrapper as sparql_service

if __name__ == "__main__":
    # conv=json_to_ontology('../playground/')
    # conv.rdf_parsed='../playground_parsed/'
    # conv.processJsonDir()
    data = open('../moi_doc_json/insert.sparql').read()
    service=sparql_service.runSparqlWrapper()
    service.insert(data)
