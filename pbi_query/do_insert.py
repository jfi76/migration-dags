import sys


sys.path.append( '../migration/' )
sys.path.append( '../migration/query/' )

from convert_json_to_ontology import json_to_ontology
import query.runSparqlWrapper as sparql_service
import query.statements as stmt
from pbi_config_load import config_load 
from load_init_rdf_json import load_init_rdf_json

if __name__ == "__main__":

    # cl=load_init_rdf_json('../init_rdf_json/','../output/')

    # conv=json_to_ontology('../playground/')
    # conv.rdf_parsed='../playground_parsed/'
    # conv.processJsonDir()
    # data = open('../moi_doc_json/insert.sparql').read()
    # service=sparql_service.runSparqlWrapper()
    # service.insert(data)

    # c=config_load(stmt.select_config_pbi,'../playground_adds')
    # c.get_and_save()    

    # conv2=json_to_ontology('../playground_adds/')
    # conv2.rdf_parsed='../playground_parsed_adds/'
    # conv2.processJsonDir()

    data = open('../moi_doc_json/insert2.sparql').read()
    service=sparql_service.runSparqlWrapper()
    service.insert(data)

############################
    # conv2=json_to_ontology('../playground_adds/')
    # conv2.rdf_parsed='../playground_parsed_adds/'
    # conv2.processJsonDir()
