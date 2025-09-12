import sys




sys.path.append( '../migration/' )
sys.path.append( '../migration/query/' )
from pbi__table_expression import process_table_expreesion
from pbi_replace_cols_tabs import replace_cols_tabs
from export_freemind import export_freemind
from pbi_create_export_query import create_export_query
from pbi_calculated_columns import calculated_columns

from convert_json_to_ontology import json_to_ontology
import query.runSparqlWrapper as sparql_service
import query.statements as stmt
from pbi_config_load import config_load 
from load_init_rdf_json import load_init_rdf_json

if __name__ == "__main__":
    stmt_all_dashes="""select ?dash ?fileName  {
    bind(js:N0500555be7d2497793899a8c2f304b34 as ?dash)
    ?dash rdf:type mig:msdash . 
    ?dash etl:hasSourceFile ?fileName . 
    
    }"""    

    # cl=load_init_rdf_json('../init_rdf_json/','../output/')

    # conv=json_to_ontology('../playground/')
    # conv.rdf_parsed='../playground_parsed/'
    # conv.processJsonDir()
    # data = open('../moi_doc_json/insert.sparql').read()
    # service=sparql_service.runSparqlWrapper()
    # service.insert(data)

    # cfrm=export_freemind(stmt.select_recursive_visualiz_pbi,'c:\\zena\\')
    # cfrm.key_name='Дашбоард init'
    # cfrm.get_dashes()


    # c=config_load(stmt.select_config_pbi,'../playground_adds')
    # c.get_and_save()    

    # conv2=json_to_ontology('../playground_adds/')
    # conv2.rdf_parsed='../playground_parsed_adds/'
    # conv2.processJsonDir()

    # c=process_table_expreesion('../playground_parsed_adds/')
    # c.iterate_expr()


    # data = open('../moi_doc_json/insert2.sparql').read()
    # service=sparql_service.runSparqlWrapper()
    # service.insert(data)

    # c=process_table_expreesion('../playground_parsed_adds/')
    # c.iterate_expr()
    # data = open('../moi_doc_json/insert2_2.sparql').read()
    # service=sparql_service.runSparqlWrapper()
    # service.insert(data)


    # cfrm=export_freemind(stmt.select_recursive_visualiz_pbi,'c:\\zena\\')
    # cfrm.key_name='Дашбоард'
    # cfrm.get_dashes()

    # calc=calculated_columns('../playground_parsed_adds/')
    # calc.replace_expression()

    # conv3=json_to_ontology('../playground_ai/')
    # conv3.rdf_parsed='../playground_ai_parsed/'
    # conv3.processJsonDir()

    # data = open('../moi_doc_json/insert2_3.sparql').read()
    # service=sparql_service.runSparqlWrapper()
    # service.insert(data)

    crepl=replace_cols_tabs('',stmt.stmt_tables_source_str,stmt.stmt_tables_cols,stmt.stmt_tables_source_str, '../playground_parsed_adds/', stmt_all_dashes)
    crepl.do_dashes()

    # cexp=create_export_query(stmt_all_dashes,'../playground_parsed_adds/')
    
    # cexp.create_view_sql()
    
    # cexp.iterate_dashes()

    # cexp.create_from_for_export_query()
    # cexp.create_mart_export_query()
    #cexp.run_views_onserver()
    
#####################################
    # data = open('../moi_doc_json/insert3.sparql').read()
    # service=sparql_service.runSparqlWrapper()
    # service.insert(data)

    # cexpfm2=export_freemind(stmt.select_recursive_vizualiz_mart,'c:\\zena\\')    
    # cexpfm2.get_marts()
