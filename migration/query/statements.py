stmt_get_all_proc="""
select * 
{
  ?iri rdf:type mig:pgprocedure .
  ?iri rdfs:label ?name .
}
"""
statement_types="""    
    select  ?iri ?label{
    ?iri rdf:type  mig:pgStatementType .
    ?iri rdfs:label ?label .
     }
    """
procedure_params="""
select  ?iri ?name ?datatype (IF(?mode_str='IN','', 'inout') as ?mode) ?position  ( coalesce(?charlen_str,'') as ?charlen) ( coalesce(?ms_name_str,'') as ?ms_name)
  { bind (?param? as ?param)
    ?param rdf:type mig:pgprocedure .
    ?param rdfs:label ?label . 
    ?iri mig:hasProcedure ?param .  
    ?iri rdf:type mig:pgprocedureparameter .

    ?iri mig:PARAMETER_NAME ?name.
    ?iri mig:DATA_TYPE ?datatype .
    ?iri mig:PARAMETER_MODE ?mode_str .
    ?iri mig:ORDINAL_POSITION ?position .  
    optional {?iri  mig:hasParameter ?ms_param .
    ?ms_param js:PARAMETER_NAME ?ms_name_str .
    } .    
    optional {?iri mig:CHARACTER_MAXIMUM_LENGTH ?len .
    bind (concat('(' , ?len ,')' ) as ?charlen_str ) .    
    } .
    bind (xsd:integer(?position) as ?position_int )  
  } order by ?position_int
"""
procedure_statements="""
select * {
  bind ( ?param? as ?param)
  ?iri mig:hasProcedure ?param .
  ?iri rdf:type mig:pgstatement .
  ?iri mig:StatementText ?text .  
  ?iri mig:StatementId ?statement_id .
  ?iri mig:haspgStatementType ?sttype .
  ?sttype rdf:type mig:pgStatementType .
  ?sttype rdf:type ?statement_type .
  ?sttype rdfs:label ?statement_type_label .
} order by ?statement_id
"""
get_declare="""
select ?iri ?StatementId_Int  ?StatementType ?StatementText (?msproc as ?procIri)
{  
  ?param rdf:type mig:pgprocedure .
  ?param mig:hasProcedure ?msproc .
  ?iri mig:hasProcedure ?msproc .
  ?iri rdf:type mig:msprocedurestatement .
  ?iri js_at:StatementId ?StatementId .
  ?iri  js_at:StatementType ?StatementType .
  ?iri js_at:StatementText ?StatementText .
  bind (xsd:integer(?StatementId) as ?StatementId_Int)
  filter (regex(?StatementText,'declare','i') )
}  order by ?param ?StatementId_Int
"""
get_proc_variable="""
select ?name ?type (coalesce(?len,'') as ?type_len) ?statement ( coalesce(?ms_name_str,'') as ?ms_name)
{
bind(?param? as ?param)  .
?param rdf:type mig:pgprocedure .  
 
?iri mig:VARIABLE_NAME ?name .
?iri mig:hasProcedure ?param .

?iri mig:DATA_TYPE ?type .
?iri rdf:type mig:pgprocedurevariable .
?iri mig:CHARACTER_MAXIMUM_LENGTH ?len.
optional {
   ?iri mig:hasVariable ?msvar .
   ?msvar rdf:type mig:msprocedurevarible . 
   ?msvar mig:VARIABLE_NAME ?ms_name_str .
   }
}

"""
insert_pgvariable="""
insert {
?uid rdf:type mig:pgprocedurevariable;
 mig:VARIABLE_NAME ?name_conv;    
 mig:DATA_TYPE ?type_conv;
 mig:CHARACTER_MAXIMUM_LENGTH ?type_len;
 mig:hasVariable ?iri;   
 mig:hasProcedure ?param .

} 
where{
select 
(iri(concat('http://www.example.com/MIGRATION#',struuid())) as ?uid)
(replace(?name,'@','var_') as ?name_conv) (IF(coalesce(?convPQ,'')='',?type,?convPQ) as ?type_conv) (coalesce(?len,'') as ?type_len) ?statement ?param ?iri
{
#bind(mig:69ee3c35-402f-4fa5-bf95-9945ed3220bd as ?param)  .
?param rdf:type mig:pgprocedure .  
?param mig:hasProcedure ?msproc .  
?iri mig:VARIABLE_NAME ?name .
?iri mig:hasProcedure ?msproc .
?iri mig:hasStatement ?statement .
?iri mig:DATA_TYPE ?type .
?iri rdf:type mig:msprocedurevarible .
?iri mig:CHARACTER_MAXIMUM_LENGTH ?len.
   optional  {
        ?conv rdf:type mig:datatypeconversion .
        ?conv js:MS ?convMS .
        ?conv js:PQ ?convPQ .
        filter (UCASE(?type)=UCASE(?convMS))    
      }    
}
}
"""
delete_variables="""
delete{  ?iri ?p ?o} where {?iri rdf:type mig:pgprocedurevariable . ?iri ?p ?o};
delete{  ?iri ?p ?o} where {?iri rdf:type mig:msprocedurevarible . ?iri ?p ?o};
"""
delete_pgstatements="""
delete{  ?iri ?p ?o} where {?iri rdf:type mig:pgstatement . ?iri ?p ?o};
"""
select_msproc_statement= """
select ?iri ?StatementId_Int  ?StatementType ?StatementText 
{  
  bind (?param? as ?param) .
  ?param etl:hasSourceFile ?file .
  ?param rdf:type mig:pgprocedure .
  ?param mig:hasProcedure ?msproc .
  ?iri mig:hasProcedure ?msproc .
  ?iri rdf:type mig:msprocedurestatement .
  ?iri js_at:StatementId ?StatementId .
  ?iri  js_at:StatementType ?StatementType .
  ?iri js_at:StatementText ?StatementText .
  bind (xsd:integer(?StatementId) as ?StatementId_Int)
} 
order by ?StatementId_Int
"""
select_recursive_visualiz_pbi="""
select distinct ?node  ?name ?parentVisualId
{
  bind(js:N6e9e6a1da69b408fa22d219906b358eb as ?iri)
  ?iri rdf:type mig:msdash .
  ?node js:parentVisualId*  ?iri .
  ?node rdfs:label ?name .
  ?node js:parentVisualId ?parentVisualId .
  ?node rdf:type ?type .  
  filter (?type not in (owl:NamedIndividual,js:ObjectJson) ) 
} 
"""

select_config_pbi="""
 select ?vc_item ?config {  
  ?sect rdf:type mig:DashSection .
  ?vc js:parentJsonId ?sect .
  ?vc js:hasJsonObjectKey 'visualContainers' .
  ?vc_item js:parentJsonId  ?vc .
  ?vc_item js:config ?config .
  ?vc_item js:config ?config .
   } 
"""
