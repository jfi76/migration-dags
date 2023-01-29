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
select  ?iri ?name ?datatype (IF(?mode_str='IN','', 'inout') as ?mode) ?position  ( coalesce(?charlen_str,'') as ?charlen)
  { bind (?param? as ?param)
    ?param rdf:type mig:pgprocedure .
    ?param rdfs:label ?label . 
    ?iri mig:hasProcedure ?param .  
    ?iri rdf:type mig:pgprocedureparameter .

    ?iri mig:PARAMETER_NAME ?name.
    ?iri mig:DATA_TYPE ?datatype .
    ?iri mig:PARAMETER_MODE ?mode_str .
    ?iri mig:ORDINAL_POSITION ?position .  
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
select ?name ?type (coalesce(?len,'') as ?type_len) ?statement  
{
bind(?param? as ?param)  .
?param rdf:type mig:pgprocedure .  
?param mig:hasProcedure ?msproc .  
?iri mig:VARIABLE_NAME ?name .
?iri mig:hasProcedure ?msproc .
?iri mig:hasStatement ?statement .
?iri mig:DATA_TYPE ?type .
?iri rdf:type mig:msprocedurevarible .
?iri mig:CHARACTER_MAXIMUM_LENGTH ?len.
}

"""