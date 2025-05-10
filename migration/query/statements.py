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
  bind(?param? as ?iri)
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

select_recursive_visualiz_eyed="""
select distinct ?node  (coalesce(?label,?name,?JsonObjectKey) as ?nodeName) ?parentVisualId
{
  ?iri rdf:type mig:msdash .
  ?node js:parentJsonId*  ?iri .
  optional{?node rdfs:label ?label .  }
  optional{?node js:name ?name .  }
  optional{?node js:hasJsonObjectKey ?JsonObjectKey .  }    
  ?node js:parentJsonId ?parentVisualId .
  ?node rdf:type ?type .  
  filter (?type not in (owl:NamedIndividual,js:ObjectJson) ) 
}  
"""
stmt_to_get_dasahes="""
 select ?iri ?hasSourceFile {  
?iri rdf:type mig:msdash .
?iri etl:hasSourceFile  ?hasSourceFile .
   } 
"""

stmt_table_expr="""
select ?table ?tabname ?expr ?jsstring 
{
?expr mig:hasMsDashTable ?table .
?expr rdf:type  mig:DashExpression . 
?expr  js:jsstring ?jsstring.
?table js:name ?tabname .   
} 
"""

stmt_table_relations="""
select *
{
  bind(?param? as ?iri)
  ?iri mig:hasRelationshipFrom ?rel1 .
  ?rel1 rdf:type mig:dashTableRelationship  .
  ?rel2 mig:hasParentRelationship* ?rel1 .
  ?rel2 rdf:type mig:dashTableRelationship  .
  ?rel2tabfrom mig:hasRelationshipFrom ?rel2.
  ?rel2tabto mig:hasRelationshipTo  ?rel2.
  ?rel2tabfrom js:name ?tabfromName .
  ?rel2tabto js:name ?tabtoName .
  optional {?rel2tabto mig:hasSqlName ?tabtohasSQLname}.
  optional {?rel2tabfrom mig:hasSqlName ?tabfromhasSQLname} .
  ?columTo mig:hasRelationshipColumnTo  ?rel2 .
  ?columFrom mig:hasRelationshipColumnFrom  ?rel2 .
  optional {?columTo mig:hasSqlName ?columToSQLname}.
  optional {?columFrom mig:hasSqlName ?columFromSQLname} .  
}
"""
stmt_tablefrom_sorted="""
select (max(?etlSource) as ?src) ?dash ?tableFrom (count(*) as ?count) (max(?hasSqlName) as ?tbname) 
{
?tableFrom  rdf:type mig:msDashTable .
?tableFrom mig:hasRelationshipFrom ?relat.
 ?tableFrom mig:hasMsDash ?dash . 
?tableFrom js:name ?tabname .  
?dash etl:hasSourceFile ?etlSource .  
?tableFrom mig:hasSqlName ?hasSqlName 
}
group by ?dash ?tableFrom
order by ?dash desc(?count) 
"""