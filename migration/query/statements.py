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

select_recursive_vizualiz_mart="""
select distinct ?node  (coalesce(?vlabelname,?sqljsname,?jsname,?lname,?miglabel) as ?name) ?parentVisualId ?type 
{
  bind(?param? as ?iri)
  ?iri rdf:type mig:dashmart.
  ?node mig:parentVisualId1*  ?iri .
  optional{?node rdfs:label ?lname } .
  optional{?node mig:hasVisualLabel ?vlabelname } .
  optional{?node js:name ?jsname } .
  optional{?node mig:hasSqlName ?sqljsname } .
  optional{?node  mig:label ?miglabel}
  ?node mig:parentVisualId1 ?parentVisualId .
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

select_recursive_visualiz_eyed_mart="""
select distinct ?node  (coalesce(?vlabelname,?sqljsname,?jsname,?lname,?miglabel,?JsonObjectKey) as ?nodeName) ?parentVisualId
{
  ?iri rdf:type mig:dashmart .  
  ?node mig:parentVisualId1*  ?iri .
  optional{?node rdfs:label ?lname } .
  optional{?node mig:hasVisualLabel ?vlabelname } .
  optional{?node js:name ?jsname } .
  optional{?node mig:hasSqlName ?sqljsname } .
  optional{?node  mig:label ?miglabel}

  optional{?node js:hasJsonObjectKey ?JsonObjectKey .  }    
  ?node mig:parentVisualId1 ?parentVisualId .
  ?node rdf:type ?type .  
  filter (?type not in (owl:NamedIndividual,js:ObjectJson) ) 
}  
"""
select_recursive_visualiz_eyed="""
select distinct ?node  (coalesce(?label,?name,?JsonObjectKey) as ?nodeName) ?parentVisualId
{
  #?iri rdf:type mig:msdash .
  bind(?param? as ?iri)
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
stmt_to_get_marts="""
 select ?iri ?hasSourceFile {  
?iri rdf:type mig:dashmart .
?iri mig:label  ?hasSourceFile .
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
select ?rel2 ?tabfromName ?tabtoName ?rel2tabto
{
  bind(uri(?param?) as ?iri)
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
select (max(?etlSource) as ?src) ?dash ?tableFrom (count(*) as ?count) (max(?hasSqlName) as ?tbname) (max(?tabname) as ?tabjsname) (min(?relat) as ?relat_tab)
{
  bind(uri(?param?) as ?dash)
?tableFrom js:name ?tabname .      
?tableFrom  rdf:type mig:msDashTable .
?tableFrom mig:hasRelationshipFrom ?relat.
 ?tableFrom mig:hasMsDash ?dash . 

?dash etl:hasSourceFile ?etlSource .  
?tableFrom mig:hasSqlName ?hasSqlName 
}
group by ?dash ?tableFrom
order by ?dash desc(?count) 
"""
stmt_relation_columns="""select (?column as ?iri)  ?colname ?type ?dataType ?sourceColumn ?expression ?sqlname{
  bind (uri(?param?) as ?table) .
  ?dash rdf:type mig:msdash .
  ?dash js:name ?dashName .
  ?table js:name ?tableName .  
  ?table mig:hasMsDash ?dash . 
  ?table rdf:type mig:msDashTable .
  ?column mig:hasMsDashTable ?table .
  ?column rdf:type mig:DashColumn .
  ?column js:name ?colname .
  optional {?column js:type ?type} .
  optional {?column js:dataType ?dataType} .
  optional {?column js:sourceColumn ?sourceColumn } .
  optional {?column js:expression ?expression } .  
  optional {?column mig:hasSqlName ?sqlname } .  
}

"""

stmt_form_from="""
select 
?tab_rel ?qrel
(concat('t',str(?fromOrder))  as ?prefixFrom) 
?columTo
?columFromSQLname
?columToSQLname
?tabtojsname ?tabtosqlname ?rel_order (concat('t',str(?rel_order))  as ?prefix)

{
  bind(uri(?param?) as ?exp_query)
  ?exp_query rdf:type mig:dashexportquery .
  ?qrel mig:hasExportQuery ?exp_query .
  ?qrel rdf:type mig:queryrelation .
  ?qrel mig:hasOrder ?rel_order .
  ?qrel mig:parentRelation ?tab_rel .
  ?tableTo  mig:hasRelationshipTo  ?tab_rel.
  ?tableTo mig:hasSqlName ?tabtosqlname .
   
  ?tableTo js:name ?tabtojsname .
  ?qrel mig:hasFromOrder ?fromOrder.
  ?columTo mig:hasRelationshipColumnTo  ?tab_rel .
  ?columFrom mig:hasRelationshipColumnFrom  ?tab_rel .
  optional {?columTo mig:hasSqlName ?columToSQLname}.
  optional {?columFrom mig:hasSqlName ?columFromSQLname} .  
  
#  ?tableFrom  mig:hasRelationshipFrom  ?tab_rel.
#  ?tableFrom js:name ?tabfromjsname .
#  ?tableFrom mig:hasSqlName ?tabfromsqlname .
}
order by xsd:integer(?rel_order)
"""
stmt_all_export_query="""select ?exp_query ?SlqName ?jsname {?exp_query rdf:type mig:dashexportquery .
 ?exp_query mig:hasMsDashTable ?mainTable .
 ?mainTable mig:hasSqlName ?SlqName . 
 ?mainTable js:name ?jsname .  
} """

stmt_mart_export_cols="""
select 
?tab_rel ?qrel ?qcol ?tabtojsname ?tabtosqlname ?rel_order (concat('t',str(?rel_order))  as ?prefix)
(IF(coalesce(?coltype,'')="calculated",concat('calc_t',str(?rel_order),'_',str(?colOrder)),
    concat('t',str(?rel_order),'_',str(?colOrder),'_',?sqlName)
  ) as ?colName) ?coljsname ?colOrder ?coltype ?dataType ?exp_query
WHERE {
  { 
    bind(?param? as ?exp_query)
    ?exp_query rdf:type mig:dashexportquery .
    ?qrel mig:hasExportQuery ?exp_query .
    ?qrel rdf:type mig:queryrelation .
    ?qrel mig:hasOrder ?rel_order . 
    filter( xsd:integer(?rel_order) = 0) .
        ?qcol mig:hasQueryRelation ?qrel .  
        ?qcol rdf:type mig:queryrelationcolumn .  
        ?qcol mig:hasColumn ?col .
        ?qcol mig:hasOrder ?colOrder .
        ?col mig:hasSqlName ?sqlName .
        ?col js:name ?coljsname .  
        optional {?col js:type ?coltype}
        ?col js:dataType ?dataType .

  } 
  
  UNION 
      {
        bind(?param? as ?exp_query)
        ?exp_query rdf:type mig:dashexportquery .
        ?qrel mig:hasExportQuery ?exp_query .
        ?qrel rdf:type mig:queryrelation .
        ?qrel mig:hasOrder ?rel_order .
        ?qrel mig:parentRelation ?tab_rel .
        ?tableTo  mig:hasRelationshipTo  ?tab_rel.
        ?tableTo mig:hasSqlName ?tabtosqlname .
        ?tableTo js:name ?tabtojsname .
        ?qcol mig:hasQueryRelation ?qrel .  
        ?qcol rdf:type mig:queryrelationcolumn .  
        ?qcol mig:hasColumn ?col .
        ?qcol mig:hasOrder ?colOrder .
        ?col mig:hasSqlName ?sqlName .
        ?col js:name ?coljsname .  
        optional {?col js:type ?coltype}
    	?col js:dataType ?dataType .
      }
  }    
order by xsd:integer(?rel_order) xsd:integer(?colOrder)      
"""
stmt_insert_mart_col="""
insert {
  ?uid rdf:type mig:martcolumn .
  ?uid mig:hasSqlName ?colName .
  ?uid mig:dataType ?dataType .
  ?uid mig:hasOrder ?colOrder .
  ?uid mig:hasQueryRelation ?qrel .   
  ?uid mig:hasQueryColumn ?qcol .
  ?uid mig:hasExportQuery ?exp_query .
  ?uid mig:hasMart ?mart .
  ?uid mig:hasMsDashTable ?tab_rel .
  ?uid mig:hasVisualLabel ?hasVisualLabel .
  ?uid mig:parentVisualId ?exp_query .
  ?exp_query mig:parentVisualId ?mart .
  ?col  mig:parentVisualId ?uid .
  
} where {
  select 
  
  ?tab_rel ?qrel ?qcol ?tabtojsname ?tabtosqlname ?rel_order (concat('t',str(?rel_order))  as ?prefix)
  (IF(coalesce(?coltype,'')="calculated",concat('calc_t',str(?rel_order),'_',str(?colOrder)),
      concat('t',str(?rel_order),'_',str(?colOrder),'_',?sqlName)
    ) as ?colName) ?coljsname ?colOrder ?coltype ?dataType
  (iri(concat('http://www.example.com/MIGRATION#',struuid())) as ?uid) 
      ?exp_query ?mart 
  (concat(?colName,':',?coljsname,':',?dataType) as ?hasVisualLabel)
  WHERE {
    { 
      ?exp_query rdf:type mig:dashexportquery .
      ?exp_query mig:hasMart ?mart .
      ?exp_query mig:hasMsDashTable ?tab_rel.
      ?mart rdf:type mig:dashmart .
      #bind(mig:Nf76bc87394d14b3abe1a6475af28a214 as ?exp_query)
      ?exp_query rdf:type mig:dashexportquery .
      ?qrel mig:hasExportQuery ?exp_query .
      ?qrel rdf:type mig:queryrelation .
      ?qrel mig:hasOrder ?rel_order . 
      filter( xsd:integer(?rel_order) = 0) .
          ?qcol mig:hasQueryRelation ?qrel .  
          ?qcol rdf:type mig:queryrelationcolumn .  
          ?qcol mig:hasColumn ?col .
          ?qcol mig:hasOrder ?colOrder .
          ?col mig:hasSqlName ?sqlName .
          ?col js:name ?coljsname .  
          optional {?col js:type ?coltype}
          ?col js:dataType ?dataType .
          
    } 
    #order by xsd:integer(?rel_order) xsd:integer(?colOrder)
    UNION 
  #    {
  #      select 
  #      ?tab_rel ?qrel ?qcol ?tabtojsname ?tabtosqlname ?rel_order (concat('t',str(?rel_order))  as ?prefix)
  #      ?sqlName ?coljsname ?colOrder
        {
          ?exp_query rdf:type mig:dashexportquery .
          ?exp_query mig:hasMart ?mart .
          ?mart rdf:type mig:dashmart .
      
#          bind(mig:Nf76bc87394d14b3abe1a6475af28a214 as ?exp_query)
          ?exp_query rdf:type mig:dashexportquery .
          ?qrel mig:hasExportQuery ?exp_query .
          ?qrel rdf:type mig:queryrelation .
          ?qrel mig:hasOrder ?rel_order .
          ?qrel mig:parentRelation ?tab_rel .
          ?tableTo  mig:hasRelationshipTo  ?tab_rel.
          ?tableTo mig:hasSqlName ?tabtosqlname .
          ?tableTo js:name ?tabtojsname .
          ?qcol mig:hasQueryRelation ?qrel .  
          ?qcol rdf:type mig:queryrelationcolumn .  
          ?qcol mig:hasColumn ?col .
          ?qcol mig:hasOrder ?colOrder .
          ?col mig:hasSqlName ?sqlName .
          ?col js:name ?coljsname .  
          optional {?col js:type ?coltype}
          ?col js:dataType ?dataType .
        }
    }    
  order by ?mart ?exp_query xsd:integer(?rel_order) xsd:integer(?colOrder)      
}
"""

stmt_all_tables="""
select ?table ?jsname ?sqlName ?hasExportSqlName (coalesce(?sql,'' ) as ?sqlstmt )
{
  ?dash rdf:type mig:msdash .
  ?table mig:hasMsDash ?dash .
  ?table rdf:type mig:msDashTable .
  ?table js:name ?jsname .
  ?table  mig:hasSqlName ?sqlName .
  ?table mig:hasExportSqlName ?hasExportSqlName .
  optional{?table mig:hasSql ?sql }.
}
order by ?dash

"""

stmt_for_create_view="""
select (concat( ?val , ' ' , ?hasExportSqlName ) as ?line ) 
#(?column as ?iri)  ?colname ?type ?dataType ?sourceColumn ?expression ?sqlname ?hasExportSqlName
?key ?colname 
{
  bind (uri(?param?) as ?table) .  
  ?dash rdf:type mig:msdash .
  ?dash js:name ?dashName .
  ?table js:name ?tableName .  
  ?table mig:hasMsDash ?dash . 
  ?table rdf:type mig:msDashTable .
  ?column mig:hasMsDashTable ?table .
  ?column rdf:type mig:DashColumn .
  ?column js:name ?colname .
  optional {?column js:type ?type} .
  optional {?column js:dataType ?dataType} .
  optional {?column js:sourceColumn ?sourceColumn } .
  optional {?column js:expression ?expression } .  
  optional {?column mig:hasSqlName ?sqlname }   
  optional{ ?column mig:hasExportSqlName ?hasExportSqlName } .
  optional{ ?column mig:hasExportCalcSql ?hasExportCalcSqlName } .
  optional {?column js:hasJsonObjectKey ?key .}
  bind (IF(coalesce(?type,'')="calculated" || coalesce(?type,'')="calculatedTableColumn", 
      coalesce(?hasExportCalcSqlName,'NULL') , ?sqlname ) as ?val)
}
order by xsd:integer(?key)
"""