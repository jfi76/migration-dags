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
 select ?iri ?hasSourceFile ?hasMainSqlName ?sql ?dash_iri ?dash_prefix{  
?iri rdf:type mig:dashmart .
?iri mig:label  ?hasSourceFile .
?iri mig:hasMsDash ?dash_iri .   
?dash_iri mig:hasPrefix ?dash_prefix .
optional{?iri mig:hasMainSqlName ?hasMainSqlName } .
optional{?iri mig:hasSqlDataset ?sql .}
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
?cfr_hasExportSqlName
?columToSQLname
?cto_hasExportSqlName
?tabtojsname ?tabtosqlname ?rel_order (concat('t',str(?rel_order))  as ?prefix)
?t_hasExportSqlName 
?t_key

{
  bind(uri(?param?) as ?exp_query)
  ?exp_query rdf:type mig:dashexportquery .
  ?qrel mig:hasExportQuery ?exp_query .
  ?qrel rdf:type mig:queryrelation .
  ?qrel mig:hasOrder ?rel_order .
  ?qrel mig:parentRelation ?tab_rel .
  ?tableTo  mig:hasRelationshipTo  ?tab_rel.
  ?tableTo mig:hasSqlName ?tabtosqlname .
  ?tableTo mig:hasExportSqlName ?t_hasExportSqlName . 
  ?tableTo js:name ?tabtojsname .
  ?qrel mig:hasFromOrder ?fromOrder.
  ?columTo mig:hasRelationshipColumnTo  ?tab_rel .
  ?columTo mig:hasExportSqlName ?cto_hasExportSqlName  .
  ?columFrom mig:hasRelationshipColumnFrom  ?tab_rel .
  ?columFrom mig:hasExportSqlName ?cfr_hasExportSqlName  .
  optional {?columTo mig:hasSqlName ?columToSQLname}.
  optional {?columFrom mig:hasSqlName ?columFromSQLname} .  
  ?tableTo js:hasJsonObjectKey ?t_key .
}
order by xsd:integer(?rel_order)
"""
stmt_all_export_query="""select ?exp_query ?SlqName ?jsname ?order ?hasExportSqlName ?key {?exp_query rdf:type mig:dashexportquery .
 ?exp_query mig:hasMsDashTable ?mainTable .
 ?exp_query mig:hasOrder ?order   .
 ?mainTable mig:hasSqlName ?SlqName . 
 ?mainTable js:name ?jsname .
 ?mainTable js:hasJsonObjectKey ?key . 
 ?mainTable mig:hasExportSqlName ?hasExportSqlName .
} 
 """

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
select ?table ?jsname ?sqlName ?hasExportSqlName (coalesce(?sql,'' ) as ?sqlstmt ) (coalesce(?hasExportCalcSql,?dist_col_val,'') as ?table_distinct_col)

{
  ?dash rdf:type mig:msdash .
  ?table mig:hasMsDash ?dash .
  ?table rdf:type mig:msDashTable .
  ?table js:name ?jsname .
  ?table  mig:hasSqlName ?sqlName .
  ?table mig:hasExportSqlName ?hasExportSqlName .
  optional{?table mig:hasSql ?sql }.
  optional {
  ?expr mig:hasMsDashTable ?table .
  ?expr rdf:type mig:DashExpression . 
  ?dist_col mig:hasExpression  ?expr . 
  ?dist_col rdf:type mig:tabledistinctcolumn  .
  ?dist_col mig:column ?dist_col_val .

      optional {
      ?col2 mig:hasMsDashTable ?table .
      ?col2 js:name ?dist_col_val .                  
      ?col2 rdf:type mig:DashColumn .
      ?col2 mig:hasExportCalcSql ?hasExportCalcSql . 
    }  


  }
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

stmt_expquery_cols="""
select ?expsqlname  ?colname ?col ?dataType
{ 
 bind(uri(?param?)  as ?expq)
  
  ?iri rdf:type mig:queryrelationcolumn  .
  ?iri mig:hasQueryRelation ?qrel .	
  ?qrel mig:hasExportQuery  ?expq .    
  ?iri mig:hasColumn  ?col .
  ?col js:name ?colname . 
  ?col rdf:type  mig:DashColumn .
  ?col mig:hasExportSqlName ?expsqlname .
  ?col js:hasJsonObjectKey ?c_key . 
  ?col mig:hasMsDashTable ?table .
  ?table  js:hasJsonObjectKey ?t_key . 
  ?table mig:hasSqlName ?sqlNameTable .
  optional{?col js:dataType ?dataType} .
} order by xsd:integer(?t_key) xsd:integer(?c_key) 

"""

stmt_exp_query_all="""select ?query ?exp_order (coalesce( ?main_query,'') as ?hasParent )
?select ?from (coalesce(?sql_init,'') as ?sql)
 {
 ?query mig:hasMart  ?mart  .
 ?query rdf:type  mig:dashexportquery .
 ?query mig:hasOrder ?exp_order .
 ?query  mig:hasSqlSelect ?select .
 ?query  mig:hasSqlFrom ?from .
  optional{?query mig:hasSql  ?sql_init .} .
 optional { ?p_query rdf:type mig:parentexportquery  .
        ?p_query mig:hasExportQuery ?query .
        ?p_query mig:hasExportParentQuery ?main_query .
  } 
}
order by ?mart xsd:integer(?exp_order)

"""
#
stmt_exp_query_with_child="""
select ?query ?exp_order (coalesce( ?main_query,'') as ?hasParent ) ?select  ?from 
 ?to_table ?from_table ?from_col_export_name ?to_col_export_name ?sql
{
 bind (uri(?param?) as ?main_query) .
 ?p_query rdf:type mig:parentexportquery  . 
 ?p_query mig:hasExportParentQuery ?main_query . 
 ?p_query mig:hasExportQuery ?query . 
 ?p_query mig:hasRelation ?Relation . 
 ?to_table mig:hasRelationshipTo ?Relation  .
 ?from_table mig:hasRelationshipFrom ?Relation .
  
 ?to_column mig:hasRelationshipColumnTo ?Relation  .
 ?from_column mig:hasRelationshipColumnFrom ?Relation .
 ?from_column mig:hasExportSqlName ?from_col_export_name .   
 ?to_column mig:hasExportSqlName ?to_col_export_name .    
 ?p_query mig:hasParentRelation ?parent_Relation .  
 ?Relation js:fromTable ?jstablefrom  .
 ?Relation js:fromColumn ?jscolfrom  . 
 ?query mig:hasMart  ?mart  .
 ?query rdf:type  mig:dashexportquery .
 ?query mig:hasOrder ?exp_order . 
 ?query  mig:hasSqlSelect ?select .
?query  mig:hasSqlFrom ?from .
?query mig:hasSql  ?sql .
}
order by ?mart xsd:integer(?exp_order)

"""

stmt_main_views="""
select ?prefix  ?mainsql ?order ?iri {
?iri rdf:type mig:dashexportquery .  
?iri mig:hasMainSql ?mainsql .
?iri mig:hasOrder ?order  . 
?iri mig:hasMsDash ?dash .  
?dash  mig:hasPrefix ?prefix .
}
"""

stmt_calc_columns_repalce_expr='''
select (?column as ?iri)   ?colsearch ?colrepl ?expression ?colname2 ?tableName
(coalesce(?summarizeBy2,'' ) as ?summarizeBy2_exp) 
(coalesce(?summarizeBy,'' ) as ?summarizeBy_col )
?hasExportSqlName2 
?column2 
{
  
  ?dash rdf:type mig:msdash .
  ?dash js:name ?dashName .
  ?table js:name ?tableName .  
  ?table mig:hasMsDash ?dash . 
  ?table rdf:type mig:msDashTable .
  ?column mig:hasMsDashTable ?table .
  ?column rdf:type mig:DashColumn .
  ?column js:name ?colname .
  ?column js:type ?type .
  optional {?column js:dataType ?dataType} .
  optional {?column js:sourceColumn ?sourceColumn } .
  ?column js:expression ?expression  .  
optional {?column mig:hasSqlName ?sqlname }  . 
optional{ ?column mig:hasExportSqlName ?hasExportSqlName }.
?column js:summarizeBy  ?summarizeBy .

  ?column2 mig:hasMsDashTable ?table2 .
  ?column2 rdf:type mig:DashColumn .
  ?column2 js:name ?colname2 .
  ?column2 mig:hasSqlName ?sqlname2 .
  ?column2 mig:hasExportSqlName ?hasExportSqlName2 .
  optional {?column2 js:summarizeBy  ?summarizeBy2} .
  #?column2 js:sourceColumn ?sourceColumn2 .
  #bind( concat(?tableName,  "\\\\'\\\\[" , ?colname2 , '\\\\]')  as ?colsearch )
  bind( concat("\\\\[" , ?colname2 , '\\\\]')  as ?colsearch )
  bind( concat( "[" , ?hasExportSqlName2 , ']')  as ?colrepl )
  filter (  regex(?expression,?colsearch,"i" ))
}

'''

stmt_pbi_section_containers="""
select ?vc ?name ?y ?vctype  
{
bind (uri(?param?) as ?dash)  
?iri mig:hasMsDash ?dash .  
?iri rdf:type mig:DashSection  .
?vcs js:parentJsonId ?iri  .
?vc js:parentJsonId ?vcs .  
?vc rdf:type mig:DashVisualContainer .
?vc rdfs:label ?name .  
?vc mig:hasDasVisualType ?vctype .  
?vc js:y ?y .  
?iri js:displayName  ?SecdisplayName .
  filter ( ?SecdisplayName='Главная страница')  

}    
order by ASC(xsd:float(?y)) 


"""
#
stmt_dataset_sql="""
select ?sqlNameTable ?col_name  ?dataType ?conv_dataType ?exportSqlName (concat(' Nullable (',?conv_dataType,')') as ?clickDt) 
?run_sql_click ?hasExportCalcSqlName ?expression ?type
(
  coalesce(IF((coalesce(?type,'')="calculated" || coalesce(?type,'')="calculatedTableColumn") && coalesce(?run_sql_click,'')='', 'null', ?run_sql_click  )  
  ,'')
  as ?calc_line)
?col   
{
bind(uri(?param?) as  ?mart  )
?colrel rdf:type mig:queryrelationcolumn .
?colrel mig:hasQueryRelation ?qrel .
?qrel mig:hasExportQuery ?expquery .  
?expquery mig:hasOrder ?expQueryOrder .  
?expquery mig:hasMart ?mart .  
?colrel mig:hasColumn ?col .  
?col js:name ?col_name .  
?col mig:hasExportSqlName ?exportSqlName .
?col js:hasJsonObjectKey ?c_key . 
?col mig:hasMsDashTable ?table .
?table  js:hasJsonObjectKey ?t_key . 
 ?table mig:hasSqlName ?sqlNameTable . 
optional{?col js:dataType ?dataType} .  
optional{ ?col mig:hasExportCalcSql ?hasExportCalcSqlName } .  
optional {?col js:expression ?expression } .    
optional {?col js:type ?type} .  
optional{
    ?rsql rdf:type mig:tansformed_click .    
    ?rsql mig:hasColumn ?col .
    ?rsql js:run_sql ?run_sql_click . 

  }
bind(IF(?dataType='string','String',
      IF(?dataType='int64','Int64',
        IF(?dataType='dateTime','DateTime32',
          IF(?dataType='double','Float64','')
         )
        )
      ) as ?conv_dataType)  
  
  #bind (IF(coalesce(?type,'')="calculated" || coalesce(?type,'')="calculatedTableColumn", 
  #    coalesce(?hasExportCalcSqlName,'NULL') , ?sqlname ) as ?val)  
  
}
order by xsd:integer(?expQueryOrder) xsd:integer(?t_key) xsd:integer(?c_key)

"""

stmt_get_layout_table="""
select ?qr  ?datasetName ?dataType ?LayoutType ?objkey (coalesce(?agg_func,'') as ?agg_func_num ) ?query_ref (coalesce(?DisplayNameStr,?datasetName) as ?DisplayName)
{
  bind(uri(?param?) as ?lt)  
?qr mig:hasLayoutTable ?lt .  
?qr  rdf:type  mig:queryref .
?qr mig:hasColumn ?col .
?col mig:hasExportSqlName ?datasetName .  
?col js:dataType ?dataType .  
?qr mig:hasLayoutType ?LayoutType . 
?qr js:hasJsonObjectKey ?objkey   .
?qr js:queryRef ?query_ref .  
  optional {?qr mig:hasAggFunc ?agg_func}  
?qr mig:hasDisplayName ?DisplayNameStr .  
} order by xsd:int(?objkey) 
""" 

stmt_get_layout_table_dash="""
select ?VisualType ?lt ?qr  ?datasetName ?dataType ?LayoutType ?objkey (coalesce(?agg_func,'') as ?agg_func_num ) ?query_ref  (coalesce(?DisplayNameStr,?datasetName) as ?DisplayName)
{
bind (uri(?param?) as ?dash)  
?iri mig:hasMsDash ?dash .  
?iri rdf:type mig:DashSection  .
?vcs js:parentJsonId ?iri  .
?lt js:parentJsonId ?vcs .  
?lt mig:hasDasVisualType ?VisualType .  
?qr mig:hasLayoutTable ?lt .  
?qr  rdf:type  mig:queryref .
?qr mig:hasColumn ?col .
?col mig:hasExportSqlName ?datasetName .  
?col js:dataType ?dataType .  
?qr mig:hasLayoutType ?LayoutType . 
?qr js:hasJsonObjectKey ?objkey   .
?qr js:queryRef ?query_ref .  
  optional {?qr mig:hasAggFunc ?agg_func}  
?qr mig:hasDisplayName ?DisplayNameStr .  
?iri js:displayName  ?SecdisplayName .
  filter ( ?SecdisplayName='Главная страница')  
      
} order by xsd:int(?objkey) 
"""

stmt_filter_details="""
select ?iri ?filterType ?queryref ?ExportSqlName (coalesce(?cultName,?jsname) as ?CulturalName) {
  bind(uri(?param?) as ?iri) .
  ?iri rdf:type mig:dashfilter .
  ?iri mig:hasFilterType ?filterType .
  ?qr mig:hasLayoutTable ?iri .
  ?qr rdf:type mig:queryref  .
  ?qr js:queryRef ?queryref  .  
  ?qr mig:hasColumn ?col .
  ?col mig:hasExportSqlName  ?ExportSqlName .
  ?col js:name ?jsname .
  optional {?col mig:hasCulturalName ?cultName } .
}
"""