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
