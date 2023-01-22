stmt_get_all_proc="""
select * 
{
  ?iri rdf:type mig:pgprocedure .
  ?iri rdfs:label ?name .
}
"""
def create_stmt(statementText,statementId,procIri,pgStatementType):
    stmt="""
    insert { 
        ?uid mig:hasStatementType ?stmtType 
        }
    select (iri(concat('http://www.example.com/MIGRATION#',struuid())) as ?uid) ?stmtType {
    ?stmtType rdf:type  mig:pgStatementType ;
    ?stmtType rdfs:label '''{statType}'''
     }
    """
    return stmt.format(statType=pgStatementType)
