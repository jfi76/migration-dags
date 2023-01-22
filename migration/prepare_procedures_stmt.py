import os
from SPARQLWrapper import SPARQLWrapper, JSON

def runquery():
    sparql = SPARQLWrapper("http://host.docker.internal:3030/test2")
    sparql.setReturnFormat(JSON)

    # Query for the description of "Capsaicin", filtered by language
    sparql.setQuery("""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX mig: <http://www.example.com/MIGRATION#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX js: <http://www.example.com/JSON#>
    PREFIX etl:<http://www.example.com/ETL#>
    PREFIX  js_at: <http://www.example.com/JSON#@>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    select   ?iri ?label ?name ?datatype ?mode ?position (replace(?name,'@','') as ?plpgsqlParam) ?convPQ ?charlen
    { bind (mig:c4c649e1-834d-45c6-ac7f-03583b3494bd as ?param)
        ?param rdf:type mig:pgprocedure .
        ?param rdfs:label ?label . 
        ?param mig:hasProcedure ?msproc .
        ?msproc rdf:type mig:msprocedure .
        ?iri rdf:type mig:msProcedureParameter .
        ?iri mig:hasProcedure ?msproc .
        ?iri js:PARAMETER_NAME ?name.
        ?iri js:DATA_TYPE ?datatype .
        ?iri js:PARAMETER_MODE ?mode .
        ?iri js:ORDINAL_POSITION ?position .  
        optional {?iri js:CHARACTER_MAXIMUM_LENGTH ?len .
        bind (concat('(' , ?len ,')' ) as ?charlen ) .    
        } .
        bind (xsd:integer(?position) as ?position_int )  
        optional  {
            ?conv rdf:type mig:datatypeconversion .
            ?conv js:MS ?convMS .
            ?conv js:PQ ?convPQ .
        }    
    filter (UCASE(?datatype)=UCASE(?convMS))
    } order by ?position_int

    """)

    # Convert results to JSON format

    try:
        ret = sparql.queryAndConvert()

#        for r in ret["results"]["bindings"]:
#            print(r['name']['value'])
    except Exception as e:
        print(e)
    return ret

