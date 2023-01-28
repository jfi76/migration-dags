import json
from rdflib import URIRef, BNode, Literal, Graph, Namespace, RDF, OWL

class rdfTTLService:
    def __init__(self):            
        self.Namespace = Namespace("http://www.example.com/MIGRATION#")           
        self.NamespaceETL = Namespace("http://www.example.com/ETL#")   
        self.graph = Graph()                
    def hashCode(self):
        return BNode()    
    def add_stmt(self,statementText,statementId,procIri,pgStatementType):        
        iri=self.Namespace[self.hashCode()]
        self.graph.add((iri , RDF.type, OWL.NamedIndividual))  
        self.graph.add((iri ,RDF.type, self.Namespace.pgstatement))
        self.graph.add((iri ,self.NamespaceETL.hasSourceFile, Literal('')))                            
        self.graph.add((iri , self.Namespace.haspgStatementType,URIRef(pgStatementType)))  
        self.graph.add((iri , self.Namespace.hasProcedure,URIRef(procIri)))
        self.graph.add((iri , self.Namespace.StatementId,Literal(statementId)))
        self.graph.add((iri , self.Namespace.StatementText,Literal(statementText)))        
    def add_variable(self,variable_name,type,char_len,procIri,stmt_iri,statementText):
        iri=self.Namespace[self.hashCode()]
        self.graph.add((iri , RDF.type, OWL.NamedIndividual))  
        self.graph.add((iri ,RDF.type, self.Namespace.msprocedurevarible))
        self.graph.add((iri , self.Namespace.hasProcedure,URIRef(procIri)))
        self.graph.add((iri , self.Namespace.hasStatement,URIRef(stmt_iri)))                
        self.graph.add((iri ,self.Namespace.VARIABLE_NAME, Literal(variable_name)))                            
        self.graph.add((iri ,self.Namespace.DATA_TYPE, Literal(type)))                            
        self.graph.add((iri ,self.Namespace.CHARACTER_MAXIMUM_LENGTH, Literal(char_len)))                            
        self.graph.add((iri , self.Namespace.StatementText,Literal(statementText)))        
        #js:PARAMETER_NAME

        # stmt="""
        # insert { 
        #     ?uid mig:hasStatementType ?stmtType
            
        #     }
        # select (iri(concat('http://www.example.com/MIGRATION#',struuid())) as ?uid) ?stmtType {
        # ?stmtType rdf:type  mig:pgStatementType ;
        # ?stmtType rdfs:label '''{statType}'''
        # }
        # """
        # return stmt.format(statType=pgStatementType)

