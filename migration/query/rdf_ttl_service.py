import json
import datetime
from rdflib import URIRef, BNode, Literal, Graph, Namespace, RDF, OWL, RDFS

class rdfTTLService:
    def __init__(self):            
        self.Namespace = Namespace("http://www.example.com/MIGRATION#")           
        self.NamespaceETL = Namespace("http://www.example.com/ETL#")   
        self.graph = Graph()                
    def emptyGraph(self):
        self.graph = Graph()     
    def hashCode(self):
        return BNode()    
    def add_stmt(self,statementText,statementId,procIri,pgStatementType,sourceStatementIri=''):        
        iri=self.Namespace[self.hashCode()]
        self.graph.add((iri , RDF.type, OWL.NamedIndividual))  
        self.graph.add((iri ,RDF.type, self.Namespace.pgstatement))
        self.graph.add((iri ,self.NamespaceETL.hasSourceFile, Literal('')))                            
        self.graph.add((iri , self.Namespace.haspgStatementType,URIRef(pgStatementType)))  
        self.graph.add((iri , self.Namespace.hasProcedure,URIRef(procIri)))
        self.graph.add((iri , self.Namespace.StatementId,Literal(statementId)))
        self.graph.add((iri , self.Namespace.StatementText,Literal(statementText)))
        if sourceStatementIri!="":
            self.graph.add((iri , self.Namespace.hasStatement,URIRef(sourceStatementIri)))  
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
    def start_run_task(self):
        iri=self.Namespace[self.hashCode()]     
        self.graph.add((iri , RDF.type, OWL.NamedIndividual))              
        self.graph.add((iri ,RDF.type, self.Namespace.runcreateprocess))                       
        self.graph.add((iri ,self.Namespace.START_AT, Literal(datetime.datetime.now())))                            
        self.graph.add((iri ,RDFS.label, Literal(datetime.datetime.now())))                            
        return iri
    def log_proc_create(self,taksIri,procIri,label):
        iri=self.Namespace[self.hashCode()]     
        self.graph.add((iri , RDF.type, OWL.NamedIndividual))              
        self.graph.add((iri ,RDF.type, self.Namespace.runcreateprocessprocedure))                               
        self.graph.add((iri , self.Namespace.hasProcedure,URIRef(procIri)))
        self.graph.add((iri , self.Namespace.hasTask,URIRef(taksIri)))
        self.graph.add((iri ,RDFS.label, Literal(label)))           
        return iri
    def log_proc_error(self,taksIri,procIri,error,label=''):
        print('do_logging:'+ error[0:10])
        iri=self.Namespace[self.hashCode()]     
        self.graph.add((iri , RDF.type, OWL.NamedIndividual))              
        self.graph.add((iri ,RDF.type, self.Namespace.runcreateprocessprocedure))                               
        self.graph.add((iri , self.Namespace.hasProcedure,URIRef(procIri)))
        self.graph.add((iri , self.Namespace.hasTask,URIRef(taksIri)))        
        self.graph.add((iri ,self.Namespace.ERROR_MSG, Literal(error)))
        self.graph.add((iri ,RDFS.label, Literal(label)))                   
        return iri
    def add_table_name(self,table_iri,shema:str,item:str):
        #iri=self.Namespace[self.hashCode()]
        iri=URIRef(table_iri)
        self.graph.add((iri , RDF.type, OWL.NamedIndividual))  
        #self.graph.add((iri ,RDF.type, self.Namespace.msprocedurevarible))
        self.graph.add((iri ,self.Namespace.hasSqlName, Literal(shema+'.'+item)))            
        self.graph.add((iri ,self.Namespace.hasSQLShema, Literal(shema)))                    
        self.graph.add((iri ,self.Namespace.hasSQLTableName, Literal(item)))                    
    def add_renamed_col(self,col_init,col_renamed,expr_iri):
        iri=self.Namespace[self.hashCode()]
        self.graph.add((iri , RDF.type, OWL.NamedIndividual))  
        self.graph.add((iri ,RDF.type, self.Namespace.dashrenamedcolumn))
        self.graph.add((iri , self.Namespace.hasExpression,URIRef(expr_iri)))
        self.graph.add((iri , self.Namespace.column_init,Literal(col_init)))                
        self.graph.add((iri , self.Namespace.column_renamed,Literal(col_renamed)))                        


    def add_mart(self,dash,label):
        iri=self.Namespace[self.hashCode()]
        self.graph.add((iri , RDF.type, OWL.NamedIndividual))  
        self.graph.add((iri ,RDF.type, self.Namespace.dashmart))
        self.graph.add((iri , self.Namespace.hasMsDash,URIRef(dash)))
        self.graph.add((iri , self.Namespace.label,Literal('витрина '+label)))   
        return iri                     

    def add_export_query(self,dash,table_iri,table_name,relation_count,mart_iri):
        iri=self.Namespace[self.hashCode()]
        self.graph.add((iri , RDF.type, OWL.NamedIndividual))  
        self.graph.add((iri ,RDF.type, self.Namespace.dashexportquery))
        self.graph.add((iri , self.Namespace.hasMsDash,URIRef(dash)))
        self.graph.add((iri , self.Namespace.hasMsDashTable,URIRef(table_iri)))
        self.graph.add((iri , self.Namespace.relationCount,Literal(relation_count,datatype='xsd:integer')))                
        #self.graph.add((iri , self.Namespace.hasSqlName,Literal('v_mart_'+table_name)))                        
        #self.graph.add((iri , self.Namespace.hasOrder,Literal(hasOrder,datatype='xsd:integer')))                        
        self.graph.add((iri , self.Namespace.hasMart,URIRef(mart_iri)))        
        self.graph.add((iri , self.Namespace.label,Literal('v_export_query_'+table_name)))
        return iri

    def add_queryrelation(self,export_query_iri,order,parent_relation_iri,label):
        iri=self.Namespace[self.hashCode()]
        self.graph.add((iri , RDF.type, OWL.NamedIndividual))  
        self.graph.add((iri ,RDF.type, self.Namespace.queryrelation))
        self.graph.add((iri , self.Namespace.hasExportQuery,URIRef(export_query_iri)))
        self.graph.add((iri , self.Namespace.parentRelation,URIRef(parent_relation_iri)))
        self.graph.add((iri , self.Namespace.hasOrder,Literal(order,datatype='xsd:integer')))    
        self.graph.add((iri , self.Namespace.label,Literal(label)))
                 
        return iri

    def add_queryrelation_column(self,column_iri,order,query_relation_iri,label):
        iri=self.Namespace[self.hashCode()]
        self.graph.add((iri , RDF.type, OWL.NamedIndividual))  
        self.graph.add((iri ,RDF.type, self.Namespace.queryrelationcolumn))
        self.graph.add((iri , self.Namespace.hasQueryRelation,URIRef(query_relation_iri)))
        self.graph.add((iri , self.Namespace.hasColumn,URIRef(column_iri)))
        # self.graph.add((iri , self.Namespace.parentRelation,URIRef(parent_relation_iri)))
        self.graph.add((iri , self.Namespace.hasOrder,Literal(order,datatype='xsd:integer')))    
        self.graph.add((iri , self.Namespace.label,Literal(label)))
        # queryrelation
                 
        return iri

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

