
from SPARQLWrapper import SPARQLWrapper, JSON, XML
import prefix as prefix
DBPATH='http://host.docker.internal:3030/test2'

class runSparqlWrapper:
    def __init__(self):    
        self.dbpath=DBPATH
    def query(self,stmt):
        sparql = SPARQLWrapper(self.dbpath)
        sparql.setReturnFormat(JSON)
        sparql.setQuery(prefix.values + '\n' + stmt)
        try:
            ret = sparql.queryAndConvert()
        except Exception as e:
            print('sparql error')
            print (stmt)
            print(e)
        return ret["results"]["bindings"]        

    def insert(self,stmt):
        sparql = SPARQLWrapper(self.dbpath)
        sparql.method = 'POST'
        sparql.setQuery(prefix.values + '\n' + stmt)
        try:
            ret = sparql.query()
        except Exception as e:
            print('sparql insert error')
            print (stmt)
            print(e)
        return ret      
