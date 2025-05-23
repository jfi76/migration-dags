import requests

from SPARQLWrapper import SPARQLWrapper, JSON, XML
import prefix as prefix
#DBPATH='http://host.docker.internal:3030/migration_rdf'
DBPATH='http://localhost:3030/migration_rdf2'

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
    def load_ttl(self,file):
        data = open(file, encoding='utf-8').read()
        headers = {'Content-Type': 'text/turtle;charset=utf-8'}        
        resp = requests.post(self.dbpath+'/data?default', data=data.encode('utf-8'), headers=headers)
        print(self.dbpath+'/data' + ' from ' + file)
        print(resp.text)