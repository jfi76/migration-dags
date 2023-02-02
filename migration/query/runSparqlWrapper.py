import requests

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
    def load_ttl(self,file):
        #myfiles = {'file': open(file ,'rb')}
        #f=open(file)
        #symbol= f.readline(10)
        #print(symbol)
        #f.close()

        #files = {'file': open(file, 'rb')}
        #resp=requests.post(self.dbpath+'/data', files=files)
        #print(self.dbpath+'/data' + ' from ' + file)
        #print(resp.text)
        data = open(file).read()
        headers = {'Content-Type': 'text/turtle;charset=utf-8'}
        resp = requests.post(self.dbpath+'/data?default', data=data, headers=headers)
        print(self.dbpath+'/data' + ' from ' + file)
        print(resp.text)