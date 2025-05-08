import os
import json
from rdflib import URIRef, BNode, Literal, Graph, Namespace, RDF, OWL
import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service

class json_to_ontology:
    def __init__(self, dirPath):
        self.dirPath=dirPath
        self.filename = ''
        self.toInsertArr=[]
        self.rdf_parsed='./dags/rdf_parsed/'
        self.queryService=sparql_service.runSparqlWrapper()
    def hashCode(self,inputStr):
        return BNode()    
    def processJsonDir(self):

        dir_list = os.listdir(self.dirPath)
        for file in dir_list:
            if file.endswith('.json'): 
                self.filename=file
                self.fileJson={
                    "label": file,
                    "hasSourceFile":file,
                    "iri": self.hashCode(file)
                }
                self.processJson(self.dirPath+file)    
        
    def processJson(self,file):
        self.toInsertArr=[]        
        print('process: ' + file)
        f = open(file,encoding='utf-8')
        symbol= f.readline(1)
        f.close()
        if len(symbol)<=0 :
            print('no process')            
            return 
        f = open(file, encoding='utf-8')
        data = json.load(f)
        f.close()        
        self.iterate_recursive2aarayNode(data,None, None, self.fileJson['iri'])
        self.Namespace = Namespace("http://www.example.com/JSON#")           
        self.NamespaceETL = Namespace("http://www.example.com/ETL#")   

        self.graph = Graph()        
        for item in self.toInsertArr:
            self.toTTL(item)
        to_file=self.rdf_parsed+self.filename.replace('.json','.ttl')
        print(to_file)
        self.graph.serialize(to_file, 'turtle')        
        self.queryService.load_ttl(to_file)        

    def toTTL(self,item):
        iri=self.Namespace[item['iri']]
        self.graph.add(( iri , RDF.type, OWL.NamedIndividual))  
        self.graph.add((iri ,RDF.type, self.Namespace.ObjectJson))
        self.graph.add((iri ,self.NamespaceETL.hasSourceFile, Literal(self.filename)))                            
        if item['isArray'] == True :
            self.graph.add((iri ,RDF.type, self.Namespace.ArrayJson))  
        if item['hasFileIri'] != None:
           self.graph.add((iri ,RDF.type, self.Namespace.FileJson))  
        if item['hasJsonObjectName'] != None:           
            self.graph.add((iri ,self.Namespace.hasJsonObjectKey, Literal(str(item['hasJsonObjectName']))))  
        if item['parentJsonId'] != None and item['parentJsonId']!='' :           
            self.graph.add((iri ,self.Namespace.parentJsonId, self.Namespace[item['parentJsonId']]))  
        if isinstance(item['parsedJson'],dict):
            for jsonPart in item['parsedJson']:
                if (jsonPart !=None and item['parsedJson']!=None and not isinstance(item['parsedJson'][jsonPart], dict) and (not isinstance(item['parsedJson'][jsonPart], list))):
                    self.graph.add((iri ,self.Namespace[jsonPart], Literal(str(item['parsedJson'][jsonPart]))))                  
        
        if isinstance(item['parsedJson'],list):                
            for jsonPart in item['parsedJson']:
                if (jsonPart !=None and item['parsedJson']!=None and not isinstance(jsonPart, dict) and (not isinstance(jsonPart, list))):                
                    try:
                        if isinstance(jsonPart,str):
                            self.graph.add((iri ,self.Namespace['jsstring'], Literal(str(jsonPart)))) 
                        else :    
                            self.graph.add((iri ,self.Namespace[jsonPart], Literal(str(item['parsedJson'][jsonPart])))) 
                    except Exception as exc:
                        print(exc) 
                        print(type(jsonPart))
                        print(jsonPart)
                        print('-----------------------------')
                        


    def iterate_recursive2aarayNode(self, parsedJson, parentJsonId, parentKey, hasFileIri):
        #print(hasFileIri + ' is obj' + str(isinstance(parsedJson, dict)) + ' is arr ' + str(isinstance(parsedJson, list)) + '  ' + json.dumps(parsedJson)[0:10]  )
        isArray=False
        isStr=False
        iri=self.hashCode(self.filename + str(parentKey) + str(parentJsonId)) 
        if isinstance(parsedJson, str):
            if hasFileIri != None: 
                iri=hasFileIri
        if isinstance(parsedJson, list):
            isArray=True
        self.toInsertArr.append({
            "parsedJson" : parsedJson,
            "parentJsonId" : parentJsonId,
            "hasFileIri" : hasFileIri,
            "isArray": isArray,
            "iri": iri,
            "hasJsonObjectName":parentKey
        })         
        
        if  (isinstance(parsedJson, list) and len(parsedJson)>0) :        
            ind=0

            for item in parsedJson:                    
                if isinstance(item, dict) or (isinstance(item, list) and len(item)>0) :
                    self.iterate_recursive2aarayNode(item,iri, ind , None) 
                ind=ind+1
                    
        if  isinstance(parsedJson, dict) :        
            #ind=0
            for item in parsedJson:                    
                if isinstance(parsedJson[item], dict) or (isinstance(parsedJson[item], list) and len(parsedJson[item])>0) :
                    self.iterate_recursive2aarayNode(parsedJson[item],iri, item , None) 

# running                
if __name__ == "__main__":
    #conv=json_to_ontology('./json_data/')
    #conv.queryService.dbpath='http://localhost:3030/test2/data'
    #conv
    conv=json_to_ontology('./moi_doc_json/')
    conv.rdf_parsed='./rdf_parsed/'
    conv.processJsonDir()