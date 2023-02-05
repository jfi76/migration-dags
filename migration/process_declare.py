import sqlparse
import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service
import statements as stmt
from rdf_ttl_service import rdfTTLService

class process_declare:
    def __init__(self):
        self.queryService=sparql_service.runSparqlWrapper()
        self.ttl_serice=rdfTTLService()
        self.variables=[]
        self.filepath = './dags/output/vars.ttl'

    def parse(self,sql,procIri,stmt_iri):
        stmts = sqlparse.parse(sql)[0]
        i=0  
        for token in stmts.tokens:
            if  isinstance(token, sqlparse.sql.IdentifierList) :
                if str(token[0])[0]=="@":
                    for subtoken in token:
                        if isinstance(subtoken, sqlparse.sql.Identifier) :
                            self.form_declare(stmts.tokens,subtoken,i,procIri,stmt_iri)
            if isinstance(token, sqlparse.sql.Identifier):
                if str(token[0])[0]=="@":
                    self.form_declare(stmts.tokens,token,i,procIri,stmt_iri)
            i=i+1

    def form_declare(self,stmts_tokens,subtoken,i,procIri,stmt_iri):
        name:str=''
        type:str=''
        for item in subtoken:  
            if (str(item)[0]=="@"):
                name=str(item)
                if str(item)!=str(subtoken[-1]):
                    type=str(subtoken[-1])
                else:
                    type=str(stmts_tokens[i+2])
        for var in self.variables:            
            if var['procIri']==procIri and var['name']==name :return
        if name !='' : 
            self.variables.append({"name":name,"type":type,"procIri":procIri,"stmt_iri":stmt_iri})

    def iterate_declare(self):
        self.queryService.insert(stmt.delete_variables)
        stmt_str=stmt.get_declare
        ret=self.queryService.query(stmt_str)
        for declare_stmt in ret:   
            self.parse(declare_stmt['StatementText']['value'],declare_stmt['procIri']['value'],declare_stmt['iri']['value'])
        for var_desc in self.variables:    
            #print(var_desc)
            self.ttl_serice.add_variable(var_desc['name'], self.get_type(var_desc['type']), self.get_len(var_desc['type']), var_desc['procIri'], var_desc['stmt_iri'],var_desc['type'])            
        self.ttl_serice.graph.serialize(self.filepath, 'turtle') 
        self.queryService.load_ttl(self.filepath)
        self.queryService.insert(stmt.insert_pgvariable)           
    def get_type(self,item):
        ind=item.find('(')
        if  ind!=-1:
            #print(str(ind) + item + ' is ' + item[0:ind])
            return item[0:ind] 
        return item
    def get_len(self,item):
        ind=item.find('(')
        if  ind!=-1:
            ind2=item.find(')')+1
            #print(str(ind2) + item + ' is ' + item[ind+1:ind2-1] )
            return item[ind+1:ind2-1] 
        return ''

if __name__ == "__main__":

    c=process_declare()
    c.iterate_declare()