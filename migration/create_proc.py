import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service
import statements as stmt
from rdf_ttl_service import rdfTTLService
from st_type_select import st_type_select 
from st_type_update import st_type_update

from st_common import replace_right_ms_vars_in_coparison
class create_proc:
    def __init__(self):
        self.procedures=[]
        self.statement_types=[]
        self.statement_types_dict:dict={}
        self.queryService=sparql_service.runSparqlWrapper()
        self.ttl_serice=rdfTTLService()
        self.statementId =0
        self.fileoutput='./dags/output/proc.ttl'
        self.taskIri=''
        self.body_statement=[]
        self.proc_variables=[]
        self.proc_parameters=[]
    def log_task_start(self):
        self.taskIri=self.ttl_serice.start_run_task()
        return self.taskIri
    
    def iterate_proc(self):        
        self.queryService.insert(stmt.delete_pgstatements)
        self.set_statement_types()        
        self.get_all_proc()
        
        for proc in self.procedures:            
            self.statementId =0
            try:
                self.process_proc(proc)
            except Exception as e:
                print('process_proc.' + str(e))    
                self.ttl_serice.log_proc_error(self.taskIri,proc['iri'],str(e))

        self.ttl_serice.graph.serialize(self.fileoutput, 'turtle')
        self.queryService.load_ttl(self.fileoutput)
        return self.procedures
    
    def process_proc(self,proc):
        print('process_proc :' + proc['name'] ) 
        self.proc_variables=[]
#create
        self.ttl_serice.add_stmt('create or replace procedure ' + proc['name'] + ' ', self.statementId , proc['iri'], self.statement_types_dict['CREATE PROC'])   

        param_str=self.prep_params(proc['iri']) 
        self.statementId =self.statementId+1    
#parameters
        self.ttl_serice.add_stmt('(' + param_str +')' , self.statementId , proc['iri'], self.statement_types_dict['CREATE PARAM'])   
        self.statementId =self.statementId+1     

        self.ttl_serice.add_stmt("""language plpgsql
as $$""", self.statementId , proc['iri'], self.statement_types_dict['CREATE LANG'])
        self.statementId =self.statementId+1     
#declare section
        declare_section=self.get_proc_variables(proc['iri'])
        self.ttl_serice.add_stmt(declare_section, self.statementId , proc['iri'], self.statement_types_dict['DECLARE'])        
        self.statementId =self.statementId+1             
#body
        self.ttl_serice.add_stmt("""begin""", self.statementId , proc['iri'], self.statement_types_dict['CREATE BEGIN'])        
        self.statementId =self.statementId+1
        self.process_body(proc['iri'])
        for stmt_item in self.body_statement:
            self.ttl_serice.add_stmt(stmt_item['stmt'], self.statementId , proc['iri'],  self.statement_types_dict[stmt_item['type']],stmt_item['ms_statement_iri'])
            self.statementId =self.statementId+1  
        self.ttl_serice.add_stmt("""end; $$""", self.statementId , proc['iri'], self.statement_types_dict['CREATE END'])        
        self.statementId =self.statementId+1                     
        
    def prep_params(self,procIri):
        stmt_str=stmt.procedure_params.replace('?param?',f"<{procIri}>")
        ret=self.queryService.query(stmt_str)
        param_str=''
        for param in ret:
            self.proc_variables.append({"name": param['name']['value'], "type": param['datatype']['value'], "type_len": param['charlen']['value'], "replace_name": param['name']['value'], "var_type":'parameter', "ms_name": param['ms_name']['value'] })

            if param_str!='' : param_str=param_str+', '
            if  param['mode']['value']!='' : param_str=param_str+' '+ param['mode']['value']            
            param_str=param_str+' '+ param['name']['value'] +  ' ' + param['datatype']['value'] + ' ' 
            if  param['charlen']['value']!='' : param_str=param_str+' '+ param['charlen']['value']+ ' '

        return param_str    
    def set_statement_types(self):
        ret=self.queryService.query(stmt.statement_types)                   
        for type in ret:
            self.statement_types.append(type['label']['value'])
            self.statement_types_dict[type['label']['value']]=type['iri']['value']
    def get_statements(self,procIri):
        stmt_str=stmt.procedure_statements.replace('?param?',f"<{procIri}>")
        ret=self.queryService.query(stmt_str)        
        return ret
    def get_proc_variables(self,procIri):

        ret_str=''
        stmt_str=stmt.get_proc_variable.replace('?param?',f"<{procIri}>")
        ret=self.queryService.query(stmt_str)        
        for declare_var in ret:
            varlen=''
            if declare_var['type_len']['value']!=None and declare_var['type_len']['value']!='' :
                varlen='(' + declare_var['type_len']['value'] + ')'
            ret_str=ret_str+declare_var['name']['value']+ ' ' +declare_var['type']['value'] + ' ' +varlen +'; \n'
            self.proc_variables.append({"name": declare_var['name']['value'], "type": declare_var['type']['value'], "type_len": declare_var['type_len']['value'], "replace_name": declare_var['name']['value'], "var_type":'variable', "ms_name": declare_var['ms_name']['value'] })
        if ret_str!='':
            ret_str='declare \n'+ret_str
        return ret_str
    def get_all_proc(self):
        ret=self.queryService.query(stmt.stmt_get_all_proc)
        for proc in ret:
            print('get_all proc :' + proc['name']['value'] + ' iri:' +proc['iri']['value'])    
            self.procedures.append({"iri":proc['iri']['value'], "name":proc['name']['value']})
        return self.procedures
    def add_body_statement(self,stmt,returned_result,msstmt_iri,procIri,type, ms_statement_iri):
        if returned_result['isChanged']==True:
            self.body_statement.append({"iri":msstmt_iri,"procIri":procIri,"stmt":stmt,"type":type, "ms_statement_iri":ms_statement_iri})

    def process_body(self,procIri):
            self.body_statement=[]
            ret=self.queryService.query(stmt.select_msproc_statement.replace('?param?',f"<{procIri}>"))
            select=st_type_select()
            update=st_type_update()
            for statement in ret:
                if (statement['StatementType']['value']=='SELECT'): 
                    ret=select.exec(statement['StatementText']['value'])
                    tmp_stmt=replace_right_ms_vars_in_coparison(self.proc_variables,ret['stmt'])+ ';'
                    #print(tmp_stmt)
                    self.add_body_statement(tmp_stmt,ret,statement['iri']['value'],procIri,statement['StatementType']['value'],statement['iri']['value'])
                if (statement['StatementType']['value']=='UPDATE'): 
                    ret=update.exec(statement['StatementText']['value'])
                    tmp_stmt=replace_right_ms_vars_in_coparison(self.proc_variables,ret['stmt'])+ ';'
                    #print(tmp_stmt)
                    self.add_body_statement(tmp_stmt,ret,statement['iri']['value'],procIri,statement['StatementType']['value'],statement['iri']['value'])


if __name__ == "__main__":
    print ('main')
    c=create_proc()
#    c.fileoutput='./output/proc.ttl'
#    c.iterate_proc()
    

    procIri='http://www.example.com/MIGRATION#ac3cd396-a02d-4e87-808e-79e872409a63'
    c.get_proc_variables(procIri)
    c.prep_params(procIri)
    c.process_body(procIri)

