
import sys 
import sqlalchemy 
from sqlalchemy import create_engine, MetaData, text
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service
import statements as stmt
from rdf_ttl_service import rdfTTLService

class create_export_query:
    def __init__(self, stmt_to_export, dir_to_save):
        self.engine = sqlalchemy.create_engine(f'''postgresql://postgres:mysecretpassword@localhost:5432/mig15''')    
        self.export_schema='export_pbi'
        self.queryService=sparql_service.runSparqlWrapper()
        self.stmt_to_export=stmt_to_export
        self.dir_to_save=dir_to_save
        self.created_dashes:dict={}
        self.ttl_service=rdfTTLService()
        self.tables={}
    def clean_uri(self,name:str):
        return name.replace('http://www.example.com/JSON#','').replace('http://www.example.com/MIGRATION#','')

    def add_tables_to_array(self,table_iri:str, table_name:str,relation_iri,order,query_export_iri):
        if table_name in self.tables.keys():
            
            return self.tables[table_name]
        else :
            self.tables[table_name]={"table_iri":self.clean_uri(table_iri),"relation_iri":'mig:'+self.clean_uri(relation_iri),"order":order,"query_export_iri":query_export_iri}
    def get_table_from_array(self,table_name):
        if table_name in self.tables.keys():
            return self.tables[table_name]
        else: return None

    def check_table_in_export_query(self,dash_iri, table_iri):    
        return True
    def add_column_to_relation(self, tableTo_iri,query_relation_iri, relat_order):
        #(?column as ?iri)  ?colname ?type ?dataType ?sourceColumn ?expression ?sqlname
        q=stmt.stmt_relation_columns.replace('?param?',f'"{tableTo_iri}"')
        
        ret=self.queryService.query(q)       
        order=0
        for export_stmt_result in ret: 
            order=order+1
            self.ttl_service.add_queryrelation_column(export_stmt_result['iri']['value'],order,query_relation_iri, 't'+str(relat_order)+'.' +export_stmt_result['sqlname']['value'])

    def add_relation(self,query_export_iri:str,master_table_iri,relat_tab_iri,master_table_name):
        self.order=self.order+1
        ret=self.queryService.query(stmt.stmt_table_relations.replace('?param?',f'"{master_table_iri}"'))                
        #self.get_table_from_array()  table_iri:table_iri,relation_iri:relation_iri,order:order
        # (max(?etlSource) as ?src) ?dash ?tableFrom (count(*) as ?count) (max(?hasSqlName) as ?tbname) (min(?relat) as ?relat_tab)
        relation_iri=self.ttl_service.add_queryrelation(query_export_iri,str(self.order),None, 
                                                       f"""{master_table_name}""",
                                                       None , None ,str(self.order))         
        self.add_tables_to_array((master_table_iri),master_table_name,relation_iri,self.order,query_export_iri)
        self.add_column_to_relation(master_table_iri,relation_iri, str(self.order)) 
        #print(self.tables)
        #self.order=0
        parent_order=0
        for export_stmt_result in ret: 
            if self.get_table_from_array(export_stmt_result['tabtoName']['value'])==None : 
            #or self.get_table_from_array(export_stmt_result['tabtoName']['value'])==None    
            #or self.get_table_from_array(export_stmt_result['tabfromName']['value'])==None:
                self.order=self.order+1
                #?rel2 ?rel2tabfrom ?tabfromName ?rel2tabto ?tabtoName 
                #table_from_iri,table_from_relation_iri,table_from_order
                # table_iri:table_iri,relation_iri:relation_iri,order:order

                fromTab=self.get_table_from_array(export_stmt_result['tabfromName']['value'])
                #print(export_stmt_result['tabfromName']['value'])
                #print(fromTab)
                relation_iri=self.ttl_service.add_queryrelation(query_export_iri,self.order,
                                                            export_stmt_result['rel2']['value'], 
                                                            f"""{export_stmt_result['tabfromName']['value']}=>{export_stmt_result['tabtoName']['value']}""",
                                                                fromTab['table_iri'], fromTab['relation_iri'] , fromTab['order']
                                                                )    
                self.add_tables_to_array(export_stmt_result['rel2tabto']['value'],export_stmt_result['tabtoName']['value'],relation_iri,self.order,query_export_iri)
                
                self.add_column_to_relation(export_stmt_result['rel2tabto']['value'],relation_iri, self.order)            
            else:
                found=self.get_table_from_array(export_stmt_result['tabtoName']['value'])

                if parent_order==0 and found['query_export_iri']!=query_export_iri and 'LocalDateTable' not in export_stmt_result['tabtoName']['value']:
                    self.ttl_service.add_parent_query(found['relation_iri'] ,query_export_iri,found['query_export_iri'],export_stmt_result['rel2']['value'],parent_order)
                parent_order=parent_order+1
    def prepare_export_query(self,mart_iri:str,dash_iri:str):
        #print(stmt.stmt_tablefrom_sorted.replace('?param?',f'"{dash_iri}"'))
        ret=self.queryService.query(stmt.stmt_tablefrom_sorted.replace('?param?',f'"{dash_iri}"')) 
        self.order=0     
        for export_stmt_result in ret:   
            #print(export_stmt_result)
            if self.check_table_in_export_query(dash_iri, export_stmt_result['tableFrom']['value']) and self.get_table_from_array(export_stmt_result['tabjsname']['value'])==None:
                export_iri=self.ttl_service.add_export_query(dash_iri,export_stmt_result['tableFrom']['value'],
                                                              export_stmt_result['tbname']['value'], 
                                                              export_stmt_result['count']['value'],mart_iri)
                
                print(export_stmt_result['tabjsname']['value'])
                #print(self.tables)
                self.add_relation(export_iri,export_stmt_result['tableFrom']['value'],
                                export_stmt_result['relat_tab']['value'],export_stmt_result['tabjsname']['value'])
                
            #break    
    def iterate_dashes(self):
        self.queryService.insert('delete {?dashmart ?p ?o} where { ?dashmart rdf:type mig:dashmart .?dashmart ?p ?o .} ')        
        self.queryService.insert('delete {?query ?p ?o} where { ?query rdf:type mig:dashexportquery . ?query ?p ?o .}')        
        self.queryService.insert('delete {?query ?p ?o} where { ?query rdf:type mig:queryrelation . ?query ?p ?o .}')
        self.queryService.insert('delete {?query ?p ?o} where { ?query rdf:type mig:queryrelationcolumn . ?query ?p ?o .}')
        self.queryService.insert('delete {?query ?p ?o} where { ?query rdf:type mig:parentexportquery . ?query ?p ?o .}')
        
        #self.queryService.insert('delete {?iri mig:hasExportSqlName ?q} where {?iri mig:hasExportSqlName ?q}')
        
        ret=self.queryService.query(self.stmt_to_export)
        mart_order=0
        for export_stmt_result in ret: 
            if (export_stmt_result['dash']['value']  in self.created_dashes.keys())==False:
                mart_order=mart_order+1                
                mart_iri=self.ttl_service.add_mart(export_stmt_result['dash']['value'],export_stmt_result['fileName']['value'],mart_order)
                self.created_dashes[export_stmt_result['dash']['value']]=mart_iri
            mart_iri=self.created_dashes[export_stmt_result['dash']['value']]    

            self.prepare_export_query(mart_iri,export_stmt_result['dash']['value'])
        filepath=self.dir_to_save+'create_export_query.ttl'  
        self.ttl_service.graph.serialize(filepath, 'turtle') 
        self.queryService.load_ttl(filepath)

    def create_from_for_export_query(self):
        ret=self.queryService.query(stmt.stmt_all_export_query)                      
        for export_query_result in ret:   
            self.create_from(export_query_result['exp_query']['value'],f"""{export_query_result['SlqName']['value']} as t0 /*{export_query_result['jsname']['value']}*/\n""")

    def create_from(self, query_iri,from_):    
        ret=self.queryService.query(stmt.stmt_form_from.replace('?param?',f'"{query_iri}"'))                  
        self.ttl_service.emptyGraph()
        for export_stmt_result in ret: 
            from_=from_ + " \njoin " + f"""{export_stmt_result['tabtosqlname']['value']} as {export_stmt_result['prefix']['value']}""" + f""" /*{export_stmt_result['tabtojsname']['value']}*/\n  on """ + f"""{export_stmt_result['prefixFrom']['value']}.{export_stmt_result['columFromSQLname']['value']}={export_stmt_result['prefix']['value']}.{export_stmt_result['columToSQLname']['value']}\n"""
        #print(from_)  
        self.ttl_service.add_from(query_iri,from_)     
        filepath=self.dir_to_save+'create_export_query_from.ttl'  
        self.ttl_service.graph.serialize(filepath, 'turtle') 
        self.queryService.load_ttl(filepath)
        self.queryService.insert(stmt.stmt_insert_mart_col)
    # def create_mart_cols(self):
    #     ret=self.queryService.query(stmt.stmt_all_export_query)                      
    #     for export_query_result in ret:   
    #         self.create_from(export_query_result['exp_query']['value'],f"""{export_query_result['SlqName']['value']} as t0 /*{export_query_result['jsname']['value']}*/\n""")
    def add_hasSql_prop(self,table_iri, sqlName, alias):
        view_name=f"""{self.export_schema}.{alias}"""
        ret=self.queryService.query(stmt.stmt_for_create_view.replace('?param?',f'"{table_iri}"'))  
        stmtSql=f"""create or replace view {view_name}  as  
select """
        i=0
        ln=len(ret)        
        for export_stmt_result in ret:             
            i=i+1  
            stmtSql=stmtSql + ' ' + export_stmt_result['line']['value'] 
            if ln!=i:            stmtSql=stmtSql + f""",/* {export_stmt_result['colname']['value'] } */
            """
        #COMMENT ON COLUMN reports.logistics_hand_dash_fram_exception_doc.doc_header_id IS 'd1';        
        stmtSql=stmtSql + ' '  +"""
 from """ + sqlName  + ' ;  ' 
        self.ttl_service.add_table_hasSql(table_iri,stmtSql)
        #self.run_on_server_create_view(stmtSql, view_name, table_iri)    
    def run_views_onserver(self):
        self.ttl_service.emptyGraph()
        self.task_iri=self.log_task_start()
        ret=self.queryService.query(stmt.stmt_all_tables)                  
        for export_stmt_result in ret: 
            self.run_on_server_create_view(export_stmt_result['sqlstmt']['value'] , export_stmt_result['hasExportSqlName']['value'], export_stmt_result['table']['value'])            
        filepath=self.dir_to_save+'create_view_sql_run.ttl'  
        self.ttl_service.graph.serialize(filepath, 'turtle') 
        self.queryService.load_ttl(filepath)

    def run_on_server_create_view(self, sql_to_run, view_name, table_iri):
        with self.engine.connect() as connection:
            try: 
                connection.begin()
                connection.execute(text(f'''drop view if exists {self.export_schema}.{view_name}; '''))
                connection.execute(text(sql_to_run))
                connection.commit()
            except Exception as e:
                connection.rollback()
                self.ttl_service.log_proc_error(self.taskIri, table_iri , str(e))
                print(e)    


    def create_view_sql(self):
        self.queryService.insert("""delete {?iri mig:hasSql ?sql} where { ?iri rdf:type mig:msDashTable . ?iri mig:hasSql ?sql}""")
        ret=self.queryService.query(stmt.stmt_all_tables)                  
        self.ttl_service.emptyGraph()
        for export_stmt_result in ret: 
            self.add_hasSql_prop(export_stmt_result['table']['value'],  export_stmt_result['sqlName']['value'], export_stmt_result['hasExportSqlName']['value']  )
        filepath=self.dir_to_save+'create_view_sql.ttl'  
        self.ttl_service.graph.serialize(filepath, 'turtle') 
        self.queryService.load_ttl(filepath)

    def log_task_start(self):
        self.taskIri=self.ttl_service.start_run_task()
        return self.taskIri

if __name__ == "__main__":
    
    stmt_all_dashes="""select ?dash ?fileName  {
    bind(js:Nf29f7affd3f94e7d96c3bda02f1f98df as ?dash)
    ?dash rdf:type mig:msdash . 
    ?dash etl:hasSourceFile ?fileName .
    } """    
    cexp=create_export_query(stmt_all_dashes,'../playground_parsed_adds/')
    cexp.iterate_dashes()
    cexp.create_from_for_export_query()
