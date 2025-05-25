
import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service
import statements as stmt
from rdf_ttl_service import rdfTTLService

class create_export_query:
    def __init__(self, stmt_to_export, dir_to_save):
        self.queryService=sparql_service.runSparqlWrapper()
        self.stmt_to_export=stmt_to_export
        self.dir_to_save=dir_to_save
        self.created_dashes:dict={}
        self.ttl_serice=rdfTTLService()
        self.tables={}
    def clean_uri(self,name:str):
        return name.replace('http://www.example.com/JSON#','').replace('http://www.example.com/MIGRATION#','')

    def add_tables_to_array(self,table_iri:str, table_name:str,relation_iri,order):
        if table_name in self.tables.keys():
            
            return self.tables[table_name]
        else :
            self.tables[table_name]={"table_iri":self.clean_uri(table_iri),"relation_iri":self.clean_uri(relation_iri),"order":order}
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
            self.ttl_serice.add_queryrelation_column(export_stmt_result['iri']['value'],order,query_relation_iri, 't'+str(relat_order)+'.' +export_stmt_result['sqlname']['value'])

    def add_relation(self,query_export_iri:str,master_table_iri,relat_tab_iri,master_table_name):
        ret=self.queryService.query(stmt.stmt_table_relations.replace('?param?',f'"{master_table_iri}"'))                
        #self.get_table_from_array()  table_iri:table_iri,relation_iri:relation_iri,order:order
        # (max(?etlSource) as ?src) ?dash ?tableFrom (count(*) as ?count) (max(?hasSqlName) as ?tbname) (min(?relat) as ?relat_tab)
        relation_iri=self.ttl_serice.add_queryrelation(query_export_iri,'0',None, 
                                                       f"""{master_table_name}""",
                                                       None , None ,'0')         
        self.add_tables_to_array((master_table_iri),master_table_name,relation_iri,0)
        self.add_column_to_relation(master_table_iri,relation_iri, '0') 
        #print(self.tables)
        order=0
        for export_stmt_result in ret: 
            order=order+1
            #?rel2 ?rel2tabfrom ?tabfromName ?rel2tabto ?tabtoName 
            #table_from_iri,table_from_relation_iri,table_from_order
            # table_iri:table_iri,relation_iri:relation_iri,order:order

            fromTab=self.get_table_from_array(export_stmt_result['tabfromName']['value'])
            #print(export_stmt_result['tabfromName']['value'])
            #print(fromTab)
            relation_iri=self.ttl_serice.add_queryrelation(query_export_iri,order,
                                                           export_stmt_result['rel2']['value'], 
                                                           f"""{export_stmt_result['tabfromName']['value']}=>{export_stmt_result['tabtoName']['value']}""",
                                                            fromTab['table_iri'], fromTab['relation_iri'] , fromTab['order']
                                                             )    
            self.add_tables_to_array(export_stmt_result['rel2tabto']['value'],export_stmt_result['tabtoName']['value'],relation_iri,order)
            
            self.add_column_to_relation(export_stmt_result['rel2tabto']['value'],relation_iri, order)            
    def prepare_export_query(self,mart_iri:str,dash_iri:str):
        #print(stmt.stmt_tablefrom_sorted.replace('?param?',f'"{dash_iri}"'))
        ret=self.queryService.query(stmt.stmt_tablefrom_sorted.replace('?param?',f'"{dash_iri}"')) 
             
        for export_stmt_result in ret:   
            print(export_stmt_result)
            if self.check_table_in_export_query(dash_iri, export_stmt_result['tableFrom']['value']):
                export_iri=self.ttl_serice.add_export_query(dash_iri,export_stmt_result['tableFrom']['value'],
                                                              export_stmt_result['tbname']['value'], 
                                                              export_stmt_result['count']['value'],mart_iri)
                self.add_relation(export_iri,export_stmt_result['tableFrom']['value'],
                                  export_stmt_result['relat_tab']['value'],export_stmt_result['tabjsname']['value'])
                
            #break    
    def iterate_dashes(self):
        self.queryService.insert('delete {?dashmart ?p ?o} where { ?dashmart rdf:type mig:dashmart .?dashmart ?p ?o .} ')        
        self.queryService.insert('delete {?query ?p ?o} where { ?query rdf:type mig:dashexportquery . ?query ?p ?o .}')        
        self.queryService.insert('delete {?query ?p ?o} where { ?query rdf:type mig:queryrelation . ?query ?p ?o .}')
        self.queryService.insert('delete {?query ?p ?o} where { ?query rdf:type mig:queryrelationcolumn . ?query ?p ?o .}')
        
        ret=self.queryService.query(self.stmt_to_export)
        
        for export_stmt_result in ret: 
            if (export_stmt_result['dash']['value']  in self.created_dashes.keys())==False:                
                mart_iri=self.ttl_serice.add_mart(export_stmt_result['dash']['value'],export_stmt_result['fileName']['value'])
                self.created_dashes[export_stmt_result['dash']['value']]=mart_iri
            mart_iri=self.created_dashes[export_stmt_result['dash']['value']]    

            self.prepare_export_query(mart_iri,export_stmt_result['dash']['value'])
        filepath=self.dir_to_save+'create_export_query.ttl'  
        self.ttl_serice.graph.serialize(filepath, 'turtle') 
        self.queryService.load_ttl(filepath)

    def create_from_for_export_query(self):
        ret=self.queryService.query(stmt.stmt_all_export_query)                      
        for export_query_result in ret:   
            self.create_from(export_query_result['exp_query']['value'],f"""{export_query_result['SlqName']['value']} as t0 /*{export_query_result['jsname']['value']}*/\n""")

    def create_from(self, query_iri,from_):    
        ret=self.queryService.query(stmt.stmt_form_from.replace('?param?',f'"{query_iri}"'))                  
        self.ttl_serice.emptyGraph()
        for export_stmt_result in ret: 
            from_=from_ + " \njoin " + f"""{export_stmt_result['tabtosqlname']['value']} as {export_stmt_result['prefix']['value']}""" + f""" /*{export_stmt_result['tabtojsname']['value']}*/\n  on """ + f"""{export_stmt_result['prefixFrom']['value']}.{export_stmt_result['columFromSQLname']['value']}={export_stmt_result['prefix']['value']}.{export_stmt_result['columToSQLname']['value']}\n"""
        print(from_)  
        self.ttl_serice.add_from(query_iri,from_)     
        filepath=self.dir_to_save+'create_export_query_from.ttl'  
        self.ttl_serice.graph.serialize(filepath, 'turtle') 
        self.queryService.load_ttl(filepath)
        self.queryService.insert(stmt.stmt_insert_mart_col)
    # def create_mart_cols(self):
    #     ret=self.queryService.query(stmt.stmt_all_export_query)                      
    #     for export_query_result in ret:   
    #         self.create_from(export_query_result['exp_query']['value'],f"""{export_query_result['SlqName']['value']} as t0 /*{export_query_result['jsname']['value']}*/\n""")

        
if __name__ == "__main__":
    
    stmt_all_dashes="""select ?dash ?fileName  {
    bind(js:Nf29f7affd3f94e7d96c3bda02f1f98df as ?dash)
    ?dash rdf:type mig:msdash . 
    ?dash etl:hasSourceFile ?fileName .
    } """    
    cexp=create_export_query(stmt_all_dashes,'../playground_parsed_adds/')
    cexp.iterate_dashes()
    cexp.create_from_for_export_query()
