
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

    def add_relation(self,query_export_iri:str,master_table_iri):
        ret=self.queryService.query(stmt.stmt_table_relations.replace('?param?',f'"{master_table_iri}"'))        
        order=0
        for export_stmt_result in ret: 
            #?tabfromName ?tabtoName   
            relation_iri=self.ttl_serice.add_queryrelation(query_export_iri,order,export_stmt_result['rel2']['value'], f"""{export_stmt_result['tabfromName']['value']}=>{export_stmt_result['tabtoName']['value']}""" )    
            order=order+1
            self.add_column_to_relation(export_stmt_result['rel2tabto']['value'],relation_iri, order)
    def prepare_export_query(self,mart_iri:str,dash_iri:str):
        ret=self.queryService.query(stmt.stmt_tablefrom_sorted.replace('?param?',f'"{dash_iri}"')) 
             
        for export_stmt_result in ret:   
            if self.check_table_in_export_query(dash_iri, export_stmt_result['tableFrom']['value']):
                export_iri=self.ttl_serice.add_export_query(dash_iri,export_stmt_result['tableFrom']['value'],
                                                              export_stmt_result['tbname']['value'], 
                                                              export_stmt_result['count']['value'],mart_iri)
                self.add_relation(export_iri,export_stmt_result['tableFrom']['value'])
                
            #stmt_table_relations    
            break    
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

            
if __name__ == "__main__":
    stmt_all_dashes="""select ?dash ?fileName  {
    bind(js:Nf29f7affd3f94e7d96c3bda02f1f98df as ?dash)
    ?dash rdf:type mig:msdash . 
    ?dash etl:hasSourceFile ?fileName .
    } """    
    cexp=create_export_query(stmt_all_dashes,'../playground_parsed_adds/')
    cexp.iterate_dashes()
