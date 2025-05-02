import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service
import statements as stmt

class config_load:
    def __init__(self, stmt_to_export, dir_to_save):
        self.queryService=sparql_service.runSparqlWrapper()
        self.stmt_to_export=stmt_to_export
        self.file_to_save=dir_to_save
    def get_and_save(self):
        ret=self.queryService.query(self.stmt_to_export)
        for export_stmt_result in ret:   
            file=f'''{self.file_to_save}/{str(export_stmt_result['vc_item']['value']).replace('http://www.example.com/JSON#','')}-conf.json'''
            f = open(file,"w",encoding='utf-8')
            f.write(export_stmt_result['config']['value'])            
            f.close()        

if __name__ == "__main__":
    c=config_load(stmt.select_config_pbi,'../playground')
    c.get_and_save()