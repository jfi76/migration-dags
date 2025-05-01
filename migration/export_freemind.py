
import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service
import statements as stmt

class export_freemind:
    def __init__(self,stmt_to_export):
        self.queryService=sparql_service.runSparqlWrapper()
        self.stmt_to_export = stmt_to_export
    def  prepare_arr(self):
        print('start')
         #self.queryService.      