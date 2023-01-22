import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service
import statements as stmt

class create_proc:
    def __init__(self):
        self.procedures=[]
        self.queryService=sparql_service.runSparqlWrapper()
    def iterate_proc(self):
        ret=self.queryService.query(stmt.stmt_get_all_proc)
        return ret            
if __name__ == "__main__":
    print ('main')
    c=create_proc()
    c.iterate_proc()