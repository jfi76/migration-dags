import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service
import statements as stmt

class replace_cols_tabs:
    def __init__(self, dash):
        print('init')