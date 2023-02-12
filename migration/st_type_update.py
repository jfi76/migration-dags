import sqlparse
import st_common

class st_type_update(st_common.base_st_type):

    def exec(self,statement_text:str):
        self.isChanged=False    
        self.statement_text=statement_text
        self.try_update()        
        
        return super().exec()
    
    def try_update(self):
        new_str=''
        positions_select= st_common.get_tokens_range(token.tokens, 'select', 'from')  
        if positions_select['start_index']==None: return   
        sel_indx=0
