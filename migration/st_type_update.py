import sqlparse
import st_common

class st_type_update(st_common.base_st_type):

    def exec(self,statement_text:str):
        self.isChanged=False    
        self.statement_text=statement_text
        self.try_update()        
        
        return super().exec()
    
    def try_update(self):
        self.isChanged=True
        self.statement_text=st_common.substr_from_word(self.statement_text,'update')
