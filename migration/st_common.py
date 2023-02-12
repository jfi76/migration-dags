import sqlparse
import re
def replace_right_ms_vars_in_coparison(variables, stmt):
    for var in variables:
        stmt=re.sub("=(\s)*" + var['ms_name'] + "(\s)*", ' ='+var['replace_name']+' ' , stmt)
    return stmt
def iterate_where(where_token):
    print('start where')
    print(str(where_token))
    for token in where_token.tokens:
        print(type(token))  
        print(str(token))

        if (isinstance(token,sqlparse.sql.Where)):
            iterate_where(token)
            print('start where recursive')
        if (token, sqlparse.sql.Comparison):
            fl=token.flatten()            
def process_variables_in_where(stmt:str):
    elements = sqlparse.parse(stmt)
    new_str=''
    isChanged=False
    for token in elements[0].tokens:
        if (isinstance(token,sqlparse.sql.Where)):
            iterate_where(token)
    return {"stmt":new_str, "isChanged":isChanged} 

def get_tokens_range(tokens:sqlparse.sql.TokenList, start_word:str, end_word:str):
    start_index=None
    end_index=None
    i=0
    for token in tokens:        
        if isinstance(token ,sqlparse.sql.Token) and str(token).lower()==start_word:
            start_index=i
        if isinstance(token ,sqlparse.sql.Token) and str(token).lower()==end_word:
            end_index=i
            return {"start_index":start_index, "end_index" : end_index}
        i=i+1
    return {"start_index":start_index, "end_index" : end_index}            
def rreplace(init_str,old,new,maxreplace=1):
    return new.join(init_str.rsplit(old, maxreplace))
class base_st_type():
    def __init__(self):
        self.isChanged=False    
        self.statement_text=''
    def set_statement(self, new_stmt:str):
        if new_stmt!='' and new_stmt!=self.statement_text:
            self.statement_text=new_stmt
            self.isChanged=True
    def return_result(self):
        return {"stmt":self.statement_text, "isChanged":self.isChanged}                         
    def exec(self):
        return self.return_result()
    
if __name__ == "__main__":
    stmt="""select obj_name
 into status
from t_testing_status ts
inner join t_testing t on ts.t_testing_status_id = t.t_testing_status_id
where  t.t_testing_id = @t_testing_id"""
    stmt2="""

begin
  
  select @t_test_id=t_test_id,@test_name=obj_name,@test_num=order_num
  
  from   t_test
  where order_num =
  (
  select min(order_num) 
  from t_testing_test tt inner join t_test te
  on tt.t_test_id= te.t_test_id
  where tt.t_testing_test_status_id = 4
  and tt.t_testing_id = @t_testing_id
  )    
    """
    process_variables_in_where(stmt2)