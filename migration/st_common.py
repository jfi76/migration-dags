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
            print(str(fl))
def process_variables_in_where(stmt:str):
    elements = sqlparse.parse(stmt)
    new_str=''
    isChanged=False
    for token in elements[0].tokens:
        print(type(token))  
        print(str(token))
        if (isinstance(token,sqlparse.sql.Where)):
            iterate_where(token)
    return {"stmt":new_str, "isChanged":isChanged} 

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