import sqlparse
import st_common

class st_type_select(st_common.base_st_type):

    def exec(self,statement_text:str):
        self.isChanged=False    
        self.statement_text=statement_text
        self.try_select_into()        
        self.try_set_variable_select()

        return super().exec()


    def try_select_into(self): # select @c1=c1, @c2=c2 from tab1 => select c1, c2 into @c1, @c2 from tab1 
        def process_identifier(token,variables):
            varstr=''
            if isinstance(token, sqlparse.sql.Comparison) and str(token[0])[0]=="@":
                variables.append(str(token[0]))
                #print(variables)
                #print('+++++++',token,type(token))
                for t in token:
                    if isinstance(t, sqlparse.sql.Identifier) and str(t)[0]!="@":
                        varstr=varstr+str(t)
                        #print('*****',t,type(t))   
                        #print('get it')
                    if isinstance(t,sqlparse.sql.Function):
                        varstr=varstr+str(t)     
                    #else:
                     #   print('-----',t,type(t))     
                        #print()    
                        
            else:varstr=varstr+(str(token))                    
            return varstr
        variables=[]    
        elements = sqlparse.parse(self.statement_text)
        print(self.statement_text)        
        positions= st_common.get_tokens_range(elements[0].tokens, 'select', 'from')      
        if (positions['start_index']==None): return
        i=0   

        new_str=''

        var_str=''
        for token in elements[0].tokens:
            if i>=positions['start_index']:
                if i==positions['end_index']: 
                    var_str=''                
                    for var in variables:                                        
                        if var_str=='': separator=''
                        else:separator=','
                        var_str=var_str + separator  + var.replace('@','var_')                     

                    if var_str!='':    
                        new_str= new_str + '\n into '+var_str + '\n'
                        isChanged=True

                if i>positions['start_index'] and i<positions['end_index']:
                    if isinstance(token, sqlparse.sql.Comparison):
                        new_str=new_str+process_identifier(token,variables)                
                    if isinstance(token,sqlparse.sql.IdentifierList):
                        for identifier in token:
                            new_str=new_str+process_identifier(identifier,variables)                
                    if not isinstance(token,sqlparse.sql.Comparison) and not isinstance(token,sqlparse.sql.IdentifierList):
                        new_str=new_str+(str(token))         
                else: 
                    new_str=new_str+(str(token))
            i=i+1
        self.set_statement(new_str)
    def try_set_variable_select(self): #  set @t_test_id = (select current_test_id from t_testing  where t_testing_id=@t_testing_id ) =>   select current_test_id into @t_test_id from t_testing  where t_testing_id=@t_testing_id          

        def process_parenthesis(token,variable):
            new_str=''
            positions_select= st_common.get_tokens_range(token.tokens, 'select', 'from')  
            if positions_select['start_index']==None: return   
            sel_indx=0
            
            for subtoken in token.tokens:
                if sel_indx>=positions_select['start_index']:
                    if sel_indx==positions_select['end_index']: 
                        new_str= new_str + '\n into '+variable + '\n'
                    new_str=new_str+(str(subtoken))                    
                sel_indx=sel_indx+1
            print(st_common.rreplace(new_str,')','',1))
            self.set_statement(st_common.rreplace(new_str,')','',1))
        elements = sqlparse.parse(self.statement_text)
        positions= st_common.get_tokens_range(elements[0].tokens, 'set', 'from')      
        if (positions['start_index']==None): return
        i=0   
        var_str=''

        variable = ''
        for token in elements[0].tokens:
            if i>=positions['start_index']:
                if isinstance(token,sqlparse.sql.Comparison) and str(token[0])[0]=="@":
                    variable=str(token[0]).replace('@','var_')
                    if (positions['start_index']!=None):
                        for subtoken in token.tokens:
                            if isinstance(subtoken,sqlparse.sql.Parenthesis):
                                print('start')
                                process_parenthesis(subtoken,variable)

            i=i+1        



if __name__ == "__main__":
    st_s= st_type_select()
    st_s.statement_text="""
begin

--set @seconds_remain=200


select  
@start_time=min(isnull(ts.view_date,getdate()))
--ts.view_date,
,@STOP_TIME_SEC=min(isnull(tt.STOP_TIME_SEC,0))
from t_testing_step ts
inner join t_test_step s on ts.t_test_step_id=s.t_test_step_id
inner join t_testing_test tt on s.t_test_id=tt.t_test_id 
where s.t_test_id=@t_test_id
and ts.t_testing_id=@t_testing_id
and tt.t_testing_id=@t_testing_id    
    """   
#     st_s.statement_text="""
#   select @test_name=te.obj_name,@test_num=te.order_num,
#   @question_btn_mark=te.user_question_mark,
#   @back_btn_mark=te.back_btn_mark,
#   @CURRENT_STOP_TIME=CURRENT_STOP_TIME,
#   @stop_time_sec=stop_time_sec,
#   @count_stop_times=tt.count_stop_times,
#   @stop_times=te.stop_times,
#   @time_limit_min=te.timelimitmin,
#   @show_question_mark = te.show_question_mark
#   from t_test te inner join t_testing_test tt
#   on te.t_test_id=tt.t_test_id 
#   where te.t_test_id=@t_test_id and tt.t_testing_id=@t_testing_id    
#     """
    st_s.try_select_into()
    print('changed to:')
    print(st_s.statement_text)