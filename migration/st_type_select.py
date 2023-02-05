import sqlparse
import st_common

class st_type_select():
    def exec(self,statement_text:str):
        self.statement_text=statement_text
        ret=self.try_select_into()
        #print(self.statement_text)
        #if (ret['isChanged']==True):
            #print('changed: '+ ret['stmt'])        
        return ret    
        #self.try_set_variable_select()
    def try_select_into(self): # select @c1=c1, @c2=c2 from tab1 => select c1, c2 into @c1, @c2 from tab1 
        def process_identifier(token,variables):
            varstr=''
            if isinstance(token, sqlparse.sql.Comparison) and str(token[0])[0]=="@":
                variables.append(str(token[0]))
                for t in token:
                    if isinstance(t, sqlparse.sql.Identifier) and str(t)[0]!="@":
                        varstr=varstr+str(t)
            else:varstr=varstr+(str(token))                    
            return varstr
        variables=[]    
        elements = sqlparse.parse(self.statement_text)
        positions= self.get_tokens_range(elements[0].tokens, 'select', 'from')      
        i=0   

        new_str=''
        isChanged=False
        var_str=''
        for token in elements[0].tokens:
            if i>=positions['start_index']:
                if i==positions['end_index']: 
                    var_str=''                
                    for var in variables:                                        
                        if var_str=='': separator=''
                        else:separator=','
                        var_str=var_str + separator  + var.replace('@','')                     

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
        return {"stmt":new_str, "isChanged":isChanged}    
    def try_set_variable_select(self): #  set @t_test_id = (select current_test_id from t_testing  where t_testing_id=@t_testing_id ) =>   select current_test_id into @t_test_id from t_testing  where t_testing_id=@t_testing_id          
        print('try_set_variable_select')
#################################################
    def get_tokens_range(self,tokens:sqlparse.sql.TokenList, start_word:str, end_word:str):
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
    def identifier(self, token):
        self.names.append(token)
    