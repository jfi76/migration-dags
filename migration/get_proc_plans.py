import pymssql
import xmltodict
import json
import os


class mssql_to_postgres:
    def __init__(self, con_server, con_passw, con_db):    
        self.con_server=con_server
        self.con_passw=con_passw
        self.con_db=con_db
        self.conn = pymssql.connect(server=self.con_server, user='sa',
                        password=self.con_passw, database=self.con_db)
        self.xml_data='./dags/xml_data/'
        self.json_data='./dags/json_data/'
    def con_close(self):
        self.conn.close()

    def execJsonSave(self,execStr,fileSave):

        execbatch="""
        SET SHOWPLAN_XML OFF             
            """
        cursor = self.conn.cursor()    
        cursor.execute(execbatch)

        cursor.execute(execStr)
        f = open(fileSave, "w")
        all=cursor.fetchall()
        for row in all:
            f.write(str(row[0]))            

        f.close()

    def getProcXMLExec(self,select_proc_name_stmt):
        execbatch="""
        SET SHOWPLAN_XML OFF             
            """
        cursor = self.conn.cursor()    
        cursor.execute(execbatch)
        cursor = self.conn.cursor(as_dict=True)
        cursor.execute(select_proc_name_stmt)
        all=cursor.fetchall()
        for proc in all:
            print('get:'+ proc['ROUTINE_NAME'])
            self.getProcParameter(proc['ROUTINE_NAME'])
    def getProcParameter(self,proc_name):
        execbatch="""
        SET SHOWPLAN_XML OFF             
            """
        cursor = self.conn.cursor()    
        cursor.execute(execbatch)        
        self.batch_lines=[]
        cursor = self.conn.cursor(as_dict=True)
        cursor.execute('SELECT  DATA_TYPE,PARAMETER_NAME, PARAMETER_MODE ,CHARACTER_MAXIMUM_LENGTH  from INFORMATION_SCHEMA.PARAMETERS  where specific_name= %s ORDER BY ORDINAL_POSITION',(proc_name,))
        all=cursor.fetchall()
        for param in all:    
            self.set_batch_line(param)
        self.exec_proc_show_plan(proc_name)                

    def set_batch_line(self, parameters:dict):
        print(parameters)
#char
#int
#nvarchar
#uniqueidentifier
#varbinary
#varchar
        print('create batch variable for: ' + parameters['PARAMETER_NAME'])
        line='\'a\''
        par:str=parameters['DATA_TYPE']
        if par=='uniqueidentifier':
            line='\'E7FAB167-C47A-4865-A8AE-429FA778414C\''
        if par=='datetime':
            line='\'2000-01-01\''
        if par=='int' or par=='bigint' or par=='decimal':
            line='1'

        #match par:
        #    case 'uniqueidentifier':
        #        line='\'E7FAB167-C47A-4865-A8AE-429FA778414C\''
        #    case 'datetime':
        #        line='\'2000-01-01\''
        #    case 'int' | 'bigint' | 'decimal':
        #        line='1'
        #    case _:
        #        line='\'a\''

        self.batch_lines.append(line)

    def exec_proc_show_plan(self,proc):
        print(self.batch_lines)
        execline = 'exec ' + proc + ' '
        params = ''
        for line in self.batch_lines:
            if params != '':
                params = params + ','
            params=params + ' ' + line
        execline = execline + params
        execbatch="""
        SET SHOWPLAN_XML ON             
            """
        cursor = self.conn.cursor()    
        cursor.execute(execbatch)
        cursor.execute(execline)

        fxml = open(self.xml_data+proc + '.xml', "w")
        fjson = open(self.json_data+proc + '.json', "w")
        plans=cursor.fetchall()        
        for row in plans:
            fjson.write(json.dumps(xmltodict.parse(row[0])))            
            fxml.write(str(row[0]))                        
        fxml.close()
        fjson.close()
        print('done : '+proc+''.ljust(50, '-'))
    def exec(self):
        self.execJsonSave('select * from INFORMATION_SCHEMA.TABLES for json path',self.json_data+'TABLES.json')
        self.execJsonSave('select * from INFORMATION_SCHEMA.PARAMETERS for json path',self.json_data+'PARAMETERS.json')
        self.execJsonSave('select * from INFORMATION_SCHEMA.ROUTINES for json path',self.json_data+'ROUTINES.json')
        self.execJsonSave('select * from INFORMATION_SCHEMA.ROUTINE_COLUMNS for json path',self.json_data+'ROUTINE_COLUMNS.json')
        self.execJsonSave('select * from master.INFORMATION_SCHEMA.COLUMNS for json path',self.json_data+'COLUMNS.json')
        self.execJsonSave('select * from INFORMATION_SCHEMA.CHECK_CONSTRAINTS for json path',self.json_data+'CHECK_CONSTRAINTS.json')
        self.execJsonSave('select * from INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE for json path',self.json_data+'CONSTRAINT_TABLE_USAGE.json')
        self.execJsonSave('select * from master.sys.databases where name=\''+self.con_db+'\' for json path ',self.json_data+'databases.json')
        self.getProcXMLExec('select ROUTINE_NAME , ROUTINE_TYPE from INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE=\'PROCEDURE\' and substring(ROUTINE_NAME,1,3)!=\'sp_\'  ')
        self.con_close()

if __name__ == "__main__":
        ms_to_pq=mssql_to_postgres(con_server=os.environ['mssql_serv'], con_passw=os.environ['mssql_pass'], con_db='PS_TEST_1')
        ms_to_pq.exec()
