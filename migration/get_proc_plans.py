import pymssql
import xmltodict
import json
import os
"""
useful info  http://www.pymssql.org/en/stable/pymssql_examples.html
https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-query-plan-transact-sql?view=azuresqldb-current&viewFallbackFrom=sql-server-ver16
https://www.w3.org/community/rax/wiki/XML_to_RDF_Transformation_processes_using_XSLT
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-text-query-plan-transact-sql?view=sql-server-ver16
airflow and the password airflow
host.docker.internal
C:\zena\airflow
docker-compose up
docker run --hostname=9683a1f947b2 --mac-address=02:42:ac:11:00:02 --env=POSTGRES_PASSWORD=mysecretpassword --env=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/postgresql/15/bin --env=GOSU_VERSION=1.14 --env=LANG=en_US.utf8 --env=PG_MAJOR=15 --env=PG_VERSION=15.1-1.pgdg110+1 --env=PGDATA=/var/lib/postgresql/data --volume=/var/lib/postgresql/data -p 5432:5432 --restart=no --runtime=runc -d postgres
fuseki admin pw123
docker run --hostname=74be21cce865 --mac-address=02:42:ac:11:00:02 --env=ADMIN_PASSWORD=pw123 --env=PATH=/usr/local/openjdk-11/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin --env=LANG=C.UTF-8 --env=JAVA_HOME=/usr/local/openjdk-11 --env=JAVA_VERSION=11.0.6 --env=JAVA_BASE_URL=https://github.com/AdoptOpenJDK/openjdk11-upstream-binaries/releases/download/jdk-11.0.6%2B10/OpenJDK11U-jre_ --env=JAVA_URL_VERSION=11.0.6_10 --env=FUSEKI_SHA512=62ac07f70c65a77fb90127635fa82f719fd5f4f10339c32702ebd664227d78f7414233d69d5b73f25b033f2fdea37b8221ea498755697eea3c1344819e4a527e --env=FUSEKI_VERSION=3.14.0 --env=ASF_MIRROR=http://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename= --env=ASF_ARCHIVE=http://archive.apache.org/dist/ --env=FUSEKI_BASE=/fuseki --env=FUSEKI_HOME=/jena-fuseki --volume=c:/fuseki:/fuseki --volume=/fuseki --workdir=/jena-fuseki -p 3030:3030 --restart=no --label='org.opencontainers.image.authors=Apache Jena Fuseki by https://jena.apache.org/; this image by https://orcid.org/0000-0001-9842-9718' --label='org.opencontainers.image.description=Fuseki is a SPARQL 1.1 server with a web interface, backed by the Apache Jena TDB RDF triple store.' --label='org.opencontainers.image.documentation=https://jena.apache.org/documentation/fuseki2/' --label='org.opencontainers.image.licenses=(Apache-2.0 AND (GPL-2.0 WITH Classpath-exception-2.0) AND GPL-3.0)' --label='org.opencontainers.image.source=https://github.com/stain/jena-docker/' --label='org.opencontainers.image.title=Apache Jena Fuseki' --label='org.opencontainers.image.url=https://github.com/stain/jena-docker/tree/master/jena-fuseki' --label='org.opencontainers.image.version=3.14.0' --runtime=runc -d stain/jena-fuseki

docker run --hostname=df0b9247528d --mac-address=02:42:ac:11:00:03 --env=ADMIN_PASSWORD=pw123 --env=PATH=/usr/local/openjdk-11/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin --env=LANG=C.UTF-8 --env=JAVA_HOME=/usr/local/openjdk-11 --env=JAVA_VERSION=11.0.6 --env=JAVA_BASE_URL=https://github.com/AdoptOpenJDK/openjdk11-upstream-binaries/releases/download/jdk-11.0.6%2B10/OpenJDK11U-jre_ --env=JAVA_URL_VERSION=11.0.6_10 --env=FUSEKI_SHA512=62ac07f70c65a77fb90127635fa82f719fd5f4f10339c32702ebd664227d78f7414233d69d5b73f25b033f2fdea37b8221ea498755697eea3c1344819e4a527e --env=FUSEKI_VERSION=3.14.0 --env=ASF_MIRROR=http://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename= --env=ASF_ARCHIVE=http://archive.apache.org/dist/ --env=FUSEKI_BASE=/fuseki --env=FUSEKI_HOME=/jena-fuseki --volume=c:/fuseki:/fuseki --volume=/fuseki --workdir=/jena-fuseki -p 3030:3030 --restart=no --label='org.opencontainers.image.authors=Apache Jena Fuseki by https://jena.apache.org/; this image by https://orcid.org/0000-0001-9842-9718' --label='org.opencontainers.image.description=Fuseki is a SPARQL 1.1 server with a web interface, backed by the Apache Jena TDB RDF triple store.' --label='org.opencontainers.image.documentation=https://jena.apache.org/documentation/fuseki2/' --label='org.opencontainers.image.licenses=(Apache-2.0 AND (GPL-2.0 WITH Classpath-exception-2.0) AND GPL-3.0)' --label='org.opencontainers.image.source=https://github.com/stain/jena-docker/' --label='org.opencontainers.image.title=Apache Jena Fuseki' --label='org.opencontainers.image.url=https://github.com/stain/jena-docker/tree/master/jena-fuseki' --label='org.opencontainers.image.version=3.14.0' --runtime=runc -d stain/jena-fuseki
docker run -d --name fuseki -p 3030:3030 --env=ADMIN_PASSWORD=pw123 --volume=c:/fuseki:/fuseki stain/jena-fuseki
https://hub.docker.com/r/stain/jena-fuseki/#!
/migr_mssql_pqsql/sparql  for selects
"""
# docker run -d --name fuseki -p 3030:3030 -v /ssd/data/fuseki:/fuseki stain/jena-fuseki

class mssql_to_postgres:
    def __init__(self, con_server, con_passw):    
        self.con_server=con_server
        self.con_passw=con_passw
        self.conn = pymssql.connect(server=self.con_server, user='sa',
                        password=self.con_passw, database='PSYCHOLOGY_TEST_1')
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
        self.execJsonSave('select * from master.sys.databases where name=\'PSYCHOLOGY_TEST_1\' for json path ',self.json_data+'databases.json')
        self.getProcXMLExec('select ROUTINE_NAME , ROUTINE_TYPE from INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE=\'PROCEDURE\' and substring(ROUTINE_NAME,1,3)!=\'sp_\'  ')
        self.con_close()

if __name__ == "__main__":
        ms_to_pq=mssql_to_postgres(con_server=os.environ['mssql_serv'], con_passw=os.environ['mssql_pass'])
        ms_to_pq.exec()
