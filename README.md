procedures migration mssql to pgsql

tools for runnging example

1. apache jena fuseki run example
get docker image according:
https://hub.docker.com/r/stain/jena-fuseki/
example of command I used for windows:
docker run -d --name fuseki -p 3030:3030 --env=ADMIN_PASSWORD=pw123 --volume=c:/fuseki:/fuseki stain/jena-fuseki

!!! create dataset "migration_rdf" in 
http://localhost:3030/manage.html

2. apache airflow run example:
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
use docker-comose.yml
in docker-compose.yml change line to add modules:
_PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:- apache-airflow-providers-microsoft-mssql SPARQLWrapper xmltodict rdflib requests sqlparse}
I have airflow folder. dags (this project) is subfolder.
airflow
    dags -- root of this repository
    logs 
    .plugins 
    docker-compose.yml

in airflow folder I run docker-compose up

3. https://github.com/jfi76/sparql-reporter
ui which used for browsing data related to migration

4. ms-sql and postgres db setup as your choice
in Airflow connections:
postgres
mssql
with any db. use host as "host.docker.internal".
in Airflow variables:
mssql_serv:host.docker.internal
mssql_pass: any your real pass of sa login
mssql_db: db name
currently get_proc_plans.py does not use hook and separate credentials which taken from vairiables, to chenge use line:
pymssql.connect(server=self.con_server, user='sa',
                        password=self.con_passw, database=self.con_db)

in ms_pg_transfer use own filter to control needed table 
sql = """ select  LOWER(table_name) as table_name from INFORMATION_SCHEMA.TABLES where substring(TABLE_NAME,1,2)='t_' """                        

in get_proc_plans.py control your procedures for transfer
self.getProcXMLExec('select ROUTINE_NAME , ROUTINE_TYPE from INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE=\'PROCEDURE\' and substring(ROUTINE_NAME,1,3)!=\'sp_\'  ')

!!!! in dags folder create 4 dirs which contains processde xml,json,rdf files:
mkdir json_data
mkdir xml_data
mkdir output
mkdir rdf_parsed


