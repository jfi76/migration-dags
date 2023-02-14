proc migration mssql to pgsql

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



