import requests
from uuid import uuid4
from pprint import pprint as pp
#import pandas as pd
import json


HEADERS = {'Content-Type': 'application/json',
'X-CSRFToken': 'IjRiOTAxODFlYTkxN2QwOGQwZGI4MzE3ZjM4Nzc0YzU1YzIzNWJjNjgi.aHDvDg.npEfGcFTJ7SWev50ua2OUqIN7sg',
'Cookie': 'session=.eJwljktqAzEQBe-idRbqn6T2ZQap1Y1NTAwazyrk7lHI5i0K6lHf6Yjl5z3d3uvyj3Q8ZrqlnEdl0h7FgnppMKc46VRT6y3QCnEnxQAOaQOZXCJMKTNYKSW6II5BooNlD3jNysoZG7FWxAYuhOxoAhqSEbkP8lLcGwCkHXKdvv5rmGkDO1cc79enf_2hoRn2SVeoM7eZ52gENajVyiZiSDKstO09X9afvp11pZ9fQPdB-Q.aHDBFA.JNU8VXXddQnJ3rzToR3jfpyR82w',
'Baggage':'sentry-environment=production,sentry-release=2.7.4,sentry-public_key=71a674d2d482096b25aa53e968436e6b,sentry-trace_id=22730b63cde7482494d43cfb893f27da,sentry-replay_id=c86ebc21e6c84f21a748afa8b3ba74a8,sentry-sample_rate=1,sentry-sampled=true'
                   }
DATABASE_ID= 161,
SCHEMA= "Reports_duk"
URL='https://ddv.dosb.rt.ru/api/v1/'

class form_send_api:
    def __init__(self, dir_to_save):        
        self.dir_to_save=dir_to_save
        
    def post_wrapper(self, url:str, data:dict):
        return 
        response = requests.post(url, json=data, headers=HEADERS, verify=False)
        print (response)
        response.raise_for_status()   
        return  response
           
    def post_dataset(self,  name:str) -> str:
        file=self.dir_to_save+'dataset.json'
        f = open(file, encoding='utf-8')
        data = json.load(f)
        f.close()        
        sql=data['sql']['value']
        url = URL+'dataset/'
        data = {
        "database": DATABASE_ID,
        "schema": SCHEMA,
        "sql": sql,
        "table_name": "PBI MIGRATION DS " + name,
        "is_managed_externally": False,
        "external_url": None}
        self.post_wrapper(url,data)
    def post_dashboard(self,  dashboard_title):
        url = URL+'dashboard/'
        data={
  "css": "",
  "dashboard_title": "PBI MIGRATION D " + dashboard_title,
  "external_url": None,
  "is_managed_externally": True,
  "json_metadata": "",
  "position_json": "",}
        self.post_wrapper(url,data)
    def form_chart_wrapper(self,type):
        print('chart')
if __name__ == "__main__":
    c=form_send_api('../playground_ai/')
