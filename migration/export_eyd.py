import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service
import statements as stmt
from lxml import etree
import xml.dom.minidom as dom
import pyyed as ed 

class export_yed:
    def __init__(self,stmt_to_export,file_to_save):
        self.queryService=sparql_service.runSparqlWrapper()
        self.stmt_to_export = stmt_to_export
        self.file_to_save=file_to_save
    def replace_url(self,str:str):
         return str.replace('http://www.example.com/','').replace("#","")
         
    def  prepare_arr(self):        

        ret=self.queryService.query(self.stmt_to_export.replace('?param?','js:Nf29f7affd3f94e7d96c3bda02f1f98df'))

        g=ed.Graph()
        count=0
        used_nodes={}
        used_edges={}
        for export_stmt_result in ret:   
            #if used_nodes[self.replace_url(export_stmt_result['node']['value'])]==None:    
            if not self.replace_url(export_stmt_result['node']['value']) in used_nodes.keys():
                g.add_node(self.replace_url(export_stmt_result['node']['value']),shape="ellipse", label=f"""{export_stmt_result['nodeName']['value']}""") 
                used_nodes[self.replace_url(export_stmt_result['node']['value'])]=1
            #if count>0: 
                #g.add_node(self.replace_url(export_stmt_result['parentVisualId']['value']),label=f"""parent""")                   
            joined = f"""{self.replace_url(export_stmt_result['node']['value'])}_{self.replace_url(export_stmt_result['node']['value'])}"""    
            if count>0 and not joined in used_edges.keys():                    
                g.add_edge(self.replace_url(export_stmt_result['parentVisualId']['value']),self.replace_url(export_stmt_result['node']['value']))
                used_edges[joined]=1
            ###############
            count=count+1
        z = g.get_graph()

        new_config_file = open(self.file_to_save,'w', encoding='utf-8')
        new_config_file.write(z)
        new_config_file.close()

if __name__ == "__main__":
    c=export_yed(stmt.select_recursive_visualiz_eyed_mart,'c:\\zena\\xx.graphml')
    c.prepare_arr()

# <graphml xmlns="http://graphml.graphdrawing.org/xmlns"  
#     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
#     xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
#   <graph id="G" edgedefault="undirected">
#     <node id="n0"/>
#     <node id="n1"/>
#     <edge id="e1" source="n0" target="n1"/>
#   </graph>
# </graphml>    