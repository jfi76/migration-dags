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
         return str.replace('http://www.example.com/','')
         
    def  prepare_arr(self):        

        ret=self.queryService.query(self.stmt_to_export.replace('?param?','js:Nf29f7affd3f94e7d96c3bda02f1f98df'))

        root = etree.Element("graphml",nsmap={'y':"http://www.yworks.com/xml/graphml"})
        graph = etree.SubElement(root, "graph") 
        g=ed.Graph()
        count=0
        for export_stmt_result in ret:   
            # node = etree.SubElement(graph, "node")
            # node.set("id", self.replace_url(export_stmt_result['node']['value']))
            g.add_node(self.replace_url(export_stmt_result['node']['value']),shape="ellipse", label=f"""{export_stmt_result['nodeName']['value']}""") 
            if count>0: 
                #g.add_node(self.replace_url(export_stmt_result['parentVisualId']['value']),label=f"""parent""")                   
                g.add_edge(self.replace_url(export_stmt_result['parentVisualId']['value']),self.replace_url(export_stmt_result['node']['value']))
            # data5=etree.SubElement(node, "data")   
            # data5.set("key", "d5")
            # data5.text=etree.CDATA(export_stmt_result['nodeName']['value'])
            # data6=etree.SubElement(node, "data")   
            # data6.set("key", "d6")
            # shape=etree.SubElement(data6, "ShapeNode",nsmap={None: 'y'}) 
            
            # nodeLabel=etree.SubElement(shape, "NodeLabel",nsmap={None: 'y'}) 
            
            # LabelModel:etree=etree.SubElement(nodeLabel, "LabelModel",nsmap={None: 'y'}) 
            
            # LabelModel.text=etree.CDATA(export_stmt_result['nodeName']['value'])
            ###############
            count=count+1
        z = g.get_graph()

        new_config_file = open(self.file_to_save,'w', encoding='utf-8')
        new_config_file.write(z)
        new_config_file.close()
        # tree = etree.ElementTree(root)    
        # tree.write(self.file_to_save, encoding="utf-8",pretty_print=True, xml_declaration=True) 

if __name__ == "__main__":
    c=export_yed(stmt.select_recursive_visualiz_eyed,'c:\\zena\\xx.graphml')
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