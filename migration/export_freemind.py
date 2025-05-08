
import sys 
sys.path.append( './query/' )
sys.path.append( './migration/query/' )

import runSparqlWrapper as sparql_service
import statements as stmt
from lxml import etree

class export_freemind:
    def __init__(self,stmt_to_export,file_to_save):
        self.queryService=sparql_service.runSparqlWrapper()
        self.stmt_to_export = stmt_to_export
        self.file_to_save=file_to_save
    def replace_url(self,str:str):
         return str.replace('http://www.example.com/','')
         
    def  prepare_arr(self,iri_dash:str,fileName:str):        
        #print(self.stmt_to_export.replace('''?param?''',iri_dash))
        ret=self.queryService.query(self.stmt_to_export.replace('''?param?''',f"iri('{iri_dash}')"))
        root = etree.Element("map")

        for export_stmt_result in ret:   
            parent_node = root.find(f""".//node[@ID='{self.replace_url(export_stmt_result['parentVisualId']['value'])}']""")
            print(parent_node)
            #print(f"""{export_stmt_result['node']['value']} {export_stmt_result['name']['value']} {export_stmt_result['parentVisualId']['value']}""")
            if parent_node==None:
                parent_node = etree.SubElement(root, "node")    
                parent_node.set("ID", self.replace_url(export_stmt_result['parentVisualId']['value']))
                parent_node.set("TEXT", 'Dash')

            node = etree.SubElement(parent_node, "node")
            node.set("ID", self.replace_url(export_stmt_result['node']['value']))
            node.set("TEXT", export_stmt_result['name']['value'])
        tree = etree.ElementTree(root)    
        tree.write(f'''{self.file_to_save}_{fileName}.mm''', encoding="utf-8",pretty_print=True, xml_declaration=False) 
    def get_dashes(self):
        ret=self.queryService.query(str(stmt.stmt_to_get_dasahes))        
        for export_stmt_result in ret:
            c.prepare_arr(export_stmt_result['iri']['value'],export_stmt_result['hasSourceFile']['value'].replace('''.json''',''))

if __name__ == "__main__":
    c=export_freemind(stmt.select_recursive_visualiz_pbi,'c:\\zena\\')
    c.get_dashes()