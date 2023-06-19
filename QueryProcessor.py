import pandas as pd
from sqlite3 import connect
from sparql_dataframe import get

class Processor(object):
    def __init__(self):
        self.dbPathOrUrl = ""
        
    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl
    
    def setDbPathOrUrl(self, pathOrUrl) -> bool:
        try:
            self.dbPathOrUrl = pathOrUrl

            return True
        except Exception as e:
            print(e)

            return False

    def escape_quotes(self, string:str):
        if '\"' in string:
            return string.replace('\"', '\\\"')
        elif '"' in string:
            return string.replace('"', '\\\"')         
        else:
            return string

    def cleaning_dataframe(self, df:pd.DataFrame):
        str_df = df.astype(str)
        return str_df.drop_duplicates()

class QueryProcessor(Processor):
    def __init__(self):
        super().__init__()
        
    def getEntityById(self, entityId:str) -> pd.DataFrame:
        df = pd.DataFrame()
        if self.dbPathOrUrl.endswith('.db'):
            with connect(self.dbPathOrUrl) as con:
                query = f'''
                     SELECT id, creator, title, NULL AS body, NULL AS target, NULL AS motivation 
                     FROM Metadata 
                     WHERE id = '{entityId}' 
                     UNION 
                     SELECT id, NULL, NULL, body, target, motivation 
                     FROM Annotation 
                     WHERE id = '{entityId}' 
                     UNION 
                     SELECT id, NULL, NULL, NULL, NULL, NULL 
                     FROM Image 
                     WHERE id = '{entityId}' 
                '''
                df = pd.read_sql(query, con).dropna(axis=1, how='all')
                return df

        else:

            endpoint = self.getDbPathOrUrl()
            query ="""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <https://github.com/leonardozilli/res/>
            PREFIX schema: <https://schema.org/>

            select ?id ?label (strafter(str(?class), 'res/') AS ?type)
            where {
            ?entity schema:identifier ?id ;
                rdf:type ?class ;
                rdfs:label ?label;
                schema:identifier "%s" .
            }
            """ % self.escape_quotes(entityId)
            df_sparql = get(endpoint, query, True)
            return self.cleaning_dataframe(df_sparql)
