import pandas as pd
from sqlite3 import connect


class Processor():
    def __init__(self):
        self.dbPathOrUrl = ""
    
    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        try:
            self.dbPathOrUrl = pathOrUrl
            
            return True
        except Exception as e:
            print(e)

            return False

class QueryProcessor(Processor):
    def __init__(self):
        super().__init__()

    def getEntityById(self, entityId: str):
        if self.dbPathOrUrl.endswith('.db'):
            with connect(self.dbPathOrUrl) as con:
                query = f'''
                    SELECT id, creator, title, NULL AS body, NULL AS target, NULL AS motivation
                    FROM Collection
                    WHERE id = '{entityId}'
                    UNION
                    SELECT id, creator, title, NULL, NULL, NULL
                    FROM Manifest
                    WHERE id = '{entityId}'
                    UNION
                    SELECT id, creator, title, NULL, NULL, NULL
                    FROM Canvas
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
                df = pd.read_sql(query, con).dropna(axis=1)
                return df
        else:
            endpoint = SPARQLWrapper(self.dbPathOrUrl) 

            #what should this dataframe describe? surely not only the id?
            query = '''
                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX lz: <http://leonardozilli.it/#>
                PREFIX schema: <https://schema.org/>

                SELECT ?id
                WHERE { ?id schema:identifier "$ENID" }
                    '''

            endpoint.setQuery(Template(query).substitute(ENID=entityId))
            endpoint.setReturnFormat(JSON)
            result = endpoint.queryAndConvert()
            #not working with canvases
            return json_normalize(result['results']['bindings'])[['id.value']].rename(columns={'id.value' : 'id'})
