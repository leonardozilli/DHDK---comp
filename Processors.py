import pandas as pd
import numpy as np
from sqlite3 import connect
from SPARQLWrapper import SPARQLWrapper, JSON
from string import Template


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
            endpoint = SPARQLWrapper(self.dbPathOrUrl) 
            query = '''
                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX lz: <http://leonardozilli.it/#>
                PREFIX schema: <https://schema.org/>

                SELECT ?id ?label
                WHERE { ?id schema:identifier "$ENID" ;
                            lz:Label ?label}
                    '''

            endpoint.setQuery(Template(query).substitute(ENID=entityId))
            endpoint.setReturnFormat(JSON)
            result = endpoint.queryAndConvert()
            try:
                return pd.json_normalize(result['results']['bindings'])[['id.value',
                                                          'label.value']].rename(columns={'id.value' : 'id',
                                                                                          'label.value' : 'label'})
            except KeyError:
                #????
                return pd.DataFrame({'id':[np.nan]})

