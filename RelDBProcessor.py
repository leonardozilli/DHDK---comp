from DM_classes import *
import pandas as pd
from sqlite3 import connect
import json

class Processor():
    def __init__(self):
        self.dbPathOrUrl = ""
    
    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> None:
        self.dbPathOrUrl = pathOrUrl

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

class AnnotationProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path: str) -> bool:
        #separate function?
        try:
            with open("entity_counter.json", "r", encoding="utf-8") as f:
                counter_data = json.load(f)
            ann_counter = counter_data['annotation']
            image_counter = counter_data['image']

        except FileNotFoundError:
            #singular o plural?
            #what if you upload the same data 2 times? the counter advances but the items remain the same??
            entity_counter = {
                "annotation": 0,
                "image": 0,
                "collection": 0,
                "manifest": 0,
                "canvas": 0
            }

            with open("entity_counter.json", "w", encoding="utf-8") as newfile:
                json.dump(entity_counter, newfile, ensure_ascii=False, indent=4)


        try:
            data = pd.read_csv(path, keep_default_na=False, dtype={
                                                              "id": "string",
                                                              "body": "string",
                                                              "target": "string",
                                                              "motivation": "string"})
            with open("entity_counter.json", "r") as f:
                counter_data = json.load(f)

            #numpy possible
            data.insert(0, "internalId", ["Annotation_" + str(i) for i in range(counter_data['annotation'], counter_data['annotation'] + len(data))])

            df_images = data[["body"]]
            df_images.insert(0, "internalId", ["Image_" + str(i) for i in range(counter_data['image'], counter_data['image'] + len(df_images))])
            df_images = df_images.rename(columns={'body': 'id'})


            counter_data['annotation'] += len(data)
            counter_data['image'] += len(df_images) 

            with open("entity_counter.json", "w", encoding="utf-8") as updatedfile:
                json.dump(counter_data, updatedfile, ensure_ascii=False, indent=4)

            with connect(self.dbPathOrUrl) as conn:
                data.to_sql('Annotation', conn, if_exists="replace", index=False, dtype={
                                                                                      "id": "string",
                                                                                      "body": "string",
                                                                                      "target": "string",
                                                                                      "motivation": "string"})
                
                df_images.to_sql('Image', conn, if_exists="replace", index=False, dtype={
                                                                                        "internalId": "string",
                                                                                         "id": "string"})
            return True

        except Exception as e:
            print(e)

            return False


class MetadataProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path: str) -> bool:
        try:
            with open("entity_counter.json", "r", encoding="utf-8") as f:
                counter_data = json.load(f)
            coll_counter = counter_data['collection']
            man_counter = counter_data['manifest']
            canv_counter = counter_data['canvas']

        except FileNotFoundError:
            #singular o plural?
            #what if you upload the same data 2 times?
            entity_counter = {
                "annotation": 0,
                "image": 0,
                "collection": 0,
                "manifest": 0,
                "canvas": 0
            }

            with open("entity_counter.json", "w", encoding="utf-8") as newfile:
                json.dump(entity_counter, newfile, ensure_ascii=False, indent=4)

        try:
            df = pd.read_csv(path, keep_default_na=False, dtype={
                                                            "id": "string",
                                                            "title": "string",
                                                            "creator": "string"})

            with open("entity_counter.json", "r") as f:
                counter_data = json.load(f)


            collections_df = df[df['id'].str.endswith('collection')]
            manifests_df = df[df['id'].str.endswith('manifest')]
            canvases_df = df[df['id'].str.contains('canvas')]

            #do we REALLY need internal IDs? they dont seem to serve any essential purpose
            collections_df.insert(0, "internalId", ["Collection_" + str(i) for i in range(counter_data['collection'], counter_data['collection'] + len(collections_df))])
            manifests_df.insert(0, "internalId", ["Manifest_" + str(i) for i in range(counter_data['manifest'], counter_data['manifest'] + len(manifests_df))])
            canvases_df.insert(0, "internalId", ["Canvas_" + str(i) for i in range(counter_data['canvas'], counter_data['canvas'] + len(canvases_df))])

            counter_data['collection'] += len(collections_df)
            counter_data['manifest'] += len(manifests_df)
            counter_data['canvas'] += len(canvases_df)

            with open("entity_counter.json", "w", encoding="utf-8") as updatedfile:
                json.dump(counter_data, updatedfile, ensure_ascii=False, indent=4)

            collection_creator_df = collections_df[collections_df['creator'] != ''][['internalId', 'creator']]
            manifest_creator_df = manifests_df[manifests_df['creator'] != ''][['internalId', 'creator']]
            canvas_creator_df = canvases_df[canvases_df['creator'] != ''][['internalId', 'creator']]

            creator_df = pd.concat([collection_creator_df, manifest_creator_df, canvas_creator_df], ignore_index=True)
            creator_df['creator'] = creator_df['creator'].str.split('; ')
            creator_df = creator_df.explode('creator')

            with connect(self.dbPathOrUrl) as conn:
                collections_df.to_sql('Collection', conn, if_exists="replace", index=False)
                manifests_df.to_sql('Manifest', conn, if_exists="replace", index=False)
                canvases_df.to_sql('Canvas', conn, if_exists="replace", index=False)
                creator_df.to_sql("EntityCreator", conn, if_exists="replace", index=False)

            return True

        except Exception as e:
            print(e)

            return False

class RelationalQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def getAllAnnotations(self):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT id, body, target, motivation FROM Annotation"
            df_sql = pd.read_sql(query, con)
        return df_sql

    def getAllImages(self):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT id FROM Image"
            df_sql = pd.read_sql(query, con)
        return df_sql

    def getAnnotationsWithBody(self, bodyId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT id, body, target, motivation FROM Annotation WHERE body == '{bodyId}'"
            df_sql = pd.read_sql(query, con)
        return df_sql

    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT id, body, target, motivation FROM Annotation WHERE body == '{bodyId}' AND target == '{targetId}'"
            df_sql = pd.read_sql(query, con)
        return df_sql
    
    def getAnnotationsWithTarget(self, targetId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT id, body, target, motivation FROM Annotation WHERE target == '{targetId}'"
            df_sql = pd.read_sql(query, con)
        return df_sql

    def getEntitiesWithCreator(self, creatorName: str):
        with connect(self.dbPathOrUrl) as con:
            query = f'''
                        SELECT c.id, c.creator, c.title, ecr.creator
                        FROM Collection c
                        JOIN EntityCreator ecr ON c.internalId = ecr.internalId
                        WHERE ecr.Creator == '{creatorName}'
                        UNION ALL
                        SELECT m.id, m.creator, m.title, ecr.creator
                        FROM Manifest m
                        JOIN EntityCreator ecr ON m.internalId = ecr.internalId
                        WHERE ecr.Creator == '{creatorName}'
                        UNION ALL
                        SELECT cv.id, cv.creator, cv.title, ecr.creator
                        FROM Canvas cv
                        JOIN EntityCreator ecr ON cv.internalId = ecr.internalId
                        WHERE ecr.Creator == '{creatorName}'
            '''
            df_sql = pd.read_sql(query, con)
        return df_sql
        
    def getEntitiesWithTitle(self, title: str):
        with connect(self.dbPathOrUrl) as con:
            query = f'''
                SELECT id, title, creator FROM 'Collection' 
                WHERE title == '{title}'
                UNION ALL
                SELECT id, title, creator FROM 'Manifest' 
                WHERE title == '{title}'
                UNION ALL
                SELECT id, title, creator FROM 'Canvas' 
                WHERE title == '{title}'
            '''
            df_sql = pd.read_sql(query, con)
        return df_sql
