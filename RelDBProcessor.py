import pandas as pd
from sqlite3 import connect
from Processors import *
import json

class AnnotationProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path: str) -> bool:
        try:
            data = pd.read_csv(path, keep_default_na=False, dtype={
                                                              "id": "string",
                                                              "body": "string",
                                                              "target": "string",
                                                              "motivation": "string"})

            df_images = data[["body"]]
            df_images = df_images.rename(columns={'body': 'id'})

            with connect(self.dbPathOrUrl) as conn:
                data.to_sql('Annotation', conn, if_exists="replace", index=False, dtype={
                                                                                      "id": "string",
                                                                                      "body": "string",
                                                                                      "target": "string",
                                                                                      "motivation": "string"})
                
                df_images.to_sql('Image', conn, if_exists="replace", index=False, dtype={"id": "string"})
            return True

        except Exception as e:
            print(e)

            return False


class MetadataProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path: str) -> bool:
        try:
            df = pd.read_csv(path, keep_default_na=False, dtype={
                                                            "id": "string",
                                                            "title": "string",
                                                            "creator": "string"})

            collections_df = df[df['id'].str.endswith('collection')]
            manifests_df = df[df['id'].str.endswith('manifest')]
            canvases_df = df[df['id'].str.contains('canvas')]

            collection_creator_df = collections_df[collections_df['creator'] != ''][['id', 'creator']]
            manifest_creator_df = manifests_df[manifests_df['creator'] != ''][['id', 'creator']]
            canvas_creator_df = canvases_df[canvases_df['creator'] != ''][['id', 'creator']]

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
                SELECT c.id, c.creator, c.title
                FROM Collection c
                JOIN EntityCreator ecr ON c.id = ecr.id
                WHERE ecr.Creator == '{creatorName}'
                UNION ALL
                SELECT m.id, m.creator, m.title
                FROM Manifest m
                JOIN EntityCreator ecr ON m.id = ecr.id
                WHERE ecr.Creator == '{creatorName}'
                UNION ALL
                SELECT cv.id, cv.creator, cv.title
                FROM Canvas cv
                JOIN EntityCreator ecr ON cv.id = ecr.id
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
