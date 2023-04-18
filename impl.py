from DM_classes import *
import re
import json
from pprint import pprint
from typing import List
from sqlite3 import connect
from pandas import read_csv, Series, read_sql, read_json
from rdflib import Graph, URIRef, Literal, Namespace, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from SPARQLWrapper import SPARQLWrapper, JSON
import sparql_dataframe

class Processor():
    def __init__(self):
        self.dbPathOrUrl = ""
    
    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> None:
        self.dbPathOrUrl = pathOrUrl


class AnnotationProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path: str) -> bool:
        data = read_csv(path, keep_default_na=False)

        annotations_id = []
        for idx, row in data.iterrows():
            annotations_id.append("annotation-" + str(idx))
        data.insert(0, "internalId", Series(annotations_id, dtype="string"))

        df_images = data[["body"]]
        images_id = []
        for idx, row in df_images.iterrows():
            images_id.append("image-" + str(idx))
        df_images.insert(0, "internalId", Series(images_id, dtype="string"))

        df_images = df_images.rename(columns={'body': 'id'})



        with connect(self.dbPathOrUrl) as conn:
            data.to_sql('Annotation', conn, if_exists="replace", index=False, dtype={
                                                                                  "id": "string",
                                                                                  "body": "string",
                                                                                  "target": "string",
                                                                                  "motivation": "string"})
            
            df_images.to_sql('Image', conn, if_exists="replace", index=False, dtype={
                                                                                     "id": "string"})


class MetadataProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path: str) -> bool:
        df = read_csv(path,
                      keep_default_na=False)

        collections_df = df[df['id'].str.endswith('collection')]
        manifests_df = df[df['id'].str.endswith('manifest')]
        canvases_df = df[df['id'].str.contains('canvas')]

        with connect(self.dbPathOrUrl) as conn:
            collections_df.to_sql('Collection', conn, if_exists="replace", index=False)
            manifests_df.to_sql('Manifest', conn, if_exists="replace", index=False)
            canvases_df.to_sql('Canvas', conn, if_exists="replace", index=False)


class CollectionProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path: str) -> bool:
        with open(path, 'r') as f:
            data = json.load(f)

        base_url = 'http://leonardozilli.it/res/'
        mywb = Namespace('http://leonardozilli.it/#')
        schema = Namespace('https://schema.org/')

        #entities
        collection_uri = URIRef(mywb.Collection)
        manifest_uri = URIRef(mywb.Manifest)
        canvas_uri = URIRef(mywb.Canvas)

        #attributes
        identifier = URIRef(schema.identifier)
        label = URIRef(mywb.Label)

        #relations
        hasItems = URIRef(mywb.hasItems)
        g = Graph()

        collection_subj = URIRef(base_url + re.findall(r'[ \w-]+?(?=\.)', path)[0])
        g.add((URIRef(collection_subj), identifier, Literal(data['id'])))
        g.add((URIRef(collection_subj), RDF.type, collection_uri))
        g.add((URIRef(collection_subj), label, Literal(data['label']['none'][0])))
        g.add((URIRef(collection_subj),  hasItems, Literal(data['items']))) #can a list of items be a Literal?
        canvas_count = 0 #counting canvases from 0 for each manifest?
        for idx, manifest in enumerate(data['items']):
            manifest_subj = URIRef(base_url + 'manifest-' + str(idx)) #this doesnt work with manifest from different .json files
            g.add((URIRef(manifest_subj), identifier, Literal(manifest['id'])))
            g.add((URIRef(manifest_subj), RDF.type, manifest_uri))
            g.add((URIRef(manifest_subj), label, Literal(manifest['label']['none'][0])))
            g.add((URIRef(manifest_subj), hasItems, Literal(manifest['items'])))
            for idx, canvas in enumerate(manifest['items']):
                canvas_count += 1
                canvas_subj = URIRef(base_url + 'canvas-' + str(canvas_count))
                g.add((URIRef(canvas_subj), identifier, Literal(canvas['id'])))
                g.add((URIRef(canvas_subj), RDF.type, canvas_uri))
                g.add((URIRef(canvas_subj), label, Literal(canvas['label']['none'][0])))

        store = SPARQLUpdateStore()

        endpoint = self.getDbPathOrUrl()

        store.open((endpoint, endpoint))

        for triple in g.triples((None, None, None)):
           store.add(triple)
        store.close()


class QueryProcessor(Processor):
    def __init__(self):
        super().__init__()

    def getEntityById(self, entityId: str):
        if '.db' in self.dbPathOrUrl:
            with connect(self.dbPathOrUrl) as con:
                query = f"SELECT * FROM Annotation JOIN Image ON Annotation.body == Image.id where Annotation.id == '{entityId}'"
                df = read_sql(query, con)
                return df
        else:
            endpoint = self.dbPathOrUrl
            query = """
                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX lz: <http://leonardozilli.it/#>
                PREFIX schema: <https://schema.org/>

                SELECT *
                WHERE {?s schema:identifier {entityId} .
                    ?s ?p ?o}
                    """
            df = sparql_dataframe.get(endpoint, query, post=True)
            return df


class RelationalQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def getAllAnnotations(self):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM Annotation"
            df_sql = read_sql(query, con)
        return df_sql

    def getAllImages(self):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM Image"
            df_sql = read_sql(query, con)
        return df_sql

    def getAnnotationsWithBody(self, bodyId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM Annotation WHERE body == '{bodyId}'"
            df_sql = read_sql(query, con)
        return df_sql

    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM Annotation WHERE body == '{bodyId}' AND target == '{targetId}'"
            df_sql = read_sql(query, con)
        return df_sql
    
    def getAnnotationsWithTarget(self, targetId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM Annotation WHERE target == '{targetId}'"
            df_sql = read_sql(query, con)
        return df_sql

    def getEntitiesWithCreator(self, creatorName: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM Annotation WHERE creator == '{creatorName}'"
            df_sql = read_sql(query, con)
        return df_sql
        
    #where to get label???
    def getEntitiesWithLabel(self, label: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM Annotation WHERE label == '{label}'"
            df_sql = read_sql(query, con)
        return df_sql
    
    def getEntitiesWithTitle(self, title: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM Annotation WHERE title == '{title}'"
            df_sql = read_sql(query, con)
        return df_sql


class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def getAllCollections(self):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = '''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>

            SELECT *
            WHERE {?s rdf:type lz:Collection .
                  ?s ?p ?o .}
        '''
        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        return result

    def getAllManifests(self):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = '''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>

            SELECT ?s
            WHERE {?s rdf:type lz:Manifest .
                  ?s ?p ?o .}
        '''
        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        return pprint(result)

    def getAllCanvases(self):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = '''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>

            SELECT *
            WHERE {?s rdf:type lz:Canvas .
                  ?s ?p ?o .}
        '''
        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        return result


