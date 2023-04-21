from DM_classes import *
import re
import json
from pprint import pprint
from string import Template
from typing import List
from sqlite3 import connect
from pandas import read_csv, Series, read_sql, read_json, json_normalize
from rdflib import Graph, URIRef, Literal, Namespace, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from SPARQLWrapper import SPARQLWrapper, JSON

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

    #why does this returns bool??
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

    #why does this returns bool??
    #where to get label??
    def uploadData(self, path: str) -> bool:
        df = read_csv(path,
                      keep_default_na=False)


        #do these need an internalId??
        #query also possible, same performances, maybe more readable?
        entitycreator = []
        collectionItems = []
        #Iteratively appending rows to a DataFrame can be more computationally intensive than a single concatenate. A better solution is to append those rows to a list and then concatenate the list with the original DataFrame all at once

        for idx, row in df.iterrows():
            if row['creator'] != '':
                if ';' in row['creator']:
                    for creator in row['creator'].split(';'):
                        entitycreator.append((row['id'], creator))
                else:
                    entitycreator.append((row['id'], row['creator']))

        collections_df = df[df['id'].str.endswith('collection')]
        manifests_df = df[df['id'].str.endswith('manifest')]
        canvases_df = df[df['id'].str.contains('canvas')]
        entitycreator_df = pd.DataFrame(entitycreator, columns=['id', 'creator'])

        with connect(self.dbPathOrUrl) as conn:
            collections_df.to_sql('Collection', conn, if_exists="replace", index=False)
            manifests_df.to_sql('Manifest', conn, if_exists="replace", index=False)
            canvases_df.to_sql('Canvas', conn, if_exists="replace", index=False)
            entitycreator_df.to_sql("EntityCreator", conn, if_exists="replace", index=False)


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

        #id has id -> id???
        #what should the id be?? a literal??
        g.add((URIRef(data['id']), identifier, Literal(data['id'])))
        g.add((URIRef(data['id']), RDF.type, collection_uri))
        g.add((URIRef(data['id']), label, Literal(data['label']['none'][0])))
        #g.add((URIRef(data['id']),  hasItems, Literal(data['items']))) #can a list of items be a Literal? "object must be an rdflib term..."
        for idx, manifest in enumerate(data['items']):
            g.add((URIRef(data['id']), hasItems, Literal(manifest['id'])))
            g.add((URIRef(manifest['id']), identifier, Literal(manifest['id'])))
            g.add((URIRef(manifest['id']), RDF.type, manifest_uri))
            g.add((URIRef(manifest['id']), label, Literal(manifest['label']['none'][0])))
            #g.add((URIRef(manifest['id']), hasItems, Literal(manifest['items'])))
            for idx, canvas in enumerate(manifest['items']):
                g.add((URIRef(manifest['id']), hasItems, Literal(canvas['id'])))
                g.add((URIRef(canvas['id']), identifier, Literal(canvas['id'])))
                g.add((URIRef(canvas['id']), RDF.type, canvas_uri))
                g.add((URIRef(canvas['id']), label, Literal(canvas['label']['none'][0]))) #only the string of the label??

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
        if '.db' in self.dbPathOrUrl: #this could be problematic
            with connect(self.dbPathOrUrl) as con:
                query = f"SELECT * FROM Annotation JOIN Image ON Annotation.body == Image.id where Annotation.id == '{entityId}'"
                df = read_sql(query, con)
                return df
        else:
            endpoint = self.dbPathOrUrl

            #what should this dataframe describe? surely not only the id?
            query = """
                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX lz: <http://leonardozilli.it/#>
                PREFIX schema: <https://schema.org/>

                SELECT *
                WHERE {?s schema:identifier {entityId} .
                    ?s ?p ?o}
                    """

            endpoint.setQuery(query)
            endpoint.setReturnFormat(JSON)
            result = endpoint.queryAndConvert()
            return json_normalize(result['results']['bindings'])[['id.value']].rename(columns={'id.value' : 'id'})


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
        
    def getEntitiesWithTitle(self, title: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM Annotation WHERE title == '{title}'"
            df_sql = read_sql(query, con)
        return df_sql


#maybe handle input errors?
class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def getAllCollections(self):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = '''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>

            SELECT *
            WHERE {?s a lz:Collection }
            '''
        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        return json_normalize(result['results']['bindings'])[['s.value']].rename(columns={'s.value' : 'id'})

    def getAllManifests(self):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = '''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>

            SELECT ?s
            WHERE {?s a lz:Manifest }
        '''

        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        return json_normalize(result['results']['bindings'])[['s.value']].rename(columns={'s.value' : 'id'})

    def getAllCanvases(self):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = '''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>

            SELECT *
            WHERE {?s a lz:Canvas}
        '''
        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        return json_normalize(result['results']['bindings'])[['s.value']].rename(columns={'s.value' : 'id'})


    def getEntitiesWithLabel(self, label: str):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = f'''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>

            SELECT ?s
            WHERE {{?s lz:Label "{label}"}}
        '''
        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        return json_normalize(result['results']['bindings'])[['s.value']].rename(columns={'s.value' : 'id'})


    def getCanvasesInCollection(self, collectionId: str):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = f'''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>
            PREFIX schema: <https://schema.org/>

            SELECT ?c
            WHERE {{?s schema:identifier "{collectionId}" ;
                      lz:hasItems ?o .
                  ?m schema:identifier ?o ;
                     lz:hasItems ?c }}
        '''

        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        return json_normalize(result['results']['bindings'])[['c.value']].rename(columns={'c.value' : 'id'})

    def getCanvasesInManifest(self, manifestId: str):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = f'''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>
            PREFIX schema: <https://schema.org/>

            SELECT ?id
            WHERE {{?s schema:identifier "{manifestId}" .
                   ?s lz:hasItems ?id}}
        '''

        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        return json_normalize(result['results']['bindings'])[['id.value']].rename(columns={'id.value' : 'id'})

    def getManifestsInCollection(self, collectionId: str):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = f'''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>
            PREFIX schema: <https://schema.org/>

            SELECT ?o
            WHERE {{?s schema:identifier "{collectionId}" .
                   ?s lz:hasItems ?o }}
        '''

        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        return json_normalize(result['results']['bindings'])[['o.value']].rename(columns={'o.value' : 'id'})
