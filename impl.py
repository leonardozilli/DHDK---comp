from DM_classes import *
import json
from pprint import pprint
from typing import List
from sqlite3 import connect
from string import Template
import pandas as pd
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

    def uploadData(self, path: str) -> bool:
        try:
            data = read_csv(path, keep_default_na=False, dtype={
                                                              "id": "string",
                                                              "body": "string",
                                                              "target": "string",
                                                              "motivation": "string"})

            annotations_id = []
            #change iterrows
            for idx, row in data.iterrows():
                annotations_id.append("Annotation_" + str(idx))
            data.insert(0, "internalId", Series(annotations_id, dtype="string"))

            df_images = data[["body"]]
            images_id = []
            for idx, row in df_images.iterrows():
                images_id.append("Image_" + str(idx))
            df_images.insert(0, "internalId", Series(images_id, dtype="string"))

            df_images = df_images.rename(columns={'body': 'id'})



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
            df = read_csv(path, keep_default_na=False, dtype={
                                                            "id": "string",
                                                            "title": "string",
                                                            "creator": "string"
            })


            entitycreator = []
            collectionItems = []
            collectionitems = dict()
            manifestitems = dict()
            targetcollection = None
            targetmanifest = None
            #Iteratively appending rows to a DataFrame can be more computationally intensive than a single concatenate. A better solution is to append those rows to a list and then concatenate the list with the original DataFrame all at once
            for idx, row in df.iterrows():
                if row['creator'] != '':
                    if ';' in row['creator']:
                        for creator in list(map(str.strip, row['creator'].split(';'))):
                            entitycreator.append((row['id'], creator))
                    else:
                        entitycreator.append((row['id'], row['creator']))

                if row['id'].endswith('collection'):
                    targetcollection = row['id']
                    collectionites[targetcollection] = list()
                elif row['id'].endswith('manifest'):
                    targetmanifest = row['id']
                    manifestitems[targetmanifest] = list()
                    collectionitems[targetcollection].append(targetmanifest)
                elif '/canvas' in row['id']:
                    manifestitems[targetmanifest].append(row['id'])


            collections_df = df[df['id'].str.endswith('collection')]
            manifests_df = df[df['id'].str.endswith('manifest')]
            canvases_df = df[df['id'].str.contains('canvas')]
            entitycreator_df = pd.DataFrame(entitycreator, columns=['id', 'creator'])
            collectionitems_df = pd.DataFrame(
                [(k, i) for k, v in collectionitems.items() for i in v],
                columns=['id', 'item'])

            manifestitems_df = pd.DataFrame(
                [(k, i) for k, v in manifestitems.items() for i in v],
                columns=['id', 'item'])

            with connect(self.dbPathOrUrl) as conn:
                collections_df.to_sql('Collection', conn, if_exists="replace", index=False)
                manifests_df.to_sql('Manifest', conn, if_exists="replace", index=False)
                canvases_df.to_sql('Canvas', conn, if_exists="replace", index=False)
                entitycreator_df.to_sql("EntityCreator", conn, if_exists="replace", index=False)
                collectionitems_df.to_sql("CollectionItems", conn, if_exists="replace", index=False)
                manifestitems_df.to_sql("ManifestItems", conn, if_exists="replace", index=False)

            return True

        except Exception as e:
            print(e)

            return False


class CollectionProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path: str) -> bool:
        try:
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

            return True

        except Exception as e:
            print(e)

            return False


class QueryProcessor(Processor):
    def __init__(self):
        super().__init__()

    def getEntityById(self, entityId: str):
        if '.db' in self.dbPathOrUrl: #this could be problematic [e.g. hidden files]
            with connect(self.dbPathOrUrl) as con:
                query = f'''
                    SELECT * FROM 'Collection' WHERE id == '{entityId}'
                    UNION ALL
                    SELECT * FROM 'Manifest' WHERE id == '{entityId}'
                    UNION ALL
                    SELECT * FROM 'Canvas' WHERE id == '{entityId}'
                    UNION ALL
                    SELECT * FROM 'Annotation' WHERE id == '{entityId}'
                    UNION ALL
                    SELECT * FROM 'Image' WHERE id == '{entityId}'
                    '''
                df = read_sql(query, con)
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


class RelationalQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def getAllAnnotations(self):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT id, body, target, motivation FROM Annotation"
            df_sql = read_sql(query, con)
        return df_sql

    def getAllImages(self):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT id FROM Image"
            df_sql = read_sql(query, con)
        return df_sql

    def getAnnotationsWithBody(self, bodyId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT id, body, target, motivation FROM Annotation WHERE body == '{bodyId}'"
            df_sql = read_sql(query, con)
        return df_sql

    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT id, body, target, motivation FROM Annotation WHERE body == '{bodyId}' AND target == '{targetId}'"
            df_sql = read_sql(query, con)
        return df_sql
    
    def getAnnotationsWithTarget(self, targetId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT id, body, target, motivation FROM Annotation WHERE target == '{targetId}'"
            df_sql = read_sql(query, con)
        return df_sql

    def getEntitiesWithCreator(self, creatorName: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT id, creator FROM EntityCreator WHERE creator == '{creatorName}'"
            df_sql = read_sql(query, con)
        return df_sql
        
    def getEntitiesWithTitle(self, title: str):
        with connect(self.dbPathOrUrl) as con:
            query = f'''
                SELECT id, title, creator FROM 'Collection' WHERE title == '{title}'
                UNION
                SELECT id, title, creator FROM 'Manifest' WHERE title == '{title}'
                UNION
                SELECT id, title, creator FROM 'Canvas' WHERE title == '{title}'
            '''
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

            SELECT ?id ?label
            WHERE {?id a lz:Collection ;
                         lz:Label ?label }
            '''
        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        return json_normalize(result['results']['bindings'])[['id.value',
                                                              'label.value']].rename(columns={'id.value' : 'id',
                                                                                             'label.value' : 'label'})

    def getAllManifests(self):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = '''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>

            SELECT ?id ?label ?item
            WHERE {?id a lz:Manifest ;
                         lz:Label ?label ;
                         lz:hasItems ?item}
        '''
        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        return json_normalize(result['results']['bindings'])[['id.value',
                                                              'label.value',
                                                              'item.value']].rename(columns={'id.value' : 'id',
                                                                                             'label.value' : 'label',
                                                                                             'item.value' : 'item'})


    def getAllCanvases(self):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = '''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>

            SELECT ?id ?label
            WHERE {?id a lz:Canvas ;
                    lz:Label ?label }
        '''
        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        return json_normalize(result['results']['bindings'])[['id.value',
                                                              'label.value']].rename(columns={'id.value' : 'id',
                                                                                              'label.value' : 'label'})


    def getEntitiesWithLabel(self, label: str):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 
#how to get the canvases if they have no items?
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


class GenericQueryProcessor():
    def __init__(self):
        self.queryProcessors = list()

    def cleanQueryProcessors(self) -> bool:
        #returns bool??
        self.queryProcessors = list()

    def addQueryProcessor(self, processor: QueryProcessor) -> bool:
        #returns bool??
        self.queryProcessors.append(processor)

    def getQueryProcessors(self):
        #NOT REQUIRED
        return self.queryProcessors

    def getAllAnnotations(self) -> List[Annotation]:
        for processor in self.queryProcessors:
            #????
            if isinstance(processor, RelationalQueryProcessor):
                result = [Annotation(identifier, body, target, motivation) for identifier, body, target, motivation in zip(processor.getAllAnnotations()['id'], processor.getAllAnnotations()['body'], processor.getAllAnnotations()['target'], processor.getAllAnnotations()['motivation'])]
                return result

    def getAllImages(self):
        for processor in self.queryProcessors:
            #???
            if isinstance(processor, RelationalQueryProcessor):
                result = [Image(identifier) for identifier in processor.getAllImages()['id']]
                return result

    def getAllCanvas(self) -> List[Canvas]:
        for processor in self.queryProcessors:
            if isinstance(processor, TriplestoreQueryProcessor):
                result = [Canvas(identifier, label, title, creator) for identifier, label in
                          zip(processor.getAllCanvases()['id'],
                              processor.getAllCanvases()['label'],
                              processor.getAllCanvases()['title'],
                              processor.getAllCanvases()['creator'])]
                return result

    def getAllCollections(self) -> List[Collection]:
        for processor in self.queryProcessors:
            #????
            if isinstance(processor, TriplestoreQueryProcessor):
                ts_df = processor.getAllCollections()
                for idx, row in ts_df.iterrows():
                    print(row)
                    print(processor.getEntityById(row['id']))

            


    def getAllManifests(self) -> List[Manifest]:
        pass

    def getAnnotationsToCanvas(self, canvasId: str) -> List[Annotation]:
        for processor in self.queryProcessors:
            #????
            if isinstance(processor, RelationalQueryProcessor):
                result = [Annotation(identifier, body, target, motivation) for
                          identifier, body, target, motivation in
                          zip(processor.getAnnotationsWithTarget(canvasId)['id'],
                              processor.getAnnotationsWithTarget(canvasId)['body'],
                              processor.getAnnotationsWithTarget(canvasId)['target'],
                              processor.getAnnotationsWithTarget(canvasId)['motivation'])]
                return result

    def getAnnotationsToCollection(self, collectionId: str) -> List[Annotation]:
        for processor in self.queryProcessors:
            #????
            if isinstance(processor, RelationalQueryProcessor):
                result = [Annotation(identifier, body, target, motivation) for
                          identifier, body, target, motivation in
                          zip(processor.getAnnotationsWithTarget(collectionId)['id'],
                              processor.getAnnotationsWithTarget(collectionId)['body'],
                              processor.getAnnotationsWithTarget(collectionId)['target'],
                              processor.getAnnotationsWithTarget(collectionId)['motivation'])]
                return result

    def getAnnotationsToManifest(self, manifestId: str) -> List[Annotation]:
        for processor in self.queryProcessors:
            #????
            if isinstance(processor, RelationalQueryProcessor):
                result = [Annotation(identifier, body, target, motivation) for
                          identifier, body, target, motivation in
                          zip(processor.getAnnotationsWithTarget(manifestId)['id'],
                              processor.getAnnotationsWithTarget(manifestId)['body'],
                              processor.getAnnotationsWithTarget(manifestId)['target'],
                              processor.getAnnotationsWithTarget(manifestId)['motivation'])]
                return result

    def getAnnotationsWithBody(self, bodyId: str) -> List[Annotation]:
        for processor in self.queryProcessors:
            #????
            if isinstance(processor, RelationalQueryProcessor):
                result = [Annotation(identifier, body, target, motivation) for
                          identifier, body, target, motivation in
                          zip(processor.getAnnotationsWithBody(bodyId)['id'],
                              processor.getAnnotationsWithBody(bodyId)['body'],
                              processor.getAnnotationsWithBody(bodyId)['target'],
                              processor.getAnnotationsWithBody(bodyId)['motivation'])]
                return result

    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId: str) -> List[Annotation]:
        for processor in self.queryProcessors:
            #????
            if isinstance(processor, RelationalQueryProcessor):
                result = [Annotation(identifier, body, target, motivation) for
                          identifier, body, target, motivation in
                          zip(processor.getAnnotationsWithBodyAndTarget(bodyId, targetId)['id'],
                              processor.getAnnotationsWithBodyAndTarget(bodyId, targetId)['body'],
                              processor.getAnnotationsWithBodyAndTarget(bodyId, targetId)['target'],
                              processor.getAnnotationsWithBodyAndTarget(bodyId, targetId)['motivation'])]
                return result

    def getAnnotationsWithTarget(self, targetId: str) -> List[Annotation]:
        for processor in self.queryProcessors:
            #????
            if isinstance(processor, RelationalQueryProcessor):
                result = [Annotation(identifier, body, target, motivation) for
                          identifier, body, target, motivation in
                          zip(processor.getAnnotationsWithTarget(targetId)['id'],
                              processor.getAnnotationsWithTarget(targetId)['body'],
                              processor.getAnnotationsWithTarget(targetId)['target'],
                              processor.getAnnotationsWithTarget(targetId)['motivation'])]
                return result

    def getCanvasesInCollection(self, collectionId: str) -> List[Canvas]:
        pass

    def getCanvasesInManifest(self, manifestId: str) -> List[Manifest]:
        pass

    def getEntityById(self, entityId: str) -> IdentifiableEntity:
        for processor in self.queryProcessors:
            try:
                return IdentifiableEntity(processor.getEntityById(entityId)['id'].astype(str).values[0])
            #???
            except KeyError:
                pass


    def getEntitiesWithCreator(self, creatorName: str) -> List[EntityWithMetadata]:
        pass

    def getEntitiesWithLabel(self, label: str) -> List[EntityWithMetadata]:
        pass

    def getEntitiesWithTitle(self, title: str) -> List[EntityWithMetadata]:
        pass

    def getImagesAnnotatingCanvas(self, canvasId: str) -> List[Image]:
        for processor in self.queryProcessors:
            #????
            if isinstance(processor, RelationalQueryProcessor):
                result = [Image(identifier) for
                          identifier in
                          processor.getAnnotationsWithTarget(canvasId)['body']]
                return result

    def getManifestsInCollection(self, collectionId: str) -> List[Manifest]:
        pass

