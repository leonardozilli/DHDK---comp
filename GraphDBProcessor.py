from DM_classes import *
from Processors import *
import json
from pprint import pprint
from typing import List
from string import Template
import pandas as pd
from pandas import read_csv, Series, read_sql, read_json, json_normalize
from rdflib import Graph, URIRef, Literal, Namespace, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from SPARQLWrapper import SPARQLWrapper, JSON


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

#maybe handle input errors?
class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def getAllCollections(self):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = '''
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>

            SELECT ?id ?label (GROUP_CONCAT(DISTINCT ?item; SEPARATOR=', ') AS ?items)
            WHERE {
              {?id a lz:Collection ;
                   lz:Label ?label ;
                   lz:hasItems ?item }
            }
            GROUP BY ?id ?label
        '''

        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = json_normalize(endpoint.queryAndConvert()['results']['bindings'])[['id.value',
                                                              'label.value', 'items.value']].rename(columns={'id.value' : 'id',
                                                                                              'label.value' : 'label',
                                                                                              'items.value' : 'items'})
        result['items'] = result['items'].apply(lambda item: item.split(', '))
        return result

    def getAllManifests(self):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = '''
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>

            SELECT ?id ?label (GROUP_CONCAT(DISTINCT ?item; SEPARATOR=', ') AS ?items)
            WHERE {
              {?id a lz:Manifest ;
                   lz:Label ?label ;
                   lz:hasItems ?item }
            }
            GROUP BY ?id ?label
        '''

        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = json_normalize(endpoint.queryAndConvert()['results']['bindings'])[['id.value',
                                                              'label.value', 'items.value']].rename(columns={'id.value' : 'id',
                                                                                              'label.value' : 'label',
                                                                                              'items.value' : 'items'})
        result['items'] = result['items'].apply(lambda item: item.split(', '))

        return result


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
        query = f'''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>

            SELECT ?id ?label
            WHERE {{?id lz:Label "{label}" ;
                        lz:Label ?label }}
        '''
        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        try:
            return json_normalize(result['results']['bindings'])[['id.value',
                                                              'label.value']].rename(columns={'id.value' : 'id', 
                                                                                              'label.value' : 'label'})
        except KeyError:
            return pd.DataFrame({'id':[]})


    def getCanvasesInCollection(self, collectionId: str):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = f'''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>
            PREFIX schema: <https://schema.org/>

            SELECT ?canvas ?label
            WHERE {{?collection schema:identifier "{collectionId}" ;
                          lz:hasItems ?manifestid .
                    ?manifest schema:identifier ?manifestid ;
                          lz:hasItems ?cid .
                    ?canvas schema:identifier ?cid ;
                          lz:Label ?label}}
        '''

        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        try:
            return json_normalize(result['results']['bindings'])[['canvas.value', 
                                                              'label.value']].rename(columns={'canvas.value' : 'id',
                                                                                              'label.value' : 'label'})
        except KeyError:
            return pd.DataFrame({'id':[]})

    def getCanvasesInManifest(self, manifestId: str):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = f'''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>
            PREFIX schema: <https://schema.org/>

            SELECT ?canvas ?label
            WHERE {{?manifest schema:identifier "{manifestId}" ;
                            lz:hasItems ?cid .
                    ?canvas schema:identifier ?cid ;
                            lz:Label ?label}}
        '''

        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        try:
            return json_normalize(result['results']['bindings'])[['canvas.value', 
                                                              'label.value']].rename(columns={'canvas.value' : 'id',
                                                                                              'label.value' : 'label'})
        except KeyError:
            return pd.DataFrame({'id':[]})

    def getManifestsInCollection(self, collectionId: str):

        endpoint = SPARQLWrapper(self.getDbPathOrUrl()) 

        query = f'''
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX lz: <http://leonardozilli.it/#>
            PREFIX schema: <https://schema.org/>

            SELECT ?manifest ?label
            WHERE {{?collection schema:identifier "{collectionId}" ;
                         lz:hasItems ?mid .
                    ?manifest schema:identifier ?mid ;
                         lz:Label ?label}}
        '''

        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        result = endpoint.queryAndConvert()
        try:
            return json_normalize(result['results']['bindings'])[['manifest.value',
                                                              'label.value']].rename(columns={'manifest.value' : 'id',
                                                                                              'label.value' : 'label'})
        except KeyError:
            return pd.DataFrame({'id':[]})

