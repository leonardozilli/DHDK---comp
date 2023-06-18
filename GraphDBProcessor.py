from QueryProcessor import *
from json import load
from sparql_dataframe import get
from rdflib import Graph, URIRef, RDF, RDFS, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

class CollectionProcessor(Processor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path):
        try:
            base_url = 'https://github.com/leonardozilli/res/'
            new_graph = Graph()

            with open(path, 'r', encoding='utf-8') as f:
                json_file = load(f)
            
            if isinstance(json_file, list): 
                for collection in json_file: 
                    self.createGraph(collection, base_url, new_graph)
            else:
                self.createGraph(json_file, base_url, new_graph)
            
            store = SPARQLUpdateStore()
            endpoint = self.getDbPathOrUrl()

            store.open((endpoint, endpoint))

            for triple in new_graph.triples((None, None, None)):
                store.add(triple)
            store.close()
            return True
        
        except Exception as e:
            print(e)
            return False
        
    def createGraph(self, json_file:dict, base_url, new_graph:Graph):
        collection_type = URIRef(base_url + 'Collection') 
        manifest_type = URIRef(base_url + 'Manifest') 
        canvas_type = URIRef(base_url + 'Canvas') 

        has_id = URIRef('https://schema.org/identifier')
        has_part = URIRef('https://www.w3.org/2002/07/owl#hasPart')

        collection_subject = URIRef(json_file['id'])

        new_graph.add((collection_subject, has_id, Literal(json_file['id'])))
        new_graph.add((collection_subject, RDF.type, collection_type))

        collection_label_key:dict = json_file['label']
        collection_label_value = list(collection_label_key.keys())[0] 
        new_graph.add((collection_subject, RDFS.label, Literal(json_file['label'][collection_label_value][0]))) 

        for manifest in json_file['items']: 
            manifest_subject = URIRef(manifest['id'])

            new_graph.add((collection_subject, has_part, manifest_subject))
            new_graph.add((manifest_subject, has_id, Literal(manifest['id'])))
            new_graph.add((manifest_subject, RDF.type, manifest_type))
            
            manifest_label_key:dict = manifest['label']
            manifest_label_value = list(manifest_label_key.keys())[0]
            new_graph.add((manifest_subject, RDFS.label, Literal(manifest['label'][manifest_label_value][0])))

            for canvas in manifest['items']: 
                canvas_subject = URIRef(canvas['id'])
                new_graph.add((manifest_subject, has_part, canvas_subject))
                new_graph.add((canvas_subject, has_id, Literal(canvas['id'])))
                new_graph.add((canvas_subject, RDF.type, canvas_type))

                canvas_label_key:dict = canvas['label']
                canvas_label_value = list(canvas_label_key.keys())[0]
                new_graph.add((canvas_subject, RDFS.label, Literal(canvas['label'][canvas_label_value][0])))


class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()
  
    def getAllCanvases(self):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX lz: <https://github.com/leonardozilli/res/>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?label (strafter(str(?class), 'res/') AS ?type)
        WHERE{
        ?canvas rdf:type lz:Canvas;
            rdfs:label ?label;
            schema:identifier ?id;
            rdf:type ?class.
        }
        """
        df_sparql = get(endpoint, query, True)
        return self.cleaning_dataframe(df_sparql)

    def getAllCollections(self):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX lz: <https://github.com/leonardozilli/res/>
        PREFIX schema: <https://schema.org/>


        SELECT ?id ?label (strafter(str(?class), 'res/') AS ?type)
        WHERE{
        ?s rdf:type lz:Collection;
           rdf:type ?class;
           rdfs:label ?label;
           schema:identifier ?id .
        }
        """
        df_sparql = get(endpoint, query, True)
        return self.cleaning_dataframe(df_sparql)
    
    def getAllManifests(self):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX lz: <https://github.com/leonardozilli/res/>
        PREFIX schema: <https://schema.org/>


        SELECT ?id ?label (strafter(str(?class), 'res/') AS ?type)
        WHERE{
        ?manifest rdf:type lz:Manifest;
            rdfs:label ?label;
            schema:identifier ?id ;
            rdf:type ?class.
        }
        """
        df_sparql = get(endpoint, query, True)
        return self.cleaning_dataframe(df_sparql)
    
    def getCanvasesInCollection(self, collectionId:str):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX lz: <https://github.com/leonardozilli/res/>
        PREFIX schema: <https://schema.org/>
        PREFIX owl: <https://www.w3.org/2002/07/owl#>

        SELECT  ?id ?label (strafter(str(?class), 'res/') AS ?type)
        WHERE {
        ?collection schema:identifier "%s" ;
            rdf:type lz:Collection ;
            owl:hasPart ?manifest .
        ?manifest owl:hasPart ?canvas .
        ?canvas schema:identifier ?id ;
            rdfs:label ?label ;
            rdf:type ?class.        
        }
        """ % collectionId
        df_sparql = get(endpoint, query, True)
        return self.cleaning_dataframe(df_sparql)
    
    def getCanvasesInManifest(self, manifestId:str):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX lz: <https://github.com/leonardozilli/res/>
        PREFIX schema: <https://schema.org/>
        PREFIX owl: <https://www.w3.org/2002/07/owl#>

        SELECT  ?id ?label (strafter(str(?class), 'res/') AS ?type)
        WHERE{
        ?manifest schema:identifier "%s" ; 
            rdf:type lz:Manifest ;            
            owl:hasPart ?canvas .
        ?canvas schema:identifier ?id ;
            rdf:type ?class ;
            rdfs:label ?label .        
        }
        """ % manifestId
        df_sparql = get(endpoint, query, True)
        return self.cleaning_dataframe(df_sparql)
    
    def getEntitiesWithLabel(self, label:str):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?label (strafter(str(?class), 'res/') AS ?type)
        WHERE{
            ?entity rdfs:label "%s" .
            ?entity rdfs:label ?label .
            ?entity schema:identifier ?id .
            ?entity rdf:type ?class .
        } 
        """% self.escape_quotes(label)
        df_sparql = get(endpoint, query, True)
        return self.cleaning_dataframe(df_sparql)
    
    def getManifestsInCollection(self, collectionId:str):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX lz: <https://github.com/leonardozilli/res/>
        PREFIX schema: <https://schema.org/>
        PREFIX owl: <https://www.w3.org/2002/07/owl#>

        SELECT ?id ?label (strafter(str(?class), 'res/') AS ?type)
        WHERE
        {
        ?collection schema:identifier "%s" ;
            rdf:type lz:Collection;
            owl:hasPart ?manifest.
        ?manifest rdf:type lz:Manifest ;
            rdf:type ?class;
            rdfs:label ?label;
            schema:identifier ?id
        }
        """ % collectionId
        df_sparql = get(endpoint, query, True)
        return self.cleaning_dataframe(df_sparql)
