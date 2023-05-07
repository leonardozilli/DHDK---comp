#this file contains Processor, QueryProcessor, TriplestoreQueryProcessor, CollectionProcessor
#Leonardo in QueryProcessor you should add the sqlquery to query the relational DB
from json import load
from data_model import *
from process_graph import *
from rdflib import Graph
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get
from pandas import DataFrame, read_sql
from sqlite3 import connect
from helpers import *

#1. Processor. WE NEED TO CHECK THIS
class Processor(object):
    def __init__(self, dbPathOrUrl:str):
        self.dbPathOrUrl = dbPathOrUrl
        self.dbPathOrUrl = "" #in this way we ensure that at the beginning is always empty
        
    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl
    
    def setDbPathOrUrl(self, pathOrUrl):
        self.dbPathOrUrl = pathOrUrl

# 2. CollectionProcessor
class CollectionProcessor(Processor):
    def __init__(self, dbPathOrUrl: str):
        super().__init__(dbPathOrUrl)

    def uploadData(self, path):
        try:
            base_url = "https://github.com/leonardozilli/res/"
            new_graph = Graph()

            with open(path, 'r', encoding='utf-8') as f:
                json_file = load(f)
            
            #this piece of code handles the creation of a new/new part of a graph
            #process_graph is an external function I developed to handle tthe case where more 
            # than one collection is included in the same json file. You can find it in process_graph.py
            #first case: if there is more than one
            if type(json_file) is list: #I have to create a json file with two collections to ensure it works
                for collection in json_file: 
                    # first
                    process_graph(collection, base_url, new_graph)
            #second case: if there is only one collection
            else:
                process_graph(json_file, base_url, new_graph)
            
                    
            #this part of the code handles the upload of the triples in the triplestore
            store = SPARQLUpdateStore()

            endpoint = self.getDbPathOrUrl()

            store.open((endpoint, endpoint))

            for triple in new_graph.triples((None, None, None)):
                store.add(triple)
            store.close()
            
            return True
        
        except Exception as e:
            print(str(e))
            return False

# 3. QueryProcessor: Leonardo you should insert your query here
class QueryProcessor(Processor):
    def __init__(self, dbPathOrUrl: str):
        super().__init__(dbPathOrUrl)
        
    def getEntityById(self, entityId:str) -> DataFrame:
        df = DataFrame()
        # I am not sure about this approach: if you all have other ideas feel free to propose them
        if self.dbPathOrUrl.endswith('.db'):

            #LEONARDO AGGIUNGI LA QUERY
            with connect(self.dbPathOrUrl) as con:
                df = read_sql(query, con)
            #codice finale di ritorno
            #cleaning_dataframe is a function that handles the presence of int64, float64 and N/A in the final dataframe.
            #it can be used in all the methods that returns a dataframe. It is stored in helpers.py
            df_cleaned = cleaning_dataframe(df)
            # return df_cleaned

        #this is the query to get the entity based on the ID in the triplestore 
        #Test passed
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX lz: <https://github.com/leonardozilli/res/>
        PREFIX schema: <https://schema.org/>

        select ?entity ?id ?label ?type
        where {
        ?entity schema:identifier ?id ;
            rdf:type ?type ;
            rdfs:label ?label;
            schema:identifier {} .
        }
        """.format(escape_quotes(entityId))
        df_sparql = get(endpoint, query, True)
        df_sparql_clined = cleaning_dataframe(df_sparql)
        return df_sparql_clined
        

# 4. TriplestoreQueryProcessor
class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self, dbPathOrUrl: str):
        super().__init__(dbPathOrUrl)
    
    #TEST PASSED
    def getAllCanvases(self):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX lz: <https://github.com/leonardozilli/res/>
        PREFIX schema: <https://schema.org/>

        SELECT ?canvas ?id ?label
        WHERE{
        ?canvas rdf:type lz:Canvas;
            rdfs:label ?label;
            schema:identifier ?id .
        }
        """
        df_sparql = get(endpoint, query, True)
        df_sparql_clined = cleaning_dataframe(df_sparql)
        return df_sparql_clined

    # TEST PASSED
    def getAllCollections(self):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdfsyntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX schema: <https://schema.org/>
        PREFIX lz: <https://github.com/leonardozilli/res/>


        SELECT ?canvas ?id ?label
        WHERE{
        ?canvas rdf:type lz:Collection;
            rdfs:label ?label;
            schema:identifier ?id .
        }
        """
        df_sparql = get(endpoint, query, True)
        df_sparql_clined = cleaning_dataframe(df_sparql)
        return df_sparql_clined
    
    #test passed
    def getAllManifests(self):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdfsyntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX schema: <https://schema.org/>
        PREFIX lz: <https://github.com/leonardozilli/res/>


        SELECT ?canvas ?id ?label
        WHERE{
        ?canvas rdf:type lz:Manifest;
            rdfs:label ?label;
            schema:identifier ?id .
        }
        """
        df_sparql = get(endpoint, query, True)
        df_sparql_clined = cleaning_dataframe(df_sparql)
        return df_sparql_clined
    
    #test passed
    def getCanvasesInCollection(self, collectionId:str):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX lz: <https://github.com/leonardozilli/res/>
        PREFIX schema: <https://schema.org/>
        PREFIX owl: <https://www.w3.org/2002/07/owl#>

        SELECT ?collection ?collectionId ?collectionLabel ?canvas ?canvasId ?canvasLabel
        WHERE {
        ?collection rdf:type lz:Collection ;
            schema:identifier {} ;
            schema:identifier ?collectionId ;
            rdfs:label ?collectionLabel ;            
            owl:hasPart ?manifest .
        ?manifest owl:hasPart ?canvas .
        ?canvas schema:identifier ?canvasId ;
            schema:identifier ?canvasId ;
            rdfs:label ?canvasLabel .        
        }
        """.format(collectionId)
        df_sparql = get(endpoint, query, True)
        df_sparql_clined = cleaning_dataframe(df_sparql)
        return df_sparql_clined
    
    #test passed
    def getCanvasesInManifest(self, manifestId:str):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX lz: <https://github.com/leonardozilli/res/>
        PREFIX schema: <https://schema.org/>
        PREFIX owl: <https://www.w3.org/2002/07/owl#>

        SELECT ?manifest ?manifestId ?manifestLabel ?canvas ?canvasId ?canvasLabel
        WHERE{
        ?manifest rdf:type lz:Manifest ;
            schema:identifier {} ;
            schema:identifier ?manifestId ;
            rdfs:label ?manifestLabel ;            
            owl:hasPart ?canvas .
        ?canvas schema:identifier ?canvasId ;
            schema:identifier ?canvasId ;
            rdfs:label ?canvasLabel .        
        }
        """.format(manifestId)
        df_sparql = get(endpoint, query, True)
        df_sparql_clined = cleaning_dataframe(df_sparql)
        return df_sparql_clined
    
    #test passed
    def getEntitiesWithLabel(self, label:str):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?entity ?entityId ?label ?type
        WHERE{
        ?entity rdfs:label ?label .
        ?entity rdfs:label {} .
        ?entity schema:identifier ?entityId .
        ?entity rdf:type ?type
        } 
        """.format(escape_quotes(label)) #escape_quotes is a function I used to ensure that the query will work
        #even if quotes are in the label. You can find it in helpers.py
        df_sparql = get(endpoint, query, True)
        df_sparql_clined = cleaning_dataframe(df_sparql)
        return df_sparql_clined
    
    # test passed
    def getManifestsInCollection(self, collectionId:str):
        endpoint = self.getDbPathOrUrl()
        query ="""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX lz: <https://github.com/leonardozilli/res/>
        PREFIX schema: <https://schema.org/>
        PREFIX owl: <https://www.w3.org/2002/07/owl#>

        SELECT ?collection ?collectionId ?collectionLabel ?manifest ?manifestId ?manifestLabel
        WHERE
        {
        ?collection rdf:type lz:Collection;
            rdfs:label ?collectionLabel;
            schema:identifier ?collectionId ;
            owl:hasPart ?manifest.
        ?manifest rdf:type lz:Manifest ;
            rdfs:label ?manifestLabel;
            schema:identifier ?manifestId
        }
        """.format(collectionId)
        df_sparql = get(endpoint, query, True)
        df_sparql_clined = cleaning_dataframe(df_sparql)
        return df_sparql_clined