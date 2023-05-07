#this file contains the processor that starting from a dictionary create a graph
#collection_counter.txt, manifest_counter.txt, canvas_counter.txt are three .txt files i have created to 
#assign to every entity in our graph a different internal id (the id is defined as <type of the entity>_<number present in counter>)
from data_model import *
from rdflib import Graph, URIRef, RDF, RDFS, Literal #RDF.type; RDFS.label


def process_graph(json_file:dict, base_url, new_graph:Graph):
    
    with open('collection_counter.txt', 'r', encoding='utf-8') as g:
        collection_counter = g.readline()

    with open('manifest_counter.txt', 'r', encoding='utf-8') as h:
        manifest_counter = h.readline()

    with open('canvas_counter.txt', 'r', encoding='utf-8') as i:
        canvas_counter = i.readline()

    #trackers: allows to create the internal ids and to be sure that each is different from those already existing
    collection_counter_tracker = int(collection_counter)
    manifest_counter_tracker = int(manifest_counter)
    canvas_counter_tracker = int(canvas_counter)

    #types uris
    collection_type = URIRef(base_url + 'Collection') 
    manifest_type = URIRef(base_url + 'Manifest') 
    canvas_type = URIRef(base_url + 'Canvas') 

    #properties
    has_id = URIRef("https://schema.org/identifier")
    has_part = URIRef("https://www.w3.org/2002/07/owl#hasPart")
    #creation of collection
    # for colleciton in json_file:

    #UNDERSTAND WHAT IF A FILE CONTAINS MORE COLLECTIONS

    #define internal id
    collection_counter_tracker += 1
    collection_internal_id = URIRef(base_url + (json_file['type']+'_'+str(collection_counter_tracker)))

    #add id and type
    new_graph.add((collection_internal_id, has_id, Literal(json_file['id'])))
    new_graph.add((collection_internal_id, RDF.type, collection_type))

    #add label accessing the 'none' value
    collection_label_key:dict = json_file['label']
    collection_label_value = list(collection_label_key.keys())[0]
    new_graph.add((collection_internal_id, RDFS.label, Literal(json_file['label'][collection_label_value][0])))

    #working on items
    for manifest in json_file['items']: #json_file['items'] è una lista di dizionari

        #creating manifest internal ids
        manifest_counter_tracker += 1
        manifest_internal_id = URIRef(base_url + (manifest['type']+'_'+str(manifest_counter_tracker)))
        
        #add the has part to the previous part
        new_graph.add((collection_internal_id, has_part, manifest_internal_id))
        
        #adding id and type
        new_graph.add((manifest_internal_id, has_id, Literal(manifest['id'])))
        new_graph.add((manifest_internal_id, RDF.type, manifest_type))
        
        #adding label avoiding none
        manifest_label_key:dict = manifest['label']
        manifest_label_value = list(manifest_label_key.keys())[0]
        new_graph.add((manifest_internal_id, RDFS.label, Literal(manifest['label'][manifest_label_value][0])))

        #working on items
        for canvas in manifest['items']: #canvas sono dict
            #creating manifest internal ids
            canvas_counter_tracker += 1
            canvas_internal_id = URIRef(base_url + (canvas['type']+'_'+str(canvas_counter_tracker)))

            #add the has part to the previous part
            new_graph.add((manifest_internal_id, has_part, canvas_internal_id))

            #adding id and type
            new_graph.add((canvas_internal_id, has_id, Literal(canvas['id'])))
            new_graph.add((canvas_internal_id, RDF.type, canvas_type))

            #adding label avoiding none
            canvas_label_key:dict = canvas['label']
            canvas_label_value = list(canvas_label_key.keys())[0]
            new_graph.add((canvas_internal_id, RDFS.label, Literal(canvas['label'][canvas_label_value][0])))

    #upload counters
    with open('collection_counter.txt', 'w') as g:
        g.write(str(collection_counter_tracker))

    with open('manifest_counter.txt', 'w') as h:
        h.write(str(manifest_counter_tracker))

    with open('canvas_counter.txt', 'w') as i:
        i.write(str(canvas_counter_tracker))