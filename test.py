from impl import *
from DM_classes import *
from sqlite3 import connect
from pprint import pprint

# Once all the classes are imported, first create the relational
# database using the related source data

rel_path = "relational.db"
ann_dp = AnnotationProcessor()
ann_dp.setDbPathOrUrl(rel_path)
ann_dp.uploadData("data/annotations.csv")

met_dp = MetadataProcessor()
met_dp.setDbPathOrUrl(rel_path)
met_dp.uploadData("data/metadata.csv")

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://192.168.122.1:9999/blazegraph/sparql"
col_dp = CollectionProcessor()
col_dp.setDbPathOrUrl(grp_endpoint)

# In the next passage, create the query processors for both
# the databases, using the related classes


rel_path = "relational.db"

met_qp = MetadataProcessor()
met_qp.setDbPathOrUrl(rel_path)

rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl(rel_path)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathOrUrl(grp_endpoint)

generic = GenericQueryProcessor()
generic.addQueryProcessor(grp_qp)
generic.addQueryProcessor(rel_qp)

print(grp_qp.getAllCollections())
