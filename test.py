from RelDBProcessor import *
from GraphDBProcessor import *
from GenericQueryProcessor import *
from DM_classes import *
from sqlite3 import connect
from pprint import pprint

# Once all the classes are imported, first create the relational
# database using the related source data

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_colwidth', True)

rel_path = "relational.db"
ann_dp = AnnotationProcessor()
ann_dp.setDbPathOrUrl(rel_path)
#ann_dp.uploadData("data/annotations.csv")


met_dp = MetadataProcessor()
met_dp.setDbPathOrUrl(rel_path)
#met_dp.uploadData("data/metadata.csv")

rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl(rel_path)

grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
col_dp = CollectionProcessor()
col_dp.setDbPathOrUrl(grp_endpoint)
#col_dp.uploadData("data/collection-1.json")
#col_dp.uploadData("data/collection-2.json")

grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathOrUrl(grp_endpoint)


generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)

print(generic.getAllCanvas())
