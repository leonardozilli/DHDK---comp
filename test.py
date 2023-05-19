from RelDBProcessor import *
from DM_classes import *
from sqlite3 import connect
from pprint import pprint

# Once all the classes are imported, first create the relational
# database using the related source data

pd.set_option('display.expand_frame_repr', False)

rel_path = "relational.db"
ann_dp = AnnotationProcessor()
ann_dp.setDbPathOrUrl(rel_path)
ann_dp.uploadData("data/annotations.csv")

met_dp = MetadataProcessor()
met_dp.setDbPathOrUrl(rel_path)
met_dp.uploadData("data/metadata.csv")

rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl(rel_path)

