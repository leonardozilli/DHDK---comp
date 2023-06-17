from data_model import *
from RelDBProcessor import *
from GraphDBProcessor import *
import numpy as np
#list comprehension indenting
#Annotations targets


class GenericQueryProcessor():
    def __init__(self):
        self.queryProcessors = list()

    def cleanQueryProcessors(self) -> bool:
        try:
            self.queryProcessors = list()

            return True

        except Exception as e:
            print(e)

            return False

    def addQueryProcessor(self, processor: QueryProcessor) -> bool:
        try:
            self.queryProcessors.append(processor)

            return True

        except Exception as e:
            print(e)

            return False

    def sortProcessors(self):
        proc_set = set(self.queryProcessors)
        sorted_processors = sorted([proc for proc in proc_set], key=lambda obj:type(obj).__name__)

        if len(sorted_processors) > 1:
            return sorted_processors
        elif len(sorted_processors) == 1:
            if isinstance(sorted_processors[0], RelationalQueryProcessor):
                raise Exception("Triplestore Query Processor not found! Both processors are required to run this method.")
            elif isinstance(sorted_processors[0], TriplestoreQueryProcessor):
                raise Exception("Relational Query Processor not found! Both processors are required to run this method.")
        else: 
            raise Exception("No processor found")


    def getAllAnnotations(self) -> List[Annotation]:
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                result = [Annotation(identifier, Image(body), IdentifiableEntity(target), motivation) for identifier, body, target, motivation in 
                          zip(processor.getAllAnnotations()['id'], 
                              processor.getAllAnnotations()['body'], 
                              processor.getAllAnnotations()['target'], 
                              processor.getAllAnnotations()['motivation'])]
                return result

    def getAllImages(self) -> List[Image]:
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                result = [Image(identifier) for identifier in processor.getAllImages()['id']]
                return result

    def getAllCanvas(self) -> List[Canvas]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getAllCanvases()
        result = []
        for idx, row in tqp_df.iterrows():
            id = row['id']
            df = self.queryProcessors[0].getEntityById(id)
            newitem = Canvas(id, row['label'], df['title'][0], df['creator'][0])
            result.append(newitem)
        return result

    def getAllManifests(self) -> List[Manifest]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getAllManifests()
        result = []
        for idx, row in tqp_df.iterrows():
            id = row['id']
            df = self.queryProcessors[0].getEntityById(id)
            newitem = Manifest(id, row['type'], self.getCanvasesInManifest(id), df['title'][0], df['creator'][0])
            result.append(newitem)
        return result

    def getAllCollections(self) -> List[Collection]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getAllCollections()
        rqp_df = pd.concat(tqp_df['id'].apply(self.queryProcessors[0].getEntityById).tolist())
        final_df = pd.merge(tqp_df, rqp_df, on='id', how='left').replace({np.nan:''})

        result = [Collection(identifier, type, title, creator, self.getManifestsInCollection(identifier)) for identifier, type, title, creator in
                  zip(final_df['id'],
                      final_df['type'],
                      final_df['title'],
                      final_df['creator'])]
        return result

    def getAnnotationsToCanvas(self, canvasId: str) -> List[Annotation]:
        return self.getAnnotationsToEntityWithMetadata(canvasId, 'Canvas')
    
    def getAnnotationsToManifest(self, manifestId: str) -> List[Annotation]:
        self.queryProcessors = self.sortProcessors()
        return self.getAnnotationsToEntityWithMetadata(manifestId, 'Manifest')
    
    def getAnnotationsToCollection(self, collectionId: str) -> List[Annotation]:
        self.queryProcessors = self.sortProcessors()
        return self.getAnnotationsToEntityWithMetadata(collectionId, 'Collection')

    def getAnnotationsToEntityWithMetadata(self, entityId: str, type: str) -> List[Annotation]:
        self.queryProcessors = self.sortProcessors()
        result =[]
        target = self.queryProcessors[1].getEntityById(entityId)
        if not target.empty:
            target_type = target['type'][0]
            if target_type == type:
                rqp_df = self.queryProcessors[0].getAnnotationsWithTarget(entityId) 
                for idx, row in rqp_df.iterrows():
                    newitem = Annotation(row['id'], row['motivation'], self.getEntityById(row['target']), Image(row['body']))
                    result.append(newitem)
        return result

    #mio
    def getAnnotationsWithBody(self, bodyId: str) -> List[Annotation]:
        for processor in self.queryProcessors:
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
            if isinstance(processor, RelationalQueryProcessor):
                result = [Annotation(identifier, body, target, motivation) for
                          identifier, body, target, motivation in
                          zip(processor.getAnnotationsWithTarget(targetId)['id'],
                              processor.getAnnotationsWithTarget(targetId)['body'],
                              processor.getAnnotationsWithTarget(targetId)['target'],
                              processor.getAnnotationsWithTarget(targetId)['motivation'])]
                return result

    def getCanvasesInCollection(self, collectionId: str) -> List[Canvas]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getCanvasesInCollection(collectionId)
        try:
            rqp_df = pd.concat(tqp_df['id'].apply(self.queryProcessors[0].getEntityById).tolist())
        except ValueError:
            return list()
        final_df = pd.merge(tqp_df, rqp_df, on='id', how='left')

        result = [Canvas(identifier, label, title, creator) for identifier, label, title, creator in
                  zip(final_df['id'],
                      final_df['label'],
                      final_df['title'],
                      final_df['creator'])]
        return result

    def getCanvasesInManifest(self, manifestId: str) -> List[Canvas]:
        result = []
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getCanvasesInManifest(manifestId)
        for idx, row in tqp_df.iterrows():
            id = row['id']
            df =self.queryProcessors[0].getEntityById(id)
            newitem = Canvas(id, row['label'], df['title'][0], df['creator'][0])
            result.append(newitem)
        return result

    def getManifestsInCollection(self, collectionId:str) -> List[Manifest]:
        result = []
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getManifestsInCollection(collectionId)
        for idx, row in tqp_df.iterrows():
            id = row['id']
            df = self.queryProcessors[0].getEntityById(id)
            newitem = Manifest(id, row['label'], self.getCanvasesInManifest(id), df['title'][0], df['creator'][0])
            result.append(newitem)
        return result

    def getEntityById(self, entityId: str) -> IdentifiableEntity:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getEntityById(entityId)
        rqp_df = self.queryProcessors[0].getEntityById(entityId)
        if tqp_df.empty and rqp_df.empty:
            return None
        elif tqp_df.empty:
            if 'body' in rqp_df:
                newitem = Annotation(entityId, rqp_df['motivation'][0], self.getEntityById(rqp_df['target'][0]), Image(rqp_df['body'][0]))
            else:
                newitem = Image(entityId)
            return newitem
        else:
            type = tqp_df['type'][0]
            if type == 'Collection':
                newitem = Collection(entityId, tqp_df['label'][0], self.getManifestsInCollection(entityId), '', '')
            elif type == 'Manifest':
                newitem = Manifest(entityId, tqp_df['label'][0], self.getCanvasesInManifest(entityId), '', '')
            else:
                newitem = Canvas(entityId, tqp_df['label'][0], '', '')
            return newitem

    def getEntitiesWithLabel(self, label: str) -> List[EntityWithMetadata] :
        result = []
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getEntitiesWithLabel(label) 
        for idx, row in tqp_df.iterrows():
            id = row['id'] 
            newitem = self.getEntityById(id)
            result.append(newitem)
        return result
    
    def getEntitiesWithTitle(self, title: str) -> List[EntityWithMetadata] :
        result = []
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[0].getEntitiesWithTitle(title) 
        for idx, row in tqp_df.iterrows():
            id = row['id'] 
            newitem = self.getEntityById(id)
            result.append(newitem)
        return result
    
    def getEntitiesWithCreator(self, creatorName: str) ->List[EntityWithMetadata]:
        result = []
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[0].getEntitiesWithCreator(creatorName) 
        for idx, row in tqp_df.iterrows():
            id = row['id'] 
            newitem = self.getEntityById(id)
            result.append(newitem)
        return result

    def getImagesAnnotatingCanvas(self, canvasId: str) -> List[Image]:
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                result = [Image(identifier) for
                          identifier in
                          processor.getAnnotationsWithTarget(canvasId)['body']]
                return result
