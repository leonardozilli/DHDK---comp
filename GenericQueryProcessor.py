from DM_classes import *
from RelDBProcessor import *
from GraphDBProcessor import *


class GenericQueryProcessor():
    def __init__(self):
        try:
            self.queryProcessors = list()
        except Exception as e:
            print(e)

            return False

    def cleanQueryProcessors(self) -> bool:
        try:
            self.queryProcessors = list()
        except Exception as e:
            print(e)

            return False

    def addQueryProcessor(self, processor: QueryProcessor) -> bool:
        try:
            self.queryProcessors.append(processor)
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
                result = [Annotation(identifier, body, target, motivation) for identifier, body, target, motivation in zip(processor.getAllAnnotations()['id'], processor.getAllAnnotations()['body'], processor.getAllAnnotations()['target'], processor.getAllAnnotations()['motivation'])]
                return result

    def getAllImages(self) -> List[Image]:
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                result = [Image(identifier) for identifier in processor.getAllImages()['id']]
                return result

    def getAllCanvas(self) -> List[Canvas]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getAllCanvases()

        rqp_df = tqp_df['id'].apply(self.queryProcessors[0].getEntityById)



        #resultù = [Canvas(identifier, label, title, creator) for identifier, label in
        #          zip(tqp_df['id'],
        #              tqp_df['label'],
        #              processor.getAllCanvases()['title'],
        #              processor.getAllCanvases()['creator'])]
        return rqp_df

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

