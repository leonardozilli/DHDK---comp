from DM_classes import *
from RelDBProcessor import *
from GraphDBProcessor import *
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
        rqp_df = pd.concat(tqp_df['id'].apply(self.queryProcessors[0].getEntityById).tolist(), ignore_index=True)
        final_df = pd.merge(tqp_df, rqp_df, on='id', how='left')

        result = [Canvas(identifier, label, title, creator) for identifier, label, title, creator in
                  zip(final_df['id'],
                      final_df['label'],
                      final_df['title'],
                      final_df['creator'])]
        return result

    def getAllCollections(self) -> List[Collection]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getAllCollections()
        rqp_df = pd.concat(tqp_df['id'].apply(self.queryProcessors[0].getEntityById).tolist())
        final_df = pd.merge(tqp_df, rqp_df, on='id', how='left').replace({np.nan:''})

        result = [Collection(identifier, label, title, creator, self.getManifestsInCollection(identifier)) for identifier, label, title, creator in
                  zip(final_df['id'],
                      final_df['label'],
                      final_df['title'],
                      final_df['creator'])]
        return result

    def getAllManifests(self) -> List[Manifest]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getAllManifests()
        rqp_df = pd.concat(tqp_df['id'].apply(self.queryProcessors[0].getEntityById).tolist())
        final_df = pd.merge(tqp_df, rqp_df, on='id', how='left')

        result = [Manifest(identifier, label, title, creator, self.getCanvasesInManifest(identifier)) for identifier, label, title, creator, items in
                  zip(final_df['id'],
                      final_df['label'],
                      final_df['title'],
                      final_df['creator'],
                      final_df['items'])]
        return result

    def getAnnotationsToCanvas(self, canvasId: str) -> List[Annotation]:
        for processor in self.queryProcessors:
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

    def getCanvasesInManifest(self, manifestId: str) -> List[Manifest]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getCanvasesInManifest(manifestId)
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

    def getManifestsInCollection(self, manifestId: str) -> List[Manifest]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getManifestsInCollection(manifestId)
        try:
            rqp_df = pd.concat(tqp_df['id'].apply(self.queryProcessors[0].getEntityById).tolist())
        except ValueError:
            return list()
        final_df = pd.merge(tqp_df, rqp_df, on='id', how='left')

        result = [Manifest(identifier, label, title, creator, self.getCanvasesInManifest(identifier)) for identifier, label, title, creator in
                  zip(final_df['id'],
                      final_df['label'],
                      final_df['title'],
                      final_df['creator'])]
        return result

    def getEntityById(self, entityId: str) -> IdentifiableEntity:
        self.queryProcessors = self.sortProcessors()
        try:
            ann = self.queryProcessors[0].getAllAnnotations()
            if ann['id'].str.contains(entityId).any():
                ann = ann[ann['id'].str.contains(entityId)]
                return Annotation(ann['id'][0], 
                                  ann['body'][0],
                                  self.getEntityById(ann['target'][0]),
                                  ann['motivation'][0])

            elif ann['body'].str.contains(entityId).any():
                ann = ann[ann['body'].str.contains(entityId)]
                return Image(ann['body'][0])

            else:
                tqp_df = self.queryProcessors[1].getEntityById(entityId)
                try:
                    rqp_df = pd.concat(tqp_df['id'].apply(self.queryProcessors[0].getEntityById).tolist())
                    final_df = pd.merge(tqp_df, rqp_df, on='id', how='left')
                except KeyError:
                    title_creator = {'title': [None],
                                     'creator': [None]}
                    final_df =  pd.concat([tqp_df, pd.DataFrame(title_creator)], axis=1) 

                if final_df['type'][0].endswith('Collection'):
                    return Collection(final_df['id'][0],
                                      final_df['label'][0],
                                      ['item1', 'item2'],
                                      final_df['title'][0],
                                      final_df['creator'][0])

                elif final_df['type'][0].endswith('Manifest'):
                    return Manifest(final_df['id'][0],
                                      final_df['label'][0],
                                      ['item1', 'item2'],
                                      final_df['title'][0],
                                      final_df['creator'][0])

                elif final_df['type'][0].endswith('Canvas'):
                    return Canvas(final_df['id'][0],
                                      final_df['label'][0],
                                      final_df['title'][0],
                                      final_df['creator'][0])
        except ValueError:
            return None

    def getEntitiesWithCreator(self, creatorName: str) -> List[EntityWithMetadata]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[0].getEntitiesWithCreator(creatorName)
        try:
            rqp_df = pd.concat(tqp_df['id'].apply(self.queryProcessors[1].getEntityById).tolist())
        except ValueError:
            return list()
        final_df = pd.merge(tqp_df, rqp_df, on='id', how='left')

        result = [EntityWithMetadata(identifier, label, title, creator) for identifier, label, title, creator in
                  zip(final_df['id'],
                      final_df['label'],
                      final_df['title'],
                      final_df['creator'])]
        return result

    def getEntitiesWithLabel(self, label: str) -> List[EntityWithMetadata]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getEntitiesWithLabel(label)
        try:
            rqp_df = pd.concat(tqp_df['id'].apply(self.queryProcessors[0].getEntityById).tolist())
        except ValueError:
            return list()
        final_df = pd.merge(tqp_df, rqp_df, on='id', how='left')

        result = [EntityWithMetadata(identifier, label, title, creator) for identifier, label, title, creator in
                  zip(final_df['id'],
                      final_df['label'],
                      final_df['title'],
                      final_df['creator'])]
        return result

    def getEntitiesWithTitle(self, title: str) -> List[EntityWithMetadata]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[0].getEntitiesWithTitle(title)
        try:
            rqp_df = pd.concat(tqp_df['id'].apply(self.queryProcessors[1].getEntityById).tolist())
        except ValueError:
            return list()
        final_df = pd.merge(tqp_df, rqp_df, on='id', how='left')

        result = [EntityWithMetadata(identifier, label, title, creator) for identifier, label, title, creator in
                  zip(final_df['id'],
                      final_df['label'],
                      final_df['title'],
                      final_df['creator'])]
        return result

    def getImagesAnnotatingCanvas(self, canvasId: str) -> List[Image]:
        for processor in self.queryProcessors:
            if isinstance(processor, RelationalQueryProcessor):
                result = [Image(identifier) for
                          identifier in
                          processor.getAnnotationsWithTarget(canvasId)['body']]
                return result


#maybe handle exceptions when nothing is found??

