from data_model import *
from RelDBProcessor import *
from GraphDBProcessor import *

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
        processor = self.sortProcessors()[0]
        df = processor.getAllAnnotations()
        df['target'] = df['target'].apply(self.getEntityById)
        result = [Annotation(identifier, motivation, target, Image(body)) 
                  for identifier, motivation, target, body
                  in zip(df['id'], 
                         df['motivation'], 
                         df['target'], 
                         df['body'])]
        return result

    def getAllImages(self) -> List[Image]:
        processor = self.sortProcessors()[0]
        result = [Image(identifier) for identifier in processor.getAllImages()['id']]
        return result

    def getAllCanvas(self) -> List[Canvas]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getAllCanvases()
        result = []
        for idx, row in tqp_df.iterrows():
            id = row['id']
            df = self.queryProcessors[0].getEntityById(id)
            if not df.empty:
                newitem = Canvas(id, row['label'], df['title'][0], df['creator'][0])
                result.append(newitem)
            else:
                newitem = Canvas(id, row['label'], '', '')
                result.append(newitem)
        return result

    def getAllManifests(self) -> List[Manifest]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getAllManifests()
        result = []
        for idx, row in tqp_df.iterrows():
            id = row['id']
            df = self.queryProcessors[0].getEntityById(id)
            if not df.empty:
                newitem = Manifest(id, row['label'], self.getCanvasesInManifest(id), df['title'][0], df['creator'][0])
                result.append(newitem)
            else:
                newitem = Manifest(id, row['label'], self.getCanvasesInManifest(id), '', '')
                result.append(newitem)
        return result

    def getAllCollections(self) -> List[Collection]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getAllCollections()
        try:
            rqp_df = pd.concat(tqp_df['id'].apply(self.queryProcessors[0].getEntityById).tolist())
        except ValueError:
            return list()
        final_df = pd.merge(tqp_df, rqp_df, on='id', how='left').replace({float("nan") : ''})

        result = [Collection(identifier, label, self.getManifestsInCollection(identifier), title, creator)
                  for identifier, label, title, creator 
                  in zip(final_df['id'],
                         final_df['label'],
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

    def getAnnotationsWithBody(self, bodyId: str) -> List[Annotation]:
        processor = self.sortProcessors()[0]
        df = processor.getAnnotationsWithBody(bodyId)
        result = [Annotation(identifier, motivation, self.getEntityById(target), Image(body)) 
                  for identifier, motivation, target, body 
                  in zip(df['id'],
                         df['motivation'],
                         df['target'],
                         df['body'])]
        return result

    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId: str) -> List[Annotation]:
        processor = self.sortProcessors()[0]
        df = processor.getAnnotationsWithBodyAndTarget(bodyId, targetId)
        result = [Annotation(identifier, motivation, self.getEntityById(target), Image(body)) 
                  for identifier, motivation, target, body 
                  in zip(df['id'],
                         df['motivation'],
                         df['target'],
                         df['body'])]
        return result

    def getAnnotationsWithTarget(self, targetId: str) -> List[Annotation]:
        processor = self.sortProcessors()[0]
        df = processor.getAnnotationsWithTarget(targetId)
        result = [Annotation(identifier, motivation, self.getEntityById(target), Image(body)) 
                  for identifier, motivation, target, body 
                  in zip(df['id'],
                         df['motivation'],
                         df['target'],
                         df['body'])]
        return result

    def getCanvasesInCollection(self, collectionId: str) -> List[Canvas]:
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getCanvasesInCollection(collectionId)
        try:
            rqp_df = pd.concat(tqp_df['id'].apply(self.queryProcessors[0].getEntityById).tolist())
        except ValueError:
            return list()
        final_df = pd.merge(tqp_df, rqp_df, on='id', how='left').replace({float("nan") : ''})

        result = [Canvas(identifier, label, title, creator) 
                  for identifier, label, title, creator 
                  in zip(final_df['id'],
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
            if not df.empty:
                newitem = Canvas(id, row['label'], df['title'][0], df['creator'][0])
                result.append(newitem)
            else:
                newitem = Canvas(id, row['label'], '', '')
                result.append(newitem)
        return result

    def getManifestsInCollection(self, collectionId:str) -> List[Manifest]:
        result = []
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[1].getManifestsInCollection(collectionId)
        for idx, row in tqp_df.iterrows():
            id = row['id']
            df = self.queryProcessors[0].getEntityById(id)
            if not df.empty:
                newitem = Manifest(id, row['label'], self.getCanvasesInManifest(id), df['title'][0], df['creator'][0])
                result.append(newitem)
            else:
                newitem = Manifest(id, row['label'], self.getCanvasesInManifest(id), '', '')
                result.append(newitem)
        return result

    def getEntityById(self, entityId: str) -> IdentifiableEntity:
        self.queryProcessors = self.sortProcessors()
        rqp_df = self.queryProcessors[0].getEntityById(entityId)
        tqp_df = self.queryProcessors[1].getEntityById(entityId)

        if tqp_df.empty and rqp_df.empty:
            return None

        elif tqp_df.empty:
            if 'body' in rqp_df:
                return Annotation(rqp_df['id'][0], 
                                  rqp_df['motivation'][0],
                                  self.getEntityById(rqp_df['target'][0]),
                                  Image(rqp_df['body'][0]))
            else:
                return Image(rqp_df['id'][0])

        else:
            try:
                final_df = pd.merge(tqp_df, rqp_df, on='id', how='left')
            except KeyError:
                title_creator = {'title': [''],
                                 'creator': ['']}
                final_df =  pd.concat([tqp_df, pd.DataFrame(title_creator)], axis=1) 

            if final_df['type'][0] == 'Collection':
                return Collection(final_df['id'][0],
                                  final_df['label'][0],
                                  self.getManifestsInCollection(entityId),
                                  final_df['title'][0],
                                  final_df['creator'][0])

            elif final_df['type'][0] == 'Manifest':
                return Manifest(final_df['id'][0],
                                final_df['label'][0],
                                self.getCanvasesInManifest(entityId),
                                final_df['title'][0],
                                final_df['creator'][0])

            elif final_df['type'][0] == 'Canvas':
                return Canvas(final_df['id'][0],
                              final_df['label'][0],
                              final_df['title'][0],
                              final_df['creator'][0])

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
    
    def getEntitiesWithCreator(self, creatorName: str) -> List[EntityWithMetadata]:
        result = []
        self.queryProcessors = self.sortProcessors()
        tqp_df = self.queryProcessors[0].getEntitiesWithCreator(creatorName) 
        for idx, row in tqp_df.iterrows():
            id = row['id'] 
            newitem = self.getEntityById(id)
            result.append(newitem)
        return result

    def getImagesAnnotatingCanvas(self, canvasId: str) -> List[Image]:
        processor = self.sortProcessors()[0]
        result = [Image(identifier) 
                  for identifier 
                  in processor.getAnnotationsWithTarget(canvasId)['body']]
        return result
