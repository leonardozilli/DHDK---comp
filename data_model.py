class IdentifiableEntity(object) : 
    def __init__(self, identifier:str):
            self.identifier = identifier
    
#METHODS
    def getId(self) -> str:
        return self.identifier


class Image(IdentifiableEntity):
    def __init__(self, identifier: str):
        super().__init__(identifier)


class Annotation(IdentifiableEntity):
    def __init__(self, identifier: str, motivation:str, target, body):
        super().__init__(identifier)
        self.motivation = motivation
        
        self.target = target

        self.body = body
    
#METHODS
    def getBody(self) -> Image:
        return self.body
    
    def getMotivation(self) -> str:
        return self.motivation
    
    def getTarget(self) -> IdentifiableEntity:
        return self.target

   
class EntityWithMetadata(IdentifiableEntity):
    def __init__(self, identifier: str, label: str, title : str=None, creators: str=None):
        super().__init__(identifier)

        self.label = label

        self.title = title

        self.creators = list()
        for creator in creators:
            self.creators.append(creator)

#METHODS
    def getLabel(self) -> str:
        return self.label

    def getTitle(self):
        return self.title
    
    def getCreators(self):
        return self.creators


class Collection(EntityWithMetadata):
    def __init__(self, identifier: str, label: str, items, title: str = None, creators: str = None):
        super().__init__(identifier, label, title, creators)

        self.items = list()
        for item in items:
            self.items.append(item)

    def getItems(self):
        return self.items
    

class Manifest(EntityWithMetadata):
    def __init__(self, identifier: str, label: str, items, title: str = None, creators: str = None):
        super().__init__(identifier, label, title, creators)

        self.items = list()
        for item in items:
            self.items.append(item)

    def getItems(self):
        return self.items


class Canvas(EntityWithMetadata):
    def __init__(self, identifier: str, label: str, title: str = None, creators: str = None):
        super().__init__(identifier, label, title, creators)
    
g = IdentifiableEntity(6)   



        
             



        

    

        

