from typing import List

class IdentifiableEntity(object) : 
    def __init__(self, identifier:str):
            self.identifier = identifier
    
    def getId(self) -> str:
        return self.identifier


class Image(IdentifiableEntity):
    def __init__(self, identifier: str):
        super().__init__(identifier)


class Annotation(IdentifiableEntity):
    def __init__(self, identifier: str, motivation:str, target: List, body: List):
        super().__init__(identifier)
        self.motivation = motivation
        self.target = target
        self.body = body

    def getBody(self) -> Image:
        return self.body
    
    def getMotivation(self) -> str:
        return self.motivation
    
    def getTarget(self) -> IdentifiableEntity:
        return self.target


class EntityWithMetadata(IdentifiableEntity):
    def __init__(self, identifier: str, label: str, title : str, creators: str):
        super().__init__(identifier)
        self.label = label
        self.title = title
        self.creators = creators

    def getLabel(self) -> str:
        return self.label

    def getTitle(self):
        return self.title
    
    def getCreators(self):
        if self.creators == None:
            return list()
        else:
            return self.creators.split("; ")


class Collection(EntityWithMetadata):
    def __init__(self, identifier: str, label: str, items: List, title: str , creators: str):
        super().__init__(identifier, label, title, creators)

        self.items = list()
        for item in items:
            self.items.append(item)

    def getItems(self):
        return self.items
    

class Manifest(EntityWithMetadata):
    def __init__(self, identifier: str, label: str, items: List, title: str, creators: str):
        super().__init__(identifier, label, title, creators)

        self.items = list()
        for item in items:
            self.items.append(item)

    def getItems(self):
        return self.items


class Canvas(EntityWithMetadata):
    def __init__(self, identifier: str, label: str, title: str, creators: str):
        super().__init__(identifier, label, title, creators)
