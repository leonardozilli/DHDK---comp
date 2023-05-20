from typing import List

class IdentifiableEntity():
    def __init__(self, identifier: str):
        self.identifier = identifier

    def getId(self) -> str:
        return self.identifier


class Annotation(IdentifiableEntity):
    def __init__(self, identifier: str, body, target, motivation: str):
        super().__init__(identifier)
        self.motivation = motivation
        self.body = body
        self.target = target

    def getMotivation(self) -> str:
        return self.motivation

    def getBody(self):
        return self.body

    def getTarget(self):
        return self.target


class Image(IdentifiableEntity):
    def __init__(self, identifier):
        super().__init__(identifier)


class EntityWithMetadata(IdentifiableEntity):
    #???
    def __init__(self, identifier, label: str, title: str=None, creators: str=None):
        super().__init__(identifier)
        self.label = label
        self.title = title
        self.creators = creators

    def getLabel(self) -> str:
        return self.label
    
    def getTitle(self):
        return self.title

    def getCreators(self) -> List[str]:
        return self.creators


class Collection(EntityWithMetadata):
    def __init__(self, identifier, label, title, creators, items):
        super().__init__(identifier, label, title, creators)
        self.items = items

    def getItems(self):
        return self.items


class Manifest(EntityWithMetadata):
    def __init__(self, identifier, label, title, creators, items):
        super().__init__(identifier, label, title, creators)
        self.items = list()

    def getItems(self):
        return self.items


class Canvas(EntityWithMetadata):
    def __init__(self, identifier, label, title, creators):
        super().__init__(label, title, creators)

