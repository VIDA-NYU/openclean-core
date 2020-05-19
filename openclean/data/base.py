class Descriptor(object):
    def __init__(self, identifier, name=None, description=None):
        self.identifier = identifier
        self.name = name if name is not None else identifier
        self.description = description if description is not None else ''
