import numpy as np

class FakeHandler:
    def __init__(self, path, **resource_kwargs):
        self.path = path
        self.shape = resource_kwargs.get("shape", [])


    def __call__(self, *, index, **datum_kwargs):
        if self.shape == []:
            return index
        else:
            return index*np.ones(self.shape)
