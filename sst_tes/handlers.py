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

class SimpleHandler:
    def __init__(self, path, **resource_kwargs):
        self.path = path
        self.shape = resource_kwargs.get("shape", [])

    def __call__(self, *, index, **datum_kwargs):
    
        data = np.loadtxt(self.path)
        #try:
        if self.shape == []:
            return data[index]
        else:
            return data[index, :]
        """
        except IndexError:
            # If the data is not there yet, need to raise IOError so that filler
            # knows to wait and try again
            raise IOError
        """
        return

class SimpleHandler2:
    def __init__(self, path, **resource_kwargs):
        self.path = path
        self.shape = resource_kwargs.get("shape", [])

    def __call__(self, *, index, **datum_kwargs):
    
        data = np.loadtxt(self.path)
        try:
            return np.array(data[index])
        except IndexError:
            # If the data is not there yet, need to raise IOError so that filler
            # knows to wait and try again
            raise IOError
        return
