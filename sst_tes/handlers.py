import numpy as np
import time

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
        self.label = resource_kwargs.get("label")
        self.column = None

    def __call__(self, *, index, **datum_kwargs):
        if self.column is None:
            with open(self.path) as f:
                header = f.readline()
            cols = header[1:-1].split()
            self.column = cols.index(self.label)
        data = np.loadtxt(self.path)
        try:
            if len(data.shape) == 1:
                return data[index]
            else:
                return data[index, self.column]
        except IndexError as exc:
            # If the data is not there yet, need to raise IOError so that filler
            # knows to wait and try again
            print("Tried to get data and failed")
            raise IOError from exc

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
