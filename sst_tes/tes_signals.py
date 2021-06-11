from ophyd.signal import Signal
from ophyd.utils.epics_pvs import data_type, data_shape

class RPCSignal(Signal):
    def __init__(self, rpc_method, **kwargs):
        super().__init__(**kwargs)
        self.rpc_get = rpc_method
        self.rpc_set = rpc_method
        self.rpc = self._get_comm_function()

    def _get_comm_function(self, parent=None):
        if parent is None:
            parent = self.parent
        if hasattr(parent, "rpc"):
            return parent.rpc
        elif hasattr(parent, "parent"):
            return self._get_comm_function(parent=parent.parent)
        else:
            raise IOError
        
    def get(self, **kwargs):
        r = self.rpc.sendrcv(self.rpc_get)
        response = r['response']
        success = r['success']
        return response
            
    def put(self, value, **kwargs):
        if not self.write_access:
            raise ReadOnlyError("RPCSignal is marked as read-only")
        old_value = self.get()
        self.rpc.sendrcv(self.rpc_set, value)
        self._run_subs(sub_type=self.SUB_VALUE, old_value=old_value,
                       value=value, timestamp=ttime.time())

    def describe(self):
        value = self.get()
        desc = {'source': 'RPC:{}:{}/{}'.format(self.rpc.address, self.rpc.port, self.rpc_get),
                'dtype': data_type(value),
                'shape': data_shape(value)}
        return {self.name: desc}

class RPCSignalPair(RPCSignal):
    def __init__(self, rpc_method, get_args=None, get_kwargs=None, **kwargs):
        super().__init__(rpc_method, **kwargs)
        self.rpc_get = rpc_method + '_get'
        self.rpc_set = rpc_method + '_set'
        self.get_args = get_args
        self.get_kwargs = get_kwargs
        
    def get(self, **kwargs):
        r = self.rpc.sendrcv(self.rpc_get, *self.get_args, **self.get_kwargs)

    def describe(self):
        desc = super().describe()
        if self.get_args is not None:
            desc['args': self.get_args]
        if self.get_kwargs is not None:
            desc['kwargs': self.get_kwargs]
        return desc
        
class RPCSignalRO(RPCSignal):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._metadata.update(write_access=False)

    def put(self, value, **kwargs):
        raise ReadOnlyError("The signal {} is readonly".format(self.name))

    def set(self, value, **kwargs):
        raise ReadOnlyError("The signal {} is readonly".format(self.name))

class ExternalFileReference(Signal):
    """
    A pure software signal where a Device can stash a datum_id
    """
    def __init__(self, *args, shape, **kwargs):
        super().__init__(*args, **kwargs)
        self.shape = shape

    def describe(self):
        res = super().describe()
        if self.shape == []:
            dtype = 'array'
        else:
            dtype = 'array'
        res[self.name].update(
            dict(
                external="FILESTORE:",
                dtype=dtype,
                shape=self.shape
            )
        )
        return res
