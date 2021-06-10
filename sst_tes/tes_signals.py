from ophyd.signal import Signal
from ophyd.utils.epics_pvs import data_type, data_shape

class RPCSignal(Signal):
    def __init__(self, rpc_method=None, **kwargs):
        self.rpc_method = rpc_method
        super().__init__(**kwargs)

    def get(self, **kwargs):
        r = self.parent._sendrcv(self.rpc_method)
        response = r['response']
        success = r['success']
        return response
            

    def put(self, value, **kwargs):
        if not self.write_access:
            raise ReadOnlyError("RPCSignal is marked as read-only")
        old_value = self.get()
        self.parent._sendrcv(self.rpc_method, value)
        self._run_subs(sub_type=self.SUB_VALUE, old_value=old_value,
                       value=value, timestamp=ttime.time())

    def describe(self):
        value = self.get()
        desc = {'source': 'RPC:{}:{}/{}'.format(self.parent.address, self.parent.port, self.rpc_method),
                'dtype': data_type(value),
                'shape': data_shape(value)}
        return {self.name: desc}
    
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
