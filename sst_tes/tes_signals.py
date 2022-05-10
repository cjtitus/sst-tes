from ophyd.signal import Signal
from ophyd.utils.epics_pvs import data_type, data_shape
from .rpc import RPCInterface
import time as ttime

class RPCSignalPair(Signal, RPCInterface):
    def __init__(self, *args, get_method, set_method, get_args=[], set_args=[],**kwargs):
        """
        A signal to define an RPC get/set pair
        """
        super().__init__(*args, **kwargs)
        self.rpc_get = get_method
        self.rpc_set = set_method
        self.get_args = get_args
        self.set_args = set_args
        
    def get(self, **kwargs):
        r = self.rpc.sendrcv(self.rpc_get, *self.get_args)
        response = r['response']
        success = r['success']
        return response

    def put(self, value, **kwargs):
        if not self.write_access:
            raise ReadOnlyError("RPCSignal is marked as read-only")
        old_value = self.get()
        _ = self.rpc.sendrcv(self.rpc_set, value, *self.set_args)
        self._run_subs(sub_type=self.SUB_VALUE, old_value=old_value,
                       value=value, timestamp=ttime.time())

    def describe(self):
        value = self.get()
        desc = {'source': '{}/{}'.format(self.describe_rpc(), self.rpc_get),
                'dtype': data_type(value),
                'shape': data_shape(value),
                'get_args': self.get_args,
                'set_args': self.set_args}
        return {self.name: desc}

class RPCSignalPairAuto(RPCSignalPair):
    """
    Convenience class for the common case where the 'get' and 'set' method names share a
    common stem, with '_get' and '_set' appended.
    """
    def __init__(self, *args, method, **kwargs):
        get_method = method + '_get'
        set_method = method + '_set'
        super().__init__(*args, get_method=get_method, set_method=set_method, **kwargs)
        
class RPCSignal(RPCSignalPair):
    """
    Convenience class for the common case where the 'get' and 'set' methods are identical,
    and behavior is controlled by whether or not a value is passed in
    """
    def __init__(self, *args, method, **kwargs):
        print("Initializing", method)
        super().__init__(*args, get_method=method, set_method=method, **kwargs)
        
        
class RPCSignalRO(RPCSignal):
    """
    Convenience class for read-only signals
    """
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
