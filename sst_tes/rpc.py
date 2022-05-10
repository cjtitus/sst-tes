from ophyd.ophydobj import OphydObject
import json
import socket


class RPCException(Exception):
    pass


class RPCInterface(OphydObject):
    def __init__(self, *args, address="", port=None, **kwargs):
        super().__init__(*args, **kwargs)
        if port is not None:
            self.rpc = JSONClient(address, port)
        else:
            self.rpc = self._get_comm_function()

    def describe_rpc(self):
        return f'RPC:{self.rpc.address}:{self.rpc.port}'
        
    def _get_comm_function(self):
        if hasattr(self, "comm"):
            return self.rpc
        else:
            parent = self.parent
            return self._get_comm_tail(parent)
        
    def _get_comm_tail(self, parent):
        if hasattr(parent, "rpc"):
            return parent.rpc
        elif hasattr(parent, "parent"):
            if parent is None:
                raise IOError("No parent has an RPC Client")
            return self._get_comm_tail(parent.parent)
        else:
            raise IOError("No parent has an RPC Client")


class JSONClient:
    def __init__(self, address, port):
        self.address = address
        self.port = port

    def formatMsg(self, method, *params, **kwargs):
        msg = {"method": method}
        if params is not None and params != []:
            msg["params"] = params
        if kwargs is not None and kwargs != {}:
            msg["kwargs"] = kwargs
        return json.dumps(msg).encode()

    def sendrcv(self, method, *params, **kwargs):
        msg = self.formatMsg(method, *params, **kwargs)
        s = socket.socket()
        s.connect((self.address, self.port))
        s.send(msg)
        m = json.loads(s.recv(1024).decode())
        s.close()
        return m

    def __getattr__(self, attr):
        def _method(*params, **kwargs):
            return self.sendrcv(attr, *params, **kwargs)
        return _method
