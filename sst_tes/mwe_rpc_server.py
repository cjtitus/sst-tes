import time
import os
from os.path import join
from pathlib import Path
import socket
import json
from inspect import signature
import collections
import textwrap
import shutil




def time_human(t=None):
    if t is None:
        t = time.localtime(time.time())
    return time.strftime("%Y%m%d_%H:%M:%S", t)

def call_method_from_data(data, dispatch, no_traceback_error_types):
    try:
        d = json.loads(data)
    except Exception as e:
        return None, None, None, None, None, f"JSON Parse Exception: {e}"
    _id = d.get("id", -1)
    if "method" not in d.keys():
        return _id, None, None, None, None, f"method key does not exist"
    method_name = d["method"]
    args = d.get('params', [])
    kwargs = d.get('kwargs', {})
    if method_name not in dispatch.keys():
        return _id, method_name, args, kwargs, None, f"Method '{method_name}' does not exit, valid methods are {list(dispatch.keys())}"
    method = dispatch[method_name]
    if not isinstance(args, list):
        return _id, method_name, args, kwargs, None, f"args must be a list, instead it is {args}"

    try:
        result = method(*args, **kwargs)
        return _id, method_name, args, kwargs, result, None
    except Exception as e:
        if isinstance(e, KeyboardInterrupt):
            raise e
        if not any(isinstance(e, et) for et in no_traceback_error_types):
            import traceback
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            s = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print("TRACEBACK")
            print("".join(s))
            print("TRACEBACK DONE")
        return _id, method_name, args, kwargs, None, f"Calling Exception: method={method_name}: {e}"

def make_simple_response(_id, method_name, args, kwargs, result, error):
    if error is not None:
        #response = f"Error: {error}"
        response = json.dumps({"response": error, "success": False})
    else:
        #response = f"{result}"
        response = json.dumps({"response": result, "success": True})
    return response

def get_message(sock):
    try:
        msg = sock.recv(2**12)
        if msg == b'':
            return None
        else:
            return msg
    except ConnectionResetError:
        return None

def handle_one_message(sock, data, dispatch, verbose, no_traceback_error_types):
    # following https://gist.github.com/limingzju/6483619
    t_s = time.time()
    t_struct = time.localtime(t_s)
    t_human = time_human(t_struct)
    if verbose:
        print(f"{t_human}")
        print(f"got: {data}")
    _id, method_name, args, kwargs, result, error = call_method_from_data(data, dispatch, no_traceback_error_types)
    # if verbose:
    #     print(f"id: {_id}, method_name: {method_name}, args: {args}, result: {result}, error: {error}")
    response = make_simple_response(_id, method_name, args, kwargs, result, error).encode()
    if verbose:
        print(f"responded: {response}")
    try:
        n = sock.send(response)
        assert n == len(response), f"only {n} of {len(response)} bytes were sent"
    except BrokenPipeError:
        print("failed to send response")
        pass
    return t_human, data, response

def make_attribute_accessor(x, a):
    def get_set_attr(*args):
        if len(args) == 0:
            return getattr(x, a)
        else:
            old_val = getattr(x, a)
            setattr(x, a, args[0])
            return old_val

    return get_set_attr

def get_dispatch_from(x):
    d = collections.OrderedDict()
    for m in sorted(dir(x)):
        if not m.startswith("_"):
            if callable(getattr(x,m)):
                d[m] = getattr(x, m)
            else:
                d[m] = make_attribute_accessor(x, m)
    return d

def start(address, port, dispatch, verbose, log_file, no_traceback_error_types):
    terminal_size = shutil.get_terminal_size((80, 20)) 
    print(f"TES Scan Server @ {address}:{port}")
    print("Ctrl-C to exit")
    if log_file is not None:
        print(f"Log File: {log_file.name}")
    print("methods:")
    for k, m in dispatch.items():
        wrapped = textwrap.wrap(f"{k}{signature(m)}", width=terminal_size.columns, 
            initial_indent="* ", subsequent_indent="\t" )
        for l in wrapped:
            print(l)
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # bind the socket to a public host, and a well-known port
    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serversocket.bind((address, port))
    # become a server socket
    serversocket.listen(1)
    if log_file is not None:
        log_file.write(f"{dispatch}\n")
    try:
        while True:
            # accept connections from outside
            (clientsocket, address) = serversocket.accept()
            print(f"connection from {address}")
            while True:
                data = get_message(clientsocket)
                if data is None:
                    print(f"data was none, breaking to wait for connection")
                    break
                a = handle_one_message(clientsocket, data, dispatch, verbose, no_traceback_error_types)  
                t_human, data, response = a
                if log_file is not None:
                    log_file.write(f"{t_human}")
                    log_file.write(f"{data}\n")
                    log_file.write(f"{response}\n")
    except KeyboardInterrupt:
        print("\nCtrl-C detected, shutting down")
        if log_file is not None:
            log_file.write(f"Ctrl-C at {time_human()}\n")
        return

class TESSim:
    
    def __init__(self, base_user_output_dir="/tmp"):
        self.base_user_output_dir = base_user_output_dir
        self.scan_num = 1
        self.state = "file_open"
        self._roi = {"tfy": (200, 1600)}
        
    def roi_get(self, key=None):
        if key is None:
            return self._roi
        else:
            return self._roi.get(key, None)

    def roi_set(self, roi_dict):
        """
        must be called before other roi functions
        roi_dict: a dictinary of {name: (lo, hi), ...} energy pairs in eV, each pair specifies a region of interest
        if roi_dict is none, reset ROIs to just tfy
        """ 
        # roi list is a a list of pairs of lo, hi energy pairs
        if roi_dict is None or len(roi_dict) == 0:
            self._roi = {"tfy": (self.tfy_llim, self.tfy_ulim)}
            return
        else:
            keys = list(roi_dict.keys())
            for key in keys:
                (lo_ev, hi_ev) = roi_dict.get(key, (None, None))
                if lo_ev is None or hi_ev is None:
                    self._roi.pop(key, None)
                    roi_dict.pop(key, None)
                else:
                    assert hi_ev > lo_ev
            self._roi.update(roi_dict)
            return

    def roi_save_counts(self):
        roi_counts = {}
        for name, (lo_ev, hi_ev) in self._roi.items():
            counts = np.random.random()*(hi_ev - lo_ev) + lo_ev
            roi_counts[name] = int(counts)
        output_file = self.get_pfy_output_file(make=True)
        roi_names = roi_counts.keys()
        data = np.array([roi_counts[name] for name in roi_names])
        header = " ".join(roi_names)
        if not os.path.isfile(output_file):
            print("ROI Save Counts", header)
            with open(output_file, "w") as f:
                np.savetxt(f, data[np.newaxis, :], header=header)
        else:
            with open(output_file, "a") as f:
                np.savetxt(f, data[np.newaxis, :])
        return roi_counts        
    
    def scan_start(self):
        self.state = "scan"
        
    def scan_end(self):
        self.state = "file_open"
        self.scan_num += 1

    def get_pfy_output_file(self, make=False):
        filename = join(self.base_user_output_dir, "pfy_test", f"scan{self.scan_num}")
        directory = dirname(filename)
        if make:
            Path(directory).mkdir(parents=True, exist_ok=True)
            
        return filename


if __name__ == "__main__":
    address = "localhost"
    port = 4000

    tesserver = TESSim()
    dispatch = get_dispatch_from(tesserver)
    start(address, port, dispatch, True, None, [])
