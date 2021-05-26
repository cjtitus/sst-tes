from ophyd import DeviceStatus, Device, Component
from ophyd.signal import AttributeSignal, Signal
import time as ttime
import threading
from queue import Queue, Empty
from collections import OrderedDict
import itertools
import json
import socket

class RCPSignal(Signal):
    def __init__(self, rpc_method, name=None, parent=None, write_access=True):
        self.rpc_method = rpc_method
        self.write_access = write_access
        super().__init__(name=name, parent=parent)

    def get(self, **kwargs):
        return self.parent._sendrcv(self.rpc_method)

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

class BaseFlyableTES(Device):
    _mode = "uncalibrated"
    _acquire_time = 1
    mode = Component(AttributeSignal, '_mode', kind='config')
    acquire_time = Component(AttributeSignal, '_acquire_time')
    
    def __init__(self, *args, verbose=False, **kwargs):
        self._hints = {'fields': ['tfy']}
        self._log = {}
        self._completion_status = None
        self.verbose = verbose
        super().__init__(*args, **kwargs)
        
    @property
    def hints(self):
        return self._hints

    def kickoff(self):
        if self.verbose: print("Kicking off TES")
        self._data_index = itertools.count()
        if self._completion_status is not None and not self._completion_status.done:
            raise RuntimeError("Kicking off a second time?!")
        self._completion_status = DeviceStatus(device=self)
        self._collection_status = DeviceStatus(device=self)
        self._data = Queue()
        self._instructions = Queue()
        def flyer_worker():
            if self.verbose: print("Started flyer worker")
            self._scan()
        threading.Thread(target=flyer_worker, daemon=True).start()
        kickoff_st = DeviceStatus(device=self)
        kickoff_st.set_finished()
        return kickoff_st

    def complete(self):
        if self.verbose: print("Complete acquisition of TES")
        self._data_index = None
        if self._completion_status is None:
            raise RuntimeError("No collection in progress")
        self._completion_status.set_finished()
        self._log = {}
        return self._completion_status

    def collect(self):
        if self.verbose: print("Collecting TES")
        t = ttime.time()
        self._instructions.put(t)
        data = []
        if self._completion_status.done:
            if self.verbose: print("Joining Queue")
            self._instructions.join()
            self._collection_status.set_finished()

        while True:
            try:
                e = self._data.get_nowait()
                data.append(e)
            except Empty:
                if self.verbose: print("No more data in the queue")
                break
        yield from data
        
    def describe_collect(self):
        if self.verbose: print("Describe collect for flyable TES")
        dd = OrderedDict({'tfy': {'source': 'TES_Detector', 'dtype': 'number', 'shape': []}})
        return {self.name: dd}
        
    def trigger(self):
        if self.verbose: print("Triggering flyable TES")
        i = next(self._data_index)
        status = DeviceStatus(self)
        threading.Thread(target=self._acquire, args=(status, i), daemon=True).start()
        return status
        
    def start_log(self, doc_name, document):
        #if self.name in document.get('detectors', []):
        if self.verbose: print(f"Found {self.name} in start")
        motor = document.get('motors', None)[0]
        sample = document.get('sample', 'sample')
        sample_id = document.get('sample_id', '0')
        uid = document.get('uid', None)
        scan_id = document.get('scan_id')
        print(motor, scan_id, sample_id, sample, uid)
        self._log = {'var_name': motor, 'scan_num': scan_id, 'sample_id': sample_id, 'sample_desc': sample, 'extra': {'uid': uid}}
        return

class SimFlyableTES(BaseFlyableTES):

    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, name=name, **kwargs)

    def _scan(self):
        while not self._collection_status.done:
            try:
                t = self._instructions.get(timeout=5)
                event = dict()
                event['time'] = ttime.time()
                event['data'] = dict()
                event['timestamps'] = dict()
                event['data']['tfy'] = 1
                event['timestamps']['tfy'] = t
                self._data.put(event)
                self._instructions.task_done()
            except Empty:
                print("_scan timeout")
                pass   

    def _acquire(self, status, i):
        if self.verbose: print("Triggering TES")
        ttime.sleep(self.acquire_time.get())
        if self.verbose: print("Done Triggering")
        status.set_finished()
        
class FlyableTES(BaseFlyableTES):
    #calibrating = Component(Signal, kind='config', value=False)
    def __init__(self, name, address, port, *args, **kwargs):
        self.address = address
        self.port = port
        super().__init__(*args, name=name, **kwargs)

    def _formatMsg(self, method, params):
        msg = {"method": method}
        if params is not None and params != []:
            msg["params"] = params
        return json.dumps(msg).encode()

    def _send(self, method, *params):
        msg = self._formatMsg(method, params)
        s = socket.socket()
        s.connect((self.address, self.port))
        s.send(msg)
        s.close()

    def _sendrcv(self, method, *params):
        msg = self._formatMsg(method, params)
        s = socket.socket()
        s.connect((self.address, self.port))
        s.send(msg)
        m = s.recv(1024).decode()
        s.close()
        return m

    def _file_start(self, path='/tmp'):
        self._send("file_start", path)

    def _file_end(self):
        self._send("file_end")

    def _calibration_start(self):
        if self.verbose: print(f"start calibration scan {scan_num}")
        var_name = self._log.get('var_name', 'mono')
        var_unit = 'eV'
        scan_num = self._log.get('scan_num', None)
        sample_id = self._log.get('sample_id', 1)
        sample_name = self._log.get('sample_desc', 'sample')
        extra = self._log.get('extra', {})
        routine = 'ssrl_10_1_mix'
        self._sendrcv("calibration_start", var_name, var_unit, scan_num, sample_id, sample_name, extra, 'none', routine)

    def _scan_start(self):
        if self.verbose: print(f"start scan {scan_num}")
        var_name = self._log.get('var_name', 'mono')
        var_unit = 'eV'
        scan_num = self._log.get('scan_num', 0)
        sample_id = self._log.get('sample_id', 1)
        sample_name = self._log.get('sample_desc', 'sample')
        extra = self._log.get('extra', {})
        self._sendrcv("scan_start", var_name, var_unit, scan_num, sample_id, sample_name, extra, 'none')        

    def _scan_point_start(self, var_val, t, extra={}):
        self._sendrcv("scan_point_start", var_val, extra, t)

    def _scan_point_end(self, t):
        self._sendrcv("scan_point_end", t)

    def _scan_end(self, try_post_processing=False):
        self._sendrcv("scan_end", try_post_processing)

    def _acquire(self, status, i):
        t1 = ttime.time()
        t2 = t1 + self.acquire_time.get()
        self._scan_point_start(i, t1)
        ttime.sleep(self.acquire_time.get())
        self._scan_point_end(t2)
        status.set_finished()
        return

    def _scan(self):
        while not self._collection_status.done:
            try:
                t = self._instructions.get(timeout=5)
                event = dict()
                event['time'] = ttime.time()
                event['data'] = dict()
                event['timestamps'] = dict()
                event['data']['tfy'] = 1
                event['timestamps']['tfy'] = t
                self._data.put(event)
                self._instructions.task_done()
            except Empty:
                print("_scan timeout")
                pass   
        print("Exiting _scan thread")
        
    def kickoff(self):
        if self.mode.get() != 'calibrating':
            self._scan_start()
        else:
            self._calibration_start()
        return super().kickoff()

    def complete(self):
        if self.mode.get() == 'calibrating':
            self.mode.set('calibrated')
        self._scan_end()
        return super().complete()

