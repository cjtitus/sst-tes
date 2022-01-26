from ophyd import DeviceStatus, Device, Component, Kind
from ophyd.signal import AttributeSignal, Signal
from ophyd.utils.epics_pvs import data_type, data_shape
import time as ttime
import threading
from queue import Queue, Empty
from collections import OrderedDict
import itertools
import json
from sst_tes.tes import TESBase

class FlyableTES(TESBase):    
    def kickoff(self):
        if self.cal_flag.get():
            self._calibration_start()
        else:
            self._scan_start()
        return super().kickoff()

    def complete(self):
        if self.cal_flag.get():
            self.cal_flag.set(False)
        self._scan_end()
        return super().complete()

    
    def kickoff(self):
        if self.cal_flag.get():
            self._calibration_start()
        else:
            self._scan_start()
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
        if self.cal_flag.get():
            self.cal_flag.set(False)
        self.rpc.scan_end(_try_post_processing=False)

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
    filename = Component(RPCSignal, rpc_method="filename", kind=Kind.config)
    calibration = Component(RPCSignal, rpc_method='calibration_state', kind=Kind.config)
    state = Component(RPCSignal, rpc_method='state', kind=Kind.config)

    def __init__(self, name, address, port, *args, **kwargs):
        self.address = address
        self.port = port
        super().__init__(*args, name=name, **kwargs)

    def _formatMsg(self, method, params):
        msg = {"method": method}
        if params is not None and params != []:
            msg["params"] = params
        return json.dumps(msg).encode()

    """
    def _send(self, method, *params):
        msg = self._formatMsg(method, params)
        s = socket.socket()
        s.connect((self.address, self.port))
        s.send(msg)
        s.close()
    """
    
    def _sendrcv(self, method, *params):
        msg = self._formatMsg(method, params)
        s = socket.socket()
        s.connect((self.address, self.port))
        s.send(msg)
        m = json.loads(s.recv(1024).decode())
        s.close()
        return m

    def _file_start(self, path='/tmp'):
        self._sendrcv("file_start", path)

    def _file_end(self):
        self._sendrcv("file_end")

    def _calibration_start(self):
        var_name = self._log.get('var_name', 'mono')
        var_unit = 'eV'
        scan_num = self._log.get('scan_num', None)
        sample_id = self._log.get('sample_id', 1)
        sample_name = self._log.get('sample_desc', 'sample')
        extra = self._log.get('extra', {})
        routine = 'ssrl_10_1_mix'
        if self.verbose: print(f"start calibration scan {scan_num}")
        self._sendrcv("calibration_start", var_name, var_unit, scan_num, sample_id, sample_name, extra, 'none', routine)

    def _scan_start(self):
        var_name = self._log.get('var_name', 'mono')
        var_unit = 'eV'
        scan_num = self._log.get('scan_num', 0)
        sample_id = self._log.get('sample_id', 1)
        sample_name = self._log.get('sample_desc', 'sample')
        extra = self._log.get('extra', {})
        if self.verbose: print(f"start scan {scan_num}")
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
                if self.verbose: print("_scan timeout")
                pass   
        if self.verbose: print("Exiting _scan thread")
        


