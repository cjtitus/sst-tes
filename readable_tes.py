from ophyd import DeviceStatus, Device, Component, Kind
from ophyd.signal import AttributeSignal, Signal
import time as ttime
import threading
from queue import Queue, Empty
from collections import OrderedDict, deque
import itertools
import json
import socket
from os.path import join
from tes_signals import RPCSignal, RPCSignalRO, ExternalFileReference
from event_model import compose_resource

class TES(Device):
    _cal_flag = False
    _acquire_time = 1
    cal_flag = Component(AttributeSignal, '_cal_flag', kind=Kind.config)
    acquire_time = Component(AttributeSignal, '_acquire_time', kind=Kind.config)
    filename = Component(RPCSignal, rpc_method="filename", kind=Kind.config)
    calibration = Component(RPCSignal, rpc_method='calibration_state', kind=Kind.config)
    state = Component(RPCSignal, rpc_method='state', kind=Kind.config)
    spectrum = Component(ExternalFileReference, shape=[], kind="normal")
    scan_num = Component(RPCSignal, rpc_method='scan_num', kind=Kind.config)
    def __init__(self, name, address, port, *args, verbose=False, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        self.address = address
        self.port = port
        self._hints = {}#{'fields': ['tfy']}
        self._log = {}
        self._completion_status = None
        self.verbose = verbose
        self._asset_docs_cache = deque()
        
    def _formatMsg(self, method, params):
        msg = {"method": method}
        if params is not None and params != []:
            msg["params"] = params
        return json.dumps(msg).encode()

    def _sendrcv(self, method, *params):
        msg = self._formatMsg(method, params)
        s = socket.socket()
        s.connect((self.address, self.port))
        s.send(msg)
        m = json.loads(s.recv(1024).decode())
        s.close()
        return m

    def _file_start(self, path='/tmp', force=False):
        if self.state.get() == "no_file" or force:
            self._sendrcv("file_start", path)
        else:
            print("TES already has file open, not forcing!")

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

    @property
    def hints(self):
        return self._hints

    def stage(self):
        if self.verbose: print("Staging TES")
        root = self._sendrcv("base_user_output_dir")['response']
        beamtime_id = "beamtime_{}".format(self._sendrcv("beamtime_id")['response'])
        scan_number = "scan_{}".format(self.scan_num.get())
        resource_path = join(beamtime_id, "pfy", scan_number)
        self._completion_status = DeviceStatus(self)
        self._data_index = itertools.count()
        # compose_resource currently needs start argument with placeholder uid, but
        # RunEngine replaces this uid with the real one for the run. In the future,
        # start argument to compose_resource will be optional
        self._resource, self._datum_factory, _ = compose_resource(
            start={"uid": "temporary lie"},
            spec="tes",
            root=root,
            resource_path=resource_path,
            resource_kwargs={},
        )
        self._resource.pop("run_start")
        self._asset_docs_cache.append(("resource", self._resource))
        if self.cal_flag.get():
            self._calibration_start()
        else:
            self._scan_start()

        return super().stage()
    
    def unstage(self):
        if self.verbose: print("Complete acquisition of TES")
        self._data_index = None
        self._log = {}
        self._resource = None
        self._datum_factory = None
        return super().unstage()
        
    def trigger(self):
        if self.verbose: print("Triggering TES")
        i = next(self._data_index)
        datum = self._datum_factory(datum_kwargs={"index": i})
        self._asset_docs_cache.append(("datum", datum))
        self.spectrum.put(datum["datum_id"])
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

    def collect_asset_docs(self):
        items = list(self._asset_docs_cache)
        self._asset_docs_cache.clear()
        for item in items:
            yield item
            
    def stop(self):
        if self._completion_status is not None:
            self._completion_status.set_finished()
            
