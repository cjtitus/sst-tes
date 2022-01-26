from ophyd import DeviceStatus, Device, Component, Kind
from ophyd.signal import AttributeSignal, Signal
import time as ttime
import threading
from queue import Queue, Empty
from collections import OrderedDict, deque
import itertools
from os.path import join, relpath
from .tes_signals import *
from .rpc import RPCInterface
from event_model import compose_resource

class TESBase(Device, RPCInterface):
    _cal_flag = False
    _acquire_time = 1

    cal_flag = Component(AttributeSignal, '_cal_flag', kind=Kind.config)
    acquire_time = Component(AttributeSignal, '_acquire_time', kind=Kind.config)
    filename = Component(RPCSignal, method="filename", kind=Kind.config)
    calibration = Component(RPCSignal, method='calibration_state', kind=Kind.config)
    state = Component(RPCSignal, method='state', kind=Kind.config)
    scan_num = Component(RPCSignal, method='scan_num', kind=Kind.config)

    def __init__(self, name, *args, verbose=False, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        self._hints = {}#{'fields': ['tfy']}
        self._log = {}
        self._completion_status = None
        self._save_roi = False
        self.verbose = verbose
        self.file_mode = "continuous" # Or "continuous"
        self.write_ljh = True
        self.write_off = True
        self.rois = {"tfy": (0, 1200)}
        self.last_time = 0
        
    def _file_start(self, path=None, force=False):
        if self.state.get() == "no_file" or force:
            self.rpc.file_start(path, write_ljh=self.write_ljh, write_off=self.write_off)
        else:
            print("TES already has file open, not forcing!")

    def _file_end(self):
        self.rpc.file_end()

    def _calibration_start(self):
        var_name = "motor"
        var_unit = "index"
        scan_num = self.scan_num.get()
        sample_id = 1
        sample_name = 'cal'
        routine = 'simulated_source'
        if self.verbose: print(f"start calibration scan {scan_num}")
        self.rpc.calibration_start(var_name, var_unit, scan_num, sample_id, sample_name, routine)

    def _scan_start(self):
        var_name = "motor"
        var_unit = "index"
        scan_num = self.scan_num.get()
        sample_id = 1
        sample_name = 'sample'
        if self.verbose: print(f"start scan {scan_num}")
        self.rpc.scan_start(var_name, var_unit, scan_num, sample_id, sample_name)
        
    def _acquire(self, status, i):
        #t1 = ttime.time()
        #t2 = t1 + self.acquire_time.get()
        self.rpc.scan_point_start(i)
        ttime.sleep(self.acquire_time.get())
        self.rpc.scan_point_end()
        self.last_time = ttime.time()
        status.set_finished()
        return

    def set_roi(self, label, llim, ulim):
        self.rois[label] = (llim, ulim)
        self.rpc.roi_set({label: (llim, ulim)})

    def clear_roi(self, label):
        self.rois.pop(label)
        self.rpc.roi_set({label: (None, None)})

    def describe(self):
        d = super().describe()
        for k in self.rois:
            key = self.name + "_" + k
            d[key] = {"dtype": "number", "shape": [], "source": key,
                      "llim": self.rois[k][0], "ulim": self.rois[k][1]}
        return d

    @property
    def hints(self):
        return self._hints

    def trigger(self):
        if self.verbose: print("Triggering TES")
        status = DeviceStatus(self)
        i = next(self._data_index)
        threading.Thread(target=self._acquire, args=(status, i), daemon=True).start()
        return status

    def stop(self):
        if self._completion_status is not None:
            self._completion_status.set_finished()
