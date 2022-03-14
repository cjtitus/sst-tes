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
from .tes import TESBase

class TESROIBase(Device, RPCInterface):
    roi_lims = Component(RPCSignalPairAuto, method="roi", kind='config')
    def __init__(self, *args, **kwargs):
        """
        Call with ROI label as first argument
        """
        super().__init__(*args, **kwargs)
        self.label = self.prefix
        self.roi_lims.get_args = [self.label]

    def set(self, llim, ulim, label=None):
        if label is not None:
            self.label = label
        else:
            self.label = self.prefix
        self.roi_lims.get_args = [self.label]
        self.roi_lims.put({self.label: (llim, ulim)})
        if llim is None or ulim is None:
            self.disable()
        else:
            self.enable()

    def enable(self):
        self.kind = Kind.normal

    def disable(self):
        self.kind = Kind.omitted


class TESROIext(TESROIBase):
    roi = Component(ExternalFileReference, shape=[], kind="normal")
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._asset_docs_cache = deque()
        self._data_index = None

    def stage(self):
        print("Staging", self.name)
        if self.kind == Kind.omitted:
            return super().stage()
        else:
            self._data_index = itertools.count()
            root, resource_path = self._get_resource_paths()
            # compose_resource currently needs start argument with placeholder uid, but
            # RunEngine replaces this uid with the real one for the run. In the future,
            # start argument to compose_resource will be optional
            self._resource, self._datum_factory, _ = compose_resource(
                start={"uid": "temporary lie"},
                spec="tes",
                root=root,
                resource_path=resource_path,
                resource_kwargs={"shape":self.roi.shape, "label": self.label},
            )
            self._resource.pop("run_start")
            self._asset_docs_cache.append(("resource", self._resource))
            return super().stage()

    def unstage(self):
        self._data_index = None
        self._resource = None
        self._datum_factory = None
        return super().unstage()

    def trigger(self):
        if self.kind == Kind.omitted:
            return
        else:
            i = next(self._data_index)
            datum = self._datum_factory(datum_kwargs={"index": i})
            self._asset_docs_cache.append(("datum", datum))
            self.roi.put(datum["datum_id"])
            return

    def collect_asset_docs(self):
        items = list(self._asset_docs_cache)
        self._asset_docs_cache.clear()
        for item in items:
            yield item

    def _get_resource_paths(self):
        root = self.rpc.base_user_output_dir()['response']
        resource_full = self.rpc.get_pfy_output_file()['response']
        resource_path = relpath(resource_full, start=root)
        return root, resource_path


class TES(TESBase):
    def read(self):
        d = super().read()
        if self.write_off:
            rois = self.rpc.roi_get_counts()['response']
            for k in self.rois:
                key = self.name + "_" + k
                val = rois[k]
                d[key] = {"value": val, "timestamp": self.last_time}
        return d

    def stage(self):
        if self.verbose: print("Staging TES")
        self._data_index = itertools.count()
        self._completion_status = DeviceStatus(self)
        self._external_devices = [dev for _, dev in self._get_components_of_kind(Kind.normal)
                                  if hasattr(dev, 'collect_asset_docs')]

        if self.file_mode == "start_stop":
            self._file_start()

        if self.state.get() == "no_file":
            self._file_start()
            # raise ValueError(f"{self.name} has no file open, cannot stage.")

        if self.cal_flag.get():
            self._calibration_start()
        else:
            self._scan_start()

        return super().stage()

    def unstage(self):
        if self.verbose: print("Complete acquisition of TES")
        self._scan_end()
        if self.file_mode == "start_stop":
            self._file_end()
        self._log = {}
        self._data_index = None
        self._external_devices = None
        return super().unstage()


class OLDTES(Device, RPCInterface):
    _cal_flag = False
    _acquire_time = 1

    cal_flag = Component(AttributeSignal, '_cal_flag', kind=Kind.config)
    acquire_time = Component(AttributeSignal, '_acquire_time', kind=Kind.config)
    filename = Component(RPCSignal, method="filename", kind=Kind.config)
    calibration = Component(RPCSignal, method='calibration_state', kind=Kind.config)
    state = Component(RPCSignal, method='state', kind=Kind.config)
    scan_num = Component(RPCSignal, method='scan_num', kind=Kind.config)
    """
    tfy = Component(TESROIext, "tfy", kind="normal")
    roi1 = Component(TESROIext, "roi1", kind="omitted")
    roi2 = Component(TESROIext, "roi2", kind="omitted")
    roi3 = Component(TESROIext, "roi3", kind="omitted")
    roi4 = Component(TESROIext, "roi4", kind="omitted")
    roi5 = Component(TESROIext, "roi5", kind="omitted")
    roi6 = Component(TESROIext, "roi6", kind="omitted")
    roi7 = Component(TESROIext, "roi7", kind="omitted")
    roi8 = Component(TESROIext, "roi8", kind="omitted")
    """
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
        
    def read(self):
        d = super().read()
        rois = self.rpc.roi_get_counts()['response']
        for k in self.rois:
            key = self.name + "_" + k
            val = rois[k]
            d[key] = {"value": val, "timestamp": self.last_time}
        return d
    
    
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

    def stage(self):
        if self.verbose: print("Staging TES")
        self._data_index = itertools.count()
        self._completion_status = DeviceStatus(self)
        self._external_devices = [dev for _, dev in self._get_components_of_kind(Kind.normal)
                                  if hasattr(dev, 'collect_asset_docs')]
        
        if self.file_mode == "start_stop":
            self._file_start()

        if self.state.get() == "no_file":
            raise ValueError(f"{self.name} has no file open, cannot stage.")
        
        if self.cal_flag.get():
            self._calibration_start()
        else:
            self._scan_start()

        return super().stage()
    
    def unstage(self):
        if self.verbose: print("Complete acquisition of TES")
        self.rpc.scan_end(_try_post_processing=False)
        if self.file_mode == "start_stop":
            self._file_end()
        self._log = {}
        self._data_index = None
        self._external_devices = None
        self.cal_flag.put(False)
        return super().unstage()
        
    def trigger(self):
        if self.verbose: print("Triggering TES")
        for dev in self._external_devices:
            dev.trigger()

        status = DeviceStatus(self)
        i = next(self._data_index)
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
        for dev in self._external_devices:
            yield from dev.collect_asset_docs()
            
    def stop(self):
        if self._completion_status is not None:
            self._completion_status.set_finished()

class SIMTES(TES):
    def __init__(self, name, motor, motor_field, *args, **kwargs):
        super().__init__(name=name, **kwargs)
        self._motor = motor
        self._motor_field = motor_field

    def _acquire(self, status, i):
        t1 = self._motor.read()[self._motor_field]['value']
        t2 = t1 + self.acquire_time.get()
        self.rpc.scan_point_start(i, t1)
        ttime.sleep(self.acquire_time.get())
        self.rpc.scan_point_end(t2)
        if self._save_roi:
            self.rpc.roi_save_counts()
        status.set_finished()
        return
