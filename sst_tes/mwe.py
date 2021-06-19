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

class MWEROI(Device, RPCInterface):

    roi = Component(ExternalFileReference, shape=[], kind="normal")
    roi_lims = Component(RPCSignalPairAuto, method="roi", kind='config')
    def __init__(self, *args, **kwargs):
        """
        Call with ROI label as first argument
        """
        super().__init__(*args, **kwargs)
        self._asset_docs_cache = deque()
        self._data_index = None
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

class MWETES(Device, RPCInterface):
    _acquire_time = 1
    acquire_time = Component(AttributeSignal, '_acquire_time', kind=Kind.config)
    tfy = Component(MWEROI, "tfy", kind="normal")
    roi1 = Component(MWEROI, "roi1", kind="omitted")
    scan_num = Component(RPCSignal, method='scan_num', kind=Kind.config)

    def __init__(self, name, *args, verbose=False, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        self._hints = {}#{'fields': ['tfy']}
        self._log = {}
        self._completion_status = None
        self._save_roi = False
        self.verbose = verbose

    def _acquire(self, status, i):
        ttime.sleep(self.acquire_time.get())
        self.rpc.roi_save_counts()
        status.set_finished()
        return

    def stage(self):
        if self.verbose: print("Staging TES")
        self._data_index = itertools.count()
        self._completion_status = DeviceStatus(self)
        self._external_devices = [dev for _, dev in self._get_components_of_kind(Kind.normal)
                                  if hasattr(dev, 'collect_asset_docs')]
        self.rpc.scan_start()

        return super().stage()
    
    def unstage(self):
        if self.verbose: print("Complete acquisition of TES")
        self.rpc.scan_end()
        self._log = {}
        self._data_index = None
        self._external_devices = None
        return super().unstage()
        
    def trigger(self):
        if self.verbose: print("Triggering TES")
        for dev in self._external_devices:
            dev.trigger()

        status = DeviceStatus(self)
        i = next(self._data_index)
        threading.Thread(target=self._acquire, args=(status, i), daemon=True).start()
        return status

    def collect_asset_docs(self):
        for dev in self._external_devices:
            yield from dev.collect_asset_docs()
            
    def stop(self):
        if self._completion_status is not None:
            self._completion_status.set_finished()

            
