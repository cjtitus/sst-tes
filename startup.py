from bluesky import RunEngine
from bluesky.plans import scan, count
import bluesky.preprocessors as bpp
import bluesky.plan_stubs as bps
from bluesky.callbacks import LiveTable
from ophyd.sim import det, motor
from databroker import Broker

from base_tes import TES
from fly_tes import SimFlyableTES, FlyableTES

RE = RunEngine({})


from bluesky.callbacks.best_effort import BestEffortCallback
bec = BestEffortCallback()

RE.subscribe(bec)

db = Broker.named('temp')

RE.subscribe(db.insert)

dets = [det]

#RE(count(dets, num=5))
#RE(scan(dets, motor, -5, 5, 30))


#tes = TES(name='tes')
#tes1 = TES(name='tes1')
ftes = FlyableTES('flytes', "", 4000)

def TESPreprocessor(tes):
    def _log_tes_wrapper(plan):
        yield from bps.subscribe("start", tes.start_log)
        return (yield from plan)
    return _log_tes_wrapper


def make_tes_scan(tes):
        
    def tes_scan(*args, **kwargs):
        def tes_trigger_and_read(devices):
            d = list(devices) + [tes]
            yield from bps.trigger_and_read(d)
            yield from bps.collect(tes, stream=True)

        def tes_per_step(detectors, step, pos_cache):
            yield from bps.one_nd_step(detectors, step, pos_cache, take_reading=tes_trigger_and_read)

        return (yield from bpp.fly_during_wrapper(scan(*args, **kwargs, per_step=tes_per_step), [tes]))
    return tes_scan

    
tpp = TESPreprocessor(ftes)

RE.preprocessors = [tpp]

tes_scan = make_tes_scan(ftes)
ftes._file_start()
#RE(scan([det, tes], motor, -5, 5, 30))
#RE(scan([det, tes1], motor, -5, 5, 30))
RE(scan([det], motor, -5, 5, 30))
RE(tes_scan([det], motor, -5, 5, 10))
#RE(bpp.fly_during_wrapper(scan([det], motor, -5, 5, 10), [ftes]))
