from bluesky import RunEngine
from bluesky.plans import scan, count
import bluesky.preprocessors as bpp
import bluesky.plan_stubs as bps
from bluesky.callbacks import LiveTable
from ophyd.sim import det, motor
from databroker import Broker

from readable_tes import TES
from handlers import FakeHandler

RE = RunEngine({})
db = Broker.named('temp')
db.reg.register_handler("tes", FakeHandler)

RE.subscribe(db.insert)

tes = TES('tes', "", 4000)

def TESPreprocessor(tes):
    def _log_tes_wrapper(plan):
        yield from bps.subscribe("start", tes.start_log)
        return (yield from plan)
    return _log_tes_wrapper

tpp = TESPreprocessor(tes)

RE.preprocessors = [tpp]

tes._file_start()
tes.verbose = True
RE(scan([det, tes], motor, -5, 5, 10))
run = db.v2[-1]
