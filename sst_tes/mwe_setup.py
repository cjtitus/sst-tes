from bluesky import RunEngine
from bluesky.plans import scan
from bluesky.callbacks.zmq import Publisher
from ophyd.sim import det, motor
from databroker import Broker
from sst_tes.mwe import MWETES

tes = MWETES('tes', port=4000)
RE = RunEngine({})
db = Broker.named('temp')

publisher = Publisher('localhost:4001')
RE.subscribe(db.insert)
RE.subscribe(publisher)

RE(scan([tes], motor, -5, 5, 10))
