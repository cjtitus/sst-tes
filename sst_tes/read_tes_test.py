from bluesky import RunEngine
from bluesky.plans import scan, count, list_scan
import bluesky.preprocessors as bpp
import bluesky.plan_stubs as bps
from bluesky.callbacks import LiveTable
from bluesky.callbacks.zmq import Publisher
from ophyd.sim import det, motor
from databroker import Broker
from bluesky_widgets.utils.streaming import stream_documents_into_runs
from sst_tes.readable_tes import TES
import yaml
#from handlers import FakeHandler
import matplotlib.pyplot as plt

tes = TES('tes', port=4000)

plt.ion()
RE = RunEngine({})
db = Broker.named('temp')

publisher = Publisher('localhost:4001')

runs = []
RE.subscribe(db.insert)
RE.subscribe(stream_documents_into_runs(runs.append))
RE.subscribe(publisher)



def TESPreprocessor(tes):
    def _log_tes_wrapper(plan):
        yield from bps.subscribe("start", tes.start_log)
        return (yield from plan)
    return _log_tes_wrapper

def stream_to_figures(fig, ax):
            
    def update_plot(event):
        print("Update plot called")
        run = event.run
        key = list(run.primary._document_cache.event_pages.keys())[0]
        data = run.primary.read()["tes_spectrum"]
        #print(run.primary._document_cache.event_pages[key][-1])
        #print(data)
        ax.plot(data[:, 0])

    def update_on_stream(run):
        run.events.new_data.connect(update_plot)

    return stream_documents_into_runs(update_on_stream)


tpp = TESPreprocessor(tes)

RE.preprocessors = [tpp]

with open("/home/jamie/work/scan_server/scan_server/tests/data/ssrl_10_1_scan/20200219_017_log", "r") as f:
    test_log = list(yaml.load_all(f))
    mono = list(test_log[1]['mono'].keys())

tes.verbose = True
tes._file_start()

RE(scan([det, tes], motor, -5, 5, 10))
RE(list_scan([det, tes], motor, mono))
#run = db.v2[-1]
