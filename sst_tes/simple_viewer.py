"""
Shamelessly copied from https://github.com/NSLS-II-BMM/BMM-app/blob/main/BMM_app.py
"""

from bluesky_widgets.utils.streaming import stream_documents_into_runs
from bluesky_widgets.qt import gui_qt
from bluesky_widgets.qt.figures import QtFigures
from bluesky_widgets.examples.utils.generate_msgpack_data import get_catalog
from bluesky_widgets.utils.list import EventedList 

from bluesky_widgets.qt.zmq_dispatcher import RemoteDispatcher
from bluesky_widgets.models.plot_builders import Lines

figures = EventedList()

models = []
import importlib

from bluesky_widgets.models.auto_plot_builders import AutoPlotter
from bluesky_widgets.models.plot_specs import Axes, Figure

class AutoTESPlot(AutoPlotter):
    
    def handle_new_stream(self, run, stream_name):
        if stream_name != 'primary':
            return

        xx = run.metadata['start']['motors'][0]
        axes1 = Axes()
        axes2 = Axes()
        figure = Figure((axes1, axes2), title='It and I0')
        model = Lines(x=xx, ys=['tes_spectrum'], max_runs=3)

        model.add_run(run)
        self.figures.append(model.figure) 
        self.plot_builders.append(model) 

address = 'localhost:4002'

model = AutoTESPlot()

with gui_qt('SST-TES app'):
    dispatcher = RemoteDispatcher(address)
    dispatcher.subscribe(stream_documents_into_runs(model.add_run))
    view = QtFigures(model.figures)
    view.show()
    dispatcher.start()
