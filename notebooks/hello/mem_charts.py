import yaml

from bokeh.layouts import column
from bokeh.models import ColumnDataSource, Slider, PreText
from bokeh.plotting import figure
from bokeh.themes import Theme
from bokeh.io import show, output_notebook
from datetime import datetime
import psutil
import os
import collections
import bisect
import string
import random


class MemUsageHistory:
    def __init__(self, size=60):
        self.size = size
        self.start = datetime.now()
        self.measurements = collections.OrderedDict()

    @staticmethod
    def current_memory_usage():
        process = psutil.Process(pid=os.getpid())
        mem_info = process.memory_info()
        return mem_info.rss / 1024 / 1024

    def measure(self):
        e = (datetime.now() - self.start).total_seconds()
        self.measurements[e] = MemUsageHistory.current_memory_usage()

    def source(self):
        time = []
        mem_usage = []
        e = (datetime.now() - self.start).total_seconds()

        def find_measurement(e):
            if e > 0:
                keys = list(self.measurements.keys())
                if len(keys) > 0:
                    index = bisect.bisect_left(keys, e)
                    if index > 0 and index < len(keys):
                        return self.measurements[keys[index]]
            return 0

        for i in range(self.size):
            time.append(e - self.size + i)
            mem_usage.append(find_measurement(e - self.size + i))

        return {"time": time, "mem_usage": mem_usage}

    
usage_history = MemUsageHistory()
source = ColumnDataSource(data=usage_history.source())


def callback(attr, old, new):
    global usage_history
    global source

    usage_history.measure()
    source.data = usage_history.source()


def mem_usage_chart(doc):
    global source

    plot = figure(x_axis_type='auto', y_range=(0, MemUsageHistory.current_memory_usage() * 5),
                  y_axis_label='Memory usage [MB]',
                  title="Memory usage of the current process")
    plot.vbar(x='time', top='mem_usage', width=0.7, source=source, fill_color="#a9ebe0", line_alpha=50)

    hidden_text = PreText(text='', css_classes=['hidden'])
    hidden_text.on_change('text', callback)

    def update():
        hidden_text.text = '0' if hidden_text.text == '1' else '1'

    doc.add_root(column(plot))
    doc.theme = Theme(json=yaml.load("""
        attrs:
            figure:
                background_fill_color: white
                outline_line_color: black
                toolbar_location: above
                height: 500
                width: 800
            Grid:
                grid_line_dash: [6, 4]
                grid_line_color: gray
    """, Loader=yaml.FullLoader))

    doc.add_periodic_callback(update, 1000)


def random_string(length = 12):
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for _ in range(length))
