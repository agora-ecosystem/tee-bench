#!/usr/bin/python3

import subprocess
import re
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
import csv
import commons
import statistics
from matplotlib.patches import Patch

fname_throughput = "data/amd-throughput.csv"
csvf = open(fname_throughput, mode='r')
csvr = csv.DictReader(csvf)
all_data = list(csvr)
algos = sorted(set(map(lambda x:x['alg'], all_data)))
datasets = sorted(set(map(lambda x:x['ds'], all_data)), reverse=True)
modes = sorted(set(map(lambda x:x['mode'], all_data)))
width = 0.4
to_modes = [[y for y in all_data if y['mode'] == x] for x in modes]

# graph per dataset
plt.rc('axes', axisbelow=True)
plt.rcParams.update({'font.size': 14})
fig = plt.figure(figsize=(8,3))
# plt.clf()
legend_elements = [Patch(label='Hatches:', alpha=0),
                   Patch(facecolor='white', edgecolor='black',
                         hatch='\\\\', label='AMD'),
                   Patch(facecolor='white', edgecolor='black',
                         label='AMD-SEV')]

to_datasets = [[y for y in all_data if y['ds'] == x] for x in datasets]
for d in range(0, len(datasets)):
    ax = plt.subplot(1, 2, d+1)
    ax.yaxis.grid(linestyle='dashed')
    to_modes = [[y for y in to_datasets[d] if y['mode'] == x] for x in modes]
    for m in range(0, len(modes)):
        if m == 0:
            br = np.arange(len(algos))
            br = [x - 0.2 for x in br]
        else:
            br = [x + width for x in br]

        label = modes[m]
        colors = list(map(lambda x: commons.color_alg(x), algos))
        hatch = '\\\\' if modes[m] == 'amd' else ''
        plt.bar(br, list(map(lambda x: float(x['throughput']), to_modes[m])),
                width=width, label=label, hatch=hatch, color=colors, edgecolor='black')
        for x, y in zip(br, list(map(lambda x: float(x['throughput']), to_modes[m]))):
            if y < 5:
                plt.text(x-0.15, y+8, str(y), rotation=90)
        if d == 0:
            plt.ylabel("Throughput [M rec/s]")
        plt.ylim([0, 160])
        plt.yticks([0, 50, 100, 150], size=12)
        plt.xticks(np.arange(len(to_modes[m])), list(map(lambda x:x['alg'],to_modes[m])),
                   rotation=30, size=12)
        plt.title('(' + chr(97+d) + ") Dataset $\it{" + datasets[d] + "}$", y=-0.42)

fig.legend(handles=legend_elements, ncol=3, frameon=False,
           bbox_to_anchor=(0.02,0.88,1,0), loc="lower left")
commons.savefig("img/amd-throughput.png")
