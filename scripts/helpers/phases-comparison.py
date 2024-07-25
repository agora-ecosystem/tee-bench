#!/usr/bin/python3
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

import commons

plot_filename = "../img/phase-comparison.png"

import numpy as np

fig, ax = plt.subplots()

labels=['build','probe']

size = 0.3

ax.pie([20,80], radius=1, colors=[commons.color_categorical(0), commons.color_categorical(90)],
       wedgeprops=dict(width=size, edgecolor='w'))

ax.pie([24,76], radius=1-size, colors=[commons.color_categorical(1), commons.color_categorical(91)],
       wedgeprops=dict(width=size, edgecolor='w'))

# ax.set(aspect="equal", title='')
# plt.legend()
# legend
legend_elements = [Line2D([0], [0], marker='o', color='w', label='v2.1', markerfacecolor=commons.color_categorical(0), markersize=10),
                   Line2D([0], [0], marker='o', color='w', label='v2.2', markerfacecolor=commons.color_categorical(1), markersize=10)]
ax.legend(handles=legend_elements, loc='center', frameon=False)
plt.savefig(plot_filename, transparent = True, pad_inches = 0.1, dpi=200)
