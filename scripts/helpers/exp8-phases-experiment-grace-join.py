#!/usr/bin/python3

import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
import csv
import commons


fname_phases = "data/phases-runtime-output-grace-join.csv"


def plot_phases_per_cycle():
    plot_fname = "img/phases-runtime-per-cycle-grace-join"
    csvf = open(fname_phases, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    modes = sorted(set(map(lambda x:x['mode'], all_data)))
    all_data = list(filter(lambda x:x['phase'] != "Total", all_data))
    datasets = commons.get_test_dataset_names()

    # a graph for both native and sgx
    to_modes = [[y for y in all_data if y['mode'] == x] for x in modes]
    plt.figure(figsize=(10,10))
    plt.clf()
    for m in range(0, len(modes)):
        to_datasets = [[y for y in to_modes[m] if y['ds'] == x] for x in datasets]

    # now just SGX
    plt.rc('axes', axisbelow=True)
    plt.rcParams.update({'font.size': 15})
    fig = plt.figure(figsize=(8,3.5))
    outer = gridspec.GridSpec(1,2)
    # first dataset
    d = 0
    ds = datasets[d]
    ax = plt.Subplot(fig, outer[d])
    to_algos = [[y for y in to_datasets[d] if y['alg'] == x] for x in algos]
    i = 0
    agg = 0
    ax.yaxis.grid(linestyle='dashed')
    for a in range(len(algos)):
        alg = algos[a]
        for j in range(len(to_algos[a])):
            val = float(to_algos[a][j]['cycles'])
            alpha = 1 - 0.7*(j%2)
            # first print a white background to cover the grid lines
            ax.bar(i, val, bottom=agg, label=to_algos[a][j]['phase'],
                   color='white', edgecolor='black')
            #now print the real bar
            ax.bar(i, val, bottom=agg, label=to_algos[a][j]['phase'],
                   color=commons.color_alg(alg), alpha=alpha, edgecolor='black')
            agg = agg + val
        agg = 0
        i = i+1
    ax.set_xticks(np.arange(len(algos)))
    ax.set_xticklabels(algos, rotation=45)
    ax.set_ylabel("CPU cycles / tuple")
    ax.set_ylim([0, 500])
    ax.set_title('(' + chr(97+d) + ") Dataset $\it{" + ds + "}$", y=-0.35)
    fig.add_subplot(ax)

    #second dataset
    d = 1
    ds = datasets[d]
    to_algos = [[y for y in to_datasets[d] if y['alg'] == x] for x in algos]
    ax2 = plt.Subplot(fig, outer[d])
    i = 0
    agg = 0
    for a in range(len(algos)):
        alg = algos[a]
        for j in range(0, len(to_algos[a])):
            val = float(to_algos[a][j]['cycles'])
            alpha = 1 - 0.7*(j%2)
            # first print a white background to cover the grid lines
            ax2.bar(i, val, bottom=agg, label=to_algos[a][j]['phase'],
                    color='white', edgecolor='black')
            #now print the real bar
            ax2.bar(i, val, bottom=agg, label=to_algos[a][j]['phase'],
                    color=commons.color_alg(alg), alpha=alpha, edgecolor='black')
            agg = agg + val
        agg = 0
        i = i+1

    ax2.set_ylim(0, 450)

    ax2.xaxis.tick_bottom()

    ax2.yaxis.grid(linestyle='dashed')
    ax2.set_xticks(np.arange(len(algos)))
    ax2.set_xticklabels(algos, rotation=45)

    ax2.set_title('(' + chr(97+1) + ") Dataset $\it{" + ds + "}$", y=-0.35)
    ax2.set_ylim([0, 500])
    fig.add_subplot(ax2)

    plt.tight_layout()
    commons.savefig(plot_fname + '-sgx.png')


if __name__ == '__main__':
    plot_phases_per_cycle()
