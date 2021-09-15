#!/usr/bin/python3

import subprocess
import re
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
import csv
import commons
import statistics
import yaml


fname_phases = "data/phases-runtime-output.csv"


def run_join(mode, alg, ds, threads, reps):

    f_phases = open(fname_phases, 'a')

    throughput_array = []
    throughput = ''
    dic_phases = {}
    print("Run=" + commons.PROG + " mode=" + mode + " alg=" + alg + " ds=" + ds + " threads=" + str(threads))
    for i in range(reps):
        stdout = subprocess.check_output(commons.PROG + " -a " + alg + " -d " + ds + " -n " + str(threads), cwd="../",
                                         shell=True).decode('utf-8')
        print(str(i+1) + '/' + str(reps) + ': ' +
              mode + "," + alg + "," + ds + "," + str(threads))
        for line in stdout.splitlines():
            # find throughput for the first graph
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                throughput_array.append(float(throughput))
            # find phase for the second graph
            if "Phase" in line:
                words = line.split()
                phase_name = words[words.index("Phase") + 1]
                value = int(re.findall(r'\d+', line)[-2])
                print (phase_name + " = " + str(value))
                if phase_name in dic_phases:
                    dic_phases[phase_name].append(value)
                else:
                    dic_phases[phase_name] = [value]

        print('Throughput = ' + str(throughput) + ' M [rec/s]')

    for x in dic_phases:
        res = statistics.mean(dic_phases[x])
        s = mode + "," + alg + "," + ds + "," + x + "," + str(res)
        f_phases.write(s + '\n')

    f_phases.close()


def plot_phases_per_cycle():
    plot_fname = "img/phases-runtime-per-cycle"
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
        mode = modes[m]
        to_datasets = [[y for y in to_modes[m] if y['ds'] == x] for x in datasets]
        for d in range(0, len(datasets)):
            ds = datasets[d]
            to_algos = [[y for y in to_datasets[d] if y['alg'] == x] for x in algos]
            plt.subplot(2,2,2*m+d+1)
            i = 0
            agg = 0
            for a in range(0, len(algos)):
                for j in range(0, len(to_algos[a])):
                    numtuples = 6553600 if ds == 'cache-fit' else 65536000
                    val = float(to_algos[a][j]['cycles']) / numtuples
                    alpha = 1 - 0.7*(j%2)
                    plt.bar(i, val, bottom=agg, label=to_algos[a][j]['phase'],
                            color=commons.color_alg(algos[a]), alpha=alpha)
                    agg = agg + val
                agg = 0
                i = i+1

            # plt.xlabel("Join algorithm")
            plt.ylabel("CPU cycles / tuple")
            plt.xticks(np.arange(len(algos)),algos)
            plt.title(mode + " - dataset " + ds)
            plt.tight_layout()
    commons.savefig(plot_fname + ".png")

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
            if ds == 'cache-fit':
                numtuples = 6553600
            elif ds == 'cache-exceed':
                numtuples = 65536000
            else:
                raise ValueError('Unknown dataset: ' + ds)

            val = float(to_algos[a][j]['cycles']) / numtuples
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
    # ax.set_xlabel("Join algorithm")
    ax.set_ylabel("CPU cycles / tuple")
    # ax.set_ylim([0, 150])
    ax.set_title('(' + chr(97+d) + ") Dataset $\it{" + ds + "}$", y=-0.4)
    fig.add_subplot(ax)

    #second dataset
    d = 1
    ds = datasets[d]
    to_algos = [[y for y in to_datasets[d] if y['alg'] == x] for x in algos]
    inner = gridspec.GridSpecFromSubplotSpec(2,1,subplot_spec=outer[1])
    ax = plt.Subplot(fig, inner[0])
    ax2 = plt.Subplot(fig, inner[1])
    i = 0
    agg = 0
    for a in range(len(algos)):
        alg = algos[a]
        for j in range(0, len(to_algos[a])):
            if ds == 'cache-fit':
                numtuples = 6553600
            elif ds == 'cache-exceed':
                numtuples = 65536000
            else:
                raise ValueError('Unknown dataset: ' + ds)
            val = float(to_algos[a][j]['cycles']) / numtuples
            alpha = 1 - 0.7*(j%2)
            # first print a white background to cover the grid lines
            ax.bar(i, val, bottom=agg, label=to_algos[a][j]['phase'],
                   color='white', edgecolor='black')
            ax2.bar(i, val, bottom=agg, label=to_algos[a][j]['phase'],
                    color='white', edgecolor='black')
            #now print the real bar
            ax.bar(i, val, bottom=agg, label=to_algos[a][j]['phase'],
                   color=commons.color_alg(alg), alpha=alpha, edgecolor='black')
            ax2.bar(i, val, bottom=agg, label=to_algos[a][j]['phase'],
                    color=commons.color_alg(alg), alpha=alpha, edgecolor='black')
            agg = agg + val
        agg = 0
        i = i+1

    ax2.set_ylim(0, 400)
    ax.set_ylim(500, 15500)
    ax.set_yticks((1000,5000,10000,15000))
    # hide the spines between ax and ax2
    ax.spines['bottom'].set_visible(False)
    ax2.spines['top'].set_visible(False)

    ax.xaxis.tick_top()
    ax.tick_params(labeltop='off')  # don't put tick labels at the top
    ax2.xaxis.tick_bottom()


    d = .015  # how big to make the diagonal lines in axes coordinates
    # arguments to pass to plot, just so we don't keep repeating them
    kwargs = dict(transform=ax.transAxes, color='k', clip_on=False)
    ax.plot((-d, +d), (-d, +d), **kwargs)        # top-left diagonal
    ax.plot((1 - d, 1 + d), (-d, +d), **kwargs)  # top-right diagonal

    kwargs.update(transform=ax2.transAxes)  # switch to the bottom axes
    ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)  # bottom-left diagonal
    ax2.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)  # bottom-right diagonal

    ax.yaxis.grid(linestyle='dashed')
    ax2.yaxis.grid(linestyle='dashed')
    ax2.set_xticks(np.arange(len(algos)))
    ax2.set_xticklabels(algos, rotation=45)
    ax.xaxis.set_visible(False)

    # ax2.set_xlabel("Join algorithm")
    ax2.set_title('(' + chr(97+1) + ") Dataset $\it{" + ds + "}$", y=-0.87)
    # fig.text(0.5, 0.57, 'CPU cycles [B]', va='center', rotation='vertical')

    fig.add_subplot(ax)
    fig.add_subplot(ax2)

    plt.tight_layout()
    commons.savefig(plot_fname + '-sgx.png')


if __name__ == '__main__':
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    if config['experiment']:
        commons.remove_file(fname_phases)
        commons.init_file(fname_phases, "mode,alg,ds,phase,cycles\n")

        for mode in config['modes']:
            flags = ['SGX_COUNTERS', 'PCM_COUNT'] if mode == 'sgx' else []
            commons.compile_app(mode, flags)
            for ds in commons.get_test_dataset_names():
                for alg in commons.get_all_algorithms_extended():
                    run_join(mode, alg, ds, config['threads'], config['reps'])

    plot_phases_per_cycle()
