#!/usr/bin/python3
import getopt
import subprocess
import re
import sys

import matplotlib.pyplot as plt
import yaml
from matplotlib.patches import Patch
import numpy as np
import csv
import commons


def run_join(prog, alg, threads, reps, fname):
    f = open(fname, "a")
    print("Run " + prog + " alg=" + alg + " threads=" + str(threads))
    res = 0
    for i in range(0, reps):
        stdout = subprocess.check_output(prog + " -a " + alg + " -r 200000 -s 400000 " + " -n " + str(threads),
                                         cwd="../../",
                                         shell=True).decode('utf-8')
        # print(stdout)
        # RHO needs special treatment because we disabled logging
        if alg == 'RHO':
            for line in stdout.splitlines():
                if "throughput" in line:
                    throughput = float(re.findall("\d+\.\d+", line)[1]) * 1000
                    s = (alg + "," + str(threads) + "," + str(throughput))
                    res = res + float(throughput)
                    print(s)
        else:
            for line in stdout.splitlines():
                if "Throughput" in line:
                    throughput = re.findall("\d+\.\d+", line)[1]
                    s = (alg + "," + str(threads) + "," + str(throughput))
                    res = res + float(throughput)
                    print(s)
    res = res / reps
    s = (alg + "," + str(threads) + "," + str(round(res, 2)))
    f.write(s + "\n")
    f.close()


def plot(fname):
    plot_filename = "../img/Figure-11-Throughput-of-secure-and-oblivious-joins"
    csvf = open(fname, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    fig = plt.figure(figsize=(4,2))
    plt.rcParams.update({'font.size': 15})
    plt.rc('axes', axisbelow=True)
    br = np.arange(len(all_data))
    bars = plt.bar(br, list(map(lambda x: float(x['throughput'])/1000, all_data)),
                  color='#ad2726')
    patterns = ('O','O','O','*','')
    # colors = ['#936ed4', '#936ed4', '#936ed4', '#03c33f', commons.color_alg('CHT')]
    colors = ['white', 'white', commons.color_alg('RHO'), 'white', commons.color_alg('RHO')]
    algs = list(map(lambda x:x['alg'],all_data))
    for bar, pattern, algo, color in zip(bars, patterns, algs, colors):
        bar.set_hatch(pattern)
        bar.set_label(algo)
        bar.set_color(color)
        bar.set_edgecolor('black')

    arr = ['OBLI', 'OPAQ', 'RHOBLI', 'KRAS', 'RHO\n(TEE)']
    plt.xticks(np.arange(len(arr)), arr, rotation=45, fontsize=12)
    plt.xlabel("Secure join algorithm")
    plt.ylabel("Throughput [M rec/s]")
    plt.tick_params(axis='x', which='minor', bottom=False)
    plt.yscale('log')
    plt.gca().yaxis.grid(linestyle='dashed')
    leg_elements = [Patch(facecolor='white', edgecolor='black',
                          label='None'),
                    Patch(facecolor='white', edgecolor='black',
                          hatch='OO',label='ORAM'),
                    Patch(facecolor='white', edgecolor='black',
                          hatch='**',label='Problem-specific')
                    ]
    # set legend on top
    legend = fig.legend(title='Types of obliviousness (hatches):',handles=leg_elements,
                        ncol=3, frameon=True,
                        loc="upper left", bbox_to_anchor=(0,1.2,1,0),
                        fontsize=10, title_fontsize=10, framealpha=0.5)
    legend._legend_box.align = "left"
    # place legend on the right
    # legend = fig.legend(title='Types of obliviousness:',handles=leg_elements,
    #                     ncol=1, frameon=False,
    #                     loc="upper right", bbox_to_anchor=(0.36,0.9,1,0),
    #                     fontsize=10, title_fontsize=10, framealpha=0.5)
    frame = legend.get_frame()
    frame.set_edgecolor('black')
    # plt.tight_layout()
    commons.savefig(plot_filename + ".png", tight_layout=False)


def plot_front_figure(fname):
    plot_filename = "../img/throughput-oblivious-algos-plot"
    csvf = open(fname, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    algos = list(map(lambda x:x['alg'], all_data))
    fig, ax = plt.subplots(figsize=(6,3))
    plt.rc('axes', axisbelow=True)
    ax.set_axisbelow(True)
    # plt.rcParams.update({'font.size': 15})
    br = np.arange(len(all_data))
    colors = list(map(lambda x: commons.color_alg(x['alg']), all_data))
    hatches = ['', '', '', '\\']
    plt.bar(br, list(map(lambda x: float(x['throughput']), all_data)), label='label',
            color=colors, hatch='.')
    bars = ax.patches
    for bar, hatch in zip(bars, hatches):
        bar.set_hatch(hatch)

    # ax.tick_params(axis='both', which='major', labelsize=15)
    plt.ylabel("Throughput [K rec/s]", fontsize=15)
    plt.yscale('log')
    plt.xticks([],[])
    plt.minorticks_off()
    plt.gca().yaxis.grid(linestyle='dashed')
    labels = ["Krastikov et al. [?]", "Opaque [?]", "ObliDB [?]", "RHO"]
    handles = [plt.Rectangle((0,0), 1, 1, color=commons.color_alg(x)) for x in algos]
    fig.legend(handles, labels, loc='lower left', bbox_to_anchor= (1, 0.47), ncol=1,
               borderaxespad=0, frameon=True, prop={'size':12}, fontsize=15,
               edgecolor='black', labelspacing=1)
    commons.savefig(plot_filename + "-front.png")


if __name__ == '__main__':
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'r:',['reps='])
    except getopt.GetoptError:
        print('Unknown argument')
        sys.exit(1)
    for opt, arg in opts:
        if opt in ('-r', '--reps'):
            config['reps'] = int(arg)

    algs = ["OBLI", "OPAQ", "RHOBLI", "OJ", "RHO"]
    threads = 1
    filename = "../data/throughput-oblivious-algos-output.csv"

    if config['experiment']:
        commons.remove_file(filename)
        commons.init_file(filename, "alg,threads,throughput\n")
        commons.compile_app(mode='sgx', flags=['RADIX_NO_TIMING'])
        for alg in algs:
            run_join(commons.PROG, alg, threads, config['reps'], filename)

    plot(filename)
    # plot_front_figure(filename)
