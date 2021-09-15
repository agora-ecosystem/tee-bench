#!/usr/bin/python3

import subprocess
import re
import matplotlib.pyplot as plt
import numpy as np
import csv
import commons


# run join with GBs of data
def run_join(mode, alg, threads, reps, fname, total_gbs):
    f = open(fname, "a")
    res = 0
    r_size = 1/5 * float(total_gbs) * 1024
    s_size = 4/5 * float(total_gbs) * 1024
    for i in range(0, reps):
        s = "./app -a " + alg + " -n " + str(threads) + " -x " + str(r_size) + " -y " + str(s_size)
        if alg == 'INL':
            s += ' --sort-s '

        stdout = subprocess.check_output(s, cwd="../",
                                         shell=True).decode('utf-8')
        print(stdout)
        for line in stdout.splitlines():
            if "Throughput (M rec/sec)" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                res = res + float(throughput)
                print (throughput)
    res = res / reps
    s = mode + "," + alg + "," + str(threads) + "," + str(total_gbs) + "," + str(round(res,2))
    print(s)
    f.write(s + "\n")
    f.close()


def plot(fname):
    plot_filename = "img/throughput-scale-input"
    csvf = open(fname, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    plt.rc('axes', axisbelow=True)
    plt.rcParams.update({'font.size': 15})

    fig, (ax,ax2) = plt.subplots(1,2,sharey=True, gridspec_kw={'width_ratios': [4, 1]},
                                 figsize=(4,3))
    # plt.clf()
    # plt.gca().yaxis.grid(linestyle='dashed')
    to_algos = [[y for y in all_data if y['alg'] == x] for x in algos]
    for a in range(0, len(algos)):
        rows = list(map(lambda x: float(x['total_gbs']), to_algos[a]))
        ax.plot(rows, list(map(lambda x: float(x['throughput']), to_algos[a])), marker=commons.marker_alg(algos[a]),
                 label=algos[a], color=commons.color_alg(algos[a]), linewidth=2, markeredgecolor='black',
                 markersize=8, markeredgewidth=0.5)
        ax2.plot(rows, list(map(lambda x: float(x['throughput']), to_algos[a])), marker=commons.marker_alg(algos[a]),
                label=algos[a], color=commons.color_alg(algos[a]), linewidth=2, markeredgecolor='black',
                markersize=8, markeredgewidth=0.5)

    ax.set_xlim(0,10)
    ax2.set_xlim(14,19)

    # hide the spines between ax and ax2
    ax.spines['right'].set_visible(False)
    ax2.spines['left'].set_visible(False)
    ax.yaxis.tick_left()
    # ax.tick_params(labelright='off')
    ax2.yaxis.tick_right()
    d = .015 # how big to make the diagonal lines in axes coordinates
    # arguments to pass plot, just so we don't keep repeating them
    kwargs = dict(transform=ax.transAxes, color='k', clip_on=False)
    ax.plot((1-d,1+d), (-d,+d), **kwargs)
    ax.plot((1-d,1+d),(1-d,1+d), **kwargs)

    kwargs.update(transform=ax2.transAxes)  # switch to the bottom axes
    ax2.plot((-d,+d), (1-d,1+d), **kwargs)
    ax2.plot((-d,+d), (-d,+d), **kwargs)
    ax.set_xlabel("Input size [GB]")
    ax.xaxis.set_label_coords(0.7, -0.15)
    ax.set_ylabel("Throughput [M rec/s]")
    # plt.xticks([2,4,6,8,10])
    # plt.ylim(bottom=0)
    # plt.tight_layout()
    fig.tight_layout()
    ax2.set_xticks([18])
    plt.subplots_adjust(wspace=0.15)
    lines, labels = fig.axes[-1].get_legend_handles_labels()
    fig.legend(lines, labels, fontsize=10, frameon=0,
               ncol=3, bbox_to_anchor = (0.19, 0.95), loc='lower left', borderaxespad=0)
    commons.savefig(filename=plot_filename + "-sgx" + ".png", tight_layout=False)


if __name__ == '__main__':
    reps = 1
    threads = 3
    # modes = ["native", "sgx"]
    modes = ["sgx"]
    filename = "data/throughput-scale-input-output.csv"

    # commons.remove_file(filename)
    # commons.init_file(filename, "mode,alg,threads,total_gbs,throughput\n")

    # for mode in modes:
    #     commons.compile_app(mode)
    #     for alg in ['RHO','RHT','RSM','PSM','INL','CHT','PHT']:#commons.get_all_algorithms_extended():
    #         # for factor in [1,2,3,4,5,6,7,8,9,10,20]:
    #         for factor in [20]:
    #             run_join(mode, alg, threads, reps, filename, factor)

    plot(filename)
