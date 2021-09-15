#!/usr/bin/python3

import subprocess
import re
import statistics
import matplotlib as mpl
import matplotlib.pyplot as plt
import csv
import commons
import yaml

fname_output = "data/multi-threading-atomic-output.csv"
plot_fname_base = "img/multi-threading-atomic"


def run_join(mode, prog, alg, dataset, threads, reps):
    f = open(fname_output, "a")
    results = []

    for i in range(0,reps):
        s = str(prog) + " -a " + str(alg) + " -d " + str(dataset) + " -n " + str(threads)
        stdout = subprocess.check_output(s, cwd="../",shell=True)\
            .decode('utf-8')
        print(stdout)
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                s = (alg + " " + dataset + " " + str(threads) + " " + str(throughput))
                results.append(float(throughput))
                print (s)

    res = statistics.mean(results)
    s = (mode + "," + alg+'-'+mode + "," + dataset + "," + str(threads) + "," + str(round(res,2)))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot_threading():
    csvf = open(fname_output, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    datasets = sorted(set(map(lambda x:x['ds'], all_data)), reverse=True)

    mpl.rcParams.update(mpl.rcParamsDefault)
    fig = plt.figure(figsize=(7,3))
    to_datasets = [[y for y in all_data if y['ds'] == x] for x in datasets]
    for i in range(0, len(datasets)):
        ds = datasets[i]
        data = to_datasets[i]
        # split data into algorithms
        splitted_to_algos = [[y for y in data if y['alg'] == x] for x in algos]
        plt.subplot(1,2,i+1)
        # print each plot
        for j in range(0, len(algos)):
            threads = sorted(list(map(lambda x: int(x['threads']), splitted_to_algos[j])))
            plt.plot(threads, list(map(lambda x: float(x['throughput']), splitted_to_algos[j])),
                     '-o', label=algos[j], color=commons.color_alg(algos[j]), linewidth=2,
                     marker=commons.marker_alg(algos[j]), markeredgecolor='black', markersize=6,
                     markeredgewidth=0.5)
        # make the figure neat
        if i == 0:
            plt.ylabel("Throughput [M rec / sec]")
        plt.xticks([2,4,6,8])
        # plt.yticks([0,10,20,30,40])
        plt.ylim(top=30, bottom=0)
        plt.gca().yaxis.grid(linestyle='dashed')
        fig.text(0.54, 0.26, 'Number of threads', ha='center')
        lines, labels = fig.axes[-1].get_legend_handles_labels()
        fig.legend(lines, labels, fontsize='small', frameon=0,
                   ncol=8, bbox_to_anchor = (0.15, 0.92), loc='lower left',
                   borderaxespad=0, handletextpad=0.25, columnspacing=1.5)
        plt.title('(' + chr(97+i) + ") Dataset $\it{" + ds + "}$", y=-0.4)
    # save fig
    commons.savefig(plot_fname_base + "-sgx.png")

    fig = plt.figure(figsize=(4,3))
    fit_data = list(filter(lambda x: x['ds'] == 'cache-fit', all_data))
    splitted_to_algos = [[y for y in fit_data if y['alg'] == x] for x in algos]
    for j in range(len(algos)):
        threads = sorted(list(map(lambda x: int(x['threads']), splitted_to_algos[j])))
        plt.plot(threads, list(map(lambda x: float(x['throughput']), splitted_to_algos[j])),
                 '-o', label=algos[j], color=commons.color_alg(algos[j]), linewidth=3,
                 marker=commons.marker_alg(algos[j]), markeredgecolor='black', markersize=8,
                 markeredgewidth=0.5)
    plt.ylabel("Throughput [M rec / sec]")
    plt.xlabel("Number of threads")
    plt.xticks([2,4,6,8])
    plt.ylim(bottom=0)
    plt.gca().yaxis.grid(linestyle='dashed')
    lines, labels = fig.axes[-1].get_legend_handles_labels()
    fig.legend(lines, labels, fontsize='small', frameon=0,
               ncol=8, bbox_to_anchor = (0.19, 0.92), loc='lower left',
               borderaxespad=0, handletextpad=0.25, columnspacing=1.5)
    commons.savefig(plot_fname_base + "-sgx-cache-fit.png")


if __name__ == '__main__':

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    threads = 8
    modes = ['sgx', 'sgx-affinity']
    algs = ['RHO', 'RHO_atomic']

    # if config['experiment']:
    #     commons.remove_file(fname_output)
    #     commons.init_selectifile(fname_output, "mode,alg,ds,threads,throughput\n")
    #
    #     for mode in modes:
    #         commons.compile_app(mode, [])
    #         for alg in algs:
    #             for ds in commons.get_test_dataset_names():
    #                 for i in range(threads):
    #                     run_join(mode, commons.PROG, alg, ds, i+1, config['reps'])

    plot_threading()
