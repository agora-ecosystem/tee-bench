#!/usr/bin/python3

import subprocess
import re
import statistics
import matplotlib as mpl
import matplotlib.pyplot as plt
import csv
import commons
import yaml

fname_output = "data/multi-threading-output.csv"
plot_fname_base = "img/multi-threading"


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
    s = (mode + "," + alg + "," + dataset + "," + str(threads) + "," + str(round(res,2)))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot_threading():
    csvf = open(fname_output, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    all_data = list(filter(lambda x: x['mode'] != 'sgx-affinity', all_data))
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    datasets = sorted(set(map(lambda x:x['ds'], all_data)), reverse=True)
    modes = sorted(set(map(lambda x:x['mode'], all_data)))
    to_modes = [[y for y in all_data if y['mode'] == x] for x in modes]
    plt.figure(figsize=(9,6))
    for m in range(0, len(modes)):
        mode = modes[m]
        to_datasets = [[y for y in to_modes[m] if y['ds'] == x] for x in datasets]
        for i in range(0, len(datasets)):
            ds = datasets[i]
            data = to_datasets[i]
            splitted_to_algos = [[y for y in data if y['alg'] == x] for x in algos]
            # create new figure
            plt.subplot(2,2,2*m+i+1)
            # print each plot
            for j in range(0, len(algos)):
                th = sorted(list(map(lambda x: int(x['threads']), splitted_to_algos[j])))
                plt.plot(th, list(map(lambda x: float(x['throughput']), splitted_to_algos[j])),
                         '-o', label=algos[j], color=commons.color_alg(algos[j]))
            # make the figure neat
            plt.xlabel("num_threads")
            plt.ylabel("throughput [M rec/ sec]")
            plt.legend()
            plt.title(mode + " - dataset " + ds)
            plt.tight_layout()
    # save fig
    commons.savefig(plot_fname_base + ".png")

    # now print the same thing but only SGX
    mpl.rcParams.update(mpl.rcParamsDefault)
    fig = plt.figure(figsize=(7,3))
    to_datasets = [[y for y in to_modes[1] if y['ds'] == x] for x in datasets]
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
        plt.yticks([0,10,20,30,40])
        plt.ylim(top=40, bottom=-1)
        plt.gca().yaxis.grid(linestyle='dashed')
        fig.text(0.54, 0.26, 'Number of threads', ha='center')
        lines, labels = fig.axes[-1].get_legend_handles_labels()
        fig.legend(lines, labels, fontsize='small', frameon=0,
                   ncol=8, bbox_to_anchor = (0.15, 0.92), loc='lower left',
                   borderaxespad=0, handletextpad=0.25, columnspacing=1.5)
        plt.title('(' + chr(97+i) + ") Dataset $\it{" + ds + "}$", y=-0.4)
    # save fig
    commons.savefig(plot_fname_base + "-sgx.png")


if __name__ == '__main__':

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    threads = 8

    # if config['experiment']:
    #     commons.remove_file(fname_output)
    #     commons.init_file(fname_output, "mode,alg,ds,threads,throughput\n")
    #
    #     for mode in config['modes']:
    #         commons.compile_app(mode, [])
    #         for alg in commons.get_all_algorithms_extended():
    #             for ds in commons.get_test_dataset_names():
    #                 for i in range(threads):
    #                     run_join(mode, commons.PROG, alg, ds, i+1, config['reps'])

    plot_threading()