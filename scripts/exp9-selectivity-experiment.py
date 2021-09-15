#!/usr/bin/python3

import subprocess
import re
import statistics
import matplotlib.pyplot as plt
import csv
import commons
import yaml

fname_output = "data/selectivity-output.csv"
plot_fname_base = "img/selectivity"


def run_join(mode, prog, alg, dataset, threads, reps, selectivity):
    f = open(fname_output, "a")
    results = []
    print("alg: " + alg + ", dataset: " + dataset + ", threads: " + str(threads) +
          ", reps: " + str(reps) + ", selectivity: " + str(selectivity))
    for i in range(0,reps):
        s = str(prog) + " -a " + str(alg) + " -d " + str(dataset) + " -n " + str(threads) + " -l " + str(selectivity)
        if alg == 'INL':
            s = s + ' --sort-s '
        stdout = subprocess.check_output(s, cwd="../",shell=True)\
            .decode('utf-8')
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                s = (alg + " " + dataset + " " + str(threads) + " " + str(throughput))
                results.append(float(throughput))
                print (s)

    res = statistics.mean(results)
    s = (mode + "," + alg + "," + dataset + "," + str(threads) + "," + str(selectivity) + "," + str(round(res,2)))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot():
    csvf = open(fname_output, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    datasets = sorted(set(map(lambda x:x['ds'], all_data)), reverse=True)
    modes = sorted(set(map(lambda x:x['mode'], all_data)))
    to_modes = [[y for y in all_data if y['mode'] == x] for x in modes]
    for m in range(len(modes)):
        fig = plt.figure(figsize=(7,2.5))
        plt.clf()
        mode = modes[m]
        to_datasets = [[y for y in to_modes[m] if y['ds'] == x] for x in datasets]
        for d in range(0, len(datasets)):
            ds = datasets[d]
            to_algos = [[y for y in to_datasets[d] if y['alg'] == x] for x in algos]
            plt.subplot(1,2,d+1)
            for a in range(len(algos)):
                alg = algos[a]
                y = sorted(to_algos[a], key=lambda x: int(x['selectivity']))
                selectivities = sorted(set(map(lambda x:int(x['selectivity']), y)))
                plt.plot(selectivities, list(map(lambda x:float(x['throughput']), y)),
                         '-o', label=alg, color=commons.color_alg(alg),linewidth=2,
                         marker=commons.marker_alg(algos[a]), markeredgecolor='black', markersize=6,
                         markeredgewidth=0.5)
            if d == 0:
                plt.ylabel("Throughput [M rec / s]", size=12)
                plt.ylim(bottom=-2)
            plt.xticks([1, 50, 100])
            plt.gca().yaxis.grid(linestyle='dashed')
            if d == 0:
                plt.title('(' + chr(97+d) + ") Dataset $\it{" + ds + "}$",
                          x=0.45, y=-0.42 , size=12)
            else:
                plt.title('(' + chr(97+d) + ") Dataset $\it{" + ds + "}$",
                          x=0.55, y=-0.42 , size=12)
            # plt.title("Dataset " + ds, y=-0.4)
        fig.text(0.53, 0.26, 'Join selectivity [%]', ha='center')
        lines, labels = fig.axes[-1].get_legend_handles_labels()
        fig.legend(lines, labels, fontsize='small', frameon=0,
                   ncol=8, bbox_to_anchor = (0.15, 0.94), loc='lower left', borderaxespad=0,
                   handletextpad=0.25, columnspacing=1.5)
        commons.savefig(plot_fname_base + '-' + mode + '.png')


if __name__ == '__main__':
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # selectivities = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    selectivities = [0, 1]

    # if config['experiment']:
    #     commons.remove_file(fname_output)
    #     commons.init_file(fname_output, "mode,alg,ds,threads,selectivity,throughput\n")
    #
    #     for mode in ['sgx']:
    #         commons.compile_app(mode)
    #         # algs = commons.get_all_algorithms_extended()
    #         algs = ["PSM", "RHT", "RHO", "RSM"]
    #         for alg in algs:
    #             datasets = commons.get_test_dataset_names()
    #             for ds in datasets:
    #                 for i in selectivities:
    #                     run_join(mode, commons.PROG, alg, ds, config['threads'], config['reps'], i)

    plot()
