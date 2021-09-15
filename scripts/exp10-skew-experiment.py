#!/usr/bin/python3

import commons
import subprocess
import csv
import matplotlib.pyplot as plt
import re
import yaml

def run_join(prog, mode, alg, ds, threads, skew, reps, fname):
    f = open(fname, "a")
    res = 0
    for i in range(0, reps):
        stdout = subprocess.check_output(prog + " -a " + alg + " -d " + ds + " -n " + str(threads) + " -z " + str(skew), cwd="../",shell=True).decode('utf-8')
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                s = (mode + "," + alg + "," + ds + "," + str(threads) + "," + str(throughput))
                res += float(throughput)

    res /= reps
    s = (mode + "," + alg + "," + ds + "," + str(skew) + "," + str(round(res, 2)))
    print(s)
    f.write(s + "\n")
    f.close()


def plot(fname):
    plot_filename = "img/skew-plot.png"
    csvf = open(fname, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    datasets = sorted(set(map(lambda x:x['ds'], all_data)), reverse=True)
    modes = sorted(set(map(lambda x:x['mode'], all_data)))
    to_modes = [[y for y in all_data if y['mode'] == x] for x in modes]
    fig = plt.figure(figsize=(6,2.6))
    for m in range(1, len(modes)):
        mode = modes[m]
        to_datasets = [[y for y in to_modes[m] if y['ds'] == x] for x in datasets]
        for d in range(0, len(datasets)):
            ds = datasets[d]
            to_algos = [[y for y in to_datasets[d] if y['alg'] == x] for x in algos]
            plt.subplot(1,2,d+1)
            plt.gca().yaxis.grid(linestyle='dashed')
            for a in range(0, len(algos)):
                alg = algos[a]
                skews = list(map(lambda x:x['skew'], to_algos[a]))
                plt.plot(skews, list(map(lambda x:float(x['throughput']), to_algos[a])),
                         '-o', label=alg, color=commons.color_alg(alg), linewidth=2,
                         marker=commons.marker_alg(algos[a]), markeredgecolor='black', markersize=6,
                         markeredgewidth=0.5)
            fig.text(0.54, 0.29, 'Zipf factor', ha='center')
            if d == 0:
                plt.ylabel("Throughput [M rec / s]")
            plt.ylim(ymin=-1)
            if d == 0:
                plt.yticks((0, 20, 40, 60))
            else:
                plt.yticks((0, 10, 20))
            lines, labels = fig.axes[-1].get_legend_handles_labels()
            fig.legend(lines, labels, fontsize=7.5, frameon=0,
                   ncol=8, bbox_to_anchor = (0.13, 0.92), loc='lower left', borderaxespad=0,
                   handletextpad=0.25, columnspacing=1.5, handlelength=2)
            plt.title('(' + chr(97+d) + ") Dataset $\it{" + datasets[d] + "}$", y=-0.45)

    commons.savefig(plot_filename)
    

if __name__ == '__main__':
    timer = commons.start_timer()
    skews = [0.5, 0.6, 0.7, 0.8, 0.9, 0.99]
    res_file = "data/skew-output.csv"

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # if config['experiment']:
    #     commons.remove_file(res_file)
    #     commons.init_file(res_file, "mode,alg,ds,skew,throughput\n")
    #
    #     for mode in ['sgx']:
    #         commons.compile_app(mode)
    #         for ds in commons.get_test_dataset_names():
    #             for alg in commons.get_all_algorithms_extended():
    #                 for skew in skews:
    #                     run_join(commons.PROG, mode, alg, ds, config['threads'], skew, config['reps'], res_file)

    plot(res_file)
    commons.stop_timer(timer)
