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
from matplotlib.patches import Patch

fname_throughput = "data/sorted-input-throughput-output.csv"

def run_join(mode, alg, ds, threads, reps):
    f_throughput = open(fname_throughput, 'a')

    throughput_array = []
    throughput = ''
    dic_phases = {}
    print("Run=" + commons.PROG + " mode=" + mode + " alg=" + alg + " ds=" + ds + " threads=" + str(threads))
    for i in range(reps):
        stdout = subprocess.check_output(commons.PROG + " -a " + alg + " -d " + ds + " -n " + str(threads) + " --sort-r --sort-s", cwd="../",
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

    throughput = statistics.mean(throughput_array)
    s = (mode + "," + alg + "," + ds + "," + str(threads) + "," + str(round(throughput, 2)))
    f_throughput.write(s + '\n')
    f_throughput.close()


def plot_throughput():
    plot_filename = "img/sorted-input-throughput"
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
    plt.clf()
    legend_elements = [Patch(label='Hatches:', alpha=0),
                       Patch(facecolor='white', edgecolor='black',
                             label='TEE unsorted input'),
                       Patch(facecolor='white', edgecolor='black',
                             hatch='..', label='TEE sorted input')]
    to_datasets = [[y for y in all_data if y['ds'] == x] for x in datasets]
    for d in range(0, len(datasets)):
        plt.subplot(1, 2, d+1)
        plt.gca().yaxis.grid(linestyle='dashed')
        to_modes = [[y for y in to_datasets[d] if y['mode'] == x] for x in modes]
        for m in range(0, len(modes)):
            if m == 0:
                br = np.arange(len(algos))
                br = [x - 0.2 for x in br]
            else:
                br = [x + width for x in br]

            label = modes[m]
            colors = list(map(lambda x: commons.color_alg(x['alg']), to_datasets[d]))
            hatch = '.' if modes[m] == 'sgx-sorted' else ''
            plt.bar(br, list(map(lambda x: float(x['throughput']), to_modes[m])),
                    width=width, label=label, hatch=hatch, color=colors, edgecolor='black')
            # plt.xlabel("Join algorithm")
            if d == 0:
                plt.ylabel("Throughput [M rec/s]")
            plt.ylim([0.01, 800])
            plt.yscale('log')
            plt.yticks(size=12)
            plt.xticks(np.arange(len(to_modes[m])), list(map(lambda x:x['alg'],to_modes[m])),
                       rotation=35, size=12)
            plt.title('(' + chr(97+d) + ") Dataset $\it{" + datasets[d] + "}$", y=-0.48)
    fig.legend(handles=legend_elements, ncol=3, frameon=False,
               bbox_to_anchor=(0.035,0.87,1,0), loc="lower left",
               handletextpad=0.5)
    commons.savefig(plot_filename + ".png", tight_layout=True)


if __name__ == '__main__':
    modes = ["sgx", "sgx-sorted"]

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # if config['experiment']:
    #     commons.remove_file(fname_throughput)
    #     commons.init_file(fname_throughput, "mode,alg,ds,threads,throughput\n")
    #
    #     for mode in modes:
    #         commons.compile_app(mode)
    #         for ds in commons.get_test_dataset_names():
    #             for alg in commons.get_all_algorithms():
    #                 run_join(mode, alg, ds, config['threads'], config['reps'])

    plot_throughput()
