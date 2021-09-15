#!/usr/bin/python3

import subprocess
import re
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
import csv
import commons
import statistics
from matplotlib.patches import Patch

fname_throughput = "data/throughput-top-graph.csv"


def run_join(mode, alg, ds, threads, reps):

    f_throughput = open(fname_throughput, 'a')
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

    # process the first experiment
    if reps > 4:
        throughput_array.remove(max(throughput_array))
        throughput_array.remove(min(throughput_array))

    throughput = statistics.mean(throughput_array)
    s = (mode + "," + alg + "," + ds + "," + str(threads) + "," + str(round(throughput, 2)))
    f_throughput.write(s + '\n')
    f_throughput.close()

    # process the second experiment
    for x in dic_phases:
        if reps > 4:
            dic_phases[x].remove(max(dic_phases[x]))
            dic_phases[x].remove(min(dic_phases[x]))
        res = statistics.mean(dic_phases[x])
        s = mode + "," + alg + "," + ds + "," + x + "," + str(res)
        f_phases.write(s + '\n')

    f_phases.close()


def plot_throughput():
    plot_filename = "img/top-graph"
    csvf = open(fname_throughput, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    # algos = ['CHT', 'PHT', 'PSM', 'RHT', 'RHO', 'RSM']
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    datasets = sorted(set(map(lambda x:x['ds'], all_data)), reverse=True)
    modes = sorted(set(map(lambda x:x['mode'], all_data)))
    # width = 0.4

    # graph per dataset
    plt.rc('axes', axisbelow=True)
    # plt.rcParams.update({'font.size': 12})
    fig = plt.figure(figsize=(2.8,1))
    # plt.clf()
    legend_elements = [Patch(facecolor='white', edgecolor='black',
                             hatch='\\\\', label='plain CPU'),
                       Patch(facecolor='white', edgecolor='black',
                             label='TEE'),
                       Patch(facecolor='white', edgecolor='black',
                             hatch='OO', label='ORAM')]

    to_datasets = [[y for y in all_data if y['ds'] == x] for x in datasets]
    for d in range(0, len(datasets)):
        # ax = plt.subplot(1, 1, d+1)
        # ax.yaxis.grid(linestyle='dashed')
        # to_modes = [[y for y in to_datasets[d] if y['mode'] == x] for x in modes]

        colors = ['#e6ab48', '#e6ab48', '#e6ab48', '#7620b4', '#e6ab48']#list(map(lambda x: commons.color_alg(x), algos))
        # hatch = '\\' if modes[m] == 'native' else ''
        throughputs = list(map(lambda x: float(x['throughput']), to_datasets[d]))
        privacies = list(map(lambda x: float(x['privacy']), to_datasets[d]))
        names = list(map(lambda x: (x['alg']), to_datasets[d]))
        names = ['RHO\n(plain)', 'RHO\n(SGX)', 'RHOBLI\n(SGX)', 'NLJ\n(HOM)', 'RHO\n(DET)']

        # bars = plt.bar(np.arange(len(algos)), list(map(lambda x: float(x['throughput']), to_datasets[d])))
        plt.scatter(throughputs,privacies, s=60, color=colors)
        # for x, y in zip(br, list(map(lambda x: float(x['throughput']), to_datasets[d]))):
        #     if y < 5:
        #         plt.text(x-0.15, y+8, str(y), rotation=90)
        annotation_size = 5
        axis_label_size = 7
        for i, txt in enumerate(names):
            if txt == 'RHO\n(plain)':
                plt.annotate(txt, (throughputs[i]-155, privacies[i]+0.11), size=annotation_size)
            elif txt == 'RHO\n(SGX)':
                plt.annotate(txt, (throughputs[i]+14, privacies[i]), size=annotation_size)
            elif txt == 'RHOBLI\n(SGX)':
                plt.annotate(txt, (throughputs[i]-0.0001, privacies[i]-0.34), size=annotation_size)
            elif txt == 'RHO\n(DET)':
                plt.annotate(txt, (throughputs[i]-7.2, privacies[i]-0.2), size=annotation_size)
            elif txt == 'NLJ\n(HOM)':
                plt.annotate(txt, (throughputs[i]-0.0000001, privacies[i]-0.33), size=annotation_size)
            else:
                plt.annotate(txt, (throughputs[i], privacies[i]), size=annotation_size)
        plt.xlabel("Throughput [M rec/s]", size=axis_label_size)
        plt.ylabel('Estimated Privacy', size=axis_label_size)
        plt.gca().xaxis.grid(linestyle='dashed')
        # plt.frame(on=None)
        # patterns = ('\\','','O')
        # for bar, pattern, color in zip(bars, patterns, colors):
        #     bar.set_hatch(pattern)
        #     bar.set_color(color)
        #     bar.set_edgecolor('black')
        plt.xscale('log')
        plt.ylim(top=1.1, bottom=-0.12)
        plt.yticks(np.array([0,1]), ['weak', 'strong'], rotation=90, size=axis_label_size-1)
        plt.xticks(size=axis_label_size-1)
        # plt.xticks(np.arange(3), ['RHO\n(CPU)', 'RHO\n(TEE)', 'RHOBLI'])
        # plt.minorticks_off()
        # plt.title('(' + chr(97+d) + ") Dataset $\it{" + datasets[d] + "}$", y=-0.2)

    # fig.legend(handles=legend_elements, ncol=2, frameon=False,
    #            bbox_to_anchor=(0.095,0.91,1,0), loc="lower left")
    # commons.savefig(plot_filename + ".png")
    # plt.tight_layout()
    plt.savefig(plot_filename + ".png",
                transparent = False,
                bbox_inches = 'tight',
                pad_inches = 0.1,
                dpi=300)


if __name__ == '__main__':
    reps = 5
    threads = 4
    modes = ["native", "sgx"]

    # commons.remove_file(fname_throughput)
    # commons.init_file(fname_throughput, "mode,alg,ds,threads,throughput\n")
    #
    # commons.remove_file(fname_phases)
    # commons.init_file(fname_phases, "mode,alg,ds,phase,cycles\n")
    #
    # for mode in modes:
    #     commons.compile_app(mode)
    #     for ds in commons.get_test_dataset_names():
    #         for alg in commons.get_all_algorithms():
    #             run_join(mode, alg, ds, threads, reps)

    plot_throughput()

