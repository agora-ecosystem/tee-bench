#!/usr/bin/python3
import getopt
import subprocess
import statistics
import sys

import matplotlib as mpl
import matplotlib.pyplot as plt
import csv
import commons
import yaml

fname_output = "../data/seal-chunk-size-output.csv"
plot_fname_base = "../img/Figure-09-Chunking-impact"


def join(mode, alg, dataset, threads, chunkSize, reps):
    f = open(fname_output, "a")
    results = []

    for i in range(0,reps):
        s = "./app -a " + str(alg) + " -d " + str(dataset) + " -n " + str(threads) + \
            " -c " + str(chunkSize) + " --seal "
        stdout = subprocess.check_output(s, cwd="../../", shell=True)\
            .decode('utf-8')
        # print(stdout)
        for line in stdout.splitlines():
            if "seal_micros" in line:
                seal_micros = float(commons.escape_ansi(line.split(" = ", 1)[1]))
                s = (alg + " " + dataset + " " + str(chunkSize) + " " + str(seal_micros/1000000))
                results.append(seal_micros)
                print(s)

    res = statistics.mean(results)
    s = (mode + "," + alg + "," + dataset + "," + str(chunkSize) + "," + str(round(res,2)))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot():
    csvf = open(fname_output, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    datasets = sorted(set(map(lambda x:x['ds'], all_data)), reverse=True)
    plt.figure(figsize=(9,6))
    to_datasets = [[y for y in all_data if y['ds'] == x] for x in datasets]

    # now print the same thing but only SGX
    mpl.rcParams.update(mpl.rcParamsDefault)
    fig = plt.figure(figsize=(3.5,2.6))
    plt.rc('axes', axisbelow=True)
    colors = ['#629246', '#e6b6c2']

    plt.axvline(x=8192, linestyle='--', color='#949494', linewidth=1.5)
    plt.axvline(x=40960, linestyle='--', color=colors[0], linewidth=1.5)
    plt.axvline(x=409600, linestyle='--', color=colors[1], linewidth=1.5)

    for i in range(0, len(datasets)):
        ds = datasets[i]
        data = to_datasets[i]
        # print each plot
        chunkSizes = list(map(lambda x: int(x['chunkSize']), data))
        plt.plot(chunkSizes, list(map(lambda x: float(x['sealMicros'])/1000000, data)),
                 '-o', label=ds, linewidth=2.5,
                 markeredgecolor='black', markersize=6,
                 markeredgewidth=0.5, color=colors[i])

    # make the figure neat
    label_size=12
    plt.ylabel('Seal time [ms]', fontsize=label_size, labelpad=2)
    plt.xlabel('Seal chunk size [kB]', fontsize=label_size, labelpad=2)
    # plt.legend()
    plt.xscale('log')
    plt.yscale('log')
    ticks_size=9
    plt.xticks([10,1000,100000],size=ticks_size)
    plt.yticks(size=ticks_size)
    # plt.gca().yaxis.grid(linestyle='dashed')
    lines, labels = fig.axes[-1].get_legend_handles_labels()
    fig.legend(lines, labels, fontsize='8', frameon=0,
               ncol=6, bbox_to_anchor = (0.28, 0.9), loc='lower left', borderaxespad=0.0001)
    # plt.vlines(x=40960, linestyle='--', color=colors[0], linewidth=1.5, ymax=0.240842, ymin=0)
    # plt.vlines(x=409600, linestyle='--', color=colors[1], linewidth=1.5, ymax=3.990599, ymin=0)

    # plt.vlines(x=40960, linestyle='--', color=colors[0], linewidth=1.5, ymax=1.2, ymin=0)
    # plt.vlines(x=409600, linestyle='--', color=colors[1], linewidth=1.5, ymax=9.9, ymin=0)

    # fig.text(0.76,0.44, "cache-fit\noutput", rotation=0,size=7, ha='right')
    # fig.text(0.89,0.8, "cache-exceed\noutput", rotation=0,size=7, ha='right')
    text_size=8
    fig.text(0.685,0.7, "L3 cache", rotation=90,size=text_size, ha='right')
    fig.text(0.8,0.44, "cache-fit\noutput", rotation=90,size=text_size, ha='right')
    fig.text(0.92,0.43, "cache-exceed\noutput", rotation=90,size=text_size, ha='right')
    # save fig
    commons.savefig(plot_fname_base + ".png")


if __name__ == '__main__':
    mode = 'sgx-seal'

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

    if config['experiment']:
        stops_per_factor = 1#4
        min_elems_factor = 1
        max_elems_factor = 20

        min = stops_per_factor * min_elems_factor
        max = stops_per_factor * max_elems_factor

        chunk_sizes = [0]

        for i in range(min, max, 1):
            chunk_size = int(pow(2, i/stops_per_factor))
            chunk_sizes.append(chunk_size)

        chunk_sizes = sorted(set(chunk_sizes))
        # print(chunk_sizes)

        commons.remove_file(fname_output)
        commons.init_file(fname_output, "mode,alg,ds,chunkSize,sealMicros\n")

        commons.compile_app(mode, enclave_config_file='Enclave/Enclave4GB.config.xml')
        for ds in commons.get_test_dataset_names():
            for i in chunk_sizes:
                join(mode, 'RHO', ds, config['threads'], i, config['reps'])

    plot()
