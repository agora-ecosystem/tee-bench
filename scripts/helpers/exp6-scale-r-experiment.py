#!/usr/bin/python3
import getopt
import sys

import yaml

import commons
import re
import statistics
import subprocess
import csv
import matplotlib.pyplot as plt

filename = "../data/scale-r-output.csv"
mb_of_data = 131072


def run_join(prog, alg, size_r, size_s, threads, reps, mode):
    f = open(filename, "a")
    results = []
    ewbs = []
    for i in range(0,reps):
        stdout = subprocess.check_output(prog + " -a " + alg + " -r " + str(size_r) + " -s " + str(size_s) +
                                         " -n " + str(threads), cwd="../../", shell=True) \
            .decode('utf-8')
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                results.append(float(throughput))
                print ("Throughput = " + str(throughput))
            elif "EWB :" in line:
                ewb = int(re.findall(r'\d+', line)[-2])
                print("EWB = " + str(ewb))
                ewbs.append(ewb)
    if len(results) == 0:
        results = [-1]
    if len(ewbs) == 0:
        ewbs = [-1]
    res = statistics.mean(results)
    ewb = int(statistics.mean(ewbs))
    s = (mode + "," + alg + "," + str(threads) + "," + str(round(size_r/mb_of_data,2)) +
         "," + str(round(size_s/mb_of_data,2)) + "," + str(round(res,2)) + "," + str(ewb))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot():
    s_sizes_names = [
        '$S_{size}$ < L2',
        'L2 < $S_{size}$ < L3',
        'L3 < $S_{size}$ < EPC',
        'EPC < $S_{size}$'
    ]
    csvf = open(filename, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    r_sizes = sorted(set(map(lambda x:float(x['sizeR']), all_data)))
    s_sizes = sorted(set(map(lambda x:float(x['sizeS']), all_data)))
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    splitted = [[[y['alg'], y['sizeR'], y['sizeS'], y['throughput']] for y in all_data if y['sizeS'] == str(x)] for x in s_sizes]
    titles = ['S < L2', 'L2 < S < L3', 'L3 < S < EPC', 'EPC < S']
    # fig,a = plt.subplots(3,2,figsize=(10,5))
    # for i in range(0, len(s_sizes)):
    #     ds = splitted[i]
    #     ds = [[[y[0], y[1], y[2], y[3]] for y in ds if y[0] == x] for x in algos]
    #     x = 1 if i & (1<<1) else 0
    #     y = 1 if i & (1<<0) else 0
    #     for j in range(0, len(algos)):
    #         x_sizes = list(filter(lambda x: x['alg'] == algos[j], all_data))
    #         x_sizes = sorted(set(map(lambda x:float(x['sizeR']), x_sizes)))
    #         a[x][y].plot(x_sizes, list(map(lambda x: float(x[3]),ds[j])), '-o', label=algos[j],
    #                      color=commons.color_alg(algos[j]))
    #     a[x][y].legend()
    #     a[x][y].set_xlabel('R size [MB]')
    #     a[x][y].set_ylabel('Throughput [M rec/s]')
    #     a[x][y].set_title(titles[i] + ' (' + str(s_sizes[i]) + ' MB)')
    #
    # commons.savefig('img/scale-r.png')

    # print graphs per algorithm
    # fig = plt.figure(figsize=(8,6))
    # plt.clf()
    # for alg in algos:
    #     data = list(filter(lambda x: x['alg'] == alg, all_data))
    #     data_splitted = [[y for y in data if y['sizeS'] == str(x)] for x in s_sizes]
    #     plt.subplot(3,2,algos.index(alg)+1)
    #     for i in range(0, len(s_sizes)):
    #         x_sizes = list(filter(lambda x: x['alg'] == alg, all_data))
    #         x_sizes = sorted(set(map(lambda x:float(x['sizeR']), x_sizes)))
    #         plt.plot(x_sizes, list(map(lambda x: float(x['throughput']), data_splitted[i])),
    #                  '-o', label=s_sizes_names[i], color=commons.color_size(i))
    #     if alg == 'PHT':
    #         plt.legend()
    #     plt.gca().yaxis.grid(linestyle='dashed')
    #     plt.xlabel('R size [MB]')
    #     plt.ylabel('Throughput [M rec/s]')
    #     plt.title(alg)
    #     plt.ylim([0,70])
    # commons.savefig('img/scale-r-algos.png')

    # print only CHT
    fig = plt.figure(figsize=(4,3))
    # plt.clf()
    data = list(filter(lambda x: x['alg'] == 'CHT', all_data))
    data_splitted = [[y for y in data if y['sizeS'] == str(x)] for x in s_sizes]
    markers = ['o', 'v', 'D', 's']
    for i in range(0, len(s_sizes)):
        x_sizes = list(filter(lambda x: x['alg'] == 'CHT', all_data))
        x_sizes = sorted(set(map(lambda x:float(x['sizeR']), x_sizes)))
        plt.plot(x_sizes, list(map(lambda x: float(x['throughput']), data_splitted[i])),
                 label=s_sizes_names[i], color=commons.color_size(i), linewidth=2,
                 marker=markers[i], markersize=8, markeredgecolor='black',
                 markeredgewidth=0.3)
    # plt.legend(fontsize='small')
    lines, labels = fig.axes[-1].get_legend_handles_labels()
    fig.legend(lines, labels, fontsize='x-small', frameon=0,
               ncol=2, bbox_to_anchor = (0.05, 0.95), loc='lower left', borderaxespad=0)
    plt.gca().yaxis.grid(linestyle='dashed')
    plt.xlabel('Size of outer table [MB]')
    plt.ylabel('Throughput [M rec/s]')
    plt.xlim(left=0)
    plt.ylim(bottom=0)
    commons.savefig('../img/Figure-16-CHTs-throughput-scaling-the-outer-relation.png')

    # print only PHT
    # fig = plt.figure(figsize=(4,4))
    # plt.clf()
    # data = list(filter(lambda x: x['alg'] == 'PHT', all_data))
    # data_splitted = [[y for y in data if y['sizeS'] == str(x)] for x in s_sizes]
    # for i in range(0, len(s_sizes)):
    #     x_sizes = list(filter(lambda x: x['alg'] == 'PHT', all_data))
    #     x_sizes = sorted(set(map(lambda x:float(x['sizeR']), x_sizes)))
    #     plt.plot(x_sizes, list(map(lambda x: float(x['throughput']), data_splitted[i])),
    #              '-o', label=s_sizes_names[i], color=commons.color_size(i))
    # plt.legend(fontsize='small')
    # plt.gca().yaxis.grid(linestyle='dashed')
    # plt.xlabel('R size [MB]')
    # plt.ylabel('Throughput [M rec/s]')
    # plt.title('PHT')
    # # plt.ylim([0,70])
    # commons.savefig('img/scale-r-PHT.png')

def plot_with_ewb():
    csvf = open(filename, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    r_sizes = sorted(set(map(lambda x:float(x['sizeR']), all_data)))
    s_sizes = sorted(set(map(lambda x:float(x['sizeS']), all_data)))
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    splitted = [[y for y in all_data if y['sizeS'] == str(x)] for x in s_sizes]
    titles = ['S < L2', 'L2 < S < L3', 'L3 < S < EPC', 'EPC < S']
    width = 1
    # fig,ax1 = plt.subplots(2,2,figsize=(20,10))
    # for i in range(0, len(s_sizes)):
    #     ds = splitted[i]
    #     ds = [[y for y in ds if y['alg'] == x] for x in algos]
    #     x = 1 if i & (1<<1) else 0
    #     y = 1 if i & (1<<0) else 0
    #     for j in range(0, len(algos)):
    #         ax1[x][y].plot(r_sizes, list(map(lambda x: float(x['throughput']),ds[j])),
    #                        '-o', label=algos[j], color=commons.color_alg(algos[j]))
    #         ax1[x][y].legend()
    #         ax1[x][y].set_xlabel('R size [MB]')
    #         ax1[x][y].set_ylabel('Throughput [M rec/s]')
    #         axes2 = ax1[x][y].twinx()
    #         br = [float(x + width*j) for x in r_sizes]
    #         axes2.bar(br, list(map(lambda x: float(x['ewb']), ds[j])), width=width,
    #                   label=algos[j], color=commons.color_alg(algos[j]), alpha=0.5)
    #         axes2.set_ylabel('EWB [MB]')
    #         axes2.set_ylim([0,65000])
    #
    #     plt.title(titles[i] + ' (' + str(s_sizes[i]) + ' MB)')
    #
    # commons.savefig('img/scale-r-with-ewb' + '.png')

    # plot only PHT for S < L2
    # fig, ax1 = plt.subplots(figsize=(5,4))
    # # plt.clf()
    # data = list(filter(lambda x: x['alg'] == 'PHT' and x['sizeS'] == '0.2', all_data))
    # rs = list(map(lambda x:float(x['sizeR']), data))
    # plot = list(map(lambda x: float(x['throughput']), data))
    # bar = list(map(lambda x: float(x['ewb']), data))
    # ax1.plot(rs, plot, '-o', color=commons.color_alg('PHT'))
    # ax1.set_xlabel('R size [MB]')
    # ax1.set_ylabel('Throughput [M rec/s]')
    # ax2 = ax1.twinx()
    # ax2.bar(rs, bar, color=commons.color_size(3), alpha=0.4, width=3)
    # ax2.set_ylabel('EPCMiss [k]')
    # commons.savefig('img/scale-r-with-ewb' + '-PHT.png')

    # plot only CHT for EPC < S
    # fig, ax1 = plt.subplots(figsize=(5,4))
    # plt.clf()
    fig = plt.figure(figsize=(5,4))
    ax1 = plt.gca()
    # plt.clf()
    data = list(filter(lambda x: x['alg'] == 'CHT' and x['sizeS'] == '100.0', all_data))
    rs = list(filter(lambda x: x['alg'] == 'CHT', all_data))
    rs = sorted(set(map(lambda x:float(x['sizeR']), rs)))
    plot = list(map(lambda x: float(x['throughput']), data))
    bar = list(map(lambda x: int(float(x['ewb'])/1000), data))
    line1, = ax1.plot(rs, plot, color=commons.color_alg('CHT'), linewidth=2,
                      marker=commons.marker_alg('CHT'), markeredgecolor='black', markersize=8, label='Throughput')
    ax1.set_xlabel('Size of outer table [MB]')
    ax1.set_ylabel('Throughput [M rec/s]')
    ax1.set_xlim([0,130])
    ax1.set_ylim(bottom=0)
    ax2 = ax1.twinx()
    bar2 = ax2.bar(rs, bar, color=commons.color_size(3), alpha=0.4, width=3,
                   label='EPC Miss')
    ax2.set_ylabel('EPC Miss [k]')
    ax2.set_ylim(bottom=0)
    ax1.yaxis.grid(linestyle='dashed')
    ax1.axvline(x=90, linestyle='--', color='#209bb4', linewidth=2)
    fig.text(0.55,0.77, "EPC", color='#209bb4', rotation=90, weight='bold')
    fig.legend(handles=[line1, bar2], ncol=2, frameon=False,
               bbox_to_anchor=(0.08,0.91,1,0), loc="lower left")
    commons.savefig('../img/Figure-05-CHTs-throughput-and-EPC-paging.png')


if __name__ == '__main__':
    timer = commons.start_timer()
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

    mode = "sgx"
    max_r_size_mb = 128
    s_sizes = [int(0.2*mb_of_data),   # ~205kB
               int(6.4 * mb_of_data), # 6.4 MB
               16 * mb_of_data,       # 16 MB
               100 * mb_of_data]      # 100 MB

    if config['experiment']:
        commons.compile_app(mode, flags=["SGX_COUNTERS"])
        commons.remove_file(filename)
        commons.init_file(filename, "mode,alg,threads,sizeR,sizeS,throughput,ewb\n")

        for s_size in s_sizes:
            for alg in ['CHT']:#commons.get_all_algorithms():
                for i in range(8, max_r_size_mb+1, 8):
                    run_join(commons.PROG, alg, i * mb_of_data, s_size, config['threads'], config['reps'], mode)

    plot()
    plot_with_ewb()
    commons.stop_timer(timer)
