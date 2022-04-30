#!/usr/bin/python3
import getopt
import os
import subprocess
import re
import sys

import matplotlib.pyplot as plt
import numpy as np
import csv

import yaml

import commons
import statistics
from matplotlib.patches import Patch

fname_throughput = "../data/throughput-top-graph.csv"


def run_encrypted_join(prog, reps, dir):
    f_throughput = open(fname_throughput, 'a')
    throughput_array = []
    throughput = ''
    for i in range(reps):
        stdout = subprocess.check_output('./' + prog, cwd=dir,
                                         shell=True).decode('utf-8')
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[0]
                throughput_array.append(float(throughput))
        print('Throughput = ' + str(throughput) + ' M [rec/s]')

    throughput = statistics.mean(throughput_array)
    s = ('native' + "," + prog + "," + str(1) + "," + str(format(throughput, '.9f')))
    if prog == 'fhejoin':
        s += ',1'
    elif prog == 'dtejoin':
        s += ',0.35'
    else:
        raise Exception('Not supported')

    f_throughput.write(s + '\n')
    f_throughput.close()


def run_join(mode, alg, r, s, threads, reps):

    f_throughput = open(fname_throughput, 'a')
    throughput_array = []
    throughput = ''
    print("Run=" + commons.PROG + " mode=" + mode + " alg=" + alg
          + " r=" + str(r) + " s=" + str(s) + " threads=" + str(threads) + " reps=" + str(reps))
    for i in range(reps):
        stdout = subprocess.check_output(commons.PROG + " -a " + alg + " -r " + str(r) + " -s " + str(s) + " -n " +
                                         str(threads), cwd="../../", shell=True).decode('utf-8')
        print(str(i+1) + '/' + str(reps) + ': ' +
              mode + "," + alg + "," + str(threads))
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                throughput_array.append(float(throughput))

        print('Throughput = ' + str(throughput) + ' M [rec/s]')

    throughput = statistics.mean(throughput_array)
    s = (mode + "," + alg + "," + str(threads) + "," + str(round(throughput, 2)))
    # privacy estimation
    if alg == 'RHO' and mode == 'native':
        s += ',0'
    elif alg == 'RHO' and mode == 'sgx':
        s += ',0.75'
    elif alg == 'RHOBLI' and mode == 'sgx':
        s += ',1'
    else:
        raise Exception('Not supported')

    f_throughput.write(s + '\n')
    f_throughput.close()


def plot_throughput():
    plot_filename = "../img/Figure-01-Secure-joins-throughput"
    csvf = open(fname_throughput, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    modes = sorted(set(map(lambda x:x['mode'], all_data)))

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


    colors = ['#e6ab48', '#e6ab48', '#e6ab48', '#7620b4', '#e6ab48']#list(map(lambda x: commons.color_alg(x), algos))
    throughputs = list(map(lambda x: float(x['throughput']), all_data))
    privacies = list(map(lambda x: float(x['privacy']), all_data))
    names = ['RHO\n(plain)', 'RHO\n(SGX)', 'RHOBLI\n(SGX)', 'NLJ\n(HOM)', 'RHO\n(DET)']

    plt.scatter(throughputs,privacies, s=60, color=colors)
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

    plt.xscale('log')
    plt.ylim(top=1.1, bottom=-0.12)
    plt.yticks(np.array([0,1]), ['weak', 'strong'], rotation=90, size=axis_label_size-1)
    plt.xticks(size=axis_label_size-1)

    plt.savefig(plot_filename + ".png",
                transparent = False,
                bbox_inches = 'tight',
                pad_inches = 0.1,
                dpi=300)
    print("Saved image file: " + plot_filename + ".png")


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

    if config['experiment']:
        r_size = 200000
        s_size = 400000
        commons.remove_file(fname_throughput)
        commons.init_file(fname_throughput, "mode,alg,threads,throughput,privacy\n")
        # native RHO
        print('********** Run native RHO **********')
        commons.compile_app(mode='native')
        run_join('native', 'RHO', r_size, s_size, 4, config['reps'])
        # SGX RHO
        print('********** Run SGX RHO **********')
        commons.compile_app(mode='sgx')
        run_join('sgx', 'RHO', r_size, s_size, 4, config['reps'])
        # SGX RHOBLI
        print('********** Run SGX RHOBLI **********')
        commons.compile_app(mode='sgx', flags=['RADIX_NO_TIMING'])
        run_join('sgx', 'RHOBLI', r_size, s_size, 1, config['reps'])
        # FHE and DET
        print('********** Run native HOM and DET **********')
        DIR = '../../FHE/build'
        try:
            os.mkdir(DIR)
        except OSError as error:
            print(error)
        subprocess.check_output('cmake .. && make', cwd=DIR, shell=True)
        run_encrypted_join('fhejoin',config['reps'], DIR)
        run_encrypted_join('dtejoin',config['reps'], DIR)

    plot_throughput()

