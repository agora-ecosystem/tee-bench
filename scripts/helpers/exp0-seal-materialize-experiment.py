#!/usr/bin/python3
import getopt
import subprocess
import re
import sys

import yaml
from matplotlib.patches import Patch
import matplotlib.pyplot as plt
import numpy as np
import csv
import commons
import statistics

phases_file = "../data/seal-phases.csv"
img_phases = '../img/Figure-10-Materialization-and-data-sealing-impact-on-RHO.png'


def join(mode, alg, ds, sel, reps):
    pf = open(phases_file, 'a')

    throughput_array = []
    join_cycles_array = []
    seal_cycles_array = []

    if mode == 'sgx-seal-chunk-buffer':
        alg = 'RHO_seal_buffer'

    print("Run " + commons.PROG + " mode=" + mode +
          " alg=" + alg + " ds=" + ds + "sel=" + str(sel))
    for i in range(reps):
        command = commons.PROG + " -a " + alg + " -d " + ds + " -n 3 " + " -l " + str(sel)
        if mode == 'sgx-seal' or mode == 'sgx-seal-chunk-buffer':
            command += ' --seal '

        stdout = subprocess.check_output(command, cwd="../../",
                                         shell=True).decode('utf-8')
        # print(stdout)
        for line in stdout.splitlines():
            if "throughput = " in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                throughput_array.append(float(throughput))
                print(str(throughput) + " : " + line)
            if 'Phase Total (cycles)' in line:
                join_cycles = float(commons.escape_ansi(line.split(": ", 1)[1]))
                #join_cycles = re.findall("\d+\.\d+", line)[1]
                join_cycles_array.append(float(join_cycles))
                print(str(join_cycles) + " : " + line)
            if 'seal_timer' in line or 'retrieve_data_timer' in line:
                seal_cycles = float(commons.escape_ansi(line.split(" = ", 1)[1]))
                #seal_cycles = re.findall("\d+\.\d+", line)[1]
                seal_cycles_array.append(float(seal_cycles))
                print(str(seal_cycles) + " : " + line)

    throughput = statistics.mean(throughput_array) if len(throughput_array) > 0 else 0 
    join_cycles = statistics.mean(join_cycles_array) if len(join_cycles_array) > 0 else 0
    seal_cycles = statistics.mean(seal_cycles_array) if len(seal_cycles_array) > 0 else 0

    s = mode + ',' + alg + ',' + ds + ',' + str(sel) + ',' + str(throughput)
    print('Average throughput: ' + s)
    if mode == 'sgx-seal-chunk-buffer':
        cycles = join_cycles + seal_cycles
        s = mode + ',' + alg + ',' + ds + ',' + str(sel) + ',seal-join,' + str(cycles)
        print('Average seal-join cycles: ' + s)
        pf.write(s + '\n')
    else:
        s = mode + ',' + alg + ',' + ds + ',' + str(sel) + ',join,' + str(join_cycles)
        print('Average join_cycles: ' + s)
        pf.write(s + '\n')
        s = mode + ',' + alg + ',' + ds + ',' + str(sel) + ',seal,' + str(seal_cycles)
        print('Average seal_cycles: ' + s)
        pf.write(s + '\n')
    pf.close()


def plot_with_selectivities():
    csvf = open(phases_file, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    modes = sorted(set(map(lambda x:x['mode'], all_data)))
    datasets = sorted(set(map(lambda x:x['ds'], all_data)), reverse=True)
    selectivities = sorted(set(map(lambda x:float(x['selectivity']), all_data)))
    colors = {'join': commons.color_alg('RHO'),
              'materialize':'white',
              'seal':'white',
              'seal-join': commons.color_alg('RHO')}
    hatches = {'plain-join': '\\\\','join':'', 'materialize':'xx', 'seal':'/', 'seal-join':'/'}
    xticks = list(map(lambda x: str(int(x)) + '%', selectivities))
    fig = plt.figure(figsize=(8,4))
    plt.rc('axes', axisbelow=True)
    plt.rcParams.update({'font.size': 15})
    to_datasets = [[y for y in all_data if y['ds'] == x] for x in datasets]
    width=0.25
    for d in range(len(datasets)):
        ax = plt.subplot(1, 2, d+1)
        ax.yaxis.grid(linestyle='dashed')
        to_modes = [[y for y in to_datasets[d] if y['mode'] == x] for x in modes]
        x = -0.2
        agg = 0
        for m in reversed(range(len(modes))): # take sgx-seal first then sgx
            x=-0.2
            if 'sgx-seal-chunk-buffer' in modes[m]:
                x += 2*width
            elif 'sgx' in modes[m]:
                x += width
            to_sels = [[y for y in to_modes[m] if float(y['selectivity']) == x] for x in selectivities]
            for s in range(len(selectivities)):
                phases = sorted(set(map(lambda x:x['phase'], to_sels[s])))
                for p in range(len(phases)):
                    numtuples = 6553600 if datasets[d] == 'cache-fit' else 65536000
                    val = float(to_sels[s][p]['cycles'])/numtuples
                    if (to_modes[m][p]['mode'] == 'sgx-seal' or to_modes[m][p]['mode'] == 'native-materialize') and to_modes[m][p]['phase'] == 'join':
                        l='materialize'
                    else:
                        l=to_modes[m][p]['phase']
                    if 'native' in modes[m] and l == 'join':
                        hatch = hatches['plain-join']
                    else:
                        hatch = hatches[l]
                    plt.bar(x, val, bottom=agg,label=l,hatch=hatch,
                            color=colors[l], edgecolor='black', width=width)
                    agg += val
                agg = 0
                x += 1
        x += width
        if d == 0:
            plt.ylabel("CPU cycles / tuple")
        plt.ylim(top=550)
        plt.xticks(np.arange(len(xticks)), xticks, rotation=0, fontsize=12)
        plt.title('(' + chr(97+d) + ") Dataset $\it{" + datasets[d] + "}$", y=-0.35)
    leg_elements = [
        Patch(label='Hatches:', alpha=0),
        Patch(facecolor='white', edgecolor='black',label='TEE join   '),
        Patch(facecolor='white', edgecolor='black',label='plain join ', hatch='\\\\\\'),
        Patch(facecolor='white', edgecolor='black',
              label='materialize', hatch='xxx'),
        Patch(facecolor='white', edgecolor='black',
              label='unseal+seal', hatch='///')
    ]
    fig.text(0.54, 0.2, 'Join selectivity [%]', ha='center', fontsize=15)
    fig.legend(handles=leg_elements, ncol=5, frameon=False,
               loc="upper left", bbox_to_anchor=(0.005,1.04,1,0), fontsize=12,
               handletextpad=0.5)

    fig.text(0.145,0.35, 'Plain CPU', rotation=90, fontsize=12, backgroundcolor='white',
             bbox=dict(boxstyle='square,pad=0.1', fc='white', ec='none'))
    fig.text(0.175,0.51, 'TEE', rotation=90, fontsize=12, backgroundcolor='white',
             bbox=dict(boxstyle='square,pad=0.1', fc='white', ec='none'))
    fig.text(0.21,0.64, 'TEE-optimized', rotation=90, fontsize=12, backgroundcolor='white',
             bbox=dict(boxstyle='square,pad=0.1', fc='white', ec='none'))
    commons.savefig(img_phases)


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

    modes = ['native','native-materialize','sgx', 'sgx-seal', 'sgx-seal-chunk-buffer']
    selectivities = [1, 50, 100]
    alg = 'RHO'

    if config['experiment']:
        commons.remove_file(phases_file)
        commons.init_file(phases_file, 'mode,alg,ds,selectivity,phase,cycles\n')

        for mode in modes:
            commons.compile_app(mode, enclave_config_file='Enclave/Enclave4GB.config.xml')
            for ds in commons.get_test_dataset_names():
                for sel in selectivities:
                    join(mode, alg, ds, sel, config['reps'])

    plot_with_selectivities()

