#!/usr/bin/python3
import getopt
import shutil
import sys

import yaml

import commons
import subprocess
import statistics
import pandas as pd

filename = "../data/Table-03-Hardware-performance-counters-for-secure-joins.csv"


class Phase:
    def __init__(self):
        self.l2Miss = []
        self.l3Miss = []
        self.l2HitRate = []
        self.l3HitRate = []
        self.IR = []
        self.IPC = []
        self.EWB = []

    def __str__(self):
        return "l2Miss=" + self.l2Miss + ", l3Miss=" + self.l3Miss + ", l2HitRate=" + self.l2HitRate + \
               ", l3HitRate=" + self.l3HitRate + ", IR=" + self.IR + ", IPC=" + self.IPC + ", EWB=" + self.EWB


def run_join(prog, alg, ds, threads, reps, filename, mode):

    f = open(filename, "a")
    dict = {}
    for i in range(0,reps):
        print("RUNNING " + alg + " " + ds + " iteration: " + str(i) + "========================================")
        stdout = subprocess.check_output("sudo " + prog + " -a " + alg + " -d " + ds + " -n " + str(threads),
                                         cwd="../../",shell=True)\
            .decode('utf-8')
        name = ""
        # print(stdout)
        for line in stdout.splitlines():
            if "Get system counter state" in line:
                name = commons.escape_ansi(line.split(": ", 1)[1])
                if name in dict.keys():
                    phase = dict.get(name)
                else:
                    phase = Phase()
            if "L2 cache misses" in line:
                val = int(commons.escape_ansi(line.split(": ", 1)[1]))
                phase.l2Miss.append(val)
            if "L3 cache misses" in line:
                val = int(commons.escape_ansi(line.split(": ", 1)[1]))
                phase.l3Miss.append(val)
            if "L2 cache hit ratio" in line:
                val = float(commons.escape_ansi(line.split(": ", 1)[1]))
                phase.l2HitRate.append(val)
            if "L3 cache hit ratio" in line:
                val = float(commons.escape_ansi(line.split(": ", 1)[1]))
                phase.l3HitRate.append(val)
            if "Instructions retired" in line:
                val = float(commons.escape_ansi(line.split(": ", 1)[1]))
                phase.IR.append(val)
            if "Instructions per Clock" in line:
                val = float(commons.escape_ansi(line.split(": ", 1)[1]))
                phase.IPC.append(val)
            if "EWBcnt" in line:
                val = float(commons.escape_ansi(line.split(": ", 1)[1]))
                phase.EWB.append(val)
            if "end====" in line:
                dict[name] = phase

    for k,v in dict.items():
        v.l2Miss = int(round(statistics.mean(v.l2Miss)))
        v.l3Miss = int(round(statistics.mean(v.l3Miss)))
        v.l2HitRate = round(statistics.mean(v.l2HitRate),2)
        v.l3HitRate = round(statistics.mean(v.l3HitRate),2)
        v.IR = int(round(statistics.mean(v.IR)))
        v.IPC = round(statistics.mean(v.IPC),2)
        v.EWB = round(statistics.mean(v.EWB), 2)

    for k,v in dict.items():
        s = mode + "," + alg + "," + ds + "," + str(threads) + "," + k + "," + str(v.l2Miss) + "," + \
             str(v.l3Miss) + "," + str(v.l2HitRate) + "," + str(v.l3HitRate) + "," + str(v.IR) + "," + \
             str(v.IPC) + "," + str(v.EWB)
        f.write(s + "\n")
        print(s)

    f.close()


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

    mode = "sgx"

    if config['experiment']:
        commons.remove_file(filename)
        commons.init_file(filename,
                          "mode,alg,ds,threads,phase,L2Miss[k],L3Miss[k],L2HitRate,L3HitRate,IR[M],IPC,EWB\n")

        commons.compile_app(mode, flags=["PCM_COUNT", "SGX_COUNTERS"],
                            enclave_config_file='Enclave/Enclave2GB.config.xml')
        for alg in commons.get_all_algorithms_extended():
            for ds in commons.get_test_dataset_names():
                run_join(commons.PROG, alg, ds, config['threads'], config['reps'], filename, mode)
    
    data = pd.read_csv(filename)
    print(data)
    shutil.copy(filename, "../img/Table-03-Hardware-performance-counters-for-secure-joins.csv")
