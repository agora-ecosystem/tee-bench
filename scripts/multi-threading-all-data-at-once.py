#!/usr/bin/python3

import subprocess
import re
import statistics
import matplotlib.pyplot as plt
import csv
import commons

fname_output = "data/all-data-at-once.csv"
plot_fname_base = "img/multi-threading-tmp"

class Phase:
    def __init__(self):
        self.l2Miss = []
        self.l3Miss = []
        self.l2HitRate = []
        self.l3HitRate = []
        self.IR = []
        self.IPC = []
        self.EWB = []
        self.cycles = []

    def __str__(self):
        return "l2Miss=" + self.l2Miss + ", l3Miss=" + self.l3Miss + ", l2HitRate=" + self.l2HitRate + \
               ", l3HitRate=" + self.l3HitRate + ", IR=" + self.IR + ", IPC=" + self.IPC + ", EWB=" + self.EWB + \
                ", cycles=" + self.cycles


def run_join(mode, prog, alg, dataset, threads, reps):
    f = open(fname_output, "a")
    results = []
    # TODO: remove this for the final experiment
    if dataset == "small10x" and mode != "native" and (alg == "PHT" or alg == 'CHT'):
        reps = 1

    dict = {}
    dic_phases = {}

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
            if "Get system counter state" in line:
                name = commons.escape_ansi(line.split(": ", 1)[1])
                if name in dict.keys():
                    phase = dict.get(name)
                else:
                    phase = Phase()
                print("New phase:" + name)
            if "L2 cache misses" in line:
                val = int(commons.escape_ansi(line.split(": ",1)[1]))
                phase.l2Miss.append(val)
                print("L2 cache misses: " + str(val))
            if "L3 cache misses" in line:
                val = int(commons.escape_ansi(line.split(": ",1)[1]))
                phase.l3Miss.append(val)
                print("L3 cache misses: " + str(val))
            if "L2 cache hit ratio" in line:
                val = float(commons.escape_ansi(line.split(": ",1)[1]))
                phase.l2HitRate.append(val)
                print("L2 cache hit ratio: " + str(val))
            if "L3 cache hit ratio" in line:
                val = float(commons.escape_ansi(line.split(": ",1)[1]))
                phase.l3HitRate.append(val)
                print("L3 cache hit ratio: " + str(val))
            if "Instructions retired" in line:
                val = float(commons.escape_ansi(line.split(": ",1)[1]))
                phase.IR.append(val)
                print("Instructions retired: " + str(val))
            if "Instructions per Clock" in line:
                val = float(commons.escape_ansi(line.split(": ",1)[1]))
                phase.IPC.append(val)
                print("IPC: " + str(val))
            if "EWB [MB]" in line:
                val = float(commons.escape_ansi(line.split(": ",1)[1]))
                phase.EWB.append(val)
                print("EWB: " + str(val))
            if "Phase" in line:
                words = line.split()
                phase_name = words[words.index("Phase") + 1]
                value = int(re.findall(r'\d+', line)[-2])
                print (phase_name + " = " + str(value))
                if phase_name in dic_phases:
                    dic_phases[phase_name].append(value)
                else:
                    dic_phases[phase_name] = [value]
                print(phase_name + ": " + str(value))
            if "end====" in line:
                dict[name] = phase
                print("end of phase")
    # remove max and min values as extreme outliers
    if reps > 3:
        results.remove(max(results))
        results.remove(min(results))

    res = statistics.mean(results)
    s = (mode + "," + alg + "," + dataset + "," + str(threads) + "," + str(round(res,2)))
    f.write(s + "\n")

    for k,v in dict.items():
        v.l2Miss = int(round(statistics.mean(v.l2Miss)))
        v.l3Miss = int(round(statistics.mean(v.l3Miss)))
        v.l2HitRate = round(statistics.mean(v.l2HitRate),2)
        v.l3HitRate = round(statistics.mean(v.l3HitRate),2)
        v.IR = int(round(statistics.mean(v.IR)))
        v.IPC = round(statistics.mean(v.IPC),2)
        v.EWB = round(statistics.mean(v.EWB), 2)

    s = ''
    for x in dic_phases:
        res = round(statistics.mean(dic_phases[x]))
        # dict[]
        s = alg + "," + ds + "," + str(threads) + x + "," + str(res)

    for k,v in dict.items():
        s = alg + "," + ds + "," + str(threads) + "," + k + "," + str(v.l2Miss) + "," + \
            str(v.l3Miss) + "," + str(v.l2HitRate) + "," + str(v.l3HitRate) + "," + str(v.IR) + "," + \
            str(v.IPC) + "," + str(v.EWB)
        f.write(s + "\n")
        print(s)



    f.close()


if __name__ == '__main__':
    reps = 1
    threads = 8
    modes = ["sgx"]

    commons.remove_file(fname_output)
    commons.init_file(fname_output, "alg,ds,threads,phase,L2Miss[k],L3Miss[k],L2HitRate,L3HitRate,IR[M],EWB[MB],Cycles\n")
    #
    # for mode in modes:
    #     flags = ['THREAD_AFFINITY'] if mode == 'sgx-affinity' else []
    commons.compile_app("sgx", ['PCM_COUNT', 'SGX_COUNTERS'])
    #
    for alg in ["PSM"]:
        for ds in commons.get_test_dataset_names():
            for i in range(threads):
                run_join('sgx', commons.PROG, alg, ds, i+1, reps)
