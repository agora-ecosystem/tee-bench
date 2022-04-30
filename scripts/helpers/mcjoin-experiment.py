#!/usr/bin/python3

import commons
import re
import statistics
import subprocess
import csv
import matplotlib.pyplot as plt

filename = "data/mcj-experiment.csv"
mb_of_data = 131072


def run_join(prog, alg, ds, memConstraint, reps, mode):
    f = open(filename, "a")
    results = []
    ewbs = []
    print(alg + " " + str(memConstraint) + " MB")
    for i in range(0,reps):
        stdout = subprocess.check_output(prog + " -a " + alg + " -d " + ds +
                                         " -n " + str(memConstraint), cwd="../", shell=True) \
            .decode('utf-8')
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                results.append(float(throughput))
                print ("Throughput = " + str(throughput))
            elif "EPC Miss =" in line:
                ewb = int(commons.escape_ansi(line.split("= ", 1)[1]))
                print("EPC Miss = " + str(ewb))
                ewbs.append(int(ewb))

    if len(results) == 0:
        results = [-1]
    if len(ewbs) == 0:
        ewbs = [-1]
    res = statistics.mean(results)
    ewb = statistics.mean(ewbs)
    s = (mode + "," + alg + "," + ds + "," + str(memConstraint) + "," + str(round(res,2)) + "," + str(round(ewb,2)))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot_with_ewb():
    csvf = open(filename, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)

    # plot only CHT for EPC < S
    # fig, ax1 = plt.subplots(figsize=(5,4))
    # plt.clf()
    fig = plt.figure(figsize=(5,4))
    ax1 = plt.gca()
    # plt.clf()
    # rs = list(filter(lambda x: x['alg'] == 'CHT', all_data))
    # rs = sorted(set(map(lambda x:float(x['sizeR']), rs)))
    mc = list(map(lambda x: int(x['memConstraint']), all_data))
    plot = list(map(lambda x: float(x['throughput']), all_data))
    bar = list(map(lambda x: int(float(x['ewb'])/1000), all_data))
    line1, = ax1.plot(mc, plot, '-o', color=commons.color_alg('CHT'), linewidth=2,
                      marker=commons.marker_alg('CHT'), markeredgecolor='black', markersize=8,
                      label='Throughput')
    ax1.set_xlabel('Memory restriction [MB]')
    ax1.set_ylabel('Throughput [M rec/s]')
    # ax1.set_xlim([0,130])
    ax1.set_ylim(bottom=0)
    ax2 = ax1.twinx()
    bar2 = ax2.bar(mc, bar, color=commons.color_size(3), alpha=0.4, width=3,
                   label='EPC Miss')
    ax2.set_ylabel('EPC Miss [k]')
    ax2.set_ylim(bottom=0)
    ax1.yaxis.grid(linestyle='dashed')
    ax1.axvline(x=90, linestyle='--', color='#209bb4', linewidth=2)
    # fig.text(0.55,0.77, "EPC", color='#209bb4', rotation=90, weight='bold')
    fig.legend(handles=[line1, bar2], ncol=2, frameon=False,
               bbox_to_anchor=(0.08,0.91,1,0), loc="lower left")
    commons.savefig('img/mcjoin' + '.png')


if __name__ == '__main__':
    timer = commons.start_timer()
    mode = "sgx"
    reps = 1
    ds = 'cache-exceed'

    # commons.make_app_with_flags(True, ["SGX_COUNTERS"])
    # commons.remove_file(filename)
    # commons.init_file(filename, "mode,alg,ds,memConstraint,throughput,ewb\n")
    #
    # for x in range (65, 200, 5):
    #     run_join(commons.PROG, "MCJ", ds, x, reps, mode)

    plot_with_ewb()
    commons.stop_timer(timer)
