#!/usr/bin/python3
import getopt
import subprocess
import re
import sys

import commons
import statistics
import yaml

fname_throughput = "data/throughput-all-algs-native.csv"


def run_join(mode, alg, ds, threads, reps):

    f_throughput = open(fname_throughput, 'a')
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

    throughput = statistics.mean(throughput_array)
    s = (mode + "," + alg + "," + ds + "," + str(threads) + "," + str(round(throughput, 2)))
    f_throughput.write(s + '\n')
    f_throughput.close()


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
        commons.remove_file(fname_throughput)
        commons.init_file(fname_throughput, "mode,alg,ds,threads,throughput\n")

        commons.compile_app('native')
        for ds in commons.get_test_dataset_names():
            for alg in commons.get_all_algorithms_extended():
                run_join('native', alg, ds, config['threads'], config['reps'])
