#!/usr/bin/python3

import subprocess
import re
import matplotlib.pyplot as plt
import matplotlib
import csv
import commons

filename = "data/perf-degradation-wrt-native.csv"
png_file = "img/perf-degradation-wrt-native.png"
mb_of_data = 131072


def run_join(mode, prog, alg, threads, size_factor, k, reps):
    f = open(filename, "a")
    r_size = mb_of_data*pow(2, size_factor)
    s_size = r_size * k
    total_size_mb = pow(2, size_factor)*(k+1)
    res = 0

    for i in range(0, reps):
        stdout = subprocess.check_output(
            prog + " -a " + alg + " -r " + str(r_size) + " -s " + str(s_size) + " -n " + str(threads),
            cwd="../", shell=True)\
            .decode('utf-8')
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                res += float(throughput)
    # calculate the average throughput
    res /= reps
    s = mode + "," + alg + "," + str(total_size_mb) + "," + str(round(res, 2))
    f.write(s + "\n")
    f.close()


def plot(k):
    csvf = open(filename, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    modes = sorted(set(map(lambda x:x['mode'], all_data)))
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    to_modes = [[y for y in all_data if y['mode'] == x] for x in modes]
    res = []
    for i in range(0, len(to_modes[0])):
        a = to_modes[0][i]
        b = to_modes[1][i]
        assert (a['mode'] == 'native'), "something wrong with splitting to modes"
        assert (b['mode'] == 'sgx'), "something wrong with splitting to modes"
        assert (a['alg'] == b['alg']), "algs don't match"
        assert (a['dsSize'] == b['dsSize']), "dsSizes don't match"
        if float(b['throughput']) == 0:
            degradation = 0
        else:
            degradation = float(a['throughput']) / float(b['throughput'])
        o = {'alg': a['alg'], 'dsSize': a['dsSize'], 'degradation': degradation}
        res.append(o)
    to_algos = [[y for y in res if y['alg'] == x] for x in algos]
    fig, ax = plt.subplots()
    for a in range(0, len(algos)):
        alg = algos[a]
        sizes = list(map(lambda x:float(x['dsSize']), to_algos[a]))
        degradations = list(map(lambda x:float(x['degradation']), to_algos[a]))
        plt.plot(sizes, degradations, '-o', label=alg)

    plt.xlabel("Size of input tables [MB]\n |S| = " + str(k) + " * |R|")
    plt.ylabel("Throughput degradation")
    # plt.xticks(np.arange(len(to_datasets[m])), list(map(lambda x:x['alg'],to_datasets[m])))
    plt.title("SGX performance degradation w.r.t. native")
    plt.legend()
    ax.set_xscale('log')
    ax.set_xticks(sizes)
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    plt.tight_layout()
    plt.savefig(png_file)


if __name__ == '__main__':
    reps = 1
    modes = ["native", "sgx"]
    k = 4
    threads = 4
    size_factor = 8 # 7 = max data size is 320 MB

    commons.remove_file(filename)
    commons.init_file(filename, "mode,alg,dsSize,throughput\n")

    for mode in modes:
        isSgx = True if mode == "sgx" else False
        prog = "./app" if isSgx else "./native"
        commons.make_app(isSgx, False)
        for alg in commons.get_all_algorithms():
            for s in range(0, size_factor):
                run_join(mode, prog, alg, threads, s, k, reps)

    plot(k)
