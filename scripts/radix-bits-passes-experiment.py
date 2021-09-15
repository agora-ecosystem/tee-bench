#!/usr/bin/python3

import subprocess
import re
import os, errno
import statistics
import matplotlib.pyplot as plt
import csv
import commons

filename = "data/radix-bits-passes-output.csv"
plot_filename = "img/radix-bits-passes-plot"


def run_join(prog, alg, mode, ds, threads, reps, passes, radix_bits):
    f = open(filename, "a")
    results = []
    for i in range(0, reps):
        s = prog + " -a " + alg + " -d " + str(ds) + " -n " + str(threads)
        stdout = subprocess.check_output(s, cwd="../",shell=True) \
            .decode('utf-8')
        print(stdout)
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                s = ("RHO" + "," + ds + "," + str(threads) + "," + str(throughput))
                results.append(float(throughput))
                print (s)

    res = statistics.mean(results)
    s = (mode + "," + alg + "," + ds + "," + str(passes) + "," + str(radix_bits) + "," + str(round(res,2)))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot():
    csvf = open(filename, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    datasets = sorted(set(map(lambda x:x['ds'], all_data)), reverse=True)
    modes = sorted(set(map(lambda x:x['mode'], all_data)))
    passes = sorted(set(map(lambda x:x['passes'], all_data)))
    algos = sorted(set(map(lambda x:x['alg'], all_data)))

    to_datasets = [[y for y in all_data if y['ds'] == x] for x in datasets]

    # plt.figure(figsize=(10,7))
    # i = 1
    # for m in range(0, len(modes)):
    #     for d in range(0, len(datasets)):
    #         ds = datasets[d]
    #         to_modes = [[y for y in to_datasets[d] if y['mode'] == x] for x in modes]
    #         plt.subplot(2, 2, i)
    #         mode = modes[m]
    #         to_algos = [[y for y in to_modes[m] if y['alg'] == x] for x in algos]
    #         for a in range(0, len(algos)):
    #             to_passes = [[y for y in to_algos[a] if y['passes'] == x] for x in passes]
    #             for p in range(0, len(passes)):
    #                 bits = list(set(map(lambda x:int(x['bits']), to_algos[a])))
    #                 plt.plot(bits, list(map(lambda x:float(x['throughput']), to_passes[p])),
    #                          '-o', label=algos[a] +" num_passes="+str(passes[p]))
    #         plt.xlabel("Number of radix bits")
    #         plt.ylabel("Throughput [M rec / s]")
    #         plt.legend()
    #         plt.title(mode + " - Dataset " + ds)
    #         i += 1
    #
    # plt.tight_layout()
    # plt.savefig(plot_filename + ".png")

    # plot only SGX
    i = 1
    plt.figure(figsize=(6,3))
    for m in range(0, len(modes)):
        for d in range(0, len(datasets)):
            ds = datasets[d]
            to_modes = [[y for y in to_datasets[d] if y['mode'] == x] for x in modes]
            plt.subplot(1, 2, i)
            mode = modes[m]
            to_algos = [[y for y in to_modes[m] if y['alg'] == x] for x in algos]
            for a in range(0, len(algos)):
                to_passes = [[y for y in to_algos[a] if y['passes'] == x] for x in passes]
                for p in range(0, len(passes)):
                    alpha = 0.5 if passes[p] == '2' else 1
                    bits = list(set(map(lambda x:int(x['bits']), to_algos[a])))
                    plt.plot(bits, list(map(lambda x:float(x['throughput']), to_passes[p])),
                             '-o', label=algos[a] +" passes="+str(passes[p]), color=commons.color_alg(algos[a]),
                             alpha=alpha)
            plt.xlabel("Number of radix bits")
            plt.ylabel("Throughput [M rec / s]")
            # plt.ylim([0, 30])
            if ds == 'cache-exceed':
                plt.legend(fontsize='small')
            plt.title("Dataset " + ds)
            i += 1

    plt.tight_layout()
    plt.savefig(plot_filename + "-sgx.png")


if __name__ == '__main__':
    reps = 1
    modes = ["sgx"]
    algos = ["RHO"]
    passes = [1, 2]
    bits = [2,3,4,5,6,7,8, 9, 10, 11, 12, 13, 14]
    threads = 3

    commons.remove_file(filename)
    commons.init_file(filename, "mode,alg,ds,passes,bits,throughput\n")

    # first, datasets small and small10x
    for mode in modes:
        for p in passes:
            for b in bits:
                isSgx = True if mode == "sgx" else False
                commons.make_app_radix_bits(isSgx, False, p, b)
                for alg in algos:
                    for ds in commons.get_test_dataset_names():
                        run_join(commons.PROG, alg, mode, ds, threads, reps, p, b)

    # now datasets X (needs more memory)
    # datasets = ['X']
    # for mode in modes:
    #     for p in passes:
    #         for b in bits:
    #             for alg in algos:
    #                 for ds in datasets:
    #                     isSgx = True if mode == "sgx" else False
    #                     prog = "./app" if isSgx else "./native"
    #                     commons.make_app_radix_bits(isSgx, False, p, b)
    #                     run_join(prog, alg, mode, ds, commons.threads(alg, ds), reps, p, b)
    plot()
