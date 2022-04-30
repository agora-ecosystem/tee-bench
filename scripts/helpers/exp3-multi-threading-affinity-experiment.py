#!/usr/bin/python3

import subprocess
import re
import statistics
import matplotlib as mpl
import matplotlib.pyplot as plt
import csv
import commons
import yaml

fname_output = "data/multi-threading-affinity-output.csv"
plot_fname_base = "img/multi-threading"


def run_join(mode, prog, alg, dataset, threads, reps):
    f = open(fname_output, "a")
    results = []

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

    res = statistics.mean(results)
    s = (mode + "," + alg + "," + dataset + "," + str(threads) + "," + str(round(res,2)))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot_affinity():
    csvf = open(fname_output, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    all_data = list(filter(lambda x: x['mode'] != 'native', all_data))
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    datasets = sorted(set(map(lambda x:x['ds'], all_data)), reverse=True)
    modes = sorted(set(map(lambda x:x['mode'], all_data)))
    to_modes = [[y for y in all_data if y['mode'] == x] for x in modes]
    plt.figure(figsize=(9,6))
    for m in range(0, len(modes)):
        mode = modes[m]
        to_datasets = [[y for y in to_modes[m] if y['ds'] == x] for x in datasets]
        for i in range(0, len(datasets)):
            ds = datasets[i]
            data = to_datasets[i]
            # split data into algorithms
            splitted_to_algos = [[y for y in data if y['alg'] == x] for x in algos]
            # create new figure
            plt.subplot(2,2,2*m+i+1)
            # print each plot
            for j in range(0, len(algos)):
                threads = sorted(list(map(lambda x: int(x['threads']), splitted_to_algos[j])))
                plt.plot(threads, list(map(lambda x: float(x['throughput']), splitted_to_algos[j])),
                         '-o', label=algos[j], color=commons.color_alg(algos[j]), linewidth=2)
            # make the figure neat
            plt.xlabel("num_threads")
            plt.ylabel("throughput [M rec/ sec]")
            plt.legend(fontsize='small')
            plt.title(mode + " - dataset " + ds)
            plt.tight_layout()
    # save fig
    commons.savefig(plot_fname_base + '-affinity.png')

    # now the improvement plot
    plt.figure(figsize=(8,4))
    plt.clf()
    to_datasets = [[y for y in all_data if y['ds'] == x] for x in datasets]
    res_array = []
    for d in range(0, len(datasets)):
        ds = datasets[d]
        data = to_datasets[d]
        to_algos = [[y for y in data if y['alg'] == x] for x in algos]
        for a in range(0, len(algos)):
            alg = algos[a]
            to_modes = [[y for y in to_algos[a] if y['mode'] == x] for x in modes]
            if alg == 'MWAY':
                range_length = 4
            else:
                range_length = len(threads)
            for t in range(0, range_length):
                a = to_modes[0][t]
                b = to_modes[1][t]
                assert(a['mode'] == 'sgx'), "something wrong with splitting to modes"
                assert(b['mode'] == 'sgx-affinity'), "something wrong with splitting to modes"
                assert(a['alg'] == b['alg']), "Algorithms are different"
                assert(a['ds'] == b['ds']), 'Different datasets'
                assert(a['threads'] == b['threads']), 'Different threads ' + a['threads'] + ' != ' + b['threads']
                print("alg = " + a['alg'])
                res = float(b['throughput'])/float(a['throughput'])
                o = {'alg': a['alg'], 'ds': a['ds'], 'threads': a['threads'], 'improvement': res}
                res_array.append(o)

    to_datasets = [[y for y in res_array if y['ds'] == x] for x in datasets]
    all_improvements = []
    for d in range(0, len(datasets)):
        plt.subplot(1,2,d+1)
        to_algos = [[y for y in to_datasets[d] if y['alg'] == x] for x in algos]
        for a in range(0, len(algos)):
            th = list(map(lambda x:int(x['threads']), to_algos[a]))
            improvements = list(map(lambda x:float(x['improvement']), to_algos[a]))
            print(datasets[d] + "-" + to_algos[a][0]['alg'] + " : ")
            print(improvements)
            plt.plot(th, improvements, '-o', label=algos[a], color=commons.color_alg(algos[a]))
            all_improvements += improvements
        # plt.ylim([0.8, 1.4])
        plt.gca().yaxis.grid(linestyle='dashed')
        plt.xlabel("Number of threads")
        plt.ylabel("Throughput improvement factor")
        plt.title('(' + chr(97+d) + ') Dataset ' + datasets[d])
        if datasets[d] == 'cache-fit':
            plt.legend(fontsize='small')
        print("Max improvement = " + str(max(all_improvements)) + ", mean improvement = " + str(statistics.mean(all_improvements)))
        all_improvements = []
        plt.tight_layout()
    commons.savefig(plot_fname_base + '-affinity-improvement.png')


if __name__ == '__main__':
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    threads = 8
    modes = ["sgx", "sgx-affinity"]

    # commons.remove_file(fname_output)
    # commons.init_file(fname_output, "mode,alg,ds,threads,throughput\n")
    #
    # for mode in modes:
    #     flags = ['THREAD_AFFINITY'] if mode == 'sgx-affinity' else []
    #     commons.compile_app(mode, flags)
    #     for alg in commons.get_all_algorithms_extended():
    #         for ds in commons.get_test_dataset_names():
    #             for i in range(threads):
    #                 run_join(mode, commons.PROG, alg, ds, i+1, config['reps'])

    plot_affinity()
