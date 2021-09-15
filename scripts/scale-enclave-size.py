import matplotlib.pyplot as plt
import csv
import commons

fname = "data/scale-enclave-size.csv"
figname = 'img/scale-enclave-size'


if __name__ == '__main__':
    csvf = open(fname, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    sizes = list(map(lambda x:int(x['size']), all_data))
    fig = plt.figure(figsize=(4,4))
    plt.rcParams.update({'font.size': 15})
    plt.gca().yaxis.grid(linestyle='dashed')
    plt.plot(sizes, list(map(lambda x: float(x['throughput']), all_data)), '-o', linewidth=2)
    plt.ylim([0,18])
    plt.xscale('log')
    plt.xlabel("Enclave size [MB]")
    plt.ylabel("Throughput [M rec/ sec]")
    plt.axvline(x=39833, linestyle='--', color='red')
    plt.tight_layout()
    fig.text(0.82,0.27, "RAM", color='red', rotation=90)
    commons.savefig(figname + '.png', tight_layout=False)