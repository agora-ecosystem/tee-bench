import errno
import os
import re
import subprocess
from timeit import default_timer as timer
from datetime import timedelta
import matplotlib.pyplot as plt
from collections import namedtuple

PROG = ' ./app '


def get_all_algorithms():
    return ["CHT", "INL", "PHT", "PSM", "RHO", "RHT", "RSM"]

def get_all_algorithms_extended():
    return ["CHT", 'INL' ,"MWAY", "PHT", "PSM", "RHT", "RHO", "RSM"]


def get_test_dataset_names():
    return ["cache-fit", "cache-exceed"]


def get_join_dataset_names():
    return ['A', 'B']


def start_timer():
    return timer()


def stop_timer(start):
    print("Execution time: " + str(timedelta(seconds=(timer()-start))))


def escape_ansi(line):
    ansi_escape = re.compile(r'(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]')
    return ansi_escape.sub('', line)


def compile_app(mode: str, flags=None, enclave_config_file=None, debug=False):
    if flags is None:
        flags = []
    print("Make clean")
    subprocess.check_output(["make", "clean"], cwd="../../")
    if "native" in mode:
        exec_mode = "native"
    # cover all sgx modes like sgx-affinity
    elif "sgx" in mode:
        exec_mode = "sgx"
    else:
        raise ValueError("Mode not found: " + mode)

    cflags = 'CFLAGS='
    if mode == 'native':
        cflags += ' -DNATIVE_COMPILATION '

    if mode == 'native-materialize':
        cflags += ' -DNATIVE_COMPILATION -DJOIN_MATERIALIZE '

    if mode == 'sgx-materialize' or mode == 'sgx-seal' or mode =='sgx-chunk-buffer':
        cflags += ' -DJOIN_MATERIALIZE '

    if mode == 'sgx-affinity':
        cflags += ' -DTHREAD_AFFINITY '

    for flag in flags:
        cflags += ' -D' + flag + ' '

    if enclave_config_file is None:
        enclave_string = ' ENCLAVE_CONFIG_FILE=Enclave/Enclave.config.xml '
    else:
        enclave_string = ' ENCLAVE_CONFIG_FILE=' + str(enclave_config_file) + ' '

    print("Make " + mode + " enclave " + enclave_string + " with flags " + cflags)
    if debug:
        subprocess.check_output(["make", '-B' , exec_mode, 'SGX_DEBUG=1', 'SGX_PRERELEASE=0',
                                      enclave_string, cflags], cwd="../../", stderr=subprocess.DEVNULL)
    else:
        subprocess.check_output(["make", "-B", exec_mode, enclave_string, cflags], cwd="../../", stderr=subprocess.DEVNULL)


def make_app_radix_bits(sgx: bool, perf_counters: bool, num_passes: int, num_radix_bits: int):
    print('Make clean')
    subprocess.check_output(['make', 'clean'], cwd='../../')
    # flags = '"'
    flags = 'CFLAGS='
    prog = 'sgx' if sgx else 'native'

    if not sgx:
        flags += ' -DNATIVE_COMPILATION '
    if perf_counters:
        flags += ' -DPCM_COUNT '
    flags += ' -DNUM_PASSES=' + str(num_passes) + " "
    flags += ' -DNUM_RADIX_BITS=' + str(num_radix_bits)

    print(prog + ' ' + flags)
    # try:
    subprocess.check_output(['make', '-B', prog, flags], cwd='../../')
    # except subprocess.CalledProcessError as e:
    #     print(e.output)


def make_app(sgx: bool, perf_counters: bool):
    print("Make clean")
    subprocess.check_output(["make", "clean"], cwd="../../")
    if sgx:
        print("Make SGX app. perf_counters=" + str(perf_counters))
        if perf_counters:
            subprocess.check_output(["make", "-B", "sgx", "CFLAGS=-DPCM_COUNT"], cwd="../../")
        else:
            subprocess.check_output(["make", "-B", "sgx"], cwd="../../")
    else:
        print("Make native app. perf_counters=" + str(perf_counters))
        if perf_counters:
            subprocess.check_output(["make", "-B", "native", "CFLAGS=-DNATIVE_COMPILATION -DPCM_COUNT"], cwd="../../")
        else:
            subprocess.check_output(["make", "-B", "native", "CFLAGS=-DNATIVE_COMPILATION"], cwd="../../")


def remove_file(fname):
    try:
        os.remove(fname)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise


def init_file(fname, first_line):
    f = open(fname, "x")
    print(first_line)
    f.write(first_line)
    f.close()

##############
# colors:
#
# 30s
# mint: bcd9be
# yellow brock road: e5cf3c
# poppy field: b33122
# powder blue: 9fb0c7
# egyptian blue: 455d95
# jade: 51725b
#
# 20s
# tuscan sun: e3b22b
# antique gold: a39244
# champagne: decd9d
# cadmium red: ad2726
# ultramarine: 393c99
# deco silver: a5a99f
#
# 80s
# acid wash: 3b6bc6
# purple rain: c77dd4
# miami: fd69a5
# pacman: f9e840
# tron turqouise: 28e7e1
# powersuit: fd2455
#
# 2010s
# millennial pink: eeb5be
# polished copper: c66d51
# quartz: dcdcdc
# one million likes: fc3e70
# succulent: 39d36e
# social bubble: 0084f9
#############

def color_alg(alg):
    # colors = {"CHT":"#e068a4", "PHT":"#829e58", "RHO":"#5ba0d0"}
    colors = {"CHT":"#b44b20", # burnt sienna
              "PHT":"#7b8b3d", # avocado
              "PSM":"#c7b186", # natural
              "RHT":"#885a20", # teak
              "RHO":"#e6ab48", # harvest gold
              "RSM":"#4c748a", # blue mustang
              "OJ":"#fd2455",
              "OPAQ":"#c77dd4",
              "OBLI":"#3b6bc6",
              "INL":'#7620b4',
              'NL':'#20b438',
              'MWAY':'#fd2455',
              'GHJ':'black',
              'RHO_atomic':'deeppink',
              'CRKJ':'dodgerblue',
              'CRKJ_CHT':'deeppink',
              'oldCRKJ':"#7b8b3d", # avocado
              'CRKJ_static':"#4c748a", # blue mustang

              'RHO-sgx': '#e6ab48',
              'RHO-sgx-affinity':'g',
              'RHO-lockless':'deeppink',
              'RHO_atomic-sgx':'deeppink'}
    # colors = {"CHT":"g", "PHT":"deeppink", "RHO":"dodgerblue"}
    return colors[alg]


def marker_alg(alg):
    markers = {
        "CHT": 'o',
        "PHT": 'v',
        "PSM": 'P',
        "RHT": 's',
        "RHO": 'X',
        "RSM": 'D',
        "INL": '^',
        'MWAY': '*',
        'RHO_atomic': 'P',
        'CRKJ':'>',
        'CRKJ_CHT':'^',
        'oldCRKJ':'X',
        'CRKJ_static': 'D',

        'RHO-sgx': 'X',
        'RHO-lockless':'h',
        'RHO_atomic-sgx':'h'
    }
    return markers[alg]

def color_categorical(i):
    colors = {
        3:'#0fb5ae',
        5:'#4046ca',
        2:'#f68511',
        0:'#de3d82',
        4:'#7e84fa',
        1:'#72e06a',
        6:'#147af3',
        7:'#7326d3',
        8:'#e8c600',
        9:'#cb5d00',
        10:'#008f5d',
        11:'#bce931',

        90:'#de3d8244',
        91:'#72e06a44',
    }
    return colors[i]

def color_size(i):
    # colors = ["g", "deeppink", "dodgerblue", "orange"]
    colors = ["#e068a4", "#829e58", "#5ba0d0", "#91319f"]
    return colors[i]


def savefig(filename, font_size=15, tight_layout=True):
    plt.rcParams.update({'font.size': font_size})
    if tight_layout:
        plt.tight_layout()
    plt.savefig(filename, transparent = False, bbox_inches = 'tight', pad_inches = 0.1, dpi=200)
    print("Saved image file: " + filename)


def threads(alg, dataset):
    Entry = namedtuple("Entry", ["alg", "dataset"])
    d = {
        Entry("CHT", "A"): 7,
        Entry("PHT", "A"): 4,
        Entry("RHO", "A"): 2,
        Entry("CHT", "B"): 2,
        Entry("PHT", "B"): 1,
        Entry("RHO", "B"): 3,
    }
    if dataset == 'L':
        return d[Entry(alg, 'M')]

    return d[Entry(alg, dataset)]


def draw_vertical_lines(plt, x_values, linestyle='--', color='#209bb4', linewidth=1):
    for x in x_values:
        plt.axvline(x=x, linestyle=linestyle, color=color, linewidth=linewidth)