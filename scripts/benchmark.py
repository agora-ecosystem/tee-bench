#!/usr/bin/python3
import os
import sys

TEE_BENCH = \
    ' _______        ____                  _ \n' + \
    '|__   __|      |  _ \                | | \n' + \
    '   | | ___  ___| |_) | ___ _ __   ___| |__\n' + \
    '   | |/ _ \/ _ \  _ < / _ \ \'_ \ / __| \'_ \ \n' + \
    '   | |  __/  __/ |_) |  __/ | | | (__| | | | \n' + \
    '   |_|\___|\___|____/ \___|_| |_|\___|_| |_|\n'

INSTRUCTIONS = 'Please enter a number between 1 and 18.'

EXPERIMENTS = ['f1', 'f4', 'f5', 'f6', 'f7', 'f8', 'f9', 't3', 'f10', 'f11', 'f12', 'f13', 'f14', 'f15', 'f16', 'f17',
               'f18', 'f19']

CAPTIONS = \
    'f1.  Figure 1: Secure joins throughput. \n' + \
    'f4.  Figure 4: Throughput of join algorithms. \n' + \
    'f5.  Figure 5: CHT’s throughput and EPC paging. \n' + \
    'f6.  Figure 6: Join algorithms’ throughput with IMDb. \n' + \
    'f7.  Figure 7: CPU cycles per tuple for each stage in joins. Dark color indicates the first stage' \
    ' (build, sort, or partition) and light color indicates the second stage (probe, merge, or join). \n' + \
    'f8.  Figure 8: Scaling dataset cache-exceed. \n' + \
    'f9.  Figure 9: Chunking impact. \n' + \
    't3.  Table 3: Hardware performance counters for secure joins. \n' + \
    'f10. Figure 10: Materialization and data sealing impact on RHO. \n' + \
    'f11. Figure 11: Throughput of secure and oblivious joins. \n' + \
    'f12. Figure 12: Lockless RHO. \n' + \
    'f13. Figure 13: Throughput when varying join selectivity. \n' + \
    'f14. Figure 14: Skewed data distribution. \n' + \
    'f15. Figure 15: Throughput with sorted and unsorted input. \n' + \
    'f16. Figure 16: CHT’s throughput – scaling the outer relation. \n' + \
    'f17. Figure 17: Throughput with IMDb on AMD CPU. \n' + \
    'f18. Figure 18: Scaling joins with the number of threads. \n' + \
    'f19. Figure 19: Join algorithms on AMD and AMD SEV. \n'


def read_args(args: []):
    EXCEPTION = 'Experiment unknown. ' \
                'Please use one of the identifiers (fxx or txx) for the following experiments: \n' + CAPTIONS
    return_args = ''
    if len(args) < 2:
        raise Exception('Not enough arguments provided')
    if str(args[1]) not in EXPERIMENTS and str(args[1]) != 'all':
        raise Exception(EXCEPTION)

    experiment = str(args[1])
    if len(args) > 2 and str(args[2]) == 'fast':
        return_args += ' -r 1 '
    elif len(args) > 2 and args[2].isnumeric() and int(args[2]) > 0:
        return_args += ' -r ' + args[2]
    elif len(args) > 2:
        raise Exception("Second argument must be a positive integer or 'fast' but is: " + args[2])

    print('Run experiment ' + experiment + ' with args: ' + return_args + '\n')
    return experiment, return_args


def print_f17_instructions():
    print('Experiments using AMD SEV require orchestration of AMD virtual machines, which exceeds the scope of\n'
          'automation we provide. Therefore, we ask the users to perform some manual work with the help of\n'
          'provided scripts. To get the results from Figure 17 from the paper, the following procedure follows\n'
          '(same as used by the authors):\n'
          '1.  Run a Google Cloud VM with AMD EPYC CPU with Confidential Computing DISABLED.\n'
          '2.  Download TEEBench code.\n'
          '3.  In file scripts/helpers/config.yaml, remove line 5 (sgx). The only remaining mode should be native.\n'
          '4.  Make sure the IMDb data files are located in the data directory.\n'
          '5.  Run script scripts/helpers/exp5-real-dataset-experiment.py. Add \' -r 1 \' for fast results.\n'
          '6.  Export file scripts/data/real-dataset-output.csv to an external storage.\n'
          '7.  Run a new instance of Google Cloud VM with AMD EPYC CPU with Confidential Computing ENABLED.\n'
          '8.  Repeat steps 2-6.\n'
          '9.  Combine results from two machines into one file scripts/data/real-dataset-amd-output.csv.\n'
          '    Note that you have to change the modes to AMD and AMD-SEV accordingly. You can see the existing\n'
          '    scripts/data/real-dataset-amd-output.csv file as an example.\n'
          '10. Run script scripts/helpers/real-dataset-amd-experiment.py from your local machine\n'
          '    (where you merged the results).\n'
          '    The figure will be available in scripts/img/Figure-17-Throughput-with-IMDb-on-AMD-CPU.png.\n')


def print_f19_instructions():
    print('Experiments using AMD SEV require orchestration of AMD virtual machines, which exceeds the scope of\n'
          'automation we provide. Therefore, we ask the users to perform some manual work with the help of\n'
          'provided scripts. To get the results from Figure 19 from the paper, the following procedure follows\n'
          '(same as used by the authors):\n'
          '1. Run a Google Cloud VM with AMD EPYC CPU with Confidential Computing DISABLED.\n'
          '2. Download TEEBench code.\n'
          '3. In file scripts/helpers/config.yaml, remove line 5 (sgx). The only remaining mode should be native.\n'
          '4. Run script scripts/helpers/throughput-all-algs-native.py. Add \' -r 1 \' for fast results.\n'
          '5. Export file scripts/data/throughput-all-algs-native.csv to an external storage.\n'
          '6. Run a new instance of Google Cloud VM with AMD EPYC CPU with Confidential Computing ENABLED.\n'
          '7. Repeat steps 2-5.\n'
          '8. Combine results from two machines into one file scripts/data/amd-throughput.csv. Note that you have to\n'
          '   change the modes to amd and amd-sev accordingly. You can see the existing\n'
          '   scripts/data/amd-throughput.csv file as an example.\n'
          '9. Run script scripts/helpers/amd-throughput.py from your local machine (where you merged the results).\n'
          '   The figure will be available in scripts/img/Figure-19-Join-algorithms-on-AMD-and-AMD-SEV.png.\n')


def run_experiment(exp, args):
    execute_flag = True
    exp_script = ''

    if exp == 'f1':
        exp_script = 'top-graph.py'
    elif exp == 'f4':
        exp_script = 'exp1-off-the-shelf-performance.py'
    elif exp == 'f5':
        exp_script = 'exp6-scale-r-experiment.py'
    elif exp == 'f6':
        exp_script = 'exp5-real-dataset-experiment.py'
    elif exp == 'f7':
        exp_script = 'exp8-phases-experiment.py'
    elif exp == 'f8':
        exp_script = 'exp13-throughput-scale-input-experiment.py'
    elif exp == 'f9':
        exp_script = 'exp14-seal-chunk-size.py'
    elif exp == 't3':
        print('This experiment requires SUDO access')
        exp_script = 'exp4-performance-counters-experiment-SUDO.py'
    elif exp == 'f10':
        exp_script = 'exp0-seal-materialize-experiment.py'
    elif exp == 'f11':
        exp_script = 'exp12-throughput-oblivious-algos-experiment.py'
    elif exp == 'f12':
        exp_script = 'exp15-multi-threading-atomic-experiment.py'
    elif exp == 'f13':
        exp_script = 'exp9-selectivity-experiment.py'
    elif exp == 'f14':
        exp_script = 'exp10-skew-experiment.py'
    elif exp == 'f15':
        exp_script = 'exp11-sorted-input-experiment.py'
    elif exp == 'f16':
        exp_script = 'exp6-scale-r-experiment.py'
    elif exp == 'f17':
        print_f17_instructions()
        execute_flag = False
    elif exp == 'f18':
        exp_script = 'exp2-multi-threading-experiment.py'
    elif exp == 'f19':
        print_f19_instructions()
        execute_flag = False
    else:
        raise Exception('Experiment unknown: ' + exp + '. Possible choices: ' + str(EXPERIMENTS))

    if execute_flag:
        print("Running experiment " + exp)
        os.chdir('helpers')
        os.system('python3 ' + exp_script + ' ' + args)
        os.chdir('..')


if __name__ == '__main__':
    print(TEE_BENCH)

    (EXPERIMENT, ARGS) = read_args(sys.argv)

    if EXPERIMENT == 'all':
        print('Run all experiments at once. Be aware - this will take hours/days.')
        for exp in EXPERIMENTS:
            run_experiment(exp, ARGS)
    else:
        run_experiment(EXPERIMENT, ARGS)
