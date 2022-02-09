# TeeBench

TeeBench was first published in [Proceedings of the VLDB Endowment, Volume 15, 2021-2022](https://vldb.org/pvldb/vol15-volume-info/).

### Abstract
Protection of personal data has been raised to be among the top requirements of modern systems. At the same time, it is now frequent that the owner of the data and the owner of the computing infrastructure are two entities with limited trust between them (e. g., volunteer computing or the hybrid-cloud). Recently, trusted execution environments (TEEs) became a viable solution to ensure the security of systems in such environments. However, the performance of relational operators in TEEs remains an open problem. We conduct a comprehensive experimental study to identify the main bottlenecks and challenges when executing relational equi-joins in TEEs. For this, we introduce TEEbench, a framework for unified benchmarking of relational operators in TEEs, and use it for conducting our experimental evaluation. In a nutshell, we perform the following experimental analysis for eight core join algorithms: off-the-shelf performance; the performance implications of data sealing and obliviousness; sensitivity and scalability. The results show that all eight join algorithms significantly suffer from different performance bottlenecks in TEEs. They can be up to three orders of magnitude slower in TEEs than on plain CPUs. Our study also indicates that existing join algorithms need a complete, hardware-aware redesign to be efficient in TEEs, and that, in secure query plans, managing TEE features is equally important to join selection.

### Paper
[What Is the Price for Joining Securely? Benchmarking Equi-Joins in Trusted Execution Environments](https://github.com/agora-ecosystem/tee-bench/blob/master/paper/What_Is_the_Price_for_Joining_Securely_Benchmarking_Equi-Joins_in_Trusted_Execution_Environments.pdf)
### BibTeX citation
```
@article{maliszewski2021price,
  title={What is the price for joining securely? benchmarking equi-joins in trusted execution environments},
  author={Maliszewski, Kajetan and Quian{\'e}-Ruiz, Jorge-Arnulfo and Traub, Jonas and Markl, Volker},
  journal={Proceedings of the VLDB Endowment},
  volume={15},
  number={3},
  pages={659--672},
  year={2021},
  publisher={VLDB Endowment}
}

```
## Prerequisites 
* Intel SGX v2.11 
* Ubuntu 18.04 LTS
*  Install the necessary dependencies:
```
  $ sudo apt update
  $ sudo apt install make gcc g++ libssl-dev python3-pip git-lfs  
  $ pip3 install matplotlib numpy pyyaml
```  
* Clone the repository usig `git-lfs`:
```
$ git lfs clone https://github.com/agora-ecosystem/tee-bench-dev.git
```

## How to run the experiments?
1. Make sure you have Intel SGX v2.11 installed and enabled on your machine
2. Go to `scripts`
3. Run any of the `exp*` scripts. The script will compile the code and run TeeBench. The results will be in `data` and the figures in `img`.
   
   It is easy to reproduce most of the experiments, however, some compile flags will have certain requirements:
   * `PCM_COUNT` - this flag requires `sudo` access. Before compilation uncomment this flag in `Enclave.edl`.
   * `SGX_COUNTERS` - this flag requires custom version of Intel SGX linux driver [1]. Before compilation uncomment this flag in `Enclave.edl`.
   * `THREAD_AFFINITY` - this flag requires custom version of Intel SGX SDK [2].
   
   Warning: Some experiments that cause EPC thrashing may take a lot of time to complete. 
   You can reduce the number of repetitions to cut down the time.
   
## Compilation
You can compile in two modes: plain CPU or Intel SGX.

1. Intel SGX
 
   1. Run this command:  
   ` $ make clean && make -B sgx `
   2. Execute with:  
   ` $ ./app `
   
2. Plain CPU  
   
    1. Run this command:  
    ` $ make clean && make -B native CFLAGS=-DNATIVE_COMPILATION `  
    2. Execute with:  
    ` $ ./app `

### Compile Flags
These flags can be set during the compilation process:  
* `PCM_COUNT` - enables Intel PCM tool that collects CPU counters, such as CPU cache hits, cache rate, instructions retired (IR), etc. - might require sudo access
* `NATIVE_COMPILATION` - used when compiled for plain CPU
* `SGX_COUNTERS` - used to collect SGX paging counters (EWB). Requires custom version of SGX driver [1] and sudo access
* `THREAD_AFFINITY` - experimental feature for multithreading. Requires custom version of SGX driver (will include it later)
* `JOIN_MATERIALIZE` - if you want to materialize join results. By default it only counts the number of matches.

## Execution
### Command line arguments
The currently working list of command line arguments:
* `-a` - join algorithm name. Currently working: `CHT`, `PHT`, `PSM`, `RHO`, `RHT`, `RSM`. Default: `RHO`
* `-c` - seal chunk size in kBs. if set to 0 then seal everything at once. Default: `0`
* `-d` - name of pre-defined dataset. Currently working: `cache-fit`, `cache-exceed`, `L`. Default: `none`
* `-h` - print help. Not implemented yet :)
* `-l` - join selectivity. Should be a number between 0 and 100. Default: `100`
* `-n` - number of threads used to execute the join algorithm. Default: `2`
* `-r` - number of tuples of R relation. Default: `2097152`
* `-s` - number of tuples of S relation. Default: `2097152`
* `-t | --r-path` - filepath to build R relation. Default: `none`
* `-u | --s-path` - filepath to build S relation. Default `none`
* `-x` - size of R in MBs. Default: `none`
* `-y` - size of S in MBs. Default: `none`
* `-z` - data skew. Default: `0`

The currently working list of command line flags:
* `--seal` - flag to seal join input data. Default: `false`
* `--sort-r` - flag to pre-sort relation R. Default: `false`
* `--sort-s` - flag to pre-sort relation S. Default: `false`

[1] https://github.com/agora-ecosystem/linux-sgx-driver/tree/ewb-monitoring  
[2] https://github.com/agora-ecosystem/linux-sgx/tree/2.11-exp-multithreading
