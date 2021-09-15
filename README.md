# TeeBench

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
