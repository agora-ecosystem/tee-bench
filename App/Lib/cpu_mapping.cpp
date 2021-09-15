#include <stdio.h>  /* FILE, fopen */
#include <stdlib.h> /* exit, perror */
#include <assert.h> /* assert() */

#include "cpu_mapping.h"

#ifdef NATIVE_COMPILATION
#else
#include "Enclave_u.h"
#endif

#define MAX_THREADS 1024

static int inited = 0;
static int max_cpus;
static int max_threads;
static int cpumapping[MAX_THREADS];

/*** NUMA-Topology related variables ***/
static int numthreads;
/* if there is no info., default is assuming machine has only-1 NUMA region */
static int numnodes = 1;
static int thrpernuma;
static int ** numa;
static char ** numaactive;
static int * numaactivecount;

/** 
 * Initializes the cpu mapping from the file defined by CUSTOM_CPU_MAPPING.
 * NUMBER-OF-THREADS(NTHR) and mapping of PHYSICAL-THR-ID for each LOGICAL-THR-ID from
 * 0 to NTHR and optionally number of NUMA-nodes (overridden by libNUMA value if
 * exists). 
 * The mapping used for our machine Intel E5-4640 is = 
 * "64 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53  54 55 56 57 58 59 60 61 62 63 4".
 */
static int
init_mappings_from_file()
{
    return 0;
}

/** 
 * Initialize NUMA-topology with libnuma.
 */
static void
numa_default_init()
{
    /* numnodes   = 1; */
        numthreads = max_cpus;
        thrpernuma = max_cpus;
        numa = (int **) malloc(sizeof(int *));
        numa[0] = (int *) malloc(sizeof(int) * numthreads);
        numaactive = (char **) malloc(sizeof(char *));
        numaactive[0] = (char *) calloc(numthreads, sizeof(char));
        numaactivecount = (int *) calloc(numnodes, sizeof(int));
        for(int i = 0; i < max_cpus; i++){
            if(max_cpus == max_threads)
                numa[0][i] = cpumapping[i];
            else 
                numa[0][i] = i;
        }
}

static void 
numa_init()
{
    fprintf(stdout, "[WARN ] NUMA is not available, using single NUMA-region as default.\n");
    numa_default_init();
}

void
cpu_mapping_cleanup()
{
    for (int i = 0; i < numnodes ; i++) {
        free(numa[i]);
        free(numaactive[i]);
    }
	free(numa);
	free(numaactive);
	free(numaactivecount);
}


/** @} */

/** 
 *  Try custom cpu mapping file first, if does not exist then round-robin
 *  initialization among available CPUs reported by the system. 
 */
void
cpu_mapping_init()
{
    max_cpus  = 8;// sysconf(_SC_NPROCESSORS_ONLN);
    if( init_mappings_from_file() == 0 ) {
        int i;
    
        max_threads = max_cpus;
        for(i = 0; i < max_cpus; i++){
            cpumapping[i] = i;
        }
    }

    numa_init();
    inited = 1;
}

void
numa_thread_mark_active(int phytid)
{
    int numaregionid = -1;
    for(int i = 0; i < numnodes; i++){
        for(int j = 0; j < thrpernuma; j++){
            if(numa[i][j] == phytid){
                numaregionid = i;
                break;
            }
        }
        if(numaregionid != -1)
            break;
    }

    int thridx = -1;
    for(int i = 0; i < numnodes; i++){
        for(int j = 0; j < thrpernuma; j++){
            if(numa[i][j] == phytid){
                thridx = j;
                break;
            }
        }
        if(thridx != -1)
            break;
    }

    if(numaactive[numaregionid][thridx] == 0){
        numaactive[numaregionid][thridx] = 1;
        numaactivecount[numaregionid] ++;
    }
}

/**
 * Returns SMT aware logical to physical CPU mapping for a given logical thr-id. 
 */
int 
get_cpu_id(int thread_id)
{
    if(!inited){
        cpu_mapping_init();
        //inited = 1;
    }

    return cpumapping[thread_id % max_threads];
}

/**
 * Topology of Intel E5-4640 used in our experiments.
 node 0 cpus: 0 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60
 node 1 cpus: 1 5 9 13 17 21 25 29 33 37 41 45 49 53 57 61
 node 2 cpus: 2 6 10 14 18 22 26 30 34 38 42 46 50 54 58 62
 node 3 cpus: 3 7 11 15 19 23 27 31 35 39 43 47 51 55 59 63
*/
/*
static int numa[][16] = {
    {0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60},
    {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61},
    {2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46, 50, 54, 58, 62},
    {3, 7, 11, 15, 19, 23, 27, 31, 35, 39, 43, 47, 51, 55, 59, 63} };
*/

int
is_first_thread_in_numa_region(int logicaltid)
{
    int phytid = get_cpu_id(logicaltid);
	int ret = 0;
	for(int i = 0; i < numnodes; i++)
	{
        int j = 0;
        while(j < thrpernuma && !numaactive[i][j])
            j++;
        if(j < thrpernuma)
            ret = ret || (phytid == numa[i][j]);
	}

    return ret;
}

int 
get_thread_index_in_numa(int logicaltid)
{
    int ret = -1;
    int phytid = get_cpu_id(logicaltid);

    for(int i = 0; i < numnodes; i++){
        int active_idx = 0;
        for(int j = 0; j < thrpernuma; j++){
            if(numa[i][j] == phytid){
                assert(numaactive[i][j]);
                ret = active_idx;
                break;
            }

            if(numaactive[i][j])
                active_idx ++;
        }
        if(ret != -1)
            break;
    }


    return ret;
}

int
get_numa_region_id(int logicaltid)
{
    int ret = -1;
    int phytid = get_cpu_id(logicaltid);

    for(int i = 0; i < numnodes; i++){
        for(int j = 0; j < thrpernuma; j++){
            if(numa[i][j] == phytid){
                ret = i;
                break;
            }
        }
        if(ret != -1)
            break;
    }

    return ret;
}

int
get_num_numa_regions(void)
{
    return numnodes;
}

int
get_num_active_threads_in_numa(int numaregionid)
{
    return numaactivecount[numaregionid];
}

int
get_numa_index_of_logical_thread(int logicaltid)
{
    return cpumapping[logicaltid];
}

int 
get_logical_thread_at_numa_index(int numaidx)
{
    return cpumapping[numaidx];
}

#ifndef NATIVE_COMPILATION
int ocall_get_num_active_threads_in_numa(int numaregionid)
{
    return get_num_active_threads_in_numa(numaregionid);
}

int ocall_get_thread_index_in_numa(int logicaltid)
{
    return get_thread_index_in_numa(logicaltid);
}

int ocall_get_cpu_id(int thread_id)
{
    return get_cpu_id(thread_id);
}

int ocall_get_num_numa_regions()
{
    return get_num_numa_regions();
}

void ocall_numa_thread_mark_active(int phytid)
{
    return numa_thread_mark_active(phytid);
}


#endif