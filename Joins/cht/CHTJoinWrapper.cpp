
#include "CHTJoin.hpp"
#include "CHTJoinWrapper.hpp"

#ifdef NATIVE_COMPILATION
#include <malloc.h>
#include "native_ocalls.h"
#endif


#include <pthread.h>

struct thr_arg_t
{
    int tid;
    CHTJoin *chtJoin;
};

static void * run(void * args)
{
    thr_arg_t * arg = reinterpret_cast<thr_arg_t*>(args);
    arg->chtJoin->join(arg->tid);
    return NULL;
}

static void print_result (join_result_t res, uint64_t numtuplesR, uint64_t numtuplesS,
                          struct timers_t timers)
{
    uint64_t numtuples = numtuplesR + numtuplesS;
    double cyclestuple = (double) timers.total / (double)numtuples;
    double throughput = (double) numtuples / (double) res.time_usec;
    logger(DBG, "Total input tuples : %lu", numtuples);
    logger(DBG, "Result tuples : %lu", res.matches);
    logger(DBG, "Phase Total (cycles) : %lu", timers.total);
    logger(DBG, "Phase Partition (cycles) : %lu", timers.timer1);
    logger(DBG, "Phase Join (build+probe) (cycles) : %lu", (timers.timer2 + timers.timer3));
    logger(DBG, "Build (cycles)              : %lu", timers.timer2);
    logger(DBG, "Probe (cycles)              : %lu", timers.timer3);
//    logger(DBG, "Phase Total (us) : %lu", res.time_usec);
//    logger(DBG, "Phase Partition (us) : %lu", res.part_usec);
//    logger(DBG, "Phase Join (us) : %lu", res.join_usec);
    logger(DBG, "Cycles-per-tuple            : %.4lf", cyclestuple);
    logger(DBG, "Cycles-per-Rtuple-partition : %.4lf", (double)timers.timer1 / numtuplesR);
    logger(DBG, "Cycles-per-tuple-join       : %.4lf", (double) (timers.timer2 + timers.timer3) / numtuples);
    logger(DBG, "Cycles-per-Rtuple-build     : %.4lf", (double) (timers.timer2) / numtuplesR);
    logger(DBG, "Cycles-per-Stuple-probe     : %.4lf", (double) (timers.timer3) / numtuplesS);
    logger(DBG, "Total Runtime (us) : %lu ", res.time_usec);
    logger(DBG, "Throughput (M rec/sec) : %.2lf", throughput);
#ifdef SGX_COUNTERS
    uint64_t ewb;
    ocall_get_total_ewb(&ewb);
    logger(DBG, "EWB : %lu", ewb);
#endif
}

template<int numbits>
join_result_t CHTJ(relation_t *relR, relation_t *relS, int nthreads)
{
	tuple_t * output = NULL;
//	posix_memalign((void**)(&output), 64, relR->size * sizeof(tuple_t));
	output = (tuple_t*) memalign(64, relR->num_tuples * sizeof(tuple_t));
	if (output == NULL) {
//		printf("[ERROR] memory allocation failed\n");
//		exit(-1);
        ocall_throw("memory allocation failed");
	}

//	numa_localize(output, relR->size, nthreads);

    CHTJoin *chtJoin = new CHTJoin(nthreads, 1<<numbits, relR, relS, output);
    pthread_t *threads = new pthread_t[nthreads];
//    pthread_attr_t attr;
//    cpu_set_t set;

    thr_arg_t *args = new thr_arg_t[nthreads];
//    pthread_attr_init(&attr);

    for (int i = 0; i < nthreads; ++i) {
        int cpu_idx = i % CORES;
//
//        CPU_ZERO(&set);
//        CPU_SET(cpu, &set);
//        pthread_attr_setaffinity_np(&attr, sizeof(set), &set);

        args[i].tid = i;
        args[i].chtJoin = chtJoin;

#if defined THREAD_AFFINITY && !defined NATIVE_COMPILATION
        int rv = pthread_create_cpuidx(&threads[i], NULL, cpu_idx, run, (void*)&args[i]);
#else
        (void) (cpu_idx);
        int rv = pthread_create(&threads[i], NULL, run, (void*)&args[i]);
#endif
        if (rv){
//            printf("[ERROR] return code from pthread_create() is %d\n", rv);
//            exit(-1);
            logger(DBG,"return code = %d", rv);
            ocall_throw("Return code from pthread_create() failed");
        }
    }

    for (int i = 0; i < nthreads; ++i) {
        pthread_join(threads[i], NULL);
    }

    join_result_t res = chtJoin->get_join_result();
    struct timers_t timers = chtJoin->get_timers();
    print_result(res, relR->num_tuples, relS->num_tuples, timers);
    delete chtJoin;
	delete output;
    delete[] threads;
    delete[] args;
    return res;
}
template join_result_t CHTJ<7>(relation_t *relR, relation_t *relS, int nthreads);
