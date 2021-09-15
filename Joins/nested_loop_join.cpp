#include <stdint.h>
#include "btree.h"
#include <pthread.h>
#include "data-types.h"
#ifdef NATIVE_COMPILATION
#include "Logger.h"
#include "native_ocalls.h"
#else
#include "Enclave_t.h"
#include "Enclave.h"
#endif

typedef struct arg_nl_t {
    tuple_t * relR;
    tuple_t * relS;

    uint64_t numR;
    uint64_t numS;

    uint64_t result;
    int32_t my_tid;
} arg_nl_t;

static void
print_timing(uint64_t total, uint64_t numtuples, int64_t result)
{
    double cyclestuple = (double) total / (double) numtuples;
    logger(DBG, "Total input tuples : %lu", numtuples);
    logger(DBG, "Result tuples : %lu", result);
    logger(DBG, "Phase Join (cycles) : %lu", total);
    logger(DBG, "Cycles-per-tuple : %.4lf", cyclestuple);
}

void * nlj_thread(void * param) {
    arg_nl_t *args = (arg_nl_t*) param;
    uint64_t results = 0;

    for (int32_t i = 0; i < args->numR; i++)
    {
        for (int32_t j = 0; j < args->numS; j++)
        {
            if (args->relR[i].key == args->relS[j].key)
            {
                results++;
            }
        }
    }

    args->result = results;
    return nullptr;
}

result_t* NL (struct table_t* relR, struct table_t* relS, int nthreads) {
    (void) (nthreads);

    int64_t result = 0;
    pthread_t tid[nthreads];
    arg_nl_t args[nthreads];
    uint64_t numperthr[2];
#ifndef NO_TIMING
    uint64_t timer1;
    ocall_startTimer(&timer1);
#endif
#ifdef PCM_COUNT
    ocall_set_system_counter_state("Start join phase");
#endif

    numperthr[0] = relR->num_tuples / nthreads;
    numperthr[1] = relS->num_tuples / nthreads;

    for (int i = 0; i < nthreads; i++) {
        args[i].my_tid = i;
        args[i].relR = relR->tuples + i * numperthr[0];
        args[i].relS = relS->tuples;
        args[i].numR = (i == (nthreads-1)) ?
                       (relR->num_tuples - i * numperthr[0]) : numperthr[0];
        args[i].numS = relS->num_tuples;
        args[i].result = 0;

        int rv = pthread_create(&tid[i], nullptr, nlj_thread, (void*)&args[i]);
        if (rv){
            logger(ERROR, "return code from pthread_create() is %d\n", rv);
            ocall_exit(-1);
        }
    }

    for (int i = 0; i < nthreads; i++) {
        pthread_join(tid[i], NULL);
        result += args[i].result;
    }

#ifdef PCM_COUNT
    ocall_get_system_counter_state("Join", 0);
#endif

#ifndef NO_TIMING
    ocall_stopTimer(&timer1);
    print_timing(timer1, relR->num_tuples + relS->num_tuples, result);
#endif

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = result;
    joinresult->nthreads = nthreads;
    return joinresult;
}

struct arg_inl_t {
    uint32_t my_tid;
    tuple_t * relR;
//    tuple_t * relS;

    uint32_t numR;
    uint32_t totalR;

    stx::btree<type_key, type_value> * indexS;

    uint32_t matches = 0;

};

void * inl_thread(void * param)
{
    uint32_t i, matches = 0;
    arg_inl_t * args = (arg_inl_t*) param;
//    uint32_t my_tid = args->my_tid;

    stx::btree<type_key, type_value> * index = args->indexS;

    // for each R scan S-index
    for (i = 0; i < args->numR; i++) {
        row_t r = args->relR[i];
        size_t count = index->count(r.key);
        if (count) {
            auto it = index->find(r.key);
            for (size_t j = 0; j < count; j++) {
                matches++;
                it++;
            }
        }
    }
//    logger(INFO, "Thread-%d matches: %u", my_tid, matches);
    args->matches = matches;
    return nullptr;
}

void print_timing(uint64_t total_cycles, uint64_t numtuples, uint64_t join_matches,
                  uint64_t start, uint64_t end)
{
    double cyclestuple = (double) total_cycles / (double) numtuples;
    uint64_t time_usec = end - start;
    double throughput = numtuples / (double) time_usec;

    logger(ENCLAVE, "Total input tuples     : %lu", numtuples);
    logger(ENCLAVE, "Result tuples          : %lu", join_matches);
    logger(ENCLAVE, "Phase Join (cycles)    : %lu", total_cycles);
    logger(ENCLAVE, "Cycles-per-tuple       : %.4lf", cyclestuple);
    logger(ENCLAVE, "Total Runtime (us)     : %lu ", time_usec);
    logger(ENCLAVE, "Throughput (M rec/sec) : %.2lf", throughput);
}

result_t* INL (struct table_t* relR, struct table_t* relS, int nthreads) {
    uint64_t i, matches = 0;
    int rv;
    stx::btree<type_key, type_value> index;

    pthread_t tid[nthreads];
    arg_inl_t args[nthreads];
    uint64_t numperthr[2];

    uint64_t timer, start, end;

    numperthr[0] = relR->num_tuples / nthreads;
    numperthr[1] = relS->num_tuples / nthreads;

    // build index on S
    for (i = 0; i < relS->num_tuples; i++) {
        index.insert(std::make_pair(relS->tuples[i].key, relS->tuples[i].payload));
    }

    logger(DBG, "Index complete. Size: %zu", index.size());

    ocall_startTimer(&timer);
    ocall_get_system_micros(&start);
#ifdef PCM_COUNT
    ocall_set_system_counter_state("Start join phase");
#endif
    for (i = 0; i < nthreads; i++) {
        args[i].relR = relR->tuples + i * numperthr[0];

        args[i].numR = (i == (nthreads-1)) ?
                       (relR->num_tuples - i * numperthr[0]) : numperthr[0];
        args[i].totalR = relR->num_tuples;

        args[i].my_tid = i;
        args[i].indexS = &index;

        rv = pthread_create(&tid[i], nullptr, inl_thread, (void*)&args[i]);

        if (rv){
            logger(ERROR, "return code from pthread_create() is %d\n", rv);
            ocall_exit(-1);
        }
    }

    for (i = 0; i < nthreads; i++) {
        pthread_join(tid[i], nullptr);
        matches += args[i].matches;
    }
#ifdef PCM_COUNT
    ocall_get_system_counter_state("Join", 0);
#endif
    ocall_get_system_micros(&end);
    ocall_stopTimer(&timer);
    print_timing(timer, relR->num_tuples + relS->num_tuples, matches, start, end);

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = matches;
    joinresult->nthreads = nthreads;
    return joinresult;
}
