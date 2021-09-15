#ifndef _NO_PARTITIONING_JOIN_H_
#define _NO_PARTITIONING_JOIN_H_

#include "npj_types.h"
#include "npj_params.h"
#include <memory>
#include "stdlib.h"
#include <stdio.h> /* vsnprintf */
#include "pthread.h"
#include "barrier.h"

#ifdef NATIVE_COMPILATION
#include <malloc.h>
#include "lock.h"
#include "Logger.h"
#include "native_ocalls.h"
#include "pcm_commons.h"
#else
#include "Enclave_t.h" /* print_string */
#include "Enclave.h"
#endif

#ifndef NEXT_POW_2
/**
 *  compute the next number, greater than or equal to 32-bit unsigned v.
 *  taken from "bit twiddling hacks":
 *  http://graphics.stanford.edu/~seander/bithacks.html
 */
#define NEXT_POW_2(V)                           \
    do {                                        \
        V--;                                    \
        V |= V >> 1;                            \
        V |= V >> 2;                            \
        V |= V >> 4;                            \
        V |= V >> 8;                            \
        V |= V >> 16;                           \
        V++;                                    \
    } while(0)
#endif

#ifndef HASH
#define HASH(X, MASK, SKIP) (((X) & MASK) >> SKIP)
#endif

struct arg_t {
    int32_t             tid;
    hashtable_t *       ht;
    struct table_t      relR;
    struct table_t      relS;
    pthread_barrier_t * barrier;
    int64_t             num_results;
#ifndef NO_TIMING
    /* stats about the thread */
    uint64_t timer1, timer2, timer3;
    uint64_t start, end;
//    struct timeval start, end;
#endif
} ;

/**
 * Initializes a new bucket_buffer_t for later use in allocating
 * buckets when overflow occurs.
 *
 * @param ppbuf [in,out] bucket buffer to be initialized
 */
void init_bucket_buffer(bucket_buffer_t ** ppbuf)
{
    bucket_buffer_t * overflowbuf;
    overflowbuf = (bucket_buffer_t*) malloc(sizeof(bucket_buffer_t));
    if (!overflowbuf) {
        logger(ERROR, "Memory alloc for overflobuf failed!");
        ocall_exit(EXIT_FAILURE);
    }
    overflowbuf->count = 0;
    overflowbuf->next  = NULL;

    *ppbuf = overflowbuf;
}

/**
 * Returns a new bucket_t from the given bucket_buffer_t.
 * If the bucket_buffer_t does not have enough space, then allocates
 * a new bucket_buffer_t and adds to the list.
 *
 * @param result [out] the new bucket
 * @param buf [in,out] the pointer to the bucket_buffer_t pointer
 */
inline void get_new_bucket(bucket_t ** result, bucket_buffer_t ** buf)
{
    if((*buf)->count < OVERFLOW_BUF_SIZE) {
        *result = (*buf)->buf + (*buf)->count;
        (*buf)->count ++;
    }
    else {
        /* need to allocate new buffer */
        bucket_buffer_t * new_buf = (bucket_buffer_t*)
                malloc(sizeof(bucket_buffer_t));
        if (!new_buf) {
            logger(ERROR, "Memory alloc for new_buf failed!");
            ocall_exit(EXIT_FAILURE);
        }
        new_buf->count = 1;
        new_buf->next  = *buf;
        *buf    = new_buf;
        *result = new_buf->buf;
    }
}

/** De-allocates all the bucket_buffer_t */
void free_bucket_buffer(bucket_buffer_t * buf)
{
    do {
        bucket_buffer_t * tmp = buf->next;
        free(buf);
        buf = tmp;
    } while(buf);
}

void allocate_hashtable(hashtable_t ** ppht, uint32_t nbuckets)
{
    hashtable_t * ht;

    ht              = (hashtable_t*)malloc(sizeof(hashtable_t));
    ht->num_buckets = nbuckets;
    NEXT_POW_2((ht->num_buckets));

    /* allocate hashtable buckets cache line aligned */
    ht->buckets = (bucket_t*) memalign(CACHE_LINE_SIZE, ht->num_buckets * sizeof(bucket_t));
    if (!ht || !ht->buckets) {
        logger(ERROR, "Memory allocation for the hashtable failed!");
        ocall_exit(EXIT_FAILURE);
    }
//    if (memalign((void**)&ht->buckets, CACHE_LINE_SIZE,
//                       ht->num_buckets * sizeof(bucket_t))){
//        printf("Aligned allocation failed!");
//        ocall_exit(EXIT_FAILURE);
//    }

    /** Not an elegant way of passing whether we will numa-localize, but this
        feature is experimental anyway. */
//    if(numalocalize) {
//        tuple_t * mem = (tuple_t *) ht->buckets;
//        uint32_t ntuples = (ht->num_buckets*sizeof(bucket_t))/sizeof(tuple_t);
//        numa_localize(mem, ntuples, nthreads);
//    }

    memset(ht->buckets, 0, ht->num_buckets * sizeof(bucket_t));
    ht->skip_bits = 0; /* the default for modulo hash */
    ht->hash_mask = (ht->num_buckets - 1) << ht->skip_bits;
    *ppht = ht;
}

/**
 * Releases memory allocated for the hashtable.
 *
 * @param ht pointer to hashtable
 */
void
destroy_hashtable(hashtable_t * ht)
{
    free(ht->buckets);
    free(ht);
}

/**
 * Single-thread hashtable build method, ht is pre-allocated.
 *
 * @param ht hastable to be built
 * @param rel the build relation
 */
void
build_hashtable_st(hashtable_t *ht, struct table_t *rel)
{
    uint64_t i;
    const uint32_t hashmask = ht->hash_mask;
    const uint32_t skipbits = ht->skip_bits;

    for(i=0; i < rel->num_tuples; i++){
        struct row_t * dest;
        bucket_t * curr, * nxt;
        int64_t idx = HASH(rel->tuples[i].key, hashmask, skipbits);

        /* copy the tuple to appropriate hash bucket */
        /* if full, follow nxt pointer to find correct place */
        curr = ht->buckets + idx;
        nxt  = curr->next;

        if(curr->count == BUCKET_SIZE) {
            if(!nxt || nxt->count == BUCKET_SIZE) {
                bucket_t * b;
                b = (bucket_t*) calloc(1, sizeof(bucket_t));
                curr->next = b;
                b->next = nxt;
                b->count = 1;
                dest = b->tuples;
            }
            else {
                dest = nxt->tuples + nxt->count;
                nxt->count ++;
            }
        }
        else {
            dest = curr->tuples + curr->count;
            curr->count ++;
        }
        *dest = rel->tuples[i];
    }
}

/**
 * Probes the hashtable for the given outer relation, returns num results.
 * This probing method is used for both single and multi-threaded version.
 *
 * @param ht hashtable to be probed
 * @param rel the probing outer relation
 *
 * @return number of matching tuples
 */
int64_t
probe_hashtable(hashtable_t *ht, struct table_t *rel)
{
    uint64_t i, j;
    int64_t matches;

    const uint32_t hashmask = ht->hash_mask;
    const uint32_t skipbits = ht->skip_bits;
#ifdef PREFETCH_NPJ
    size_t prefetch_index = PREFETCH_DISTANCE;
#endif

    matches = 0;

    for (i = 0; i < rel->num_tuples; i++)
    {
#ifdef PREFETCH_NPJ
        if (prefetch_index < rel->num_tuples) {
			intkey_t idx_prefetch = HASH(rel->tuples[prefetch_index++].key,
                                         hashmask, skipbits);
			__builtin_prefetch(ht->buckets + idx_prefetch, 0, 1);
        }
#endif

        type_key idx = HASH(rel->tuples[i].key, hashmask, skipbits);
        bucket_t * b = ht->buckets+idx;

        do {
            for(j = 0; j < b->count; j++) {
                if(rel->tuples[i].key == b->tuples[j].key){
                    matches ++;
                    /* we don't materialize the results. */
                }
            }

            b = b->next;/* follow overflow pointer */
        } while(b);
    }

    return matches;
}

/** print out the execution time statistics of the join */
static void
print_timing(uint64_t start, uint64_t end, uint64_t total, uint64_t build,
             uint64_t numtuples, int64_t result)
{
    double cyclestuple = (double) total / (double) numtuples;
    uint64_t time_usec = end - start;
    double throughput =  (double)( numtuples / time_usec);
    logger(DBG, "Total input tuples : %lu", numtuples);
    logger(DBG, "Result tuples : %lu", result);
    logger(DBG, "Phase Total (cycles) : %lu", total);
    logger(DBG, "Phase Build (cycles) : %lu", build);
    logger(DBG, "Phase Probe (cycles) : %lu", total-build);
    logger(DBG, "Cycles-per-tuple : %.4lf", cyclestuple);
    logger(DBG, "Total Runtime (us) : %lu ", time_usec);
    logger(DBG, "Throughput (M rec/sec) : %.2lf", throughput);
#ifdef SGX_COUNTERS
    uint64_t ewb;
    ocall_get_total_ewb(&ewb);
    logger(DBG, "EWB : %lu", ewb);
#endif

}

/**
 * Multi-thread hashtable build method, ht is pre-allocated.
 * Writes to buckets are synchronized via latches.
 *
 * @param ht hastable to be built
 * @param rel the build relation
 * @param overflowbuf pre-allocated chunk of buckets for overflow use.
 */
void build_hashtable_mt(hashtable_t *ht, struct table_t *rel,
                        bucket_buffer_t ** overflowbuf)
{
    uint64_t i;
    const uint32_t hashmask = ht->hash_mask;
    const uint32_t skipbits = ht->skip_bits;

#ifdef PREFETCH_NPJ
    size_t prefetch_index = PREFETCH_DISTANCE;
#endif

    for(i=0; i < rel->num_tuples; i++){
        struct row_t * dest;
        bucket_t * curr, * nxt;

#ifdef PREFETCH_NPJ
        if (prefetch_index < rel->num_tuples) {
            intkey_t idx_prefetch = HASH(rel->tuples[prefetch_index++].key,
                                         hashmask, skipbits);
			__builtin_prefetch(ht->buckets + idx_prefetch, 1, 1);
        }
#endif

        int32_t idx = HASH(rel->tuples[i].key, hashmask, skipbits);
        /* copy the tuple to appropriate hash bucket */
        /* if full, follow nxt pointer to find correct place */
        curr = ht->buckets+idx;
#ifdef NATIVE_COMPILATION
        lock(&curr->latch);
#else
        sgx_spin_lock(&curr->latch);
#endif
        nxt = curr->next;

        if(curr->count == BUCKET_SIZE) {
            if(!nxt || nxt->count == BUCKET_SIZE) {
                bucket_t * b;
                /* b = (bucket_t*) calloc(1, sizeof(bucket_t)); */
                /* instead of calloc() everytime, we pre-allocate */
                get_new_bucket(&b, overflowbuf);
                curr->next = b;
                b->next    = nxt;
                b->count   = 1;
                dest       = b->tuples;
            }
            else {
                dest = nxt->tuples + nxt->count;
                nxt->count ++;
            }
        }
        else {
            dest = curr->tuples + curr->count;
            curr->count ++;
        }

        *dest = rel->tuples[i];
#ifdef NATIVE_COMPILATION
        unlock(&curr->latch);
#else
        sgx_spin_unlock(&curr->latch);
#endif
    }

}

/**
 * Just a wrapper to call the build and probe for each thread.
 *
 * @param param the parameters of the thread, i.e. tid, ht, reln, ...
 *
 * @return
 */
void *
npo_thread(void * param)
{
    int rv = 0;
    arg_t * args = (arg_t*) param;

    /* allocate overflow buffer for each thread */
    bucket_buffer_t * overflowbuf;
    init_bucket_buffer(&overflowbuf);

//#ifdef PERF_COUNTERS
//    if(args->tid == 0){
//        PCM_initPerformanceMonitor(NULL, NULL);
//        PCM_start();
//    }
//#endif

#ifdef PCM_COUNT
    if (args->tid == 0) {
        ocall_set_system_counter_state("Start build phase");
    }
#endif

    /* wait at a barrier until each thread starts and start timer */
    barrier_arrive(args->barrier, rv);

#ifndef NO_TIMING
    /* the first thread checkpoints the start time */
    if(args->tid == 0){
//        gettimeofday(&args->start, NULL);
        args->timer3 = 0; /* no partitionig phase */

        ocall_get_system_micros(&args->start);
        ocall_startTimer(&args->timer1);
        ocall_startTimer(&args->timer2);
    }
#endif

    /* insert tuples from the assigned part of relR to the ht */
    build_hashtable_mt(args->ht, &args->relR, &overflowbuf);

    /* wait at a barrier until each thread completes build phase */
    barrier_arrive(args->barrier, rv);

//#ifdef PERF_COUNTERS
//    if(args->tid == 0){
//      PCM_stop();
//      PCM_log("========== Build phase profiling results ==========\n");
//      PCM_printResults();
//      PCM_start();
//    }
//    /* Just to make sure we get consistent performance numbers */
//    barrier_arrive(args->barrier, rv);
//#endif

#ifdef PCM_COUNT
    if (args->tid == 0) {
        ocall_get_system_counter_state("Build", 0);
        ocall_set_system_counter_state("Start probe");
    }
    barrier_arrive(args->barrier, rv);
#endif

#ifndef NO_TIMING
    /* build phase finished, thread-0 checkpoints the time */
    if(args->tid == 0){
        ocall_stopTimer(&args->timer2);
    }
#endif

    /* probe for matching tuples from the assigned part of relS */
    args->num_results = probe_hashtable(args->ht, &args->relS);

#ifndef NO_TIMING
    /* for a reliable timing we have to wait until all finishes */
    barrier_arrive(args->barrier, rv);

    /* probe phase finished, thread-0 checkpoints the time */
    if(args->tid == 0){
        ocall_stopTimer(&args->timer1);
        ocall_get_system_micros(&args->end);
    }
#endif

//#ifdef PERF_COUNTERS
//    if(args->tid == 0) {
//        PCM_stop();
//        PCM_log("========== Probe phase profiling results ==========\n");
//        PCM_printResults();
//        PCM_log("===================================================\n");
//        PCM_cleanup();
//    }
//    /* Just to make sure we get consistent performance numbers */
//    BARRIER_ARRIVE(args->barrier, rv);
//#endif

#ifdef PCM_COUNT
    if (args->tid == 0) {
        ocall_get_system_counter_state("Probe", 0);
    }
    barrier_arrive(args->barrier, rv);
#endif

    /* clean-up the overflow buffers */
    free_bucket_buffer(overflowbuf);

    return 0;
}

result_t* NPO_st(struct table_t *relR, struct table_t *relS, int nthreads) {

    (void) (nthreads);
    hashtable_t * ht;
    int64_t result = 0;
#ifndef NO_TIMING
    uint64_t timer1, timer2, start, end;
#endif
    uint32_t nbuckets = (relR->num_tuples / BUCKET_SIZE);
    allocate_hashtable(&ht, nbuckets);

#ifndef NO_TIMING
    ocall_get_system_micros(&start);
    ocall_startTimer(&timer1);
    ocall_startTimer(&timer2);
#endif

#ifdef SGX_COUNTERS
    ocall_set_sgx_counters("Start NPO_st build phase");
#endif

#ifdef PCM_COUNT
    ocall_set_system_counter_state("Start build phase");
#endif

    build_hashtable_st(ht, relR);

#ifdef PCM_COUNT
    ocall_get_system_counter_state("Build", 0);
#endif

#ifndef NO_TIMING
    ocall_stopTimer(&timer2); /* for build */
#endif

#ifdef SGX_COUNTERS
    ocall_get_sgx_counters("Start NPO_st probe phase");
#endif

#ifdef PCM_COUNT
    ocall_set_system_counter_state("Start probe phase");
#endif

    result = probe_hashtable(ht, relS);

#ifdef PCM_COUNT
    ocall_get_system_counter_state("Probe", 0);
#endif

#ifdef SGX_COUNTERS
    ocall_get_sgx_counters("Finish NPO_st");
#endif

#ifndef NO_TIMING
    ocall_get_system_micros(&end);
    ocall_stopTimer(&timer1); /* over all */
    /* now print the timing results: */
    print_timing(start, end, timer1, timer2, relR->num_tuples + relS->num_tuples, result);
#endif

    destroy_hashtable(ht);

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = result;
    joinresult->nthreads = 1;
    return joinresult;
}

result_t*
PHT (struct table_t *relR, struct table_t *relS, int nthreads) {
    hashtable_t * ht;
    int64_t result = 0;
    int32_t numR, numS, numRthr, numSthr; /* total and per thread num */
    int i, rv;
//    cpu_set_t set;
    arg_t args[nthreads];
    pthread_t tid[nthreads];
//    pthread_attr_t attr;
    pthread_barrier_t barrier;

    uint32_t nbuckets = (relR->num_tuples / BUCKET_SIZE);
    allocate_hashtable(&ht, nbuckets);

    numR = relR->num_tuples;
    numS = relS->num_tuples;
    numRthr = numR / nthreads;
    numSthr = numS / nthreads;

    rv = pthread_barrier_init(&barrier, NULL, nthreads);
    if(rv != 0){
        logger(DBG, "Couldn't create the barrier");
        ocall_exit(EXIT_FAILURE);
    }

//    pthread_attr_init(&attr);
    for(i = 0; i < nthreads; i++){
        args[i].tid = i;
        args[i].ht = ht;
        args[i].barrier = &barrier;

        /* assing part of the relR for next thread */
        args[i].relR.num_tuples = (i == (nthreads - 1)) ? numR : numRthr;
        args[i].relR.tuples = relR->tuples + numRthr * i;
        numR -= numRthr;

        /* assing part of the relS for next thread */
        args[i].relS.num_tuples = (i == (nthreads - 1)) ? numS : numSthr;
        args[i].relS.tuples = relS->tuples + numSthr * i;
        numS -= numSthr;

        /*  The attr is not supported inside the Enclave, so the new thread will be
            created with PTHREAD_CREATE_JOINABLE.
        */
#if defined THREAD_AFFINITY && !defined NATIVE_COMPILATION
        int cpu_idx = i % CORES;
        logger(DBG, "Assigning thread-%d to CPU-%d", i, cpu_idx);
        rv = pthread_create_cpuidx(&tid[i], NULL, cpu_idx, npo_thread, (void*)&args[i]);
#else
        rv = pthread_create(&tid[i], NULL, npo_thread, (void*)&args[i]);
#endif
        if (rv){
            logger(ERROR, "ERROR; return code from pthread_create() is %d\n", rv);
            ocall_exit(-1);
        }

    }

    for(i = 0; i < nthreads; i++){
        pthread_join(tid[i], NULL);
        /* sum up results */
        result += args[i].num_results;
    }


#ifndef NO_TIMING
    /* now print the timing results: */
    print_timing(args[0].start, args[0].end, args[0].timer1, args[0].timer2,
                 relR->num_tuples + relS->num_tuples, result);
#endif

    destroy_hashtable(ht);

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = result;
    joinresult->nthreads = nthreads;

    return joinresult;
}

#endif // _NO_PARTITIONING_JOIN_H_
