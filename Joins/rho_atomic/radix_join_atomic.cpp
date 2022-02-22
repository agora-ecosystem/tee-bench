#include "radix_join_atomic.h"
#include <stdlib.h>
#include "data-types.h"
#include "prj_params.h"
#include "barrier.h"
#include <pthread.h>
#include "task_queue_atomic.hpp"
#include "util.h"
#ifdef NATIVE_COMPILATION
#include "Logger.h"
#include "native_ocalls.h"
#include "malloc.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)
#define MAX(X, Y) (((X) > (Y)) ? (X) : (Y))
#define SYNC_TIMERS_START(A, TID)
#define SYNC_TIMER_STOP(T)
#define SYNC_GLOBAL_STOP(T, TID)
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

#ifndef TUPLESPERCACHELINE
#define TUPLESPERCACHELINE (CACHE_LINE_SIZE/sizeof(tuple_t))
#endif

typedef union {
    struct {
        tuple_t tuples[CACHE_LINE_SIZE/sizeof(tuple_t)];
    } tuples;
    struct {
        tuple_t tuples[CACHE_LINE_SIZE/sizeof(tuple_t) - 1];
        int32_t slot;
    } data;
} cache_line_t;


typedef struct arg_t_radix  arg_t_radix;
typedef struct part_t part_t;
//typedef int64_t (*JoinFunction)(const struct table_t * const,
//                                const struct table_t * const,
//                                struct table_t * const,
//                                output_list_t ** output);

/** holds the arguments passed to each thread */
struct arg_t_radix {
    int32_t ** histR;
    struct row_t *  relR;
    struct row_t *  tmpR;
    int32_t ** histS;
    struct row_t *  relS;
    struct row_t *  tmpS;

    uint64_t numR;
    uint64_t numS;
    uint64_t totalR;
    uint64_t totalS;

    task_queue_atomic_t *      join_queue;
    task_queue_atomic_t *      part_queue;
#ifdef SKEW_HANDLING
    task_queue_t *      skew_queue;
    task_t **           skewtask;
#endif
    pthread_barrier_t * barrier;
    JoinFunction        join_function;
    int64_t result;
    int32_t my_tid;
    int     nthreads;

    /* stats about the thread */
    int32_t        parts_processed;
    uint64_t       timer1, timer2, timer3;
    uint64_t       start, end;
    uint64_t       pass1, pass2;
#ifdef SYNCSTATS
    /** Thread local timers : */
    synctimer_t localtimer;
    /** Global synchronization timers, only filled in by thread-0 */
    synctimer_t * globaltimer;
#endif

#ifdef JOIN_MATERIALIZE
    /* results of the thread */
    threadresult_t * threadresult;
#endif
} __attribute__((aligned(CACHE_LINE_SIZE)));

/** holds arguments passed for partitioning */
struct part_t {
    struct row_t *  rel;
    struct row_t *  tmp;
    int32_t ** hist;
    int32_t *  output;
    arg_t_radix   *  thrargs;
    uint32_t   num_tuples;
    uint32_t   total_tuples;
    int32_t    R;
    uint32_t   D;
    int        relidx;  /* 0: R, 1: S */
    uint32_t   padding;
} __attribute__((aligned(CACHE_LINE_SIZE)));

static void *
alloc_aligned(size_t size)
{
    void * ret;
    ret = memalign(CACHE_LINE_SIZE, size);

    malloc_check(ret);

    return ret;
}

void print_timing_atomic(uint64_t total, uint64_t build, uint64_t part,
                  uint64_t numtuples, int64_t result,
                  uint64_t start, uint64_t end,
                  uint64_t pass1, uint64_t pass2) {
    double cyclestuple = (double) total / (double) numtuples;
    uint64_t time_usec = end - start;
    double throughput = numtuples  / (double)time_usec;
    logger(ENCLAVE, "Total input tuples : %u", numtuples);
    logger(ENCLAVE, "Result tuples : %lu", result);
    logger(ENCLAVE, "Phase Total (cycles) : %lu", total);
    logger(ENCLAVE, "Phase Partition (cycles) : %lu", part);
    logger(ENCLAVE, "Phase Join (cycles) : %lu", build);
    logger(ENCLAVE, "Cycles-per-tuple           : %.4lf", cyclestuple);
    logger(ENCLAVE, "Cycles-per-tuple-partition : %.4lf", (double)part/numtuples);
    logger(ENCLAVE, "Cycles-per-tuple-pass1     : %.4lf", (double)pass1/numtuples);
    logger(ENCLAVE, "Cycles-per-tuple-pass2     : %.4lf", (double)pass2/numtuples);
    logger(ENCLAVE, "Cycles-per-tuple-join      : %.4lf", (double)build/numtuples);
    logger(ENCLAVE, "Total Runtime (us) : %lu ", time_usec);
    logger(ENCLAVE, "Throughput (M rec/sec) : %.2lf", throughput);
#ifdef SGX_COUNTERS
    uint64_t ewb;
    ocall_get_total_ewb(&ewb);
    logger(DBG, "EWB : %lu", ewb);
#endif
}
/**
 *  This algorithm builds the hashtable using the bucket chaining idea and used
 *  in PRO implementation. Join between given two relations is evaluated using
 *  the "bucket chaining" algorithm proposed by Manegold et al. It is used after
 *  the partitioning phase, which is common for all algorithms. Moreover, R and
 *  S typically fit into L2 or at least R and |R|*sizeof(int) fits into L2 cache.
 *
 * @param R input relation R
 * @param S input relation S
 *
 * @return number of result tuples
 */
int64_t
bucket_chaining_join_atomic(const struct table_t *const R,
                            const struct table_t *const S,
                            struct table_t *const tmpR,
                            output_list_t ** output)
{
    (void) (tmpR);
    (void) (output);
    int * next, * bucket;
    const uint64_t numR = R->num_tuples;
    uint32_t N = numR;
    int64_t matches = 0;

    NEXT_POW_2(N);
    /* N <<= 1; */
    const uint32_t MASK = (N-1) << (NUM_RADIX_BITS);

    next   = (int*) malloc(sizeof(int) * numR);
    /* posix_memalign((void**)&next, CACHE_LINE_SIZE, numR * sizeof(int)); */
    bucket = (int*) calloc(N, sizeof(int));

    const struct row_t * const Rtuples = R->tuples;
    for(uint32_t i=0; i < numR; ){
        uint32_t idx = HASH_BIT_MODULO(R->tuples[i].key, MASK, NUM_RADIX_BITS);
        next[i]      = bucket[idx];
        bucket[idx]  = ++i;     /* we start pos's from 1 instead of 0 */

        /* Enable the following tO avoid the code elimination
           when running probe only for the time break-down experiment */
        /* matches += idx; */
    }

    const struct row_t * const Stuples = S->tuples;
    const uint64_t        numS    = S->num_tuples;

    /* Disable the following loop for no-probe for the break-down experiments */
    /* PROBE- LOOP */
    for(uint32_t i=0; i < numS; i++ ){

        uint32_t idx = HASH_BIT_MODULO(Stuples[i].key, MASK, NUM_RADIX_BITS);

        for(int hit = bucket[idx]; hit > 0; hit = next[hit-1]){

            if(Stuples[i].key == Rtuples[hit-1].key){
                matches ++;
#ifdef JOIN_MATERIALIZE
                insert_output(output, Stuples[i].key, Rtuples[hit-1].payload, Stuples[i].payload);
#endif
            }
        }
    }
    /* PROBE-LOOP END  */

    /* clean up temp */
    free(bucket);
    free(next);

    return matches;
}

/** computes and returns the histogram size for join */
inline
uint32_t
get_hist_size_atomic(uint32_t relSize) __attribute__((always_inline));

inline
uint32_t
get_hist_size_atomic(uint32_t relSize)
{
    NEXT_POW_2(relSize);
    relSize >>= 2;
    if(relSize < 4) relSize = 4;
    return relSize;
}

/**
 * Histogram-based hash table build method together with relation re-ordering as
 * described by Kim et al. It joins partitions Ri, Si of relations R & S.
 * This is version is not optimized with SIMD and prefetching. The parallel
 * radix join implementation using this function is PRH.
 */
int64_t
histogram_join_atomic(const relation_t * const R,
               const relation_t * const S,
               relation_t * const tmpR,
               output_list_t ** output)
{
    (void) (output);
    int32_t * hist;
    const tuple_t * const Rtuples = R->tuples;
    const uint64_t numR  = R->num_tuples;
    uint32_t       Nhist = get_hist_size_atomic(numR);
    const uint32_t MASK  = (Nhist-1) << NUM_RADIX_BITS;

    hist   = (int32_t*) calloc(Nhist+2, sizeof(int32_t));

    for( uint32_t i = 0; i < numR; i++ ) {

        uint32_t idx = HASH_BIT_MODULO(Rtuples[i].key, MASK, NUM_RADIX_BITS);

        hist[idx+2] ++;
    }

    /* prefix sum on histogram */
    for( uint32_t i = 2, sum = 0; i <= Nhist+1; i++ ) {
        sum     += hist[i];
        hist[i]  = sum;
    }

    tuple_t * const tmpRtuples = tmpR->tuples;
    /* reorder tuples according to the prefix sum */
    for( uint32_t i = 0; i < numR; i++ ) {

        uint32_t idx = HASH_BIT_MODULO(Rtuples[i].key, MASK, NUM_RADIX_BITS) + 1;

        tmpRtuples[hist[idx]] = Rtuples[i];

        hist[idx] ++;
    }

    int64_t              match   = 0;
    const uint64_t        numS    = S->num_tuples;
    const tuple_t * const Stuples = S->tuples;
    /* now comes the probe phase, TODO: implement prefetching */
    for( uint32_t i = 0; i < numS; i++ ) {

        uint32_t idx = HASH_BIT_MODULO(Stuples[i].key, MASK, NUM_RADIX_BITS);

        int j = hist[idx], end = hist[idx+1];

        /* Scalar comparisons */
        for(; j < end; j++) {

            if(Stuples[i].key == tmpRtuples[j].key) {

                ++ match;
#ifdef JOIN_MATERIALIZE
                insert_output(output, Stuples[i].key, tmpRtuples[j].payload, Stuples[i].payload);
#endif
            }

        }
    }

    /* clean up */
    free(hist);

    return match;
}


/**
 * Radix clustering algorithm which does not put padding in between
 * clusters. This is used only by single threaded radix join implementation RJ.
 *
 * @param outRel
 * @param inRel
 * @param hist
 * @param R
 * @param D
 */
void
radix_cluster_nopadding_atomic(struct table_t * outRel, struct table_t * inRel, int R, int D)
{
    row_t ** dst;
    row_t * input;
    /* tuple_t ** dst_end; */
    uint32_t * tuples_per_cluster;
    uint32_t i;
    uint32_t offset;
    const uint32_t M = ((1 << D) - 1) << R;
    const uint32_t fanOut = 1 << D;
    const uint64_t ntuples = inRel->num_tuples;

    tuples_per_cluster = (uint32_t*)calloc(fanOut, sizeof(uint32_t));
    /* the following are fixed size when D is same for all the passes,
       and can be re-used from call to call. Allocating in this function
       just in case D differs from call to call. */
    dst     = (row_t**)malloc(sizeof(row_t*)*fanOut);
    /* dst_end = (tuple_t**)malloc(sizeof(tuple_t*)*fanOut); */

    input = inRel->tuples;
    /* count tuples per cluster */
    for( i=0; i < ntuples; i++ ){
        uint32_t idx = (uint32_t)(HASH_BIT_MODULO(input->key, M, R));
        tuples_per_cluster[idx]++;
        input++;
    }

    offset = 0;
    /* determine the start and end of each cluster depending on the counts. */
    for ( i=0; i < fanOut; i++ ) {
        dst[i]      = outRel->tuples + offset;
        offset     += tuples_per_cluster[i];
        /* dst_end[i]  = outRel->tuples + offset; */
    }

    input = inRel->tuples;
    /* copy tuples to their corresponding clusters at appropriate offsets */
    for( i=0; i < ntuples; i++ ){
        uint32_t idx   = (uint32_t)(HASH_BIT_MODULO(input->key, M, R));
        *dst[idx] = *input;
        ++dst[idx];
        input++;
        /* we pre-compute the start and end of each cluster, so the following
           check is unnecessary */
        /* if(++dst[idx] >= dst_end[idx]) */
        /*     REALLOCATE(dst[idx], dst_end[idx]); */
    }

    /* clean up temp */
    /* free(dst_end); */
    free(dst);
    free(tuples_per_cluster);
}

/**
 * Radix clustering algorithm (originally described by Manegold et al)
 * The algorithm mimics the 2-pass radix clustering algorithm from
 * Kim et al. The difference is that it does not compute
 * prefix-sum, instead the sum (offset in the code) is computed iteratively.
 *
 * @warning This method puts padding between clusters, see
 * radix_cluster_nopadding for the one without padding.
 *
 * @param outRel [out] result of the partitioning
 * @param inRel [in] input relation
 * @param hist [out] number of tuples in each partition
 * @param R cluster bits
 * @param D radix bits per pass
 * @returns tuples per partition.
 */
void
radix_cluster_atomic(struct table_t * outRel,
              struct table_t * inRel,
              int64_t * hist,
              int R,
              int D)
{
    uint64_t i;
    uint32_t M = ((1 << D) - 1) << R;
    uint32_t offset;
    uint32_t fanOut = 1 << D;

    /* the following are fixed size when D is same for all the passes,
       and can be re-used from call to call. Allocating in this function
       just in case D differs from call to call. */
    uint32_t dst[fanOut];

    /* count tuples per cluster */
    for( i=0; i < inRel->num_tuples; i++ ){
        uint64_t idx = HASH_BIT_MODULO(inRel->tuples[i].key, M, R);
        hist[idx]++;
    }
    offset = 0;
    /* determine the start and end of each cluster depending on the counts. */
    for ( i=0; i < fanOut; i++ ) {
        /* dst[i]      = outRel->tuples + offset; */
        /* determine the beginning of each partitioning by adding some
           padding to avoid L1 conflict misses during scatter. */
        dst[i] = (uint32_t) (offset + i * SMALL_PADDING_TUPLES);
        offset += hist[i];
    }

    /* copy tuples to their corresponding clusters at appropriate offsets */
    for( i=0; i < inRel->num_tuples; i++ ){
        uint32_t idx   = HASH_BIT_MODULO(inRel->tuples[i].key, M, R);
        outRel->tuples[ dst[idx] ] = inRel->tuples[i];
        ++dst[idx];
    }
}


/**
 * This function implements the radix clustering of a given input
 * relations. The relations to be clustered are defined in task_t and after
 * clustering, each partition pair is added to the join_queue to be joined.
 *
 * @param task description of the relation to be partitioned
 * @param join_queue task queue to add join tasks after clustering
 */
void serial_radix_partition_atomic(task_atomic_t * const task,
                            task_queue_atomic_t * join_queue,
                            const int R, const int D)
{
    int i;
    uint64_t offsetR = 0, offsetS = 0;
    const int fanOut = 1 << D;  /*(NUM_RADIX_BITS / NUM_PASSES);*/
    int64_t * outputR, * outputS;

    outputR = (int64_t*)calloc(fanOut+1, sizeof(int64_t));
    outputS = (int64_t*)calloc(fanOut+1, sizeof(int64_t));
    /* TODO: measure the effect of memset() */
    /* memset(outputR, 0, fanOut * sizeof(int32_t)); */
    radix_cluster_atomic(&task->tmpR, &task->relR, outputR, R, D);

    /* memset(outputS, 0, fanOut * sizeof(int32_t)); */
    radix_cluster_atomic(&task->tmpS, &task->relS, outputS, R, D);

    /* task_t t; */
    for(i = 0; i < fanOut; i++) {
        if(outputR[i] > 0 && outputS[i] > 0) {
            task_atomic_t * t = task_queue_atomic_get_slot(join_queue);
            t->relR.num_tuples = outputR[i];
            t->relR.tuples = task->tmpR.tuples + offsetR
                             + i * SMALL_PADDING_TUPLES;
            t->tmpR.tuples = task->relR.tuples + offsetR
                             + i * SMALL_PADDING_TUPLES;
            offsetR += outputR[i];

            t->relS.num_tuples = outputS[i];
            t->relS.tuples = task->tmpS.tuples + offsetS
                             + i * SMALL_PADDING_TUPLES;
            t->tmpS.tuples = task->relS.tuples + offsetS
                             + i * SMALL_PADDING_TUPLES;
            offsetS += outputS[i];

            /* task_queue_copy_atomic(join_queue, &t); */
//            task_queue_atomic_add(join_queue, t);
        }
        else {
            offsetR += outputR[i];
            offsetS += outputS[i];
        }
    }
    free(outputR);
    free(outputS);
}

/**
 * This function implements the parallel radix partitioning of a given input
 * relation. Parallel partitioning is done by histogram-based relation
 * re-ordering as described by Kim et al. Parallel partitioning method is
 * commonly used by all parallel radix join algorithms.
 *
 * @param part description of the relation to be partitioned
 */
void
parallel_radix_partition_atomic(part_t * const part)
{
    const struct row_t * rel = part->rel;
    int32_t **           hist  = part->hist;
    int32_t *       output   = part->output;

    const uint32_t my_tid     = part->thrargs->my_tid;
    const uint32_t nthreads   = part->thrargs->nthreads;
    const uint32_t size       = part->num_tuples;

    const int32_t  R       = part->R;
    const int32_t  D       = part->D;
    const uint32_t fanOut  = 1 << D;
    const uint32_t MASK    = (fanOut - 1) << R;
    const uint32_t padding = part->padding;

    if(my_tid == 0)
    {
        logger(DBG, "Radix partitioning. R=%d, D=%d, fanout=%d, MASK=%d",
               R, D, fanOut, MASK);
    }

    int32_t sum = 0;
    uint32_t i, j;
    int rv = 0;

    int32_t dst[fanOut+1];

    /* compute local histogram for the assigned region of rel */
    /* compute histogram */
    int32_t * my_hist = hist[my_tid];

    for(i = 0; i < size; i++) {
        uint32_t idx = HASH_BIT_MODULO(rel[i].key, MASK, R);
        my_hist[idx] ++;
    }

    /* compute local prefix sum on hist */
    for(i = 0; i < fanOut; i++){
        sum += my_hist[i];
        my_hist[i] = sum;
    }

    SYNC_TIMER_STOP(&part->thrargs->localtimer.sync1[part->relidx]);
    /* wait at a barrier until each thread complete histograms */
    barrier_arrive(part->thrargs->barrier, rv);
    /* barrier global sync point-1 */
    SYNC_GLOBAL_STOP(&part->thrargs->globaltimer->sync1[part->relidx], my_tid);

    /* determine the start and end of each cluster */
    for(i = 0; i < my_tid; i++) {
        for(j = 0; j < fanOut; j++)
            output[j] += hist[i][j];
    }
    for(i = my_tid; i < nthreads; i++) {
        for(j = 1; j < fanOut; j++)
            output[j] += hist[i][j-1];
    }

    for(i = 0; i < fanOut; i++ ) {
        output[i] += i * padding; //PADDING_TUPLES;
        dst[i] = output[i];
    }
    output[fanOut] = part->total_tuples + fanOut * padding; //PADDING_TUPLES;

    struct row_t * tmp = part->tmp;

    /* Copy tuples to their corresponding clusters */
    for(i = 0; i < size; i++ ){
        uint32_t idx = HASH_BIT_MODULO(rel[i].key, MASK, R);
        tmp[dst[idx]] = rel[i];
        ++dst[idx];
    }
}

/**
 * Makes a non-temporal write of 64 bytes from src to dst.
 * Uses vectorized non-temporal stores if available, falls
 * back to assignment copy.
 *
 * @param dst
 * @param src
 *
 * @return
 */
static inline void
store_nontemp_64B_atomic(void * dst, void * src)
{
    /* just copy with assignment */
    *(cache_line_t *)dst = *(cache_line_t *)src;
}


/**
 * This function implements the parallel radix partitioning of a given input
 * relation. Parallel partitioning is done by histogram-based relation
 * re-ordering as described by Kim et al. Parallel partitioning method is
 * commonly used by all parallel radix join algorithms. However this
 * implementation is further optimized to benefit from write-combining and
 * non-temporal writes.
 *
 * @param part description of the relation to be partitioned
 */
void
parallel_radix_partition_optimized_atomic(part_t * const part)
{
    const tuple_t * rel    = part->rel;
    int32_t **      hist   = part->hist;
    int32_t *       output = part->output;

    const uint32_t my_tid     = part->thrargs->my_tid;
    const uint32_t nthreads   = part->thrargs->nthreads;
    const uint32_t num_tuples = part->num_tuples;

    const int32_t  R       = part->R;
    const int32_t  D       = part->D;
    const uint32_t fanOut  = 1 << D;
    const uint32_t MASK    = (fanOut - 1) << R;
    const uint32_t padding = part->padding;

    int32_t sum = 0;
    uint32_t i, j;
    int rv = 0;

    /* compute local histogram for the assigned region of rel */
    /* compute histogram */
    int32_t * my_hist = hist[my_tid];

    for(i = 0; i < num_tuples; i++) {
        uint32_t idx = HASH_BIT_MODULO(rel[i].key, MASK, R);
        my_hist[idx] ++;
    }

    /* compute local prefix sum on hist */
    for(i = 0; i < fanOut; i++){
        sum += my_hist[i];
        my_hist[i] = sum;
    }

    SYNC_TIMER_STOP(&part->thrargs->localtimer.sync1[part->relidx]);
    /* wait at a barrier until each thread complete histograms */
    barrier_arrive(part->thrargs->barrier, rv);
    /* barrier global sync point-1 */
    SYNC_GLOBAL_STOP(&part->thrargs->globaltimer->sync1[part->relidx], my_tid);

    /* determine the start and end of each cluster */
    for(i = 0; i < my_tid; i++) {
        for(j = 0; j < fanOut; j++)
            output[j] += hist[i][j];
    }
    for(i = my_tid; i < nthreads; i++) {
        for(j = 1; j < fanOut; j++)
            output[j] += hist[i][j-1];
    }

    /* uint32_t pre; /\* nr of tuples to cache-alignment *\/ */
    tuple_t * tmp = part->tmp;
    /* software write-combining buffer */
    cache_line_t buffer[fanOut] __attribute__((aligned(CACHE_LINE_SIZE)));

    for(i = 0; i < fanOut; i++ ) {
        uint32_t off = output[i] + i * padding;
        /* pre        = (off + TUPLESPERCACHELINE) & ~(TUPLESPERCACHELINE-1); */
        /* pre       -= off; */
        output[i]  = off;
        buffer[i].data.slot = off;
    }
    output[fanOut] = part->total_tuples + fanOut * padding;

    /* Copy tuples to their corresponding clusters */
    for(i = 0; i < num_tuples; i++ ){
        uint32_t  idx     = HASH_BIT_MODULO(rel[i].key, MASK, R);
        uint32_t  slot    = buffer[idx].data.slot;
        tuple_t * tup     = (tuple_t *)(buffer + idx);
        uint32_t  slotMod = (slot) & (TUPLESPERCACHELINE - 1);
        tup[slotMod]      = rel[i];

        if(slotMod == (TUPLESPERCACHELINE-1)){
            /* write out 64-Bytes with non-temporal store */
            store_nontemp_64B_atomic((tmp+slot-(TUPLESPERCACHELINE-1)), (buffer+idx));
            /* writes += TUPLESPERCACHELINE; */
        }

        buffer[idx].data.slot = slot+1;
    }
    /* _mm_sfence (); */

    /* write out the remainders in the buffer */
    for(i = 0; i < fanOut; i++ ) {
        uint32_t slot  = buffer[i].data.slot;
        uint32_t sz    = (slot) & (TUPLESPERCACHELINE - 1);
        slot          -= sz;
        for(j = 0; j < sz; j++) {
            tmp[slot]  = buffer[i].data.tuples[j];
            slot ++;
        }
    }
}


/**
 * The main thread of parallel radix join. It does partitioning in parallel with
 * other threads and during the join phase, picks up join tasks from the task
 * queue and calls appropriate JoinFunction to compute the join task.
 *
 * @param param
 *
 * @return
 */
void *
prj_thread_atomic(void * param)
{
    arg_t_radix * args   = (arg_t_radix*) param;
    int32_t my_tid = args->my_tid;

    const int fanOut = 1 << (NUM_RADIX_BITS / NUM_PASSES);
    const int R = (NUM_RADIX_BITS / NUM_PASSES);
    const int D = (NUM_RADIX_BITS - (NUM_RADIX_BITS / NUM_PASSES));
    const int thresh1 = (const int) (MAX((1<<D), (1<<R)) * THRESHOLD1((unsigned long)args->nthreads));

    if (args->my_tid == 0)
    {
        logger(DBG, "NUM_PASSES=%d, RADIX_BITS=%d", NUM_PASSES, NUM_RADIX_BITS);
        logger(DBG, "fanOut = %d, R = %d, D = %d, thresh1 = %d", fanOut, R, D, thresh1);
    }
    uint64_t results = 0;
    int i;
    int rv = 0;

    part_t part;
    task_atomic_t * task;
    task_queue_atomic_t * part_queue;
    task_queue_atomic_t * join_queue;
#ifdef SKEW_HANDLING
    task_queue_t * skew_queue;
#endif

    int32_t * outputR = (int32_t *) calloc((fanOut+1), sizeof(int32_t));
    int32_t * outputS = (int32_t *) calloc((fanOut+1), sizeof(int32_t));
    malloc_check((void*)(outputR && outputS));

    part_queue = args->part_queue;
    join_queue = args->join_queue;
#ifdef SKEW_HANDLING
    skew_queue = args->skew_queue;
#endif

    args->histR[my_tid] = (int32_t *) calloc(fanOut, sizeof(int32_t));
    args->histS[my_tid] = (int32_t *) calloc(fanOut, sizeof(int32_t));

    /* in the first pass, partitioning is done together by all threads */

    args->parts_processed = 0;

#ifdef PCM_COUNT
    if (my_tid == 0) {
        ocall_set_system_counter_state("Partition");
    }
#endif

    /* wait at a barrier until each thread starts and then start the timer */
    barrier_arrive(args->barrier, rv);

    /* if monitoring synchronization stats */
    SYNC_TIMERS_START(args, my_tid);

#ifndef RADIX_NO_TIMING
    if(my_tid == 0){
        /* thread-0 checkpoints the time */
        ocall_get_system_micros(&args->start);
        ocall_startTimer(&args->timer1);
        ocall_startTimer(&args->timer2);
        ocall_startTimer(&args->timer3);
        ocall_startTimer(&args->pass1);
    }
#endif

    /********** 1st pass of multi-pass partitioning ************/
    part.R       = 0;
    part.D       = NUM_RADIX_BITS / NUM_PASSES;
    part.thrargs = args;
    part.padding = PADDING_TUPLES;

    /* 1. partitioning for relation R */
    part.rel          = args->relR;
    part.tmp          = args->tmpR;
    part.hist         = args->histR;
    part.output       = outputR;
    part.num_tuples   = args->numR;
    part.total_tuples = args->totalR;
    part.relidx       = 0;

#ifdef USE_SWWC_OPTIMIZED_PART
    logger(DBG, "Use SSWC optimized part");
    parallel_radix_partition_optimized(&part);
#else
    parallel_radix_partition_atomic(&part);
#endif

    /* 2. partitioning for relation S */
    part.rel          = args->relS;
    part.tmp          = args->tmpS;
    part.hist         = args->histS;
    part.output       = outputS;
    part.num_tuples   = args->numS;
    part.total_tuples = args->totalS;
    part.relidx       = 1;

#ifdef USE_SWWC_OPTIMIZED_PART
    parallel_radix_partition_optimized(&part);
#else
    parallel_radix_partition_atomic(&part);
#endif


    /* wait at a barrier until each thread copies out */
    barrier_arrive(args->barrier, rv);

    /********** end of 1st partitioning phase ******************/

    /* 3. first thread creates partitioning tasks for 2nd pass */
    if(my_tid == 0) {
        for(i = 0; i < fanOut; i++) {
            int32_t ntupR = outputR[i+1] - outputR[i] - (int32_t) PADDING_TUPLES;
            int32_t ntupS = outputS[i+1] - outputS[i] - (int32_t) PADDING_TUPLES;

#ifdef SKEW_HANDLING
            if(ntupR > thresh1 || ntupS > thresh1){
//                logger(ENCLAVE, "Adding to skew_queue= R:%d, S:%d", ntupR, ntupS);

                task_t * t = task_queue_get_slot(skew_queue);

                t->relR.num_tuples = t->tmpR.num_tuples = ntupR;
                t->relR.tuples = args->tmpR + outputR[i];
                t->tmpR.tuples = args->relR + outputR[i];

                t->relS.num_tuples = t->tmpS.num_tuples = ntupS;
                t->relS.tuples = args->tmpS + outputS[i];
                t->tmpS.tuples = args->relS + outputS[i];

                task_queue_add(skew_queue, t);
            }
            else
#endif
            if(ntupR > 0 && ntupS > 0) {
                task_atomic_t * t = task_queue_atomic_get_slot(part_queue);

                t->relR.num_tuples = t->tmpR.num_tuples = ntupR;
                t->relR.tuples = args->tmpR + outputR[i];
                t->tmpR.tuples = args->relR + outputR[i];

                t->relS.num_tuples = t->tmpS.num_tuples = ntupS;
                t->relS.tuples = args->tmpS + outputS[i];
                t->tmpS.tuples = args->relS + outputS[i];

//                task_queue_atomic_add(part_queue, t);
            }
        }

        /* debug partitioning task queue */
        logger(ENCLAVE, "Pass-2: # partitioning tasks = %d", part_queue->count);
#ifdef SKEW_HANDLING
        logger(ENCLAVE, "Pass-2: # skew tasks         = %d", skew_queue->count);
#endif
        ocall_stopTimer(&args->pass1);
        ocall_startTimer(&args->pass2);
    }

    SYNC_TIMER_STOP(&args->localtimer.sync3);
    /* wait at a barrier until first thread adds all partitioning tasks */
    barrier_arrive(args->barrier, rv);
    /* global barrier sync point-3 */
    SYNC_GLOBAL_STOP(&args->globaltimer->sync3, my_tid);

    /************ 2nd pass of multi-pass partitioning ********************/
    /* 4. now each thread further partitions and add to join task queue **/

#if NUM_PASSES==1
    /* If the partitioning is single pass we directly add tasks from pass-1 */
    task_queue_t * swap = join_queue;
    join_queue = part_queue;
    /* part_queue is used as a temporary queue for handling skewed parts */
    part_queue = swap;

#elif NUM_PASSES==2

    while((task = task_queue_atomic_get_atomic(part_queue))){

        serial_radix_partition_atomic(task, join_queue, R, D);

    }

#else
#warning Only 2-pass partitioning is implemented, set NUM_PASSES to 2!
#endif

#ifdef SKEW_HANDLING
    /* Partitioning pass-2 for skewed relations */
    part.R         = R;
    part.D         = D;
    part.thrargs   = args;
    part.padding   = SMALL_PADDING_TUPLES;

    while(1) {
        if(my_tid == 0) {
            *args->skewtask = task_queue_get_atomic(skew_queue);
        }
        barrier_arrive(args->barrier, rv);
        if( *args->skewtask == NULL)
            break;

//        logger(ENCLAVE, "Got skew task = R: %d, S: %d",
//                 (*args->skewtask)->relR.num_tuples,
//                 (*args->skewtask)->relS.num_tuples);

        int32_t numperthr = (*args->skewtask)->relR.num_tuples / args->nthreads;
        const int fanOut2 = (1 << D);

        free(outputR);
        free(outputS);

        outputR = (int32_t*) calloc(fanOut2 + 1, sizeof(int32_t));
        outputS = (int32_t*) calloc(fanOut2 + 1, sizeof(int32_t));

        free(args->histR[my_tid]);
        free(args->histS[my_tid]);

        args->histR[my_tid] = (int32_t*) calloc(fanOut2, sizeof(int32_t));
        args->histS[my_tid] = (int32_t*) calloc(fanOut2, sizeof(int32_t));

        /* wait until each thread allocates memory */
        barrier_arrive(args->barrier, rv);

        /* 1. partitioning for relation R */
        part.rel          = (*args->skewtask)->relR.tuples + my_tid * numperthr;
        part.tmp          = (*args->skewtask)->tmpR.tuples;
        part.hist         = args->histR;
        part.output       = outputR;
        part.num_tuples   = (my_tid == (args->nthreads - 1)) ?
                            ((*args->skewtask)->relR.num_tuples - my_tid * numperthr)
                                                             : numperthr;
        part.total_tuples = (*args->skewtask)->relR.num_tuples;
        part.relidx       = 2; /* meaning this is pass-2, no syncstats */
        parallel_radix_partition(&part);

        numperthr = (*args->skewtask)->relS.num_tuples / args->nthreads;
        /* 2. partitioning for relation S */
        part.rel          = (*args->skewtask)->relS.tuples + my_tid * numperthr;
        part.tmp          = (*args->skewtask)->tmpS.tuples;
        part.hist         = args->histS;
        part.output       = outputS;
        part.num_tuples   = (my_tid == (args->nthreads - 1)) ?
                            ((*args->skewtask)->relS.num_tuples - my_tid * numperthr)
                                                             : numperthr;
        part.total_tuples = (*args->skewtask)->relS.num_tuples;
        part.relidx       = 2; /* meaning this is pass-2, no syncstats */
        parallel_radix_partition(&part);

        /* wait at a barrier until each thread copies out */
        barrier_arrive(args->barrier, rv);

        /* first thread adds join tasks */
        if(my_tid == 0) {
            const int THR1 = (const int) THRESHOLD1((unsigned long) args->nthreads);

            for(i = 0; i < fanOut2; i++) {
                int32_t ntupR = outputR[i+1] - outputR[i] - (int32_t) SMALL_PADDING_TUPLES;
                int32_t ntupS = outputS[i+1] - outputS[i] - (int32_t) SMALL_PADDING_TUPLES;
                if(ntupR > THR1 || ntupS > THR1){

//                    logger(ENCLAVE, "Large join task = R: %d, S: %d", ntupR, ntupS);

                    /* use part_queue temporarily */
                    for(int k=0; k < args->nthreads; k++) {
                        int ns = (k == args->nthreads-1)
                                 ? (ntupS - k*(ntupS/args->nthreads))
                                 : (ntupS/args->nthreads);
                        task_t * t = task_queue_get_slot(part_queue);

                        t->relR.num_tuples = t->tmpR.num_tuples = ntupR;
                        t->relR.tuples = (*args->skewtask)->tmpR.tuples + outputR[i];
                        t->tmpR.tuples = (*args->skewtask)->relR.tuples + outputR[i];

                        t->relS.num_tuples = t->tmpS.num_tuples = ns; //ntupS;
                        t->relS.tuples = (*args->skewtask)->tmpS.tuples + outputS[i] //;
                                         + k*(ntupS/args->nthreads);
                        t->tmpS.tuples = (*args->skewtask)->relS.tuples + outputS[i] //;
                                         + k*(ntupS/args->nthreads);

                        task_queue_add(part_queue, t);
                    }
                }
                else
                if(ntupR > 0 && ntupS > 0) {
                    task_t * t = task_queue_get_slot(join_queue);

                    t->relR.num_tuples = t->tmpR.num_tuples = ntupR;
                    t->relR.tuples = (*args->skewtask)->tmpR.tuples + outputR[i];
                    t->tmpR.tuples = (*args->skewtask)->relR.tuples + outputR[i];

                    t->relS.num_tuples = t->tmpS.num_tuples = ntupS;
                    t->relS.tuples = (*args->skewtask)->tmpS.tuples + outputS[i];
                    t->tmpS.tuples = (*args->skewtask)->relS.tuples + outputS[i];

                    task_queue_add(join_queue, t);

                    logger(ENCLAVE, "Join added = R: %d, S: %d",
                           t->relR.num_tuples, t->relS.num_tuples);
                }
            }

        }
    }

    /* add large join tasks in part_queue to the front of the join queue */
    if(my_tid == 0) {
        while((task = task_queue_get_atomic(part_queue)))
            task_queue_add(join_queue, task);
    }

#endif

    free(outputR);
    free(outputS);

    SYNC_TIMER_STOP(&args->localtimer.sync4);
    /* wait at a barrier until all threads add all join tasks */
    barrier_arrive(args->barrier, rv);
    /* global barrier sync point-4 */
    SYNC_GLOBAL_STOP(&args->globaltimer->sync4, my_tid);

#ifndef RADIX_NO_TIMING
    if(my_tid == 0)
    {
        ocall_stopTimer(&args->pass2);
        ocall_stopTimer(&args->timer3);/* partitioning finished */
    }
#endif

    if(my_tid == 0)
    {
        logger(ENCLAVE, "Number of join tasks = %d", join_queue->count);
    }

#ifdef PCM_COUNT
    if (my_tid == 0) {
        ocall_get_system_counter_state("Partition", 0);
        ocall_set_system_counter_state("Join");
    }
    barrier_arrive(args->barrier, rv);
#endif

    output_list_t * output;

    while((task = task_queue_atomic_get_atomic(join_queue))){
        /* do the actual join. join method differs for different algorithms,
           i.e. bucket chaining, histogram-based, histogram-based with simd &
           prefetching  */
        results += args->join_function(&task->relR, &task->relS, &task->tmpR, &output);

        args->parts_processed ++;
    }

    args->result = results;

#ifdef JOIN_MATERIALIZE
    args->threadresult->nresults = results;
    args->threadresult->threadid = args->my_tid;
    args->threadresult->results  = output;
#endif

    /* this thread is finished */
    SYNC_TIMER_STOP(&args->localtimer.finish_time);

#ifndef RADIX_NO_TIMING
    /* this is for just reliable timing of finish time */
    barrier_arrive(args->barrier, rv);
    if(my_tid == 0) {
        /* Actually with this setup we're not timing build */
        ocall_stopTimer(&args->timer2);/* build finished */
        ocall_stopTimer(&args->timer1);/* probe finished */
        ocall_get_system_micros(&args->end);
    }
#endif

#ifdef PCM_COUNT
    if (my_tid == 0) {
        ocall_get_system_counter_state("Join", 0);
    }
    barrier_arrive(args->barrier, rv);
#endif

    return 0;
}


/**
 * The template function for different joins: Basically each parallel radix join
 * has a initialization step, partitioning step and build-probe steps. All our
 * parallel radix implementations have exactly the same initialization and
 * partitioning steps. Difference is only in the build-probe step. Here are all
 * the parallel radix join implemetations and their Join (build-probe) functions:
 *
 * - PRO,  Parallel Radix Join Optimized --> bucket_chaining_join()
 * - PRH,  Parallel Radix Join Histogram-based --> histogram_join()
 * - PRHO, Parallel Radix Histogram-based Optimized -> histogram_optimized_join()
 */
result_t *
join_init_run_atomic(struct table_t * relR, struct table_t * relS, JoinFunction jf, int nthreads)
{
    int i, rv;
    pthread_t tid[nthreads];
    pthread_barrier_t barrier;
//    cpu_set_t set;
    arg_t_radix args[nthreads];

    int32_t ** histR, ** histS;
    struct row_t * tmpRelR, * tmpRelS;
    uint64_t numperthr[2];
    int64_t result = 0;

    task_queue_atomic_t * part_queue, * join_queue;
#ifdef SKEW_HANDLING
    task_queue_t * skew_queue;
    task_t * skewtask = NULL;
    skew_queue = task_queue_init(FANOUT_PASS1);
#endif
    part_queue = task_queue_atomic_init(FANOUT_PASS1);
    join_queue = task_queue_atomic_init((1<<NUM_RADIX_BITS));


    /* allocate temporary space for partitioning */
    tmpRelR = (struct row_t*) alloc_aligned(relR->num_tuples * sizeof(struct row_t) +
                                            RELATION_PADDING);
    tmpRelS = (struct row_t*) alloc_aligned(relS->num_tuples * sizeof(struct row_t) +
                                            RELATION_PADDING);
    malloc_check((void*)(tmpRelR && tmpRelS));

    /* allocate histograms arrays, actual allocation is local to threads */
    histR = (int32_t**) alloc_aligned(nthreads * sizeof(int32_t*));
    histS = (int32_t**) alloc_aligned(nthreads * sizeof(int32_t*));
    malloc_check((void*)(histR && histS));

    rv = pthread_barrier_init(&barrier, NULL, nthreads);
    if(rv != 0){
        logger(ERROR, "Couldn't create the barrier");
        ocall_exit(EXIT_FAILURE);
    }


#ifdef SYNCSTATS
    /* thread-0 keeps track of synchronization stats */
    args[0].globaltimer = (synctimer_t*) malloc(sizeof(synctimer_t));
#endif

    /* first assign chunks of relR & relS for each thread */
    numperthr[0] = relR->num_tuples / nthreads;
    numperthr[1] = relS->num_tuples / nthreads;

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->resultlist = (threadresult_t *) malloc(sizeof(threadresult_t)
                                                       * nthreads);

    for(i = 0; i < nthreads; i++){
        args[i].relR = relR->tuples + i * numperthr[0];
        args[i].tmpR = tmpRelR;
        args[i].histR = histR;

        args[i].relS = relS->tuples + i * numperthr[1];
        args[i].tmpS = tmpRelS;
        args[i].histS = histS;

        args[i].numR = (i == (nthreads-1)) ?
                       (relR->num_tuples - i * numperthr[0]) : numperthr[0];
        args[i].numS = (i == (nthreads-1)) ?
                       (relS->num_tuples - i * numperthr[1]) : numperthr[1];
        args[i].totalR = relR->num_tuples;
        args[i].totalS = relS->num_tuples;

        args[i].my_tid = i;
        args[i].part_queue = part_queue;
        args[i].join_queue = join_queue;
#ifdef SKEW_HANDLING
        args[i].skew_queue = skew_queue;
        args[i].skewtask   = &skewtask;
#endif
        args[i].barrier = &barrier;
        args[i].join_function = jf;
        args[i].nthreads = nthreads;

#ifdef JOIN_MATERIALIZE
        args[i].threadresult        = &(joinresult->resultlist[i]);
#endif

#if defined THREAD_AFFINITY && !defined NATIVE_COMPILATION
        int cpu_idx = i % CORES;
        logger(DBG, "Assigning thread-%d to CPU-%d", i, cpu_idx);
        rv = pthread_create_cpuidx(&tid[i], NULL, cpu_idx, prj_thread_atomic, (void*)&args[i]);
#else
        rv = pthread_create(&tid[i], nullptr, prj_thread_atomic, (void*)&args[i]);
#endif
        if (rv){
            logger(ERROR, "return code from pthread_create() is %d\n", rv);
            ocall_exit(-1);
        }
    }

    /* wait for threads to finish */
    for(i = 0; i < nthreads; i++){
        pthread_join(tid[i], NULL);
        result += args[i].result;
    }

    joinresult->totalresults = result;
    joinresult->nthreads     = nthreads;

#ifdef SYNCSTATS
    /* #define ABSDIFF(X,Y) (((X) > (Y)) ? ((X)-(Y)) : ((Y)-(X))) */
    fprintf(stdout, "TID JTASKS T1.1 T1.1-IDLE T1.2 T1.2-IDLE "\
            "T3 T3-IDLE T4 T4-IDLE T5 T5-IDLE\n");
    for(i = 0; i < nthreads; i++){
        synctimer_t * glob = args[0].globaltimer;
        synctimer_t * local = & args[i].localtimer;
        fprintf(stdout,
                "%d %d %llu %llu %llu %llu %llu %llu %llu %llu "\
                "%llu %llu\n",
                (i+1), args[i].parts_processed, local->sync1[0],
                glob->sync1[0] - local->sync1[0],
                local->sync1[1] - glob->sync1[0],
                glob->sync1[1] - local->sync1[1],
                local->sync3 - glob->sync1[1],
                glob->sync3 - local->sync3,
                local->sync4 - glob->sync3,
                glob->sync4 - local->sync4,
                local->finish_time - glob->sync4,
                glob->finish_time - local->finish_time);
    }
#endif

#ifndef RADIX_NO_TIMING
    /* now print the timing results: */
    print_timing_atomic(args[0].timer1, args[0].timer2 - args[0].timer3,
                 args[0].timer3, relR->num_tuples + relS->num_tuples,
                 result, args[0].start, args[0].end,
                 args[0].pass1, args[0].pass2);
#endif

    /* clean up */
    for(i = 0; i < nthreads; i++) {
        free(histR[i]);
        free(histS[i]);
    }
    free(histR);
    free(histS);
    task_queue_atomic_free(part_queue);
    task_queue_atomic_free(join_queue);
#ifdef SKEW_HANDLING
    task_queue_free(skew_queue);
#endif
    free(tmpRelR);
    free(tmpRelS);
#ifdef SYNCSTATS
    free(args[0].globaltimer);
#endif

    return joinresult;
}


result_t*
RHO_atomic (relation_t * relR, relation_t * relS, int nthreads)
{
    return join_init_run_atomic(relR, relS, bucket_chaining_join_atomic, nthreads);
}
