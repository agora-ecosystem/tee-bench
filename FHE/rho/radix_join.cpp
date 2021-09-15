#include "radix_join.h"
#include <stdlib.h>
#include "../data-types.h"
#include "task_queue.h"
#include "prj_params.h"
#include <pthread.h>
#include <malloc.h>
#include <functional>
#include <cstring>
#include <iostream>
#include "barrier.h"

using namespace std;

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)
#define MAX(X, Y) (((X) > (Y)) ? (X) : (Y))

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

typedef struct arg_t_radix  arg_t_radix;
typedef struct part_t part_t;

struct part_t {
    tuple_enc_t *  rel;
    tuple_enc_t *  tmp;
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

struct arg_t_radix {
    int32_t ** histR;
    tuple_enc_t *  relR;
    tuple_enc_t *  tmpR;
    int32_t ** histS;
    tuple_enc_t *  relS;
    tuple_enc_t *  tmpS;

    int32_t numR;
    int32_t numS;
    int32_t totalR;
    int32_t totalS;

    task_queue_t *      join_queue;
    task_queue_t *      part_queue;

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
} __attribute__((aligned(CACHE_LINE_SIZE)));

static void *
alloc_aligned(size_t size)
{
    void * ret;
    ret = memalign(CACHE_LINE_SIZE, size);

    return ret;
}

uint32_t hash_c_string(const char key[ENC_KEY_LENGTH]) {
    uint32_t result = 0;
    const uint32_t prime = 31;
    for (uint32_t i = 0; i < ENC_KEY_LENGTH; ++i) {
        result = key[i] + (result * prime);
    }
    return result;
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
bucket_chaining_join_enc(const relation_enc_t * const R,
                         const relation_enc_t * const S,
                         relation_enc_t * const tmpR,
                         output_list_t ** output)
{
    (void) (tmpR);
    (void) (output);
    int * next, * bucket;
    const uint32_t numR = R->num_tuples;
    uint32_t N = numR;
    int64_t matches = 0;

    NEXT_POW_2(N);
    /* N <<= 1; */
    const uint32_t MASK = (N-1) << (NUM_RADIX_BITS);

    next   = (int*) malloc(sizeof(int) * numR);
    /* posix_memalign((void**)&next, CACHE_LINE_SIZE, numR * sizeof(int)); */
    bucket = (int*) calloc(N, sizeof(int));

    const struct row_enc_t * const Rtuples = R->tuples;
    for(uint32_t i=0; i < numR; ){
        uint32_t hash = hash_c_string(R->tuples[i].key);
        uint32_t idx = HASH_BIT_MODULO(hash, MASK, NUM_RADIX_BITS);
        next[i]      = bucket[idx];
        bucket[idx]  = ++i;     /* we start pos's from 1 instead of 0 */

        /* Enable the following tO avoid the code elimination
           when running probe only for the time break-down experiment */
        /* matches += idx; */
    }

    const struct row_enc_t * const Stuples = S->tuples;
    const uint32_t        numS    = S->num_tuples;

    /* Disable the following loop for no-probe for the break-down experiments */
    /* PROBE- LOOP */
    for(uint32_t i=0; i < numS; i++ ){

        uint32_t hash = hash_c_string(Stuples[i].key);
        uint32_t idx = HASH_BIT_MODULO(hash, MASK, NUM_RADIX_BITS);

        for(int hit = bucket[idx]; hit > 0; hit = next[hit-1]){

//            if(Stuples[i].key == Rtuples[hit-1].key){
            if(strcmp(Stuples[i].key, Rtuples[hit-1].key) == 0){
                matches ++;
            }
        }
    }
    /* PROBE-LOOP END  */

    /* clean up temp */
    free(bucket);
    free(next);

    return matches;
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
bucket_chaining_join(const struct table_t * const R,
                     const struct table_t * const S,
                     struct table_t * const tmpR,
                     output_list_t ** output)
{
    (void) (tmpR);
    (void) (output);
    int * next, * bucket;
    const uint32_t numR = R->num_tuples;
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
    const uint32_t        numS    = S->num_tuples;

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
radix_cluster_nopadding_enc(relation_enc_t * outRel, relation_enc_t * inRel, int R, int D)
{
    row_enc_t ** dst;
    row_enc_t * input;
    /* tuple_t ** dst_end; */
    uint32_t * tuples_per_cluster;
    uint32_t i;
    uint32_t offset;
    const uint32_t M = ((1 << D) - 1) << R;
    const uint32_t fanOut = 1 << D;
    const uint32_t ntuples = inRel->num_tuples;

    tuples_per_cluster = (uint32_t*)calloc(fanOut, sizeof(uint32_t));
    /* the following are fixed size when D is same for all the passes,
       and can be re-used from call to call. Allocating in this function
       just in case D differs from call to call. */
    dst     = (row_enc_t**)malloc(sizeof(row_enc_t*)*fanOut);
    /* dst_end = (tuple_t**)malloc(sizeof(tuple_t*)*fanOut); */

    input = inRel->tuples;
    /* count tuples per cluster */
    for( i=0; i < ntuples; i++ ){
        uint32_t hash = hash_c_string(input->key);
        uint32_t idx = (uint32_t)(HASH_BIT_MODULO(hash, M, R));
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
        uint32_t hash = hash_c_string(input->key);
        uint32_t idx   = (uint32_t)(HASH_BIT_MODULO(hash, M, R));
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
radix_cluster_nopadding(struct table_t * outRel, struct table_t * inRel, int R, int D)
{
    row_t ** dst;
    row_t * input;
    /* tuple_t ** dst_end; */
    uint32_t * tuples_per_cluster;
    uint32_t i;
    uint32_t offset;
    const uint32_t M = ((1 << D) - 1) << R;
    const uint32_t fanOut = 1 << D;
    const uint32_t ntuples = inRel->num_tuples;

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


result_t*
RJ_enc (relation_enc_t * relR, relation_enc_t * relS) {
    int64_t result = 0;
    uint32_t i;

    relation_enc_t *outRelR, *outRelS;

    outRelR = (relation_enc_t*) malloc(sizeof(relation_enc_t));
    outRelS = (relation_enc_t*) malloc(sizeof(relation_enc_t));

    size_t sz = relR->num_tuples * sizeof(row_enc_t) + RELATION_PADDING;
    outRelR->tuples     = (row_enc_t*) malloc(sz);
    outRelR->num_tuples = relR->num_tuples;

    sz = relS->num_tuples * sizeof(row_enc_t) + RELATION_PADDING;
    outRelS->tuples     = (row_enc_t*) malloc(sz);
    outRelS->num_tuples = relS->num_tuples;

    /***** do the multi-pass partitioning *****/
#if NUM_PASSES==1
    /* apply radix-clustering on relation R for pass-1 */
    radix_cluster_nopadding_enc(outRelR, relR, 0, NUM_RADIX_BITS);
    relR = outRelR;

    /* apply radix-clustering on relation S for pass-1 */
    radix_cluster_nopadding_enc(outRelS, relS, 0, NUM_RADIX_BITS);
    relS = outRelS;

#elif NUM_PASSES==2
    /* apply radix-clustering on relation R for pass-1 */
    radix_cluster_nopadding_enc(outRelR, relR, 0, NUM_RADIX_BITS/NUM_PASSES);

    /* apply radix-clustering on relation S for pass-1 */
    radix_cluster_nopadding_enc(outRelS, relS, 0, NUM_RADIX_BITS/NUM_PASSES);

    /* apply radix-clustering on relation R for pass-2 */
    radix_cluster_nopadding_enc(relR, outRelR,
                            NUM_RADIX_BITS/NUM_PASSES,
                            NUM_RADIX_BITS-(NUM_RADIX_BITS/NUM_PASSES));

    /* apply radix-clustering on relation S for pass-2 */
    radix_cluster_nopadding_enc(relS, outRelS,
                            NUM_RADIX_BITS/NUM_PASSES,
                            NUM_RADIX_BITS-(NUM_RADIX_BITS/NUM_PASSES));

    /* clean up temporary relations */
    free(outRelR->tuples);
    free(outRelS->tuples);
    free(outRelR);
    free(outRelS);

#else
#error Only 1 or 2 pass partitioning is implemented, change NUM_PASSES!
#endif

    int * R_count_per_cluster = (int*)calloc((1<<NUM_RADIX_BITS), sizeof(int));
    int * S_count_per_cluster = (int*)calloc((1<<NUM_RADIX_BITS), sizeof(int));

    /* compute number of tuples per cluster */
    for( i=0; i < relR->num_tuples; i++ ){
        uint32_t hash = hash_c_string(relR->tuples[i].key);
        uint32_t idx = (hash) & ((1<<NUM_RADIX_BITS)-1);
        R_count_per_cluster[idx] ++;
    }
    for( i=0; i < relS->num_tuples; i++ ){
        uint32_t hash = hash_c_string(relS->tuples[i].key);
        uint32_t idx = (hash) & ((1<<NUM_RADIX_BITS)-1);
        S_count_per_cluster[idx] ++;
    }

//#ifdef JOIN_RESULT_MATERIALIZE
//    chainedtuplebuffer_t * chainedbuf = chainedtuplebuffer_init();
//#else
//    void * chainedbuf = NULL;
//#endif

    /* build hashtable on inner */
    int r, s; /* start index of next clusters */
    r = s = 0;
    for( i=0; i < (1<<NUM_RADIX_BITS); i++ ){
        relation_enc_t tmpR, tmpS;

        if(R_count_per_cluster[i] > 0 && S_count_per_cluster[i] > 0){

            tmpR.num_tuples = R_count_per_cluster[i];
            tmpR.tuples = relR->tuples + r;
            r += R_count_per_cluster[i];

            tmpS.num_tuples = S_count_per_cluster[i];
            tmpS.tuples = relS->tuples + s;
            s += S_count_per_cluster[i];

            result += bucket_chaining_join_enc(&tmpR, &tmpS, NULL, nullptr);
        }
        else {
            r += R_count_per_cluster[i];
            s += S_count_per_cluster[i];
        }
    }

    /* clean-up temporary buffers */
    free(S_count_per_cluster);
    free(R_count_per_cluster);

#if NUM_PASSES == 1
    /* clean up temporary relations */
    free(outRelR->tuples);
    free(outRelS->tuples);
    free(outRelR);
    free(outRelS);
#endif

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = result;
    joinresult->nthreads = 1;
    return joinresult;

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
parallel_radix_partition(part_t * const part)
{
    const tuple_enc_t * rel = part->rel;
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
        cout << "Radix partitioning. R=" << R<<", D="<<D<<", fanout="<<fanOut<<", MASK="<<MASK<<endl;
    }

    int32_t sum = 0;
    uint32_t i, j;
    int rv = 0;

    int32_t dst[fanOut+1];

    /* compute local histogram for the assigned region of rel */
    /* compute histogram */
    int32_t * my_hist = hist[my_tid];

    for(i = 0; i < size; i++) {
        uint32_t hash = hash_c_string(rel[i].key);
        uint32_t idx = HASH_BIT_MODULO(hash, MASK, R);
        my_hist[idx] ++;
    }

    /* compute local prefix sum on hist */
    for(i = 0; i < fanOut; i++){
        sum += my_hist[i];
        my_hist[i] = sum;
    }

    /* wait at a barrier until each thread complete histograms */
    barrier_arrive(part->thrargs->barrier, rv);

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

    tuple_enc_t * tmp = part->tmp;

    /* Copy tuples to their corresponding clusters */
    for(i = 0; i < size; i++ ){
        auto hash = hash_c_string(rel[i].key);
        uint32_t idx = HASH_BIT_MODULO(hash, MASK, R);
        tmp[dst[idx]] = rel[i];
        ++dst[idx];
    }
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
radix_cluster(relation_enc_t * outRel,
              relation_enc_t * inRel,
              int32_t * hist,
              int R,
              int D)
{
    uint32_t i;
    uint32_t M = ((1 << D) - 1) << R;
    uint32_t offset;
    uint32_t fanOut = 1 << D;

    /* the following are fixed size when D is same for all the passes,
       and can be re-used from call to call. Allocating in this function
       just in case D differs from call to call. */
    uint32_t dst[fanOut];

    /* count tuples per cluster */
    for( i=0; i < inRel->num_tuples; i++ ){
        uint32_t hash = hash_c_string(inRel->tuples[i].key);
        uint32_t idx = HASH_BIT_MODULO(hash, M, R);
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
        auto hash = hash_c_string(inRel->tuples[i].key);
        uint32_t idx   = HASH_BIT_MODULO(hash, M, R);
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
void serial_radix_partition(task_t * const task,
                            task_queue_t * join_queue,
                            const int R, const int D)
{
    int i;
    uint32_t offsetR = 0, offsetS = 0;
    const int fanOut = 1 << D;  /*(NUM_RADIX_BITS / NUM_PASSES);*/
    int32_t * outputR, * outputS;

    outputR = (int32_t*)calloc(fanOut+1, sizeof(int32_t));
    outputS = (int32_t*)calloc(fanOut+1, sizeof(int32_t));
    /* TODO: measure the effect of memset() */
    /* memset(outputR, 0, fanOut * sizeof(int32_t)); */
    radix_cluster(&task->tmpR, &task->relR, outputR, R, D);

    /* memset(outputS, 0, fanOut * sizeof(int32_t)); */
    radix_cluster(&task->tmpS, &task->relS, outputS, R, D);

    /* task_t t; */
    for(i = 0; i < fanOut; i++) {
        if(outputR[i] > 0 && outputS[i] > 0) {
            task_t * t = task_queue_get_slot_atomic(join_queue);
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
            task_queue_add_atomic(join_queue, t);
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
 * The main thread of parallel radix join. It does partitioning in parallel with
 * other threads and during the join phase, picks up join tasks from the task
 * queue and calls appropriate JoinFunction to compute the join task.
 *
 * @param param
 *
 * @return
 */
void *
prj_thread(void * param)
{
    arg_t_radix * args   = (arg_t_radix*) param;
    int32_t my_tid = args->my_tid;

    const int fanOut = 1 << (NUM_RADIX_BITS / NUM_PASSES);
    const int R = (NUM_RADIX_BITS / NUM_PASSES);
    const int D = (NUM_RADIX_BITS - (NUM_RADIX_BITS / NUM_PASSES));
    const int thresh1 = (const int) (MAX((1<<D), (1<<R)) * THRESHOLD1((unsigned long)args->nthreads));

    if (args->my_tid == 0)
    {
        cout << "NUM_PASSES=" << NUM_PASSES << " RADIX_BITS=" << NUM_RADIX_BITS << endl;
        cout << "fanOut=" << fanOut << ", R=" << R << ", D=" << D << ", thresh1=" << thresh1 << endl;
    }
    uint64_t results = 0;
    int i;
    int rv = 0;

    part_t part;
    task_t * task;
    task_queue_t * part_queue;
    task_queue_t * join_queue;
#ifdef SKEW_HANDLING
    task_queue_t * skew_queue;
#endif

    int32_t * outputR = (int32_t *) calloc((fanOut+1), sizeof(int32_t));
    int32_t * outputS = (int32_t *) calloc((fanOut+1), sizeof(int32_t));

    part_queue = args->part_queue;
    join_queue = args->join_queue;

    args->histR[my_tid] = (int32_t *) calloc(fanOut, sizeof(int32_t));
    args->histS[my_tid] = (int32_t *) calloc(fanOut, sizeof(int32_t));

    /* in the first pass, partitioning is done together by all threads */

    args->parts_processed = 0;

    /* wait at a barrier until each thread starts and then start the timer */
    barrier_arrive(args->barrier, rv);

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
    parallel_radix_partition(&part);
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
    parallel_radix_partition(&part);
#endif


    /* wait at a barrier until each thread copies out */
    barrier_arrive(args->barrier, rv);

    /********** end of 1st partitioning phase ******************/

    /* 3. first thread creates partitioning tasks for 2nd pass */
    if(my_tid == 0) {
        for(i = 0; i < fanOut; i++) {
            int32_t ntupR = outputR[i+1] - outputR[i] - (int32_t) PADDING_TUPLES;
            int32_t ntupS = outputS[i+1] - outputS[i] - (int32_t) PADDING_TUPLES;

            if(ntupR > 0 && ntupS > 0) {
                task_t * t = task_queue_get_slot(part_queue);

                t->relR.num_tuples = t->tmpR.num_tuples = ntupR;
                t->relR.tuples = args->tmpR + outputR[i];
                t->tmpR.tuples = args->relR + outputR[i];

                t->relS.num_tuples = t->tmpS.num_tuples = ntupS;
                t->relS.tuples = args->tmpS + outputS[i];
                t->tmpS.tuples = args->relS + outputS[i];

                task_queue_add(part_queue, t);
            }
        }

        /* debug partitioning task queue */
        cout << "Pass-2: # partitioning tasks = " << part_queue->count << endl;
    }

    barrier_arrive(args->barrier, rv);

    /************ 2nd pass of multi-pass partitioning ********************/
    /* 4. now each thread further partitions and add to join task queue **/

#if NUM_PASSES==1
    /* If the partitioning is single pass we directly add tasks from pass-1 */
    task_queue_t * swap = join_queue;
    join_queue = part_queue;
    /* part_queue is used as a temporary queue for handling skewed parts */
    part_queue = swap;

#elif NUM_PASSES==2

    while((task = task_queue_get_atomic(part_queue))){

        serial_radix_partition(task, join_queue, R, D);

    }

#else
#warning Only 2-pass partitioning is implemented, set NUM_PASSES to 2!
#endif

    free(outputR);
    free(outputS);

    /* wait at a barrier until all threads add all join tasks */
    barrier_arrive(args->barrier, rv);
    /* global barrier sync point-4 */


    if(my_tid == 0)
    {
        cout << "number of join tasks = " << join_queue->count << endl;
    }

    output_list_t * output;

    while((task = task_queue_get_atomic(join_queue))){
        /* do the actual join. join method differs for different algorithms,
           i.e. bucket chaining, histogram-based, histogram-based with simd &
           prefetching  */
        results += args->join_function(&task->relR, &task->relS, &task->tmpR, &output);

        args->parts_processed ++;
    }

    args->result = results;

    return 0;
}


result_t*
RHO_enc (relation_enc_t * relR, relation_enc_t * relS, int nthreads)
{
    int i, rv;
    pthread_t tid[nthreads];
    pthread_barrier_t barrier;
//    cpu_set_t set;
    arg_t_radix args[nthreads];

    int32_t ** histR, ** histS;
    tuple_enc_t * tmpRelR, * tmpRelS;
    int32_t numperthr[2];
    int64_t result = 0;

    task_queue_t * part_queue, * join_queue;

    part_queue = task_queue_init(FANOUT_PASS1);
    join_queue = task_queue_init((1<<NUM_RADIX_BITS));


    /* allocate temporary space for partitioning */
    tmpRelR = (tuple_enc_t*) alloc_aligned(relR->num_tuples * sizeof(tuple_enc_t) +
                                            RELATION_PADDING);
    tmpRelS = (tuple_enc_t*) alloc_aligned(relS->num_tuples * sizeof(tuple_enc_t) +
                                            RELATION_PADDING);

    /* allocate histograms arrays, actual allocation is local to threads */
    histR = (int32_t**) alloc_aligned(nthreads * sizeof(int32_t*));
    histS = (int32_t**) alloc_aligned(nthreads * sizeof(int32_t*));

    rv = pthread_barrier_init(&barrier, NULL, nthreads);
    if(rv != 0){
        cout << "Couldn't create the barrier " << endl;
        exit(EXIT_FAILURE);
    }


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
        args[i].barrier = &barrier;
        args[i].join_function = bucket_chaining_join_enc;
        args[i].nthreads = nthreads;

        rv = pthread_create(&tid[i], nullptr, prj_thread, (void*)&args[i]);
        if (rv){
            cout << "\"return code from pthread_create() is" << rv << endl;
            exit(-1);
        }
    }

    /* wait for threads to finish */
    for(i = 0; i < nthreads; i++){
        pthread_join(tid[i], NULL);
        result += args[i].result;
    }

    joinresult->totalresults = result;
    joinresult->nthreads     = nthreads;

    /* clean up */
    for(i = 0; i < nthreads; i++) {
        free(histR[i]);
        free(histS[i]);
    }
    free(histR);
    free(histS);
    task_queue_free(part_queue);
    task_queue_free(join_queue);
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