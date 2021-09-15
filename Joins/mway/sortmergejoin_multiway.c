#include "sortmergejoin_multiway.h"
#include "joincommon.h"
#include "barrier.h"
#include "math.h"
#include "partition.h"
#include "scalarsort.h"
#include "scalar_multiwaymerge.h"

#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "pcm_commons.h"
#include "Logger.h"
#include "malloc.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif
/**
 * Main thread of First Sort-Merge Join variant with partitioning and complete
 * sorting of both input relations. The merging step in this algorithm tries to
 * overlap the first merging and transfer of remote chunks. However, in compared
 * to other variants, merge phase still takes a significant amount of time.
 *
 * @param param parameters of the thread, see arg_t for details.
 *
 */
void *
sortmergejoin_multiway_thread(void * param);

/**
 * NUMA-local partitioning of both input relations into PARTFANOUT.
 *
 * @param[out] relRparts partitions of relation R
 * @param[out] relSparts partitions of relation S
 * @param[in] args thread arguments
 */
void
partitioning_phase(relation_t *** relRparts, relation_t *** relSparts, arg_t * args);

void
partitioning_cleanup(relation_t ** relRparts, relation_t ** relSparts)
{
    free(relRparts[0]);
    free(relRparts);
    free(relSparts);
}

/**
 * NUMA-local sorting of partitions of both relations.
 *
 * @note sorted outputs are written back to the original relation, therefore
 * input relation is not preserved in this implementation.
 *
 * @param[in] relRparts partitions of relation R
 * @param[in] relSparts partitions of relation S
 * @param[in] args thread arguments
 */
void
sorting_phase(relation_t ** relRparts, relation_t ** relSparts, arg_t * args);

/**
 * Multi-way merging of sorted NUMA-runs with in-cache resident buffers.
 *
 * @param[in] numaregionid NUMA region id of the executing thread
 * @param[out] relRparts sorted partitions of relation R for joining when nthreads=1
 * @param[out] relSparts sorted partitions of relation S for joining when nthreads=1
 * @param[in] args thread arguments
 * @param[out] mergedRelR completely merged relation R
 * @param[out] mergedRelS completely merged relation S
 */
void
multiwaymerge_phase(int numaregionid,
                    relation_t ** relRparts, relation_t ** relSparts, arg_t * args,
                    relation_t * mergedRelR, relation_t * mergedRelS);

/**
 * Evaluate the merge-join over NUMA-local sorted runs.
 *
 * @param[in] relRparts sorted parts of NUMA-local relation-R if nthr=1
 * @param[in] relSparts sorted parts of NUMA-local relation-S if nthr=1
 * @param[in] mergedRelR sorted entire NUMA-local relation-R if nthr>1
 * @param[in] mergedRelS sorted entire NUMA-local relation-S if nthr>1
 * @param[in,out] args return values are stored in args
 */
void
mergejoin_phase(relation_t ** relRparts, relation_t ** relSparts,
                relation_t * mergedRelR, relation_t * mergedRelS, arg_t * args);



result_t *
sortmergejoin_multiway(relation_t * relR, relation_t * relS, joinconfig_t * joincfg)
{
    /* check whether nr. of threads is a power of 2 */
    if((joincfg->NTHREADS & (joincfg->NTHREADS-1)) != 0){
        logger(INFO,"[ERROR] m-way sort-merge join runs with a power of 2 #threads.");
        return 0;
    }

    return sortmergejoin_initrun(relR, relS, joincfg,
                                 sortmergejoin_multiway_thread);
}

/**
 * Main execution thread of "m-way" sort-merge join.
 *
 * @param param
 */
void *
sortmergejoin_multiway_thread(void * param)
{
    arg_t * args   = (arg_t*) param;
    int32_t my_tid = args->my_tid;
    int rv;

    DEBUGMSG(1, "Thread-%d started running ... \n", my_tid);
#ifdef PERF_COUNTERS
    if(my_tid == 0){
        PCM_initPerformanceMonitor(NULL, NULL);
        PCM_start();
    }
    BARRIER_ARRIVE(args->barrier, rv);
#endif

#ifdef PCM_COUNT
    if (my_tid == 0) {
        ocall_set_system_counter_state("Sort");
    }
#endif

    BARRIER_ARRIVE(args->barrier, rv);
    if(my_tid == 0) {
        ocall_get_system_micros(&args->start);
        ocall_startTimer(&args->sort);
    }


    /*************************************************************************
     *
     *   Phase.1) NUMA-local partitioning.
     *
     *************************************************************************/
    relation_t ** partsR = NULL;
    relation_t ** partsS = NULL;
    partitioning_phase(&partsR, &partsS, args);


#ifdef PERF_COUNTERS
    BARRIER_ARRIVE(args->barrier, rv);
    if(my_tid == 0) {
        PCM_stop();
        PCM_log("========= 1) Profiling results of Partitioning Phase =========\n");
        PCM_printResults();
    }
#endif

    BARRIER_ARRIVE(args->barrier, rv);
//    if(my_tid == 0) {
//        ocall_stopTimer(&args->part);
//    }

#ifdef PERF_COUNTERS
    if(my_tid == 0){
        PCM_start();
    }
    BARRIER_ARRIVE(args->barrier, rv);
#endif

    /*************************************************************************
     *
     *   Phase.2) NUMA-local sorting of cache-sized chunks
     *
     *************************************************************************/
    sorting_phase(partsR, partsS, args);

    /**
     * Allocate shared merge buffer for multi-way merge tree.
     * This buffer is further divided into given number of threads
     * active in the same NUMA-region.
     *
     * @note the first thread in each NUMA region allocates the shared L3 buffer.
     */
    int numaregionid = 1; //get_numa_region_id(my_tid);
//    if(is_first_thread_in_numa_region(my_tid)) {
    if(1) {
        /* TODO: make buffer size runtime parameter */
        tuple_t * sharedmergebuffer = (tuple_t *)
                malloc_aligned(args->joincfg->MWAYMERGEBUFFERSIZE);
        args->sharedmergebuffer[numaregionid] = sharedmergebuffer;

        DEBUGMSG(1, "Thread-%d allocated %.3lf KiB merge buffer in NUMA-region-%d to be used by %d active threads.\n",
                 my_tid, (double)(args->joincfg->MWAYMERGEBUFFERSIZE/1024.0),
                 numaregionid, get_num_active_threads_in_numa(numaregionid));
    }


    BARRIER_ARRIVE(args->barrier, rv);
    if(my_tid == 0) {
        ocall_stopTimer(&args->sort);
        ocall_startTimer(&args->merge);
    }
    /* check whether local relations are sorted? */
#if 0
    {
    tuple_t * tmparr = (tuple_t *) malloc(sizeof(tuple_t)*args->numR);
    uint32_t off = 0;
    int i;
    const int PARTFANOUT = args->joincfg->PARTFANOUT;
    for(i = 0; i < PARTFANOUT; i ++) {
        relationpair_t * rels = & args->threadrelchunks[my_tid][i];
        memcpy((void*)(tmparr+off), (void*)(rels->R.tuples), rels->R.num_tuples*sizeof(tuple_t));
        off += rels->R.num_tuples;
    }
    if(is_sorted_helper((int64_t*)tmparr, args->numR))
        logger(INFO,"[INFO ] %d-thread -> relR is sorted, size = %d\n", my_tid, args->numR);
    else
        logger(INFO,"[ERROR] %d-thread -> relR is NOT sorted, size = %d, off=%d********\n", my_tid, args->numR, off);
    free(tmparr);
    tmparr = (tuple_t *) malloc(sizeof(tuple_t)*args->numS);
    off = 0;
    for(i = 0; i < PARTFANOUT; i ++) {
        relationpair_t * rels = & args->threadrelchunks[my_tid][i];
        memcpy((void*)(tmparr+off), (void*)(rels->S.tuples), rels->S.num_tuples*sizeof(tuple_t));
        off += rels->S.num_tuples;
    }
    if(is_sorted_helper((int64_t*)tmparr, args->numS))
        logger(INFO,"[INFO ] %d-thread -> relS is sorted, size = %d\n", my_tid, args->numS);
    else
        logger(INFO,"[ERROR] %d-thread -> relS is NOT sorted, size = %d\n", my_tid, args->numS);
    }
#endif

#ifdef PERF_COUNTERS
    if(my_tid == 0){
        PCM_start();
    }
    BARRIER_ARRIVE(args->barrier, rv);
#endif



    /*************************************************************************
     *
     *   Phase.3) Apply multi-way merging with in-cache resident buffers.
     *
     *************************************************************************/
    relation_t mergedRelR;
    relation_t mergedRelS;
    multiwaymerge_phase(numaregionid, partsR, partsS, args, &mergedRelR, &mergedRelS);


    BARRIER_ARRIVE(args->barrier, rv);
    if(my_tid == 0) {
//        ocall_stopTimer(&args->mergedelta);
//        args->merge = args->mergedelta; /* since we do merge in single go. */
        DEBUGMSG(1, "Multi-way merge is complete!\n");
        /* the thread that allocated the merge buffer releases it. */
//        if(is_first_thread_in_numa_region(my_tid)) {
        if(1) {
            free(args->sharedmergebuffer[numaregionid]);
            //free_threadlocal(args->sharedmergebuffer[numaregionid], MWAY_MERGE_BUFFER_SIZE);
        }
    }

#ifdef PERF_COUNTERS
    BARRIER_ARRIVE(args->barrier, rv);
    if(my_tid == 0) {
        PCM_stop();
        PCM_log("========= 3) Profiling results of Multi-Way NUMA-Merge Phase =========\n");
        PCM_printResults();
        PCM_cleanup();
    }
    BARRIER_ARRIVE(args->barrier, rv);
#endif

#ifdef PCM_COUNT
    if (my_tid == 0) {
        ocall_get_system_counter_state("Sort", 0);
        ocall_set_system_counter_state("Merge");
    }
    barrier_arrive(args->barrier, rv);
#endif


    /* To check whether sorted? */
    /*
    check_sorted((int64_t *)tmpoutR, (int64_t *)tmpoutS,
                 mergeRtotal, mergeStotal, my_tid);
     */

    /*************************************************************************
     *
     *   Phase.4) NUMA-local merge-join on local sorted runs.
     *
     *************************************************************************/
    mergejoin_phase(partsR, partsS, &mergedRelR, &mergedRelS, args);

    /* for proper timing */
    BARRIER_ARRIVE(args->barrier, rv);
    if(my_tid == 0) {
//        ocall_stopTimer(&args->join);
        ocall_stopTimer(&args->merge);
        ocall_get_system_micros(&args->end);
    }

    /* clean-up */
    partitioning_cleanup(partsR, partsS);

    free(args->threadrelchunks[my_tid]);
    /* clean-up temporary relations */
    if(args->nthreads > 1){
        free(mergedRelR.tuples);
        free(mergedRelS.tuples);
    }

#ifdef PCM_COUNT
    if (my_tid == 0) {
        ocall_get_system_counter_state("Merge", 0);
    }
    barrier_arrive(args->barrier, rv);
#endif


    return 0;
}

result_t *
MWAY(struct table_t * relR, struct table_t * relS, int nthreads)
{
    joinconfig_t config;
    config.NTHREADS = nthreads;
    config.PARTFANOUT = PARTFANOUT_DEFAULT;
    config.NUMASTRATEGY = RANDOM;
    config.SCALARSORT = 1;
    config.SCALARMERGE = 1;
    config.MWAYMERGEBUFFERSIZE = MWAY_MERGE_BUFFER_SIZE_DEFAULT;

    return sortmergejoin_multiway(relR, relS, &config);
}

void
partitioning_phase(relation_t *** relRparts, relation_t *** relSparts, arg_t * args)
{
    const int PARTFANOUT = args->joincfg->PARTFANOUT;
    const int NRADIXBITS = log2(PARTFANOUT);

    relation_t ** partsR = (relation_t **)
            malloc_aligned(PARTFANOUT * sizeof(relation_t *));
    relation_t ** partsS = (relation_t **)
            malloc_aligned(PARTFANOUT * sizeof(relation_t *));

    /** note: only free prels[0] when releasing memory */
    relation_t * prels = (relation_t *) malloc(2 * PARTFANOUT * sizeof(relation_t));
    for(int i = 0; i < PARTFANOUT; i++) {
        partsR[i] = prels + i;
        partsS[i] = prels + PARTFANOUT + i;
    }

    relation_t relR, relS;
    relation_t tmpR, tmpS;
    relR.tuples     = args->relR;
    relR.num_tuples = args->numR;
    relS.tuples     = args->relS;
    relS.num_tuples = args->numS;
    tmpR.tuples     = args->tmp_partR;
    tmpR.num_tuples = args->numR;
    tmpS.tuples     = args->tmp_partS;
    tmpS.num_tuples = args->numS;

    /* a maximum of one cache-line padding between partitions in the output */
    /*
    tuple_t * partoutputR = malloc_aligned(relR.num_tuples * sizeof(tuple_t)
                                            + PARTFANOUT * CACHE_LINE_SIZE);
    tuple_t * partoutputS = malloc_aligned(relS.num_tuples * sizeof(tuple_t)
                                            + PARTFANOUT * CACHE_LINE_SIZE);
    tmpR.tuples = partoutputR;
    tmpS.tuples = partoutputS;
    tmpR.num_tuples = args->numR;
    tmpS.num_tuples = args->numS;
    */
    /* after partitioning tmpR, tmpS holds the partitioned data */
    int bitshift = ceil(log2(relR.num_tuples * args->nthreads)) - 1;
    if(args->nthreads == 1)
        bitshift = bitshift - NRADIXBITS + 1;
    else {
#if SKEW_HANDLING
        /* NOTE: Special to skew handling code, we must set the radix bits in a
           way that the MSB-Radix partitioning results in range partitioning
           assuming keys are dense. */
        bitshift = bitshift - NRADIXBITS + 1;
#else
        bitshift = bitshift - NRADIXBITS - 1;
#endif
    }

    DEBUGMSG(1, "[INFO ] bitshift = %d\n", bitshift);

    partition_relation_optimized(partsR, &relR, &tmpR, NRADIXBITS, bitshift);
    partition_relation_optimized(partsS, &relS, &tmpS, NRADIXBITS, bitshift);

    /** return parts */
    *relRparts = partsR;
    *relSparts = partsS;
}

void
sorting_phase(relation_t ** relRparts, relation_t ** relSparts, arg_t * args)
{
    const int PARTFANOUT = args->joincfg->PARTFANOUT;
    const int scalarsortflag = args->joincfg->SCALARSORT;

    int32_t my_tid = args->my_tid;

    args->threadrelchunks[my_tid] = (relationpair_t *)
            malloc_aligned(PARTFANOUT * sizeof(relationpair_t));

    uint64_t ntuples_per_part;
    uint64_t offset = 0;
    tuple_t * optr = args->tmp_sortR + my_tid * CACHELINEPADDING(PARTFANOUT);

    for(int i = 0; i < PARTFANOUT; i++) {
        tuple_t * inptr  = (relRparts[i]->tuples);
        tuple_t * outptr = (optr + offset);
        ntuples_per_part       = relRparts[i]->num_tuples;
        offset                += ALIGN_NUMTUPLES(ntuples_per_part);

//        DEBUGMSG(0, "PART-%d-SIZE: %"PRIu64"\n", i, relRparts[i]->num_tuples);

        scalarsort_tuples(&inptr, &outptr, ntuples_per_part);

#if DEBUG_SORT_CHECK
        if(!is_sorted_helper((int64_t*)outptr, ntuples_per_part)){
            logger(INFO,"===> %d-thread -> R is NOT sorted, size = %d\n", my_tid,
                   ntuples_per_part);
        }
#endif

        args->threadrelchunks[my_tid][i].R.tuples     = outptr;
        args->threadrelchunks[my_tid][i].R.num_tuples = ntuples_per_part;
    }


    offset = 0;
    optr = args->tmp_sortS + my_tid * CACHELINEPADDING(PARTFANOUT);
    for(int i = 0; i < PARTFANOUT; i++) {
        tuple_t * inptr  = (relSparts[i]->tuples);
        tuple_t * outptr = (optr + offset);

        ntuples_per_part       = relSparts[i]->num_tuples;
        offset                += ALIGN_NUMTUPLES(ntuples_per_part);
        /*
        if(my_tid==0)
             flogger(INFO,stdout, "PART-%d-SIZE: %d\n", i, relSparts[i]->num_tuples);
        */
        scalarsort_tuples(&inptr, &outptr, ntuples_per_part);

#if DEBUG_SORT_CHECK
        if(!is_sorted_helper((int64_t*)outptr, ntuples_per_part)){
            logger(INFO,"===> %d-thread -> S is NOT sorted, size = %d\n",
            my_tid, ntuples_per_part);
        }
#endif

        args->threadrelchunks[my_tid][i].S.tuples = outptr;
        args->threadrelchunks[my_tid][i].S.num_tuples = ntuples_per_part;
        /* if(my_tid == 0) */
        /* printf("S-MYTID=%d FAN=%d OUT-START=%llu\nS-MYTID=%d FAN=%d OUT-END=%llu\n", */
        /*        my_tid, i, outptr, my_tid, i, (outptr+ntuples_per_part)); */

    }

}

void
multiwaymerge_phase(int numaregionid,
                    relation_t ** relRparts, relation_t ** relSparts, arg_t * args,
                    relation_t * mergedRelR, relation_t * mergedRelS)
{
    const int PARTFANOUT = args->joincfg->PARTFANOUT;
    const int scalarmergeflag = args->joincfg->SCALARMERGE;

    int32_t my_tid = args->my_tid;
    uint64_t mergeRtotal = 0, mergeStotal = 0;
    tuple_t * tmpoutR = NULL;
    tuple_t * tmpoutS = NULL;

    if(args->nthreads == 1) {
        /* single threaded execution; no multi-way merge. */
        for(int i = 0; i < PARTFANOUT; i ++) {
            relationpair_t * rels = & args->threadrelchunks[my_tid][i];
            mergeRtotal += rels->R.num_tuples;
            mergeStotal += rels->S.num_tuples;

            /* evaluate join between each sorted part */
            relRparts[i]->tuples = rels->R.tuples;
            relRparts[i]->num_tuples = rels->R.num_tuples;
            relSparts[i]->tuples = rels->S.tuples;
            relSparts[i]->num_tuples = rels->S.num_tuples;
        }
    }
    else {
        uint32_t       j;
        const uint32_t perthread   = PARTFANOUT / args->nthreads;

        /* multi-threaded execution */
        /* merge remote relations and bring to local memory */
        const uint32_t start = my_tid * perthread;
        const uint32_t end = start + perthread;

        relation_t * Rparts[PARTFANOUT];
        relation_t * Sparts[PARTFANOUT];

        /* compute the size of merged relations to be stored locally */
        uint32_t f = 0;
        for(j = start; j < end; j ++) {
            for(int i = 0; i < args->nthreads; i ++) {
                uint32_t tid = (my_tid + i) % args->nthreads;
//                uint32_t tid = get_numa_shuffle_strategy(my_tid, i, args->nthreads);
                //printf("SHUF %d %d --> %d\n", i, my_tid, tid);
                relationpair_t * rels = & args->threadrelchunks[tid][j];
                //fprintf(stdout, "TID=%d Part-%d-size = %d\n", my_tid, f, rels->S.num_tuples);
                Rparts[f] = & rels->R;
                Sparts[f] = & rels->S;
                f++;

                mergeRtotal += rels->R.num_tuples;
                mergeStotal += rels->S.num_tuples;
            }
        }

        /* allocate memory at local node for temporary merge results */
        tmpoutR = (tuple_t *) malloc_aligned(mergeRtotal*sizeof(tuple_t));
        tmpoutS = (tuple_t *) malloc_aligned(mergeStotal*sizeof(tuple_t));

        /* determine the L3 cache-size per thread */
        /* int nnuma = get_num_numa_regions(); */

        /* active number of threads in the current NUMA-region: */
        int active_nthreads_in_numa = args->nthreads; //get_num_active_threads_in_numa(numaregionid);

        /* index of the current thread in its NUMA-region: */
        int numatidx = my_tid;//get_thread_index_in_numa(my_tid);

        /* get the exclusive part of the merge buffer for the current thread */
        int bufsz_thr = (args->joincfg->MWAYMERGEBUFFERSIZE/active_nthreads_in_numa)
                        / sizeof(tuple_t);
        tuple_t * mergebuf = args->sharedmergebuffer[numaregionid]
                             + (numatidx * bufsz_thr);

        /* now do the multi-way merging */
        scalar_multiway_merge(tmpoutR, Rparts, PARTFANOUT, mergebuf, bufsz_thr);
        scalar_multiway_merge(tmpoutS, Sparts, PARTFANOUT, mergebuf, bufsz_thr);

    }

    /** returned merged relations, only if nthreads > 1 */
    mergedRelR->tuples = tmpoutR;
    mergedRelR->num_tuples = mergeRtotal;
    mergedRelS->tuples = tmpoutS;
    mergedRelS->num_tuples = mergeStotal;
}

void
mergejoin_phase(relation_t ** relRparts, relation_t ** relSparts,
                relation_t * mergedRelR, relation_t * mergedRelS, arg_t * args)
{

//#ifdef JOIN_MATERIALIZE
//    chainedtuplebuffer_t * chainedbuf = chainedtuplebuffer_init();
//#else
    void * chainedbuf = NULL;
//#endif

    const int PARTFANOUT = args->joincfg->PARTFANOUT;
    uint64_t nresults = 0;

    if(args->nthreads > 1){
        tuple_t * rtuples = (tuple_t *) mergedRelR->tuples;
        tuple_t * stuples = (tuple_t *) mergedRelS->tuples;

        nresults = merge_join(rtuples, stuples,
                              mergedRelR->num_tuples, mergedRelS->num_tuples, chainedbuf);

    } else {
        /* single-threaded execution: just join sorted partition-pairs */
        for(int i = 0; i < PARTFANOUT; i ++) {
            /* evaluate join between each sorted part */
            nresults += merge_join(relRparts[i]->tuples, relSparts[i]->tuples,
                                   relRparts[i]->num_tuples, relSparts[i]->num_tuples,
                                   chainedbuf);
        }

    }
    args->result = nresults;
    /* printf("TID=%d --> #res = %d %d\n", my_tid, args->result, nresults); */

//#ifdef JOIN_MATERIALIZE
//    args->threadresult->nresults = nresults;
//    args->threadresult->threadid = args->my_tid;
//    args->threadresult->results  = (void *) chainedbuf;
//#endif

}
