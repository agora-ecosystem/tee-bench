/**
 * @file    joincommon.c
 * @author  Cagri Balkesen <cagri.balkesen@inf.ethz.ch>
 * @date    Sat Dec 15 15:39:54 2012
 * @version $Id $
 *
 * @brief   Common structures, macros and functions of sort-merge join algorithms.
 *
 * (c) 2012-2014, ETH Zurich, Systems Group
 *
 * \ingroup Joins
 */


#include "joincommon.h"
#include "util.h"
#include "radix_join.h"

#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "pcm_commons.h"
#include "Logger.h"
#include <malloc.h>
#else
#include "Enclave_t.h"
#include "Enclave.h"
#endif


#define REQUIRED_STACK_SIZE (32*1024*1024)

void *
malloc_aligned(size_t size)
{
    void * ret;
    ret = memalign(CACHE_LINE_SIZE, size);

    malloc_check(ret);

    return ret;
}

result_t *
sortmergejoin_initrun(relation_t * relR,relation_t * relS, joinconfig_t * joincfg,
                      void * (*jointhread)(void *))
{
    uint64_t i, rv;
    int nthreads = joincfg->NTHREADS;
    int PARTFANOUT = joincfg->PARTFANOUT;
    pthread_t tid[nthreads];
    pthread_attr_t attr;
    pthread_barrier_t barrier;
    arg_t args[nthreads];

    int64_t numperthr[2];
    int64_t result = 0;


    /**** allocate temporary space for partitioning ****/
    tuple_t * tmpRelpartR = NULL, * tmpRelpartS = NULL;
    tmpRelpartR = (tuple_t*) malloc_aligned(relR->num_tuples * sizeof(tuple_t)
                                       + RELATION_PADDING(nthreads, PARTFANOUT));
    tmpRelpartS = (tuple_t*) malloc_aligned(relS->num_tuples * sizeof(tuple_t)
                                       + RELATION_PADDING(nthreads, PARTFANOUT));


    /**** allocate temporary space for sorting ****/
    tuple_t * tmpRelsortR = NULL, * tmpRelsortS = NULL;
    tmpRelsortR = (tuple_t*) malloc_aligned(relR->num_tuples * sizeof(tuple_t)
                                       + RELATION_PADDING(nthreads, PARTFANOUT));
    tmpRelsortS = (tuple_t*) malloc_aligned(relS->num_tuples * sizeof(tuple_t)
                                       + RELATION_PADDING(nthreads, PARTFANOUT));


    relationpair_t ** threadrelchunks;
    threadrelchunks = (relationpair_t **) malloc(nthreads *
                                                sizeof(relationpair_t*));

    /* allocate histograms arrays, actual allocation is local to threads */
    uint32_t ** histR = (uint32_t**) malloc(nthreads * sizeof(uint32_t*));

    rv = pthread_barrier_init(&barrier, NULL, nthreads);
    if(rv != 0){
        logger(ERROR,"[ERROR] Couldn't create the barrier");
        ocall_exit(EXIT_FAILURE);
    }

    /*  Initialize the attribute */
    int             err = 0;
    size_t          stackSize = 0;

    /* first assign chunks of relR & relS for each thread */
    numperthr[0] = relR->num_tuples / nthreads;
    numperthr[1] = relS->num_tuples / nthreads;

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->resultlist = (threadresult_t *) malloc(sizeof(threadresult_t)
                                                       * nthreads);

    int num_numa = 1;//get_num_numa_regions();
    tuple_t ** ptrs_to_sharedmergebufs = (tuple_t **)
            malloc_aligned(num_numa*sizeof(tuple_t*));

    for(i = 0; i < nthreads; i++){
        int cpu_idx = i % CORES; /* this is the physical CPU-ID */
        DEBUGMSG(1, "Assigning thread-%d to CPU-%d\n", i, cpu_idx);


        args[i].relR = relR->tuples + i * (numperthr[0]);
        args[i].relS = relS->tuples + i * (numperthr[1]);

        /* temporary relations */
        args[i].tmp_partR = tmpRelpartR + i * (numperthr[0] + CACHELINEPADDING(PARTFANOUT));
        args[i].tmp_partS = tmpRelpartS + i * (numperthr[1] + CACHELINEPADDING(PARTFANOUT));
        args[i].tmp_sortR = tmpRelsortR + i * (numperthr[0]);
        args[i].tmp_sortS = tmpRelsortS + i * (numperthr[1]);

        args[i].numR = (i == (nthreads-1)) ?
            (relR->num_tuples - i * numperthr[0]) : numperthr[0];
        args[i].numS = (i == (nthreads-1)) ?
            (relS->num_tuples - i * numperthr[1]) : numperthr[1];

        args[i].my_tid        = i;/* this is the logical CPU-ID */
        args[i].nthreads      = nthreads;
        args[i].joincfg       = joincfg;
        args[i].barrier       = &barrier;
        args[i].threadrelchunks = threadrelchunks;
        args[i].sharedmergebuffer = ptrs_to_sharedmergebufs;

        /** information specific to mpsm-join */
        args[i].histR         = histR;
        args[i].tmpRglobal    = tmpRelpartR;
        args[i].totalR        = relR->num_tuples;

        /* run the selected join algorithm thread */
        rv = pthread_create(&tid[i], &attr, jointhread, (void*)&args[i]);

        if (rv) {
            logger(ERROR,"[ERROR] return code from pthread_create() is %d\n", rv);
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

//    /* stats */
//    uint64_t total = args[0].join;
//    logger(INFO,"Total, Partitioning, Sort, First-Merge, Merge, Join");
//    logger(INFO,"%llu, %llu, %llu, %llu, %llu, %llu\n", total, args[0].part,
//            args[0].sort, args[0].mergedelta, args[0].merge, args[0].join);
//    logger(INFO,"Perstage: ");
//    logger(INFO,"%llu, %llu, %llu, %llu, %llu, ",
//            args[0].part,
//            args[0].sort - args[0].part,
//            args[0].mergedelta - args[0].sort,
//            args[0].merge - args[0].mergedelta,
//            args[0].join - args[0].merge);
//    logger(INFO,"");

#ifndef NO_TIMING
    /* now print the timing results: */
//    print_timing(relS->num_tuples, args[0].start, args[0].end);
//    print_timing(relR->num_tuples + relS->num_tuples, &args[0]);
    print_timing(args[0].merge+args[0].sort, args[0].merge, args[0].sort, relR->num_tuples + relS->num_tuples,
                 result, args[0].start, args[0].end, 0, 0);
#endif

    /* clean-up */
    free(threadrelchunks);
    free(ptrs_to_sharedmergebufs);
    /* clean-up the temporary space */
    if(args[0].tmp_partR != 0){
        free(tmpRelpartR);
        free(tmpRelpartS);
        free(tmpRelsortR);
        free(tmpRelsortS);
    }
    free(histR);
    pthread_barrier_destroy(&barrier);

    return joinresult;
}

void
print_timing(uint64_t numtuples, uint64_t start, uint64_t end)
{
    double diff_usec = end - start;

    logger(INFO,"NUM-TUPLES = %lld TOTAL-TIME-USECS = %.4lf ", numtuples, diff_usec);
    logger(INFO,"TUPLES-PER-SECOND = ");

    logger(INFO,"%.4lf ", (numtuples/(diff_usec/1000000L)));
}

void
print_timing(uint64_t numtuples, arg_t * args)
{
    /* stats */
    uint64_t total = args->join;
    logger(INFO,"Total, Partitioning, Sort, First-Merge, Merge, Join");
    logger(INFO,"%llu, %llu, %llu, %llu, %llu, %llu\n", total, args->part,
           args->sort, args->mergedelta, args->merge, args->join);
    logger(INFO,"Perstage: ");
    logger(INFO,"%llu, %llu, %llu, %llu, %llu, ",
           args->part,
           args->sort - args->part,
           args->mergedelta - args->sort,
           args->merge - args->mergedelta,
           args->join - args->merge);

    uint64_t diff_usec = args->end - args->start;

    logger(ENCLAVE, "Total input tuples : %lu", numtuples);
    logger(ENCLAVE, "Total Runtime (us) : %lu ", diff_usec);
    logger(ENCLAVE,"Throughput (M rec/sec) : %.2lf", (numtuples/(diff_usec/1000000.0)/1000000.0));
#ifdef SGX_COUNTERS
    uint64_t ewb;
    ocall_get_total_ewb(&ewb);
    logger(DBG, "EWB : %lu", ewb);
#endif
}

/**
 * Does merge join on two sorted relations. Just a naive scalar
 * implementation. TODO: consider AVX for this code.
 *
 * @param rtuples sorted relation R
 * @param stuples sorted relation S
 * @param numR number of tuples in R
 * @param numS number of tuples in S
 * @param output join results, if JOIN_MATERIALE defined.
 */
uint64_t
merge_join(tuple_t * rtuples, tuple_t * stuples,
           const uint64_t numR, const uint64_t numS, void * output)
{
    uint64_t i = 0, j = 0;
    uint64_t matches = 0;

#if DEBUG_SORT_CHECK
    if(is_sorted_helper((int64_t*)rtuples, numR))
        logger(INFO,"[INFO ] merge_join() -> R is sorted, size = %d\n", numR);
    else
        logger(INFO,"[ERROR] merge_join() -> R is NOT sorted, size = %d\n", numR);

    if(is_sorted_helper((int64_t*)stuples, numS))
        logger(INFO,"[INFO ] merge_join() -> S is sorted, size = %d\n", numS);
    else
        logger(INFO,"[ERROR] mmerge_join() -> S is NOT sorted, size = %d\n", numS);
#endif

//#ifdef JOIN_MATERIALIZE
//    chainedtuplebuffer_t * chainedbuf = (chainedtuplebuffer_t *) output;
//#endif

    while( i < numR && j < numS ) {
        if( rtuples[i].key < stuples[j].key )
            i ++;
        else if(rtuples[i].key > stuples[j].key)
            j ++;
        else {
            /* rtuples[i].key is equal to stuples[j].key */
            uint64_t jj;
            do {
                jj = j;

                do {
                    // join match outpit: <R[i], S[jj]>
//#ifdef JOIN_MATERIALIZE
//                    /** We materialize only <S-key, S-RID> */
//                    tuple_t * outtuple = cb_next_writepos(chainedbuf);
//                    outtuple->key = stuples[jj].key;
//                    outtuple->payload = stuples[jj].payload;
//#endif

                    matches ++;
                    jj++;

                } while(jj < numS && rtuples[i].key == stuples[jj].key);

                i++;

            } while(i < numR && rtuples[i].key == stuples[j].key);

            j = jj;


#if 0 /* previous version with primary-key assumption: */
//#ifdef JOIN_MATERIALIZE
//            /** We materialize only <S-key, S-RID> */
//            tuple_t * outtuple = cb_next_writepos(chainedbuf);
//            outtuple->key = stuples[j].key;
//            outtuple->payload = stuples[j].payload;
//#endif

            matches ++;
            //i ++;
            j ++;
#endif
        }
    }
    /* printf("More-S = %d, More-R = %d, remS=%d\n", (j<numS), (i<numR), numS-j); */
    /* if(rtuples[numR-1].key == stuples[j].key) */
    /*     printf("lastS equal lastR = %d\n", 1); */
    /* matches = merge_join8((int64_t *)rtuples, (int64_t*)stuples, 0, numR); */

    return matches;
}

/**
 * Does merge join on two sorted relations with interpolation
 * searching in the beginning to find the search start index. Just a
 * naive scalar implementation. TODO: consider AVX for this code.
 *
 * @param rtuples sorted relation R
 * @param stuples sorted relation S
 * @param numR number of tuples in R
 * @param numS number of tuples in S
 * @param output join results, if JOIN_MATERIALIZE defined.
 */
uint64_t
merge_join_interpolation(tuple_t * rtuples, tuple_t * stuples,
                         const uint64_t numR, const uint64_t numS,
                         void * output)
{
    uint64_t i = 0, j = 0, k = 0;
    uint64_t matches = 0;

//#ifdef JOIN_MATERIALIZE
//    chainedtuplebuffer_t * chainedbuf = (chainedtuplebuffer_t *) output;
//#endif

    /* find search start index with interpolation, only 2-steps */
    if( rtuples[0].key > stuples[0].key ) {
        double r0 = rtuples[0].key;
        double s0 = stuples[0].key;
        double sN = stuples[numS-1].key;
        double sK;

        k = (numS - 1) * (r0 - s0) / (sN - s0) + 1;

        sK = stuples[k].key;

        k = (k - 1) * (r0 - s0) / (sK - s0) + 1;

        /* go backwards for the interpolation error */
        while(stuples[k].key >= r0)
            k--;

        j = k;
    }
    else if( rtuples[0].key < stuples[0].key ){
        double s0 = stuples[0].key;
        double r0 = rtuples[0].key;
        double rN = rtuples[numS-1].key;
        double rK;

        k = (numR - 1) * (s0 - r0) / (rN - r0) + 1;

        rK = rtuples[k].key;

        k = (k - 1) * (s0 - r0) / (rK - r0) + 1;

        /* go backwards for the interpolation error */
        while(rtuples[k].key >= s0)
            k--;

        i = k;
    }

    while( i < numR && j < numS ) {
        if( rtuples[i].key < stuples[j].key )
            i ++;
        else if(rtuples[i].key > stuples[j].key)
            j ++;
        else {

//#ifdef JOIN_MATERIALIZE
//            tuple_t * outtuple = cb_next_writepos(chainedbuf);
//            *outtuple = stuples[j];
//#endif

            matches ++;
            //i ++;
            j ++;
        }
    }

    return matches;
}


int
is_sorted_helper(int64_t * items, uint64_t nitems)
{
#if defined(KEY_8B)

    intkey_t curr = 0;
    uint64_t i;

    tuple_t * tuples = (tuple_t *) items;

    for (i = 0; i < nitems; i++)
    {
        if(tuples[i].key < curr)
            return 0;

        curr = tuples[i].key;
    }

    return 1;

#else

    /* int64_t curr = 0; */
    /* uint64_t i; */

    /* for (i = 0; i < nitems; i++) */
    /* { */
    /*     if(items[i] < curr){ */
    /*         printf("ERR: item[%d]=%llu ; item[%d]=%llu\n", i, items[i], i-1, curr); */
    /*         /\* return 0; *\/ */
    /*         exit(0); */
    /*     } */

    /*     curr = items[i]; */
    /* } */

    /* return 1; */

    /* int64_t curr = 0; */
    /* uint64_t i; */
    /* int warned = 0; */
    /* for (i = 0; i < nitems; i++) */
    /* { */
    /*     if(items[i] == curr) { */
    /*         if(!warned){ */
    /*             printf("[WARN ] Equal items, still ok... "\ */
    /*                    "item[%d]=%lld is equal to item[%d]=%lld\n", */
    /*                    i, items[i], i-1, curr); */
    /*             warned =1; */
    /*         } */
    /*     } */
    /*     else if(items[i] < curr){ */
    /*         printf("[ERROR] item[%d]=%lld is less than item[%d]=%lld\n", */
    /*                i, items[i], i-1, curr); */
    /*         return 0; */
    /*     } */

    /*     curr = items[i]; */
    /* } */

    /* return 1; */

    intkey_t curr = 0;
    uint64_t i;
    int warned = 0;
    tuple_t * tuples = (tuple_t *)items;
    for (i = 0; i < nitems; i++)
    {
        if(tuples[i].key == curr) {
            if(!warned){
                logger(INFO,"[WARN ] Equal items, still ok... "\
                       "item[%d].key=%d is equal to item[%d].key=%d\n",
                       i, tuples[i].key, i-1, curr);
                warned =1;
            }
        }
        else if(tuples[i].key < curr){
            logger(INFO,"[ERROR] item[%d].key=%d is less than item[%d].key=%d\n",
                   i, tuples[i].key, i-1, curr);
            return 0;
        }

        curr = tuples[i].key;
    }

    return 1;

#endif
}

/** utility method to check whether arrays are sorted */
void
check_sorted(int64_t * R, int64_t * S, uint64_t nR, uint64_t nS, int my_tid)
{

    if(is_sorted_helper(R, nR))
        logger(INFO,"%d-thread -> R is sorted, size = %d\n", my_tid, nR);
    else
        logger(INFO,"%d-thread -> R is NOT sorted, size = %d\n", my_tid, nR);

    if(is_sorted_helper(S, nS))
        logger(INFO,"%d-thread -> S is sorted, size = %d\n", my_tid, nS);
    else
        logger(INFO,"%d-thread -> S is NOT sorted, size = %d\n", my_tid, nS);
}

