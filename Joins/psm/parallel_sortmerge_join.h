#ifndef PARALLEL_SORTMERGE_JOIN_H
#define PARALLEL_SORTMERGE_JOIN_H

#include "data-types.h"
#include "parallel_sort.h"
#include <stdlib.h>
#include "utility.h"

#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "pcm_commons.h"
#include "Logger.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

static int64_t merge(relation_t *relR, relation_t *relS)
{
    int64_t matches = 0;
    row_t *tr = relR->tuples;
    row_t *ts;
    row_t *gs = relS->tuples;
    std::size_t tr_s = 0, gs_s = 0;
    logger(DBG, "Merge1");
    while ( tr_s < relR->num_tuples && gs_s < relS->num_tuples)
    {
        while (tr->key < gs->key)
        {
            tr++; tr_s++;
        }
        while (tr->key > gs->key)
        {
            gs++; gs_s++;
        }
        ts = gs;
        while (tr->key == gs->key)
        {
            ts = gs;
            while (ts->key == tr->key)
            {
                matches++;
                ts++;
            }
            tr++; tr_s++;
        }
        gs = ts;
    }
    logger(DBG, "Merge2");
    return matches;
}
/*
 * Parallel Sort Merge (PSM) Join.
 * This algorithm has two phases - sort and merge. Sorting is done using
 * a parallel quicksort algorithm originally published by Intel [1].
 * The merge phase follows an algorithm proposed in the book
 * "Database Management Systems" by Ramakrishnan and Gehrke.
 *
 * [1] https://software.intel.com/content/www/us/en/develop/articles/an-efficient-parallel-three-way-quicksort-using-intel-c-compiler-and-openmp-45-library.html
 * */
result_t* PSM (struct table_t * relR, struct table_t * relS, int nthreads)
{
//    bool is_sorted;
//    std::size_t position = 0L;
    int64_t matches = 0L;
    result_t *result = static_cast<result_t*> (malloc(sizeof(result_t)));
    uint64_t start = 0, end = 0, sort_timer = 0, merge_timer = 0, total_timer = 0;
    std::size_t sizeR = relR->num_tuples;
    std::size_t sizeS = relS->num_tuples;
    ocall_get_system_micros(&start);
    ocall_startTimer(&total_timer);

    /* SORT PHASE */
    ocall_startTimer(&sort_timer);
#ifdef PCM_COUNT
    ocall_set_system_counter_state("Start sort phase");
#endif
    if (!relR->sorted)
    {
        internal::parallel_sort(&relR->tuples[0], &relR->tuples[sizeR], nthreads);
    }
    logger(DBG, "R sorted");
    if (!relS->sorted)
    {
        internal::parallel_sort(&relS->tuples[0], &relS->tuples[sizeS], nthreads);
    }
    logger(DBG, "S sorted");
#ifdef PCM_COUNT
    ocall_get_system_counter_state("Sort", 0);
#endif
    ocall_stopTimer(&sort_timer);

    /* MERGE PHASE */
    ocall_startTimer(&merge_timer);
#ifdef PCM_COUNT
    ocall_set_system_counter_state("Start merge");
#endif

    matches = merge(relR, relS);

#ifdef PCM_COUNT
    ocall_get_system_counter_state("Merge", 0);
#endif
    ocall_stopTimer(&merge_timer);
    ocall_stopTimer(&total_timer);
    ocall_get_system_micros(&end);
    print_timing(total_timer, merge_timer, sort_timer, (uint64_t) (sizeR + sizeS), matches, start, end, 0,0 );
    result->totalresults = matches;
    result->nthreads = nthreads;
    return result;
}
#endif //PARALLEL_SORTMERGE_JOIN_H
