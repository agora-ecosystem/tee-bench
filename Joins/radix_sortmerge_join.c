#include <stdlib.h>
#include <pthread.h>
#include "prj_params.h"
#include "radix_sortmerge_join.h"
#include "radix_join.h"
#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "pcm_commons.h"
#include "Logger.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

void is_table_sorted(const struct table_t * const t)
{
    intkey_t curr = 0;

    for (uint64_t i = 0; i < t->num_tuples; i++)
    {
        intkey_t key = t->tuples[i].key;
        if (key < curr)
        {
            logger(ERROR, "ERROR! Table is not sorted!");
            ocall_exit(EXIT_FAILURE);
        }
        curr = key;
    }
}

static inline int __attribute__((always_inline))
compare(const void * k1, const void * k2)
{
    const tuple_t *r1 = (const tuple_t *) k1;
    const tuple_t *r2 = (const tuple_t *) k2;
    type_key val = r1->key - r2->key;
    return (int) val;
}

static int64_t merge(const relation_t *relR, const relation_t *relS)
{
    int64_t matches = 0;
    row_t *tr = relR->tuples;
    row_t *ts;
    row_t *gs = relS->tuples;
    int64_t tr_s = 0, gs_s = 0;
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
    return matches;
}


int64_t sortmerge_join(const struct table_t * const R,
                       const struct table_t * const S,
                       struct table_t * const tmpR,
                       output_list_t ** output)
{
    (void) (tmpR);
    (void) (output);

    const uint64_t numR = R->num_tuples;
    const uint64_t numS = S->num_tuples;
    int64_t matches = 0;

    /* SORT PHASE */
    if (!R->sorted)
    {
        qsort(R->tuples, numR, sizeof(tuple_t*),compare);
    }
//    is_table_sorted(R);
    if (!S->sorted)
    {
        qsort(S->tuples, numS, sizeof(tuple_t*), compare);
    }
//    is_table_sorted(S);
    /* MERGE PHASE */
    matches =  merge(R, S);
    return matches;
}

result_t* RSM (struct table_t * relR, struct table_t * relS, int nthreads)
{
    return join_init_run(relR, relS, sortmerge_join, nthreads);
}
