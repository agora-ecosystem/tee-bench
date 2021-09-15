#include "StitchJoin.h"
#include <pthread.h>
#include "barrier.h"
#include "prj_params.h"
#include "util.h"
#include <algorithm>
#include <vector>

#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "pcm_commons.h"
#include "Logger.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)

using namespace std;

struct arg_t_stj {
    tuple_t * relR;
    tuple_t * relS;

    int64_t numR;
    int64_t numS;
    int64_t totalR;
    int64_t totalS;

    pthread_barrier_t * barrier;
    int32_t my_tid;
    int nthreads;
} __attribute__((aligned(CACHE_LINE_SIZE)));

struct row_annot_t {
    tuple_t tuple;
    uint32_t partition;
} __attribute__((aligned(CACHE_LINE_SIZE)));

bool row_annotated_sorter(row_annot_t const& lhs, row_annot_t const& rhs)
{
    return lhs.partition > rhs.partition;
}

result_t *StitchJoin::STJ(relation_t *relR, relation_t *relS, int nthreads) {
    int i, rv;
    pthread_t tid[nthreads];
    pthread_barrier_t barrier;
    arg_t_stj args[nthreads];
    int64_t numperthr[2];

    rv = pthread_barrier_init(&barrier, NULL, nthreads);
    if(rv != 0){
        logger(ERROR, "Couldn't create the barrier");
        ocall_exit(EXIT_FAILURE);
    }

    /* assign chunks of R and S to thread*/
    numperthr[0] = relR->num_tuples / nthreads;
    numperthr[1] = relS->num_tuples / nthreads;

    for (i = 0; i < nthreads; i++)
    {
        args[i].relR = relR->tuples + i * numperthr[0];

        args[i].relS = relS->tuples + i * numperthr[0];
        args[i].numR = (i == (nthreads-1)) ?
                       (relR->num_tuples - i * numperthr[0]) : numperthr[0];
        args[i].numS = (i == (nthreads-1)) ?
                       (relS->num_tuples - i * numperthr[1]) : numperthr[1];
        args[i].totalR = relR->num_tuples;
        args[i].totalS = relS->num_tuples;

        args[i].my_tid = i;
        args[i].barrier = &barrier;
        args[i].nthreads = nthreads;

        rv = pthread_create(&tid[i], nullptr, StitchJoin::stj_thread, (void*)&args[i]);
        if (rv){
            logger(ERROR, "return code from pthread_create() is %d\n", rv);
            ocall_exit(-1);
        }
    }

    /* wait for all threads to finish */
    for (i = 0; i < nthreads; i++)
    {
        pthread_join(tid[i], nullptr);
    }

    return nullptr;
}

void * StitchJoin::stj_thread(void * param) {
    uint32_t i, j, k;
    int32_t sum = 0;
    int32_t R = 0, D = NUM_RADIX_BITS / NUM_PASSES;
    uint32_t fanOut = 1 << D;
    uint32_t MASK = (fanOut - 1) << R;
    auto * args = (arg_t_stj*) param;
    logger(DBG, "STJ thread-%d", args->my_tid);
    /* Annotate R tuples */
//    auto * relR_annot = (row_annot_t*) memalign(CACHE_LINE_SIZE, sizeof(row_annot_t) * L1_CACHE_TUPLES);

//    malloc_check(relR_annot);
//    auto * temp_hist = (uint32_t*) calloc(fanOut, sizeof(uint32_t));
    std::vector<uint32_t> temp_hist(fanOut);
//    malloc_check(temp_hist);
    /* for each chunk of R */
    for ( i = 0; i * L1_CACHE_TUPLES < args->numR; i++ )
    {
        uint32_t num_tuples = (args->numR - i*L1_CACHE_TUPLES < L1_CACHE_TUPLES) ?
                              (args->numR - i*L1_CACHE_TUPLES) : L1_CACHE_TUPLES;
        std::vector<row_annot_t> relR_annot(num_tuples);
        /* annotate the tuples with the partition number */
        for ( j = 0; j < num_tuples; j++ )
        {
            k = i * L1_CACHE_TUPLES + j;
            uint32_t idx = HASH_BIT_MODULO(args->relR[k].key, MASK, R);
            relR_annot[j].tuple = args->relR[k];
            relR_annot[j].partition = idx;
            temp_hist.at(idx)++;
        }
        logger(DBG, "************************ annotate **************");
        for (j = 0; j < num_tuples; j++)
        {
            k = i * L1_CACHE_TUPLES + j;
            printf("%d = %d -> %d ", args->relR[k].key, relR_annot.at(j).tuple.key, relR_annot.at(j).partition);
        }
        /* sort the chunk by partition */
        std::sort(std::begin(relR_annot),
                  std::end(relR_annot),
                  [](row_annot_t e1, row_annot_t e2) {return e1.partition < e2.partition; }
                  );
//        std::sort(relR_annot.at(0),
//                  relR_annot.at(num_tuples - 1),
//                  &row_annotated_sorter);
        logger(DBG, "************************ sort **************");
        for (j = 0; j < num_tuples; j++)
        {
            printf("%d -> %d ", relR_annot.at(j).tuple.key, relR_annot.at(j).partition);
        }
        /* calculate the prefix sum on histogram */
        for ( j = 0; j < fanOut; j++ )
        {
            sum += temp_hist.at(j);
            temp_hist.at(j) = sum;
        }
        /* write back to R */
        for ( j = 0; j < num_tuples; j++ )
        {
            k = i * L1_CACHE_TUPLES + j;
            args->relR[k] = relR_annot[j].tuple;
        }
        logger(DBG, "************************ write back **************");
        for ( j = 0; j < num_tuples; j++ )
        {
            k = i * L1_CACHE_TUPLES + j;
            printf("%d -> %d ", args->relR[k].key, temp_hist.at(k));
        }
    }

    return nullptr;
}
