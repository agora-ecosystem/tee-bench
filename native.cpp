#include <stdio.h>
#include <math.h>
#include <time.h>
#include <mway/sortmergejoin_multiway.h>

#include "data-types.h"
#include "commons.h"
#include "generator.h"
#include "Logger.h"

#include "no_partitioning_join.h"
#include "nested_loop_join.h"
#include "radix_join.h"
#include "CHTJoinWrapper.hpp"
#include "radix_sortmerge_join.h"
#include "parallel_sortmerge_join.h"

#include "Include/data-types.h"
#include "App/Lib/commons.h"
#include "nested_loop_join.h"

using namespace std;

struct timespec ts_start;

result_t * CHT(relation_t *relR, relation_t* relS, int nthreads)
{
    join_result_t join_result =  CHTJ<7>(relR, relS, nthreads);
    result_t* res = (result_t *) malloc(sizeof(result_t));
    res->totalresults = join_result.matches;
    return res;
}

static struct algorithm_t algorithms[] = {
        {"PHT", PHT},
        {"NPO_st", NPO_st},
        {"NL", NL},
        {"RJ", RJ},
        {"PSM", PSM},
        {"RHO", RHO},
        {"RHT", RHT},
        {"RSM", RSM},
        {"CHT", CHT},
        {"INL", INL},
        {"MWAY", MWAY}
};

int main(int argc, char *argv[]) {
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    logger(INFO, "Welcome from native!");

    struct table_t tableR;
    struct table_t tableS;
    int64_t results;

    /* Cmd line parameters */
    args_t params;

    /* Set default values for cmd line params */
    params.algorithm       = &algorithms[0]; /* NPO_st */
    params.r_size          = 2097152; /* 2*2^20 */
    params.s_size          = 2097152; /* 2*2^20 */
    params.r_seed          = 11111;
    params.s_seed          = 22222;
    params.nthreads        = 2;
    params.selectivity     = 100;
    params.skew            = 0;
    params.sort_r          = 0;
    params.sort_s          = 0;
    params.r_from_path     = 0;
    params.s_from_path     = 0;
    params.seal_chunk_size = 0;
    params.three_way_join  = 0;

    parse_args(argc, argv, &params, algorithms);

    logger(DBG, "Number of threads = %d (N/A for every algorithm)", params.nthreads);

    seed_generator(params.r_seed);
    if (params.r_from_path)
    {
        logger(INFO, "Build relation R from file %s", params.r_path);
        create_relation_from_file(&tableR, params.r_path, params.sort_r);
        params.r_size = tableR.num_tuples;
    }
    else
    {
        logger(INFO, "Build relation R with size = %.2lf MB (%d tuples)",
               (double) sizeof(struct row_t) * params.r_size/pow(2,20),
               params.r_size);
        create_relation_pk(&tableR, params.r_size, params.sort_r);
    }
    logger(DBG, "DONE");

    seed_generator(params.s_seed);
    if (params.s_from_path)
    {
        logger(INFO, "Build relation S from file %s", params.s_path);
        create_relation_from_file(&tableS, params.s_path, params.sort_s);
        params.s_size = tableS.num_tuples;
    }
    else
    {
        logger(INFO, "Build relation S with size = %.2lf MB (%d tuples)",
               (double) sizeof(struct row_t) * params.s_size/pow(2,20),
               params.s_size);
        if (params.skew > 0) {
            create_relation_zipf(&tableS, params.s_size, params.r_size, params.skew, params.sort_s);
        }
        else if (params.selectivity != 100)
        {
            logger(INFO, "Table S selectivity = %d", params.selectivity);
            uint32_t maxid = params.selectivity != 0 ? (100 * params.r_size / params.selectivity) : 0;
            create_relation_fk_sel(&tableS, params.s_size, maxid, params.sort_s);
        }
        else {
            create_relation_fk(&tableS, params.s_size, params.r_size, params.sort_s);
        }
    }
    logger(DBG, "DONE");

    logger(INFO, "Running algorithm %s", params.algorithm->name);

    clock_t start = clock();
    result_t* matches = params.algorithm->join(&tableR, &tableS, params.nthreads);
    logger(INFO, "Total join runtime: %.2fs", (clock() - start)/ (float)(CLOCKS_PER_SEC));
    logger(INFO, "Matches = %lu", matches->totalresults);
    delete_relation(&tableR);
    delete_relation(&tableS);
}