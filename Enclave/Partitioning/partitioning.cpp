#include "partitioning.h"
#include "Enclave.h"
#include "Enclave_t.h"
#include <algorithm>
#include <tlibc/mbusafecrt.h>

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)

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

using namespace std;

void print_relation1(relation_t *rel, uint32_t num, uint32_t offset)
{
    logger(DBG, "****************** Relation sample ******************");
    for (uint64_t i = offset; i < rel->num_tuples && i < num + offset; i++)
    {
        logger(DBG, "%u -> %u", rel->tuples[i].key, rel->tuples[i].payload);
    }
    logger(DBG, "******************************************************");
}

bool compareTuples (tuple_t const& t1, tuple_t const& t2)
{
    return (t1.key < t2.key);
}

relation_t * partition_non_in_place_in_cache (relation_t * rel, int radix_bits, int32_t * histogram)
{
    const uint32_t fan_out = 1 << radix_bits;
    const uint32_t R       = 0;
    const uint32_t MASK    = (fan_out - 1) << R;
    uint64_t size          = rel->num_tuples;

//    histogram = (int32_t*) (malloc(sizeof(int32_t) * fan_out));
    int32_t offset [fan_out];

    memset(offset, 0, fan_out*sizeof*offset);

    auto * out = (relation_t*) malloc(sizeof(relation_t));
    out->tuples = (tuple_t*) malloc(size * sizeof(tuple_t));
    out->num_tuples = size;
    out->sorted = 0;

    uint64_t i, idx, sum = 0;

    // build the histogram
    for (i = 0; i < rel->num_tuples; i++)
    {
        idx = HASH_BIT_MODULO(rel->tuples[i].key, MASK, R);
        histogram[idx]++;
    }

    // calculate offsets
    for ( i = 0; i < fan_out - 1; i++)
    {
        sum += histogram[i];
        offset[i+1] = sum;
    }

    // partition the relation
    for (i = 0; i < size; i++)
    {
        idx = HASH_BIT_MODULO(rel->tuples[i].key, MASK, R);
        out->tuples[offset[idx]].key = rel->tuples[i].key;
        out->tuples[offset[idx]].payload = rel->tuples[i].payload;
        ++offset[idx];
    }

    // sort partitions by key
    sum = 0;
    for (i = 0; i < fan_out; i++)
    {
        sort(&out->tuples[sum],
                  &out->tuples[sum+histogram[i]],
                  compareTuples);
        sum += histogram[i];
    }
    return out;
}

int64_t
bucket_chaining_join(const tuple_t * const Rtuples,
                     const uint32_t numR,
                     const tuple_t * const Stuples,
                     const uint32_t numS,
                     const int radix_bits)
{
    int * next, * bucket;
    uint32_t N = numR;
    int64_t matches = 0;


    NEXT_POW_2(N);
    /* N <<= 1; */
    const uint32_t MASK = (N-1) << (radix_bits);

    next   = (int*) malloc(sizeof(int) * numR);
    /* posix_memalign((void**)&next, CACHE_LINE_SIZE, numR * sizeof(int)); */
    bucket = (int*) calloc(N, sizeof(int));

    for(uint32_t i=0; i < numR; ){
        uint32_t idx = HASH_BIT_MODULO(Rtuples[i].key, MASK, radix_bits);
        next[i]      = bucket[idx];
        bucket[idx]  = ++i;     /* we start pos's from 1 instead of 0 */

        /* Enable the following tO avoid the code elimination
           when running probe only for the time break-down experiment */
        /* matches += idx; */
    }

    /* Disable the following loop for no-probe for the break-down experiments */
    /* PROBE- LOOP */
    for(uint32_t i=0; i < numS; i++ ){

        uint32_t idx = HASH_BIT_MODULO(Stuples[i].key, MASK, radix_bits);

        for(int hit = bucket[idx]; hit > 0; hit = next[hit-1]){

            if(Stuples[i].key == Rtuples[hit-1].key){
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
