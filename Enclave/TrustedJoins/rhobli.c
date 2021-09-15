#include "rhobli.h"
#include "enclave_data_structures.h"
#include "Enclave_t.h"
#include "Enclave.h"
#include "prj_params.h"
#include "radix_join.h"
#include "util.h"

#ifndef HASH_BIT_MODULO
#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)
#endif

#ifndef MAX
#define MAX(X, Y) (((X) > (Y)) ? (X) : (Y))
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

static void *
alloc_memalign(size_t size)
{
    void * ret;
    ret = memalign(CACHE_LINE_SIZE, size);

    malloc_check(ret);

    return ret;
}

#define MALLOC(SZ) alloc_memalign(SZ+RELATION_PADDING)

extern Schema schemas[NUM_STRUCTURES];
extern char* tableNames[NUM_STRUCTURES];
extern int rowsPerBlock[NUM_STRUCTURES]; //let's make this always 1; helpful for security and convenience; set block size appropriately for testing
extern int numRows[NUM_STRUCTURES];
extern int oblivStructureSizes[NUM_STRUCTURES]; //actual size, not logical size for orams
extern Obliv_Type oblivStructureTypes[NUM_STRUCTURES];

char* realRetTableName = "JoinReturn";
char* relR_name = "relR";
char* relS_name = "relS";
int insertionCounter = 0;
int dummyVal = 0;

static void
print_timing(struct timers_t timers, uint64_t numtuples, int64_t result)
{
    double cyclestuple = (double) (timers.total) / (double) numtuples;
    uint64_t time_usec = timers.end - timers.start;
    double throughput = (double) (1000*numtuples)  / (double) time_usec;
    logger(ENCLAVE, "Total input tuples : %lu", numtuples);
    logger(ENCLAVE, "Result tuples : %lu", result);
    logger(ENCLAVE, "Phase Total [cycles] : %lu", timers.total);
    logger(ENCLAVE, "Cycles-per-tuple : %.4lf", cyclestuple);
    logger(ENCLAVE, "Total Runtime [us] : %lu ", time_usec);
    logger(ENCLAVE, "Throughput [K rec/sec] : %.2lf", throughput);
}

int64_t
oblivious_bucket_chaining_join(const struct table_t * const R,
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
    uint8_t* row = (uint8_t*)malloc(BLOCK_DATA_SIZE);
    uint8_t* block = (uint8_t*)malloc(BLOCK_DATA_SIZE);

    int realRetStructId = getTableId(realRetTableName);

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

            // it's a match - assembly an output row
            if(Stuples[i].key == Rtuples[hit-1].key)
            {
                matches ++;
                int shift = 0;
                memcpy(&row[shift], &Rtuples[hit-1].key, sizeof(type_key));
                shift += sizeof(type_key);
                memcpy(&row[shift], &Rtuples[hit-1].payload, sizeof(type_value));
                shift += sizeof(type_value);
                memcpy(&row[shift], &Stuples[i].payload, sizeof(type_value));
                numRows[realRetStructId]++;
//#ifdef JOIN_MATERIALIZE
//                insert_output(output, Stuples[i].key, Rtuples[hit-1].payload, Stuples[i].payload);
//#endif
            }
            // it's not a match - assembly a dummy row
            else
            {
                memset(&row[0], 0, BLOCK_DATA_SIZE);
                row[0] = '\0';
                dummyVal++;
            }
            memcpy(block, &row[0], BLOCK_DATA_SIZE);
            opOneLinearScanBlock(realRetStructId, insertionCounter, (Linear_Scan_Block*)block, 1);
            insertionCounter++;
        }
    }
    /* PROBE-LOOP END  */

    /* clean up temp */
    free(bucket);
    free(next);
    free(row);
    free(block);

    return matches;

}

result_t* rhobli_join (struct table_t *relR, struct table_t *relS, int nthreads)
{
    struct timers_t timers{};
    uint8_t *row, *row1, *row2;
    uint32_t total_result = 0;

    total_init();
    transformTable(relR, relR_name);
    transformTable(relS, relS_name);

#ifndef NO_TIMING
    ocall_get_system_micros(&timers.start);
    ocall_startTimer(&timers.total);
#endif

    int structureIdR = getTableId(relR_name);
    int structureIdS = getTableId(relS_name);
    int realRetStructId = -1;
    dummyVal = 0;

    row = (uint8_t*)malloc(BLOCK_DATA_SIZE);
    row1 = (uint8_t*)malloc(BLOCK_DATA_SIZE);
    row2 = (uint8_t*)malloc(BLOCK_DATA_SIZE);
    uint8_t* block = (uint8_t*)malloc(BLOCK_DATA_SIZE);
    if (!row || !row1 || !row2 || !block) {
        logger(ENCLAVE, "Failed to allocate memory for rows/blocks - %d bytes", BLOCK_DATA_SIZE);
    }
    insertionCounter = 0;

    int psR = oblivStructureSizes[structureIdR];
    int psS = oblivStructureSizes[structureIdS];

    logger(DBG, "structIdR = %d, oblivStructSizeR = %d", structureIdR, psR);
    logger(DBG, "structIdS = %d, oblivStructSizeS = %d", structureIdS, psS);

    Schema s2 = schemas[structureIdS];//need the second schema sometimes

    Schema s;
    s.numFields = 4;
    s.fieldOffsets[0] = 0;
    s.fieldOffsets[1] = 1;
    s.fieldOffsets[2] = 5;
    s.fieldOffsets[3] = 9;
    s.fieldSizes[0] = 1;
    s.fieldSizes[1] = 4;
    s.fieldSizes[2] = 4;
    s.fieldSizes[3] = 4;
    s.fieldTypes[0] = CHAR;
    s.fieldTypes[1] = INTEGER;
    s.fieldTypes[2] = INTEGER;
    s.fieldTypes[3] = INTEGER;

    createTable(&s, realRetTableName, (int) strlen(realRetTableName), TYPE_LINEAR_SCAN, (psR*4/ROWS_IN_ENCLAVE_JOIN+1)*psS, &realRetStructId);
    int iteration = 0;

    relation_t *Rn, *Sn;
    Rn = (relation_t*) malloc(sizeof(relation_t));
    Sn = (relation_t*) malloc(sizeof(relation_t));
    Rn->tuples = (tuple_t*) alloc_memalign(RELATION_PADDING+ROWS_IN_ENCLAVE_JOIN/2 * sizeof(tuple_t));
    Sn->tuples = (tuple_t*) alloc_memalign(RELATION_PADDING+ROWS_IN_ENCLAVE_JOIN/2 * sizeof(tuple_t));
    Rn->sorted = 0;
    Sn->sorted = 0;

    for (int i = 0; i < oblivStructureSizes[structureIdR]; i += (ROWS_IN_ENCLAVE_JOIN/2))
    {
        int items = 0;
        for (int j = 0; j < (ROWS_IN_ENCLAVE_JOIN/2) && i+j < oblivStructureSizes[structureIdR]; j++)
        {
            // first pass for R
            opOneLinearScanBlock(structureIdR, i+j, (Linear_Scan_Block*) row, 0);
            if(row[0] == '\0') continue;
            memcpy(&Rn->tuples[items].key, &row[s.fieldOffsets[1]], 4);
            items++;
        }
        Rn->num_tuples = items;
        //logger(DBG," R = <%d, %d> (%d items)", i, i+items, items);

        for (int j = 0; j < oblivStructureSizes[structureIdS]; j += (ROWS_IN_ENCLAVE_JOIN/2))
        {
            items = 0;
            for (int k = 0; k < (ROWS_IN_ENCLAVE_JOIN/2) && j+k < oblivStructureSizes[structureIdS]; k++)
            {
                int index = j + k;
                opOneLinearScanBlock(structureIdS, index, (Linear_Scan_Block*) row, 0);
                if(row[0] == '\0') continue;
                type_key key;
                memcpy(&key, &row[s.fieldOffsets[1]], 4);
                Sn->tuples[items].key = key;
                items++;
            }
            //logger(DBG," S = <%d, %d>  (%d items)", j, j+items, items);
            Sn->num_tuples = items;
            result_t *res = join_init_run(Rn, Sn, oblivious_bucket_chaining_join, 1);
//            logger(INFO, "Partial result %lu matches", res->totalresults);
            total_result += res->totalresults;
            //logger(DBG,"partial result = %d", res->totalresults);
        }

    }

#ifndef NO_TIMING
    ocall_stopTimer(&timers.total);
    ocall_get_system_micros(&timers.end);
    print_timing(timers, relR->num_tuples + relS->num_tuples, numRows[realRetStructId]);
#endif
    if (total_result != numRows[realRetStructId])
    {
        ocall_throw("total result error");
    }
    free(Sn->tuples);
    free(Rn->tuples);
    free(row);
    free(row1);
    free(row2);
    return nullptr;
}