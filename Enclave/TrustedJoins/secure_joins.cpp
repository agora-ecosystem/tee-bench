#include "Enclave_t.h"
#include "data-types.h"
#include "nested_loop_join.h"
#include "no_partitioning_join.h"
#include "rhobli.h"
#include "radix_join.h"
#include "CHTJoinWrapper.hpp"
#include "radix_sortmerge_join.h"
#include <sgx_tseal.h>
#include <stitch/StitchJoin.h>
#include <tlibc/mbusafecrt.h>
#include <grace/grace_join.h>
#include <mway/sortmergejoin_multiway.h>
#include "obliv_join.h"
#include "opaque_join.h"
#include "oblidb_join.h"
#include "parallel_sortmerge_join.h"
#include "util.h"
#include "MCJoin.h"
//#include "StitchJoin.h"
#include "partitioning.h"
#include "rho_atomic/radix_join_atomic.h"

extern char aad_mac_text[256];
//extern result_t* rhobli_join (struct table_t *relR, struct table_t *relS, int nthreads);

void print_relation(relation_t *rel, uint32_t num, uint32_t offset)
{
    logger(DBG, "****************** Relation sample ******************");
    for (uint32_t i = offset; i < rel->num_tuples && i < num + offset; i++)
    {
        logger(DBG, "%u -> %u", rel->tuples[i].key, rel->tuples[i].payload);
    }
    logger(DBG, "******************************************************");
}
void *
part_thread(void * param) {
    arg_t_partition *args = (arg_t_partition *) param;

    int radix_bits = 2;
    uint32_t num_partitions = 1 << radix_bits;
    uint32_t result = 0;
    uint32_t offsetR = 0, offsetS = 0;

    auto * histR = (int32_t*) calloc(num_partitions, sizeof(int32_t));
    auto * histS = (int32_t*) calloc(num_partitions, sizeof(int32_t));

    relation_t * partRelR = partition_non_in_place_in_cache(args->relR, radix_bits, histR);
    relation_t * partRelS = partition_non_in_place_in_cache(args->relS, radix_bits, histS);

//    char buf[256], *pos = buf;
//    for (int i = 0; i < num_partitions; i++) {
//        pos += sprintf_s(pos, 256, "%d ", histR[i]);
//    }
//    logger(DEBUG, "histR = %s", buf);

    if (args->tid == 0) {
        ocall_get_system_micros(&args->start);
    }
    /* bucket chaining join */
    for (int i = 0; i < num_partitions; i++)
    {
        result += bucket_chaining_join(partRelR->tuples+offsetR,
                                       histR[i],
                                       partRelS->tuples+offsetS,
                                       histS[i],
                                       radix_bits);
        offsetR += histR[i];
        offsetS += histS[i];
    }

    if (args->tid == 0) {
        ocall_get_system_micros(&args->end);
    }

    args->result = result;

    free(histR);
    free(histS);
    free(partRelR);
    free(partRelS);
}

result_t*
partitioning_test(struct table_t * relR, struct table_t * relS, int nthreads) {
    uint32_t result = 0;

    arg_t_partition args[nthreads];
    pthread_t tid [nthreads];

    args[0].tid = 0;
    args[0].relR = relR;
    args[0].relS = relS;

    int rv = pthread_create(&tid[0], nullptr, part_thread, (void*)&args[0]);

    if (rv){
        logger(ERROR, "return code from pthread_create() is %d\n", rv);
        ocall_exit(-1);
    }

    /* wait for threads to finish */
    for(int i = 0; i < 1; i++){
        pthread_join(tid[i], nullptr);
        result += args[i].result;
    }

    logger(DBG, "Found %d matches", result);
    logger(DBG, "Join timer: %lu us", (args[0].end - args[0].start));
    return nullptr;
}

result_t*
CHT(struct table_t * relR, struct table_t * relS, int nthreads)
{
    join_result_t  join_result = CHTJ<7>(relR, relS, nthreads);
    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = join_result.matches;
    joinresult->nthreads = nthreads;
    return joinresult;
}

result_t*
STJ(struct table_t * relR, struct table_t * relS, int nthreads)
{
    result_t * joinresult;
    StitchJoin * stj = new StitchJoin();
    stj->STJ(relR, relS, nthreads);
    return joinresult;
}

result_t*
MCJ(struct table_t * relR, struct table_t * relS, int nthreads)
{
    result_t * joinresult = nullptr;
    SMetrics metrics;
    uint32_t memoryConstraintMB = nthreads;
    uint32_t packedPartitionMemoryCardinality = 16000000;
    uint32_t flipFlopCardinality = 16000000;
    uint32_t bitRadixLength = 24;
    uint32_t maxBitsPerFlipFlopPass = 8;
    uint32_t outputBufferCardinality = 1000;
    // parse arguments
    CMemoryManagement memManagement;
    logger(DEBUG, "Memory constraint %d MB", nthreads);

    if (memoryConstraintMB != 0)
    {
        if(memManagement.optimiseForMemoryConstraint(relR->num_tuples, memoryConstraintMB, bitRadixLength))
        {
            // Based on the results, we need to change our options settings
            packedPartitionMemoryCardinality = memManagement.getRIdealTuples();
            flipFlopCardinality = memManagement.getSIdealTuples();
            metrics.r_bias = memManagement.getRBias();
        }
        else
        {
            // It didn't work - this is usually because the histogram is simply too large to fit within the given memory constraint
            logger(ERROR, "Unable to allocate memory (histogram too large for constraint?)");
            ocall_exit(-1);
        }
    }

    CMemoryManagement memoryManagement;


    MCJoin mcj = MCJoin();
    mcj.doMCJoin(relR,
                 relS,
                 &metrics,
                 bitRadixLength,
                 maxBitsPerFlipFlopPass,
                 nthreads,
                 flipFlopCardinality,
                 packedPartitionMemoryCardinality,
                 outputBufferCardinality);
    return joinresult;
}

static struct algorithm_t sgx_algorithms[] = {
        {"PHT", PHT},
        {"NPO_st", NPO_st},
        {"NL", NL},
        {"INL", INL},
        {"RJ", RJ},
        {"RHO", RHO},
        {"RHT", RHT},
        {"PSM", PSM},
        {"RSM", RSM},
        {"CHT", CHT},
        {"OBLI", oblidb_join},
        {"OPAQ", opaque_join},
        {"OJ", OJ_wrapper},
        {"RHOBLI", rhobli_join},
        {"MCJ", MCJ},
        {"STJ", STJ},
        {"TEST", partitioning_test},
        {"GHT", GHT},
        {"MWAY", MWAY},
        {"RHO_seal_buffer", RHO_seal_buffer},
        {"RHO_atomic", RHO_atomic}
};

uint8_t* unseal_rel(const uint8_t *sealed_rel, size_t size)
{
    uint32_t mac_text_len = sgx_get_add_mac_txt_len((const sgx_sealed_data_t *)sealed_rel);
    uint32_t decrypt_data_len = sgx_get_encrypt_txt_len((const sgx_sealed_data_t *) sealed_rel);
    if (mac_text_len == UINT32_MAX || decrypt_data_len == UINT32_MAX)
    {
//        return SGX_ERROR_UNEXPECTED;
        return nullptr;
    }
    if (mac_text_len > size || decrypt_data_len > size)
    {
        // SGX_ERROR_INVALID_PARAMETER
        return 0;
    }
    uint8_t *de_mac_text = (uint8_t *) malloc(mac_text_len);
    if (de_mac_text == nullptr)
    {
        //SGX_ERROR_OUT_OF_MEMORY
        return nullptr;
    }
    uint8_t *decrypt_data = (uint8_t *) malloc(decrypt_data_len);
    if (decrypt_data == nullptr)
    {
        //SGX_ERROR_OUT_OF_MEMORY
        free(de_mac_text);
        return nullptr;
    }
    sgx_status_t ret = sgx_unseal_data((const sgx_sealed_data_t *)sealed_rel,
                                       de_mac_text,
                                       &mac_text_len,
                                       decrypt_data,
                                       &decrypt_data_len);
    if (ret != SGX_SUCCESS)
    {
        free(de_mac_text);
        free(decrypt_data);
        return nullptr;
    }

    if (memcmp(de_mac_text, aad_mac_text, strlen(aad_mac_text)))
    {
        //SGX_ERROR_UNEXPECTED
        return nullptr;
    }
    free(de_mac_text);
    return decrypt_data;
}

uint8_t* sealed_buf;

uint32_t seal_relation(relation_t * rel, uint32_t seal_chunk_size)
{
    uint32_t output_size = sizeof(relation_t) + rel->num_tuples * (sizeof(row_t));
    logger(DBG, "Size of unsealed data = %.2lf MB", B_TO_MB(output_size));
    if (seal_chunk_size != 0) {
        logger(DBG, "Size of seal chunk = %d kB", seal_chunk_size);
        uint32_t chunk_size, offset = 0, temp_sealed_size;
        uint32_t * tmp;
        uint32_t available_space = seal_chunk_size * 1024 - sizeof(sgx_sealed_data_t) - (uint32_t) strlen(aad_mac_text); // reverse sgx_calc_sealed_data_size
        uint32_t chunks = (output_size - 1) / available_space + 1; // get the number of chunks rounded up
        logger(DBG, "Available space in seal_chunk_size: %d, total chunks: %d", available_space, chunks);
        uint8_t * temp_sealed_buf = (uint8_t*) malloc(seal_chunk_size * 1024);
        for (uint32_t i = 0; i < chunks; i ++) {
            chunk_size = (i == (chunks - 1)) ?
                                  (output_size - i * available_space) : available_space;
            tmp = (uint32_t *) rel;
            tmp += offset;
            temp_sealed_size = sgx_calc_sealed_data_size((uint32_t)strlen(aad_mac_text), chunk_size);
//            logger(DBG, "Chunk %d: size=%d, offset=%d, sealed_size=%d", i+1, chunk_size, offset, temp_sealed_size);
            sgx_status_t err = sgx_seal_data((uint32_t) strlen(aad_mac_text),
                                             (const uint8_t *) aad_mac_text,
                                             chunk_size,
                                             (uint8_t*) tmp,
                                             temp_sealed_size,
                                             (sgx_sealed_data_t *) temp_sealed_buf);
            if (err == SGX_SUCCESS) {
                sealed_buf = temp_sealed_buf;
                offset += chunk_size;
            } else {
                logger(ERROR, "[Chunk %d/%d] sealing error: %d", i+1, chunks, err);
                ocall_exit(1);
            }
        }
        logger(DBG, "Sealing relation in chunks successful");
        return seal_chunk_size;
    } else {
        uint32_t sealed_data_size = sgx_calc_sealed_data_size((uint32_t)strlen(aad_mac_text), output_size);
        logger(DBG, "Size of seal chunk = %d kB", sealed_data_size);
        uint8_t* temp_sealed_buf = (uint8_t *) malloc(sealed_data_size);
        sgx_status_t err = sgx_seal_data((uint32_t)strlen(aad_mac_text),
                                         (const uint8_t *) aad_mac_text,
                                         output_size,
                                         (uint8_t*) rel,
                                         sealed_data_size,
                                         (sgx_sealed_data_t *) temp_sealed_buf);
        if (err == SGX_SUCCESS)
        {
            logger(DBG, "Sealing relation successful");
            sealed_buf = temp_sealed_buf;
            return sealed_data_size;
        }
    }
    return 0;
}

sgx_status_t ecall_get_sealed_data(uint8_t* sealed_blob, uint32_t data_size)
{
    if (sealed_buf == nullptr)
    {
        printf("Nothing to return...");
        return SGX_ERROR_UNEXPECTED;
    }
    memcpy(sealed_blob, sealed_buf, data_size);
    free(sealed_buf);
    return SGX_SUCCESS;
}

result_t* ecall_join(struct table_t * relR, struct table_t * relS, char *algorithm_name, int nthreads)
{
    int i =0, found = 0;
    algorithm_t *algorithm = nullptr;
    while(sgx_algorithms[i].join)
    {
        if (strcmp(algorithm_name, sgx_algorithms[i].name) == 0)
        {
            found = 1;
            algorithm = &sgx_algorithms[i];
            break;
        }
        i++;
    }
    if (found == 0)
    {
        printf("Algorithm not found: %s", algorithm_name);
        ocall_exit(EXIT_FAILURE);
    }
    struct rusage_reduced_t usage;
    usage.ru_utime_sec = 0;
    usage.ru_utime_usec = 0;
    usage.ru_stime_sec = 0;
    usage.ru_stime_usec = 0;
    usage.ru_minflt = 0;
    usage.ru_majflt = 0;
    usage.ru_nvcsw = 0;
    usage.ru_nivcsw = 0;
    ocall_getrusage(&usage, 0);
    result_t *res = algorithm->join(relR, relS, nthreads);
    ocall_getrusage(&usage, 1);

    return res;
}

relation_t *to_relation(result_t *result) {
#ifndef JOIN_MATERIALIZE
    logger(WARN, "JOIN_MATERIALIZE not defined. to_relation might fail.");
#endif
    relation_t * output = (relation_t*) malloc(sizeof(relation_t));
    malloc_check(output);
    output->tuples = (tuple_t*) malloc(sizeof(tuple_t)*result->totalresults);
    malloc_check(output->tuples);
    uint64_t items = 0;
    for (int i = 0; i < result->nthreads; i++)
    {
        output_list_t *list = result->resultlist[i].results;
        while (list != nullptr)
        {
            memcpy(output->tuples + items, list, sizeof(type_key)+sizeof(type_value));
            items++;
            list = list->next;
        }
    }
    output->num_tuples = result->totalresults;
    output->sorted = 0;
    output->ratio_holes = 0;
    logger(DBG, "to_relation check if %lu == %lu", result->totalresults, items);
    return output;
}

uint32_t ecall_join_sealed_tables(const uint8_t *sealed_r,
                                  size_t size_r,
                                  const uint8_t *sealed_s,
                                  size_t size_s,
                                  char *algorithm,
                                  int nthreads,
                                  uint32_t seal_chunk_size)
{
    uint64_t seal_timer = 0, unseal_timer = 0, join_timer = 0;
    relation_t * output = nullptr;
    uint32_t sealed_data_size = 0;
    ocall_startTimer(&unseal_timer);
    struct table_t * relR = (struct table_t *) unseal_rel(sealed_r, size_r);
    printf("Unseal R successful");
    struct table_t * relS = (struct table_t *) unseal_rel(sealed_s, size_s);
    printf("Unseal S successful");
    ocall_stopTimer(&unseal_timer);
    ocall_startTimer(&join_timer);
    result_t* result = ecall_join(relR, relS, algorithm, nthreads);
    if (strcmp(algorithm, "RHO_seal_buffer") != 0) {
        output = to_relation(result);
    }
    ocall_stopTimer(&join_timer);

    if (strcmp(algorithm, "RHO_seal_buffer") != 0) {
        ocall_startTimer(&seal_timer);
        uint64_t start, end;
        ocall_get_system_micros(&start);
        sealed_data_size = seal_relation(output, seal_chunk_size);
        ocall_get_system_micros(&end);
        ocall_stopTimer(&seal_timer);
        logger(INFO, "seal_micros = %lu", (end-start));
    }
    logger(INFO, "uns_timer = %lu (%.2lf%%)", unseal_timer, (double) unseal_timer*100/(unseal_timer + seal_timer));
    logger(INFO, "s_timer   = %lu (%.2lf%%)", seal_timer, (double) seal_timer*100/(unseal_timer + seal_timer));
    logger(INFO, "seal_timer = %lu", (unseal_timer + seal_timer));
    logger(INFO, "join_timer = %lu", join_timer);


    free(relR);
    free(relS);
    //TODO: free result
    return sealed_data_size;
}

uint32_t ecall_three_way_join_sealed_tables(const uint8_t *sealed_r,
                                            size_t size_r,
                                            const uint8_t *sealed_s,
                                            size_t size_s,
                                            const uint8_t *sealed_t,
                                            size_t size_tt,
                                            char *algorithm,
                                            int nthreads,
                                            uint32_t seal_chunk_size) {
    uint64_t seal_timer, unseal_timer, join1_timer, join2_timer;
    ocall_startTimer(&unseal_timer);
    relation_t * relR = (relation_t *) unseal_rel(sealed_r, size_r);
    printf("Unseal R successful");
    relation_t * relS = (relation_t *) unseal_rel(sealed_s, size_s);
    printf("Unseal S successful");
    relation_t * relT = (relation_t *) unseal_rel(sealed_t, size_tt);
    printf("Unseal T successful");
    ocall_stopTimer(&unseal_timer);

    ocall_startTimer(&join1_timer);
    result_t* result1 = ecall_join(relR, relS, "RHO", nthreads);
    relation_t * output1 = to_relation(result1);
    ocall_stopTimer(&join1_timer);

    ocall_startTimer(&join2_timer);
    result_t* result2 = ecall_join(relT, output1, "RHO", nthreads);
    relation_t * output2 = to_relation(result2);
    ocall_stopTimer(&join2_timer);

    ocall_startTimer(&seal_timer);
    uint64_t start, end;
    ocall_get_system_micros(&start);
    uint32_t sealed_data_size = seal_relation(output2, seal_chunk_size);
    ocall_get_system_micros(&end);
//    free(sealed_buf);
//    seal_relation(relS);
    ocall_stopTimer(&seal_timer);
    logger(INFO, "uns_timer = %lu (%.2lf%%)", unseal_timer, (double) unseal_timer*100/(unseal_timer + seal_timer));
    logger(INFO, "s_timer   = %lu (%.2lf%%)", seal_timer, (double) seal_timer*100/(unseal_timer + seal_timer));
    logger(INFO, "seal_timer = %lu", (unseal_timer + seal_timer));
    logger(INFO, "join1_timer = %lu", join1_timer);
    logger(INFO, "join2_timer = %lu", join2_timer);
    logger(INFO, "seal_micros = %lu", (end-start));
    logger(INFO, "total sealing share = %.2lf%%",
           (double) (unseal_timer+seal_timer)*100/(unseal_timer+seal_timer+join1_timer+join2_timer));

    free(relR);
    free(relS);
    free(relT);
    //TODO: free result
    return sealed_data_size;
}
