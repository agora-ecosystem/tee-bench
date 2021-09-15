/*
 * Copyright (C) 2011-2020 Intel Corporation. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *   * Neither the name of Intel Corporation nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */


#include <string.h>
# include <pwd.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <math.h>
#include <time.h>

#include "sgx_urts.h"
#include "App.h"
#include "Lib/ErrorSupport.h"
#include "data-types.h"
#include "Lib/Logger.h"
#include "Lib/generator.h"
#include "commons.h"
#include "parallel_sort.h"

#ifndef NATIVE_COMPILATION
#include "Enclave_u.h"
#endif

#ifdef PCM_COUNT
#include "pcm_commons.h"
#include "cpucounters.h"
#endif

#ifdef SGX_COUNTERS
#include "sgx_counters.h"
#endif

extern void ocall_set_sgx_counters(const char *message);

/* Global EID shared by multiple threads */
sgx_enclave_id_t global_eid = 0;

struct timespec ts_start;

/* Initialize the enclave:
 *   Call sgx_create_enclave to initialize an enclave instance
 */
int initialize_enclave(void)
{
    sgx_status_t ret = SGX_ERROR_UNEXPECTED;
    
    /* Call sgx_create_enclave to initialize an enclave instance */
    /* Debug Support: set 2nd parameter to 1 */
    ret = sgx_create_enclave(ENCLAVE_FILENAME, SGX_DEBUG_FLAG, NULL, NULL, &global_eid, NULL);
    if (ret != SGX_SUCCESS) {
        ret_error_support(ret);
        return -1;
    }
    logger(INFO, "Enclave id = %d", global_eid);
#ifdef SGX_COUNTERS
    ocall_get_sgx_counters("Start enclave");
#endif
    return 0;
}

/* OCall functions */
static inline u_int64_t rdtsc(void)
{
    u_int32_t hi, lo;

    __asm__ __volatile__("rdtsc"
    : "=a"(lo), "=d"(hi));

    return (u_int64_t(hi) << 32) | u_int64_t(lo);
}

void ocall_startTimer(u_int64_t* t) {
    *t = rdtsc();
}

void ocall_stopTimer(u_int64_t* t) {
    *t = rdtsc() - *t;
}

void ocall_exit(int exit_status) {
    exit(exit_status);
}

void ocall_print_string(const char *str)
{
    /* Proxy/Bridge will check the length and null-terminate 
     * the input string to prevent buffer overflow. 
     */
    logger(ENCLAVE, str);
//    printf("%s", str);
}

uint64_t ocall_get_system_micros() {
    struct timeval tv{};
    gettimeofday(&tv, nullptr);
    return tv.tv_sec*(uint64_t)1000000+tv.tv_usec;
}

void ocall_throw(const char* message)
{
    logger(ERROR, "%s", message);
    exit(EXIT_FAILURE);
}

uint64_t ocall_get_total_ewb()
{
#ifdef SGX_COUNTERS
    return get_total_ewb();
#endif
    return 0.0;
}

void ocall_getrusage(rusage_reduced_t* usage_red, int print)
{
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    usage_red->ru_utime_sec = usage.ru_utime.tv_sec - usage_red->ru_utime_sec;
    usage_red->ru_utime_usec = usage.ru_utime.tv_usec - usage_red->ru_utime_usec;
    usage_red->ru_stime_sec = usage.ru_stime.tv_sec - usage_red->ru_stime_sec;
    usage_red->ru_stime_usec = usage.ru_stime.tv_usec - usage_red->ru_stime_usec;
    usage_red->ru_minflt = usage.ru_minflt - usage_red->ru_minflt;
    usage_red->ru_majflt = usage.ru_majflt - usage_red->ru_majflt;
    usage_red->ru_nvcsw = usage.ru_nvcsw - usage_red->ru_nvcsw;
    usage_red->ru_nivcsw = usage.ru_nivcsw - usage_red->ru_nivcsw;
    if (print)
    {
        logger(DBG, "************************** RUSAGE **************************");
        logger(DBG, "user CPU time used               : %ld.%lds", usage_red->ru_utime_sec, usage_red->ru_utime_usec);
        logger(DBG, "system CPU time used             : %ld.%lds", usage_red->ru_stime_sec, usage_red->ru_stime_usec);
        logger(DBG, "page reclaims (soft page faults) : %lu", usage_red->ru_minflt);
        logger(DBG, "page faults (hard page faults)   : %lu", usage_red->ru_majflt);
        logger(DBG, "voluntary context switches       : %lu", usage_red->ru_nvcsw);
        logger(DBG, "involuntary context switches     : %lu", usage_red->ru_nivcsw);
        logger(DBG, "************************** RUSAGE **************************");
    }
}

uint8_t* seal_relation(struct table_t * rel, uint32_t* size)
{
    sgx_status_t ret = ecall_get_sealed_data_size(global_eid, size, rel);
    if (ret != SGX_SUCCESS )
    {
        ret_error_support(ret);
        return nullptr;
    }
    else if (*size == UINT32_MAX)
    {
        logger(ERROR, "get_sealed_data_size failed");
        return nullptr;
    }

    logger(DBG, "Allocate for sealed relation %.2lf MB", B_TO_MB(*size));
    uint8_t* sealed_rel_tmp = (uint8_t *) malloc(*size);
    if (sealed_rel_tmp == nullptr)
    {
        logger(ERROR, "Out of memory");
        return nullptr;
    }
    sgx_status_t retval;
    ret = ecall_seal_data(global_eid, &retval, rel, sealed_rel_tmp, *size);
    if (ret != SGX_SUCCESS)
    {
        ret_error_support(ret);
        free(sealed_rel_tmp);
        return nullptr;
    }
    else if (retval != SGX_SUCCESS)
    {
        ret_error_support(retval);
        free(sealed_rel_tmp);
        return nullptr;
    }
    logger(DBG, "Sealing successful");
    return sealed_rel_tmp;
}

/* Application entry */
int SGX_CDECL main(int argc, char *argv[])
{
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    struct table_t tableR;
    struct table_t tableS;
    result_t* results;
    uint8_t * sealed_result;
    sgx_status_t ret;
    struct timespec tw1, tw2;
    double time;
    uint8_t  *sealed_R = nullptr, *sealed_S = nullptr;
    uint32_t sealed_data_size_r = 0, sealed_data_size_s = 0;

#ifdef PCM_COUNT
    PCM *m = PCM::getInstance();
    if (0) {
        m->program (PCM::DEFAULT_EVENTS, NULL);
    } else {
        PCM::CustomCoreEventDescription events[2];
        // MEM_INST_RETIRED.STLB_MISS_LOADS
        events[0].event_number = 0xD0;
        events[0].umask_value = 0x11;
        // MEM_INST_RETIRED.STLB_MISS_STORES
        events[1].event_number = 0xD0;
        events[1].umask_value = 0x12;
        m->program(PCM::CUSTOM_CORE_EVENTS, events);
    }

    ensurePmuNotBusy(m, true);
    logger(PCMLOG, "PCM Initialized");
#endif

    /* Cmd line parameters */
    args_t params;

    /* Set default values for cmd line params */
//    params.algorithm    = &algorithms[0]; /* NPO_st */
    params.r_size          = 2097152; /* 2*2^20 */
    params.s_size          = 2097152; /* 2*2^20 */
    params.r_seed          = 11111;
    params.s_seed          = 22222;
    params.nthreads        = 2;
    params.selectivity     = 100;
    params.skew            = 0;
    params.seal_chunk_size = 0;
    params.seal            = 0;
    params.sort_r          = 0;
    params.sort_s          = 0;
    params.r_from_path     = 0;
    params.s_from_path     = 0;
    params.three_way_join  = 0;
    strcpy(params.algorithm_name, "RHO");

    parse_args(argc, argv, &params, NULL);
    logger(DBG, "Number of threads = %d (N/A for every algorithm)", params.nthreads);


    seed_generator(params.r_seed);
    // This is just a hacky way to run the three-way join experiment. This should not be in this file forever.
    if (params.three_way_join) {
        struct table_t tableT;
        uint8_t *sealed_T = nullptr;
        uint32_t sealed_data_size_t = 0;
        logger(INFO, "Running the three-way join experiment");
        logger(INFO, "Build relation R with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * params.r_size),
               params.r_size);
        create_relation_pk(&tableR, params.r_size, params.sort_r);
        logger(DBG, "DONE");
        seed_generator(params.s_seed);
        logger(INFO, "Build relation S with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * params.r_size),
               params.r_size);
        logger(INFO, "Build relation T with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * params.s_size),
               params.s_size);
        if (params.selectivity != 100) {
            logger(INFO, "Table S selectivity = %d", params.selectivity);
            uint32_t maxid = params.selectivity != 0 ? (100 * params.r_size / params.selectivity) : 0;
            create_relation_fk_sel(&tableS, params.r_size, maxid, params.sort_s);
            logger(DBG, "DONE");
            create_relation_fk_sel(&tableT, params.s_size, maxid, params.sort_s);
            logger(DBG, "DONE");
        }
        else {
            create_relation_fk(&tableS, params.r_size, params.r_size, params.sort_s);
            logger(DBG, "DONE");
            create_relation_fk(&tableT, params.s_size, params.r_size, params.sort_s);
            logger(DBG, "DONE");
        }

        initialize_enclave();
        sealed_R = seal_relation(&tableR, &sealed_data_size_r);
        if (sealed_R == nullptr)
        {
            sgx_destroy_enclave(global_eid);
            delete_relation(&tableR);
            delete_relation(&tableS);
            delete_relation(&tableT);
            exit(EXIT_FAILURE);
        }
        sealed_S = seal_relation(&tableS, &sealed_data_size_s);
        if (sealed_S == nullptr)
        {
            sgx_destroy_enclave(global_eid);
            delete_relation(&tableR);
            delete_relation(&tableS);
            delete_relation(&tableT);
            exit(EXIT_FAILURE);
        }
        sealed_T = seal_relation(&tableT, &sealed_data_size_t);
        if (sealed_T == nullptr)
        {
            sgx_destroy_enclave(global_eid);
            delete_relation(&tableR);
            delete_relation(&tableS);
            delete_relation(&tableT);
            exit(EXIT_FAILURE);
        }
        logger(INFO, "Running algorithm %s", params.algorithm_name);
        uint32_t  sealed_data_size = 0;

        ret = ecall_three_way_join_sealed_tables(global_eid,
                                                 &sealed_data_size,
                                                 sealed_R,
                                                 sealed_data_size_r,
                                                 sealed_S,
                                                 sealed_data_size_s,
                                                 sealed_T,
                                                 sealed_data_size_t,
                                                 params.algorithm_name,
                                                 params.nthreads,
                                                 params.seal_chunk_size);
        if (ret != SGX_SUCCESS)
        {
            ret_error_support(ret);
        }
        logger(DBG, "Sealed data size = %.2lf MB", B_TO_MB(sealed_data_size));
        sealed_result = (uint8_t*)malloc(sealed_data_size);
        if (sealed_result == nullptr)
        {
            logger(ERROR, "Out of memory");
            exit(EXIT_FAILURE);
        }
        sgx_status_t retval;
        ret = ecall_get_sealed_data(global_eid, &retval, sealed_result, sealed_data_size);
        if (ret != SGX_SUCCESS)
        {
            ret_error_support(ret);
        }
        sgx_destroy_enclave(global_eid);
        delete_relation(&tableR);
        delete_relation(&tableS);
        delete_relation(&tableT);
        free(sealed_R);
        free(sealed_S);
        free(sealed_T);
        free(sealed_result);
        // End three-way experiment
        exit(EXIT_SUCCESS);
    }

    if (params.r_from_path)
    {
        logger(INFO, "Build relation R from file %s", params.r_path);
        create_relation_from_file(&tableR, params.r_path, params.sort_r);
        params.r_size = tableR.num_tuples;
    }
    else
    {
        logger(INFO, "Build relation R with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * params.r_size),
               params.r_size);
        create_relation_pk(&tableR, params.r_size, params.sort_r);
//        if (params.selectivity != 100)
//        {
//            create_relation_pk_selectivity(&tableR, params.r_size, params.sort_r, params.selectivity);
//        }
//        else
//        {
//            create_relation_pk(&tableR, params.r_size, params.sort_r);
//        }
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
        logger(INFO, "Build relation S with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * params.s_size),
               params.s_size);
        if (params.skew > 0)
        {
            logger(INFO, "Skew relation: %.2lf", params.skew);
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

    initialize_enclave();

    if (params.seal)
    {
        sealed_R = seal_relation(&tableR, &sealed_data_size_r);
        if (sealed_R == nullptr)
        {
            sgx_destroy_enclave(global_eid);
            delete_relation(&tableR);
            delete_relation(&tableS);
            exit(EXIT_FAILURE);
        }
        sealed_S = seal_relation(&tableS, &sealed_data_size_s);
        if (sealed_S == nullptr)
        {
            sgx_destroy_enclave(global_eid);
            delete_relation(&tableR);
            delete_relation(&tableS);
            exit(EXIT_FAILURE);
        }
    }

    logger(INFO, "Running algorithm %s", params.algorithm_name);

    clock_gettime(CLOCK_MONOTONIC, &tw1); // POSIX; use timespec_get in C11
    if (params.seal)
    {
        uint64_t total_cycles, retrieve_data_timer;
        uint32_t sealed_data_size = 0;
        ocall_startTimer(&total_cycles);
        ret = ecall_join_sealed_tables(global_eid,
                                       &sealed_data_size,
                                       sealed_R,
                                       sealed_data_size_r,
                                       sealed_S,
                                       sealed_data_size_s,
                                       params.algorithm_name,
                                       params.nthreads,
                                       params.seal_chunk_size);
        if (ret != SGX_SUCCESS)
        {
            ret_error_support(ret);
        }
        logger(DBG, "Sealed data size = %.2lf MB", B_TO_MB(sealed_data_size));
        sealed_result = (uint8_t*)malloc(sealed_data_size);
        if (sealed_result == nullptr)
        {
            logger(ERROR, "Out of memory");
            exit(EXIT_FAILURE);
        }
        sgx_status_t retval;
        ocall_startTimer(&retrieve_data_timer);
        ret = ecall_get_sealed_data(global_eid, &retval, sealed_result, sealed_data_size);
        ocall_stopTimer(&retrieve_data_timer);
        if (ret != SGX_SUCCESS)
        {
            ret_error_support(ret);
        }
        ocall_stopTimer(&total_cycles);
        logger(INFO, "retrieve_data_timer = %lu", retrieve_data_timer);
        logger(INFO, "total_cycles        = %lu", total_cycles);
    }
    else {
#ifdef SGX_COUNTERS
        uint64_t ewb = ocall_get_ewb();
#endif
//#ifdef PCM_COUNT
//        ocall_set_system_counter_state("Start ecall join");
//#endif
        ret = ecall_join(global_eid,
                         &results,
                         &tableR,
                         &tableS,
                         params.algorithm_name,
                         (int) params.nthreads);
//#ifdef PCM_COUNT
//        ocall_get_system_custom_counter_state("End ecall join");
//#endif
#ifdef SGX_COUNTERS
        ewb = ocall_get_ewb();
        logger(DBG, "ewb after join = %lu", ewb);
#endif
    }
    clock_gettime(CLOCK_MONOTONIC, &tw2);
    time = 1000.0*(double)tw2.tv_sec + 1e-6*(double)tw2.tv_nsec
                        - (1000.0*(double)tw1.tv_sec + 1e-6*(double)tw1.tv_nsec);
    logger(INFO, "Total join runtime: %.2fs", time/1000);
    logger(INFO, "throughput = %.2lf [M rec / s]",
           (double)(params.r_size + params.s_size)/(1000*time));
    if (ret != SGX_SUCCESS) {
        ret_error_support(ret);
    }

#ifdef SGX_COUNTERS
    ocall_get_sgx_counters("Destroy enclave");
#endif
    sgx_destroy_enclave(global_eid);
    delete_relation(&tableR);
    delete_relation(&tableS);
    if (params.seal)
    {
        free(sealed_R);
        free(sealed_S);
        free(sealed_result);
    }
    return 0;
}
