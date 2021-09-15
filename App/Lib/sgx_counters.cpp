#ifndef NATIVE_COMPILATION
#include "Enclave_u.h"
#endif

#include "sgx_counters.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <clocale>
#include "Logger.h"

u_int64_t ewb_cnt = 0;
u_int64_t ewb_last = 0;

u_int64_t read_sgx_enclaves_values()
{
    FILE *f;
    char buf[255];
    u_int64_t res;


    f = fopen("/proc/sgx_enclaves", "r");

    while( fgets(buf, 255, f) != NULL ) {}
//    logger(DBG, "Last line of /proc/sgx_enclaves: %s", buf);
    fclose(f);
    int i = 0;
    char * token, * str;
    str = strdup(buf);
    while ((token = strsep(&str, " \n"))) {
        if (i == 4)
        {
            ewb_cnt = strtol(token, NULL, 10);
        }
        i++;
    }
    // calculate how much EPC paging since last check
    res = ewb_cnt - ewb_last;
//    logger(ENCLAVE, "ewb_cnt: %lu, ewb_last: %lu, res: %lu", ewb_cnt, ewb_last, res);
    // store the current EPC paging
    ewb_last = ewb_cnt;
    return res;
}

void ocall_get_sgx_counters(const char *message)
{
#ifdef SGX_COUNTERS
    read_sgx_enclaves_values();
    setlocale(LC_ALL,"");
    logger(ENCLAVE, "%s | EWB_COUNTER = %'lu", message, ewb_cnt);
#else
    logger(WARN, "SGX_COUNTERS flag not enabled");
    return;
#endif
}

void ocall_set_sgx_counters(const char *message)
{
#ifdef SGX_COUNTERS
    read_sgx_enclaves_values();
    logger(ENCLAVE, "Set SGX_COUNTERS | %s", message);
#else
    logger(WARN, "SGX_COUNTERS flag not enabled");
    return;
#endif
}

uint64_t ocall_get_ewb()
{

#ifdef SGX_COUNTERS
    uint64_t ewb =  get_ewb();
//    setlocale(LC_ALL,"");
    logger(ENCLAVE, "EPC Miss = %lu", ewb);
    return ewb;
#else
    logger(WARN, "SGX_COUNTERS flag not enabled");
    return 0L;
#endif
}

u_int64_t get_ewb()
{
    u_int64_t res = read_sgx_enclaves_values();

    return res;
}

u_int64_t get_total_ewb()
{
    read_sgx_enclaves_values();
    return ewb_last;
}