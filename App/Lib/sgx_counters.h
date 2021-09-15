#ifndef SGXJOINEVALUATION_SGX_COUNTERS_H
#define SGXJOINEVALUATION_SGX_COUNTERS_H

#include <stdlib.h>
#include <stdint.h>

#ifdef NATIVE_COMPILATION
    void ocall_get_sgx_counters(const char *message);
    void ocall_set_sgx_counters(const char *message);
#endif

u_int64_t get_ewb();
u_int64_t get_total_ewb();
#endif //SGXJOINEVALUATION_SGX_COUNTERS_H
