#include "native_ocalls.h"

#include <sys/time.h>
#include <sys/resource.h>
#include <stdlib.h>
#include "cpu_mapping.h"
#include "Logger.h"

static inline u_int64_t rdtsc(void)
{
    u_int32_t hi, lo;

    __asm__ __volatile__("rdtsc"
    : "=a"(lo), "=d"(hi));

    return (u_int64_t(hi) << 32) | u_int64_t(lo);
}

void ocall_startTimer(uint64_t* t)
{
    *t = rdtsc();
}

void ocall_stopTimer(uint64_t* t)
{
    *t = rdtsc() - *t;
}

void ocall_getrusage()
{
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    logger(DBG, "************************** RUSAGE **************************");
    logger(DBG, "user CPU time used               : %ld.%lds", usage.ru_utime.tv_sec, usage.ru_utime.tv_usec);
    logger(DBG, "system CPU time used             : %ld.%lds", usage.ru_stime.tv_sec, usage.ru_stime.tv_usec);
    logger(DBG, "page reclaims (soft page faults) : %lu", usage.ru_minflt);
    logger(DBG, "page faults (hard page faults)   : %lu", usage.ru_minflt);
    logger(DBG, "voluntary context switches       : %lu", usage.ru_nvcsw);
    logger(DBG, "involuntary context switches     : %lu", usage.ru_nivcsw);
    logger(DBG, "************************** RUSAGE **************************");
}

void ocall_get_system_micros(uint64_t* t)
{
    struct timeval tv{};
    gettimeofday(&tv, nullptr);
    *t = (uint64_t) (1000000 * tv.tv_sec + tv.tv_usec);
}

void ocall_exit(int status)
{
    exit(status);
}

void ocall_get_num_active_threads_in_numa(int *res, int numaregionid)
{
    *res = get_num_active_threads_in_numa(numaregionid);
}

void ocall_get_thread_index_in_numa(int * res, int logicaltid)
{
    *res = get_thread_index_in_numa(logicaltid);
}

void ocall_get_cpu_id(int * res, int thread_id)
{
    *res = get_cpu_id(thread_id);
}

void ocall_get_num_numa_regions(int * res)
{
    *res = get_num_numa_regions();
}

void ocall_numa_thread_mark_active(int phytid)
{
    numa_thread_mark_active(phytid);
}

void ocall_throw(const char* message)
{
    logger(ERROR, "%s", message);
    exit(EXIT_FAILURE);
}