#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>
#include "Logger.h"
#include "pcm_commons.h"

#ifndef NATIVE_COMPILATION
#include "Enclave_u.h"
#endif

#ifdef SGX_COUNTERS
#include "sgx_counters.h"
#endif

static bool init = false;

#ifdef PCM_COUNT
static SystemCounterState state, start, end;

void ensurePmuNotBusy(PCM *m, bool forcedProgramming) {
    PCM::ErrorCode status;

    do {
        status = m->program();
        switch (status) {
            case PCM::PMUBusy: {
                if (!forcedProgramming) {
                    std::cout << "Warning: PMU appears to be busy, do you want to reset it? (y/n)\n";
                    char answer;
                    std::cin >>answer;
                    if (answer == 'y' || answer == 'Y')
                        m->resetPMU();
                    else
                        exit(0);
                } else {
                    m->resetPMU();
                }
                break;
            }
            case PCM::Success:
                break;
            case PCM::MSRAccessDenied:
            case PCM::UnknownError:
            default:
                exit(1);
        }
    } while (status != PCM::Success);
}
#endif

void ocall_set_system_counter_state(const char *message) {
#ifdef PCM_COUNT
    if (!init)
    {
        start = getSystemCounterState();
        init = true;
    }
    state = getSystemCounterState();
    logger(PCMLOG,"Set system counter state: %s", message);
#else
    logger(WARN, "PCM_COUNT flag not enabled");
    return;
#endif
}

#ifdef PCM_COUNT
static void print_state(const char *message, SystemCounterState old, SystemCounterState tmp)
{
    logger(PCMLOG, "=====================================================");
    logger(PCMLOG, "Get system counter state: %s", message);
    logger(PCMLOG, "Cycles [B]              : %.2lf", (double) getCycles(old, tmp)/1000000000);
    logger(PCMLOG, "Instructions per Clock  : %.4lf", getIPC(old, tmp));
    logger(PCMLOG, "Instructions retired [M]: %.2lf", (double) getInstructionsRetired(old, tmp)/1000000);
    logger(PCMLOG, "L3 cache misses [k]     : %lu", getL3CacheMisses(old, tmp)/1000);
    logger(PCMLOG, "L3 cache hit ratio      : %.2lf", getL3CacheHitRatio(old, tmp));
    logger(PCMLOG, "L2 cache misses [k]     : %lu", getL2CacheMisses(old, tmp)/1000);
    logger(PCMLOG, "L2 cache hit ratio      : %.2lf", getL2CacheHitRatio(old, tmp));
    logger(PCMLOG, "MBytes read from DRAM   : %lu", getBytesReadFromMC(old, tmp)/1024/1024);
    logger(PCMLOG, "MBytes written from DRAM: %lu", getBytesWrittenToMC(old, tmp)/1024/1024);
    logger(PCMLOG, "LLC read miss latency   : %.4lf", getLLCReadMissLatency(old, tmp));
#ifdef SGX_COUNTERS
    logger(PCMLOG, "EWBcnt                  : %lu", get_ewb());
#endif
    logger(PCMLOG, "end==================================================");
}
#endif

void ocall_get_system_counter_state(const char *message, int start_to_end) {
#ifdef PCM_COUNT
    SystemCounterState tmp = getSystemCounterState();

    print_state(message, state, tmp);

    if (start_to_end)
    {
        print_state("Overall results", start, tmp);
    }

    state = getSystemCounterState();
#else
    logger(WARN, "PCM_COUNT flag not enabled");
    return;
#endif
}

void ocall_get_system_custom_counter_state(const char *message) {
#ifdef PCM_COUNT
    SystemCounterState tmp = getSystemCounterState();

    for (int i = 0; i < 2; i++) {
        uint64_t value = getNumberOfCustomEvents(i, state, tmp);
        logger(PCMLOG, "Custom event %d: %lld", i+1, value);
    }

    print_state(message, state, tmp);
    state = getSystemCounterState();
#else
    logger(WARN, "PCM_COUNT flag not enabled");
    return;
#endif
}
