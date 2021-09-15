#ifndef MCJOIN_H
#define MCJOIN_H

#include "SMetrics.h"
#include "COptions.h"
#include <vector>
#ifdef NATIVE_COMPILATION
#include <iostream>
#else
#include <libcxx/ostream>
#endif
#include "COptions.h"
#include "CMCJoin.h"
#include "Timer.h"
#include "SMetrics.h"
#include "CMemoryManagement.h"

using namespace std;

class MCJoin {
public:
    MCJoin()
    = default;

    void doMCJoin(relation_t* relR,
                  relation_t* relS,
                  SMetrics *metrics,
                  uint32_t bitRadixLength,
                  uint32_t maxBitsPerFlipFlopPass,
                  uint32_t maxThreads,
                  uint32_t flipFlopCardinality,
                  uint32_t packedPartitionMemoryCardinality,
                  uint32_t outputBufferCardinality)
    {
        CMCJoin* mcjoin;
        Timer tmrJoin;
        uint64_t ewb;

        metrics->r_cardinality = relR->num_tuples;
        metrics->s_cardinality = relS->num_tuples;

#ifdef SGX_COUNTERS
        ocall_set_sgx_counters("MCJoin set counter");
#endif
        tmrJoin.update();
        mcjoin = new CMCJoin(metrics, bitRadixLength, relR, relS, maxBitsPerFlipFlopPass, maxThreads);
        mcjoin->doJoin(flipFlopCardinality, packedPartitionMemoryCardinality, outputBufferCardinality);
        tmrJoin.update();
#ifdef SGX_COUNTERS
        ocall_get_ewb(&ewb);
#endif
        metrics->time_total = tmrJoin.getElapsedTime();
        metrics->time_total_us = tmrJoin.getElapsedTimeMicros();

        metrics->logMetrics();
    }

private:
    void showUsage();
    bool parseConfigFile(const string &configFile, COptions *options, SMetrics *metrics);
    bool parseArguments(std::vector<std::string> &arguments, COptions *options, SMetrics *metrics);

//    void doMCJoin_old(COptions *options, SMetrics *metrics);

};
#endif //MCJOIN_H
