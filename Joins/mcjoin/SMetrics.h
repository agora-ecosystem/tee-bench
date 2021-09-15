/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#pragma once

#include "DataTypes.h"
#include <iostream>
#ifdef NATIVE_COMPILATION
#include "Logger.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

using namespace std;

struct SMetrics
{
	SMetrics() : r_cardinality(0), s_cardinality(0), output_cardinality(0), time_total(0), time_flipFlop_r(0), time_flipFlop_s(0), time_scatter(0), time_join(0), time_restitch(0), r_bias(0), r_chunksProcessed(0), s_chunksProcessed(0)
	{

	}

	void logMetrics()
	{
		double throughput = (double) (r_cardinality + s_cardinality)  / (double) time_total_us;
		logger(INFO, "intput cardinalities   : %lu", r_cardinality + s_cardinality);
		logger(INFO, "output_cardinality     : %lu", output_cardinality);
		logger(INFO, "time_total             : %.2lf", time_total);
		logger(INFO, "time_flipFlop_r        : %.2lf", time_flipFlop_r);
		logger(INFO, "time_flipFlop_s        : %.2lf", time_flipFlop_s);
		logger(INFO, "time_scatter           : %.2lf", time_scatter);
		logger(INFO, "time_join              : %.2lf", time_join);
		logger(INFO, "time_restitch          : %.2lf", time_restitch);
		logger(INFO, "r_bias                 : %.2lf", r_bias);
		logger(INFO, "r_chunksProcessed      : %d", r_chunksProcessed);
		logger(INFO, "s_chunksProcessed      : %d", s_chunksProcessed);
		logger(INFO, "Throughput (M rec/sec) : %.2lf", throughput);
#ifdef SGX_COUNTERS

#endif
	}

//	friend ostream& operator<<(ostream& os, const SMetrics& m);

	uint64_t r_cardinality;
	uint64_t s_cardinality;
	uint64_t output_cardinality;
	double time_total;
	uint64_t time_total_us;
	double time_flipFlop_r;
	double time_flipFlop_s;
	double time_scatter;
	double time_join;
	double time_restitch;	// included in time_join;
	double r_bias;
	uint32 r_chunksProcessed;
	uint32 s_chunksProcessed;
};