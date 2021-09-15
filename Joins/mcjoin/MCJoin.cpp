/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/

#include "MCJoin.h"

using namespace std;

void showUsage()
{
	logger(INFO, "Usage: mcjoin -c<configfile>");
}

bool parseConfigFile(const string &configFile, COptions *options, SMetrics *metrics)
{
	bool result(true);

	result = options->load(configFile);

	// If we have a memory constraint set, then we need to update the configuration options to meet this constraint
	if(options->getMemoryConstraintMB() != 0)
	{
		CMemoryManagement memManagement;
		if(memManagement.optimiseForMemoryConstraint(options))
		{
			// Based on the results, we need to change our options settings
			options->setPackedPartitionMemoryCardinality(memManagement.getRIdealTuples());
			options->setFlipFlopCardinality(memManagement.getSIdealTuples());
			metrics->r_bias = memManagement.getRBias();
		}
		else
		{
			// It didn't work - this is usually because the histogram is simply too large to fit within the given memory constraint
			logger(ERROR, "Unable to allocate memory (histogram too large for constraint?)");
			result = false;
		}
	}


	return result;
}

bool parseArguments(std::vector<std::string> &arguments, COptions *options, SMetrics *metrics)
{
	bool returnValue(true);
	string buffer;	

	if(arguments.size() == 1)
	{
		showUsage();
	}
	else
	{
		for(std::string& s : arguments)
		{
			if (s[0] == '-')
			{
				buffer.clear();
				switch (s[1])
				{
				case 'c':					
					buffer += s.substr(2);	
					returnValue = parseConfigFile(buffer, options, metrics);
					break;
				case 'z':
					options->setLoadBinaryRelations(true);
					break;
				default:
					logger(ERROR, "unknown option?");
					returnValue = false;
				}
			}
		}
	}

	return returnValue;
}

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

    mcjoin = new CMCJoin(metrics, bitRadixLength, relR, relS, maxBitsPerFlipFlopPass, maxThreads);
    mcjoin->doJoin(flipFlopCardinality, packedPartitionMemoryCardinality, outputBufferCardinality);
    logger(INFO, "Completed in %.2lf seconds.", metrics->time_total);

}

//void doMCJoin_old(COptions *options, SMetrics *metrics)
//{
//	Timer tmrJoin;
////	stringstream condensedOptions;
//	CMCJoin* mcjoin;
//
////	options->getCondensedOptions(&condensedOptions);
////	cerr << "Executing test: " << condensedOptions.str() << endl; // This is just so I can see some output during batched tests. It's not important otherwise.
//
//	tmrJoin.update();
//	mcjoin = new CMCJoin(metrics, options->getBitRadixLength(), options->getRelationR(), options->getRelationS(), options->getMaxBitsPerFlipFlopPass(), options->getThreads());
//	mcjoin->doJoin(options->getFlipFlopCardinality(), options->getPackedPartitionMemoryCardinality(), options->getOutputBufferCardinality());
//	tmrJoin.update();
//
//	metrics->time_total = tmrJoin.getElapsedTime();
//
//	// Send metrics to output stream
////	cout << condensedOptions.str() << "," << *metrics << endl;
//
//	// Report something to console. Remove this if it's annoying.
//	logger(INFO, "Completed in %.2lf seconds.", metrics->time_total);
//}

//int main(int argc, char* argv[])
//{
//	int returnValue(-1);
//	COptions options;
//	SMetrics metrics;
//
//	std::vector<std::string> arguments(argv, argv + argc);
//	if(arguments.size() == 2)
//	{
//		if(parseArguments(arguments, &options, &metrics))
//		{
//			doMCJoin(&options, &metrics);
//			returnValue = 0;
//		}
//	}
//	else
//	{
//		logger(ERROR, "No config file specified! Aborting.");
//	}
//
//	return returnValue;
//}
