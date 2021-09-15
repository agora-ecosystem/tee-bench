/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#pragma once

#include "CFlipFlopBuffer.h"
#include "CPackedPartitionMemory.h"
#include "Timer.h"
#include <vector>
#include "SMetrics.h"

using namespace std;

struct NLJThreadParams
{
    relation_t* setQ;
	uint32 Q_items;
	uint32 bitMask;
	uint32 R_chunkIndex;
	uint32 startTupleInclusive;
	uint32 endTupleExclusive;
};

class CMCJoin
{
public:
	CMCJoin(SMetrics* const metrics, const uint32 bitRadixLength, const relation_t* relationR, const relation_t* relationS, const uint32 maxBitsPerFlipFlopPass = 8, const uint32 maxThreads = 1) : _metrics(metrics), _relationR(relationR), _relationS(relationS), _bitRadixLength(bitRadixLength), _storageR(NULL), _maxBitsPerFlipFlopPass(maxBitsPerFlipFlopPass), _maxThreads(1)
	{	
		// Forcing _maxThreads = 1 in this version of MCJoin.
	}

	~CMCJoin()
	{
	}

	void doJoin(uint32 sizeInputBuffer, uint32 sizePackedStorageR, uint32 sizeOutputBuffer)
	{
		uint32 R_relationDesiredChunkSize(0);		// Chunk sizes are in number of tuples
		uint32 R_relationActualChunkSize(0);		// Chunk sizes are in number of tuples
		uint32 R_relationCardinality(0);	// Number of tuples
		
		// I would calculate my chunk sizes here		
		R_relationDesiredChunkSize = sizePackedStorageR;

		// I would allocate my output buffer stuff here		
		_outputMaxSize = sizeOutputBuffer;

		// Get ready to process data sets
		R_relationCardinality = _relationR->num_tuples;
		
		uint32 R_chunkIndex(0);
		
		// Now we mash our way through the chunks of R and thrash the chunks of S.
		_storageR = new CPackedPartitionMemory(_bitRadixLength);

		while(R_chunkIndex < R_relationCardinality)
		{
			// Bite off as big a chunk as possible...
			R_relationActualChunkSize = R_relationDesiredChunkSize;

			// ...but make sure we don't bite off more than we can chew!
			if ( (R_chunkIndex + R_relationActualChunkSize) > R_relationCardinality )
			{
				R_relationActualChunkSize = R_relationCardinality - R_chunkIndex;
			}

			// Partition up this chunk of R
			_storageR->resetBuffers(R_relationActualChunkSize);	
			setupStorageR(_storageR, _relationR, R_chunkIndex, (R_chunkIndex + R_relationActualChunkSize), sizeInputBuffer);

			// ->>>> WE THRASH RELATION S IN HERE <<<<-
			doVanillaProcessingOfS(sizeInputBuffer, R_chunkIndex);
			// ->>>> NO MORE THRASHING, IT MAKES ME SICK! <<<<-

			//_storageR->showDebug("R");			
			R_chunkIndex += R_relationActualChunkSize;		

			// Update number of R chunks processed for metrics
			_metrics->r_chunksProcessed += 1;
		}

		delete _storageR;				
	}

	__forceinline CPackedPartitionMemory* getStorageR()
	{
		return _storageR;
	}

	__forceinline void setupStorageR(CPackedPartitionMemory* storage, const relation_t* relation, const uint32 from, const uint32 to, uint32 inputBufferSize)
	{				
		Timer timer_flipFlopR;
		Timer timer_scatter;

		relation_t * setQ(nullptr);

		storage->getHistogram()->reset();	
		storage->getHistogram()->buildHistogramKey(relation, from, to);
		//storage->getHistogram()->showDebug();
		storage->getHistogram()->buildPrefixSum();				

		uint32 flipBaseIndex = from;
		uint32 current = flipBaseIndex;

		while(current < to)
		{
			if((current + inputBufferSize) > to)
			{
				inputBufferSize = to - current;
			}		
			
			timer_flipFlopR.update();
			CFlipFlopBuffer* ffb = new CFlipFlopBuffer(relation, _bitRadixLength, _maxBitsPerFlipFlopPass);
			setQ = ffb->doPartitioning(current, current + inputBufferSize, flipBaseIndex, false); 
			timer_flipFlopR.update();
			_metrics->time_flipFlop_r += timer_flipFlopR.getElapsedTime();
			//setQ = ffb->doPartitioning(current, current + inputBufferSize, flipBaseIndex, true); // Use this to view true contents (not offsets) for debug.
			//setQ->showDebug("Q");
			
			timer_scatter.update();
			storage->scatterKey(setQ, 0, setQ->num_tuples);
			timer_scatter.update();
			_metrics->time_scatter += timer_scatter.getElapsedTime();

			current += inputBufferSize;

//			delete setQ;
			free(setQ->tuples);
			free(setQ);
//			setQ = NULL;

			delete ffb;
		}

		storage->getHistogram()->rollbackPrefixSum();
		//storage->showDebug();
	}

private:
	void doVanillaProcessingOfS(uint32 sizeInputBuffer, uint32 R_chunkIndex)
	{
		Timer timer_join;
		Timer timer_flipFlopS;
		
		uint32 S_relationCardinality(0);
		
		uint32 S_relationDesiredChunkSize(0);
		uint32 S_relationActualChunkSize(0);

		uint32 S_chunkIndex(0);

		S_relationCardinality = _relationS->num_tuples;
		S_relationDesiredChunkSize = sizeInputBuffer;

        relation_t* setQ(NULL);

		while(S_chunkIndex < S_relationCardinality)
		{
			// Nom some of relation S.
			S_relationActualChunkSize = S_relationDesiredChunkSize;

			// Don't nom too much!
			if ( (S_chunkIndex + S_relationActualChunkSize) > S_relationCardinality )
			{
				S_relationActualChunkSize = S_relationCardinality - S_chunkIndex;
			}

			timer_flipFlopS.update();
			CFlipFlopBuffer* ffb = new CFlipFlopBuffer(_relationS, _bitRadixLength, _maxBitsPerFlipFlopPass);				
			setQ = ffb->doPartitioning(S_chunkIndex, S_chunkIndex + S_relationActualChunkSize, 0, true);				
			delete ffb;
			timer_flipFlopS.update();
			_metrics->time_flipFlop_s += timer_flipFlopS.getElapsedTime();

			timer_join.update();
			InnerPartNLJoinMT(setQ, R_chunkIndex);				
			timer_join.update();
			_metrics->time_join += timer_join.getElapsedTime();

//			delete setQ;
//			setQ = NULL;
            free(setQ->tuples);
            free(setQ);

			S_chunkIndex += S_relationActualChunkSize;

			// Update number of S chunks processed for metrics
			_metrics->s_chunksProcessed += 1;
		}
	}

	void InnerPartNLJoinMT(relation_t* setQ, uint32 R_chunkIndex)
	{
		uint32 Q_items = setQ->num_tuples;
		uint32 bitMask = (1 << _bitRadixLength) - 1;

		uint32 Q_tuplesPerThread(Q_items / _maxThreads);
		uint32 threadBaseTuple(0);

		NLJThreadParams nljtp;
		nljtp.setQ = setQ;
		nljtp.Q_items = Q_items;
		nljtp.bitMask = bitMask;
		nljtp.R_chunkIndex = R_chunkIndex;

		nljtp.startTupleInclusive = threadBaseTuple;
		nljtp.endTupleExclusive = (threadBaseTuple + Q_tuplesPerThread);

		InnerPartNLJoinMT_Thread(nljtp);
	}

	void InnerPartNLJoinMT_Thread(NLJThreadParams nljtp)
	{
		Timer timer_restitch;

		uint32 R_items(0);		
		uint32 Q_partition(0);
		uint32 R_index(0);
		uint32 R_value(0);
		uint32 R_offset(0);

		uint32 S_value(0);

		uint32 localMatches(0);
#ifdef MATERIALIZE
		tuple_t* localOutputSet = (tuple_t*) malloc(_outputMaxSize * sizeof(tuple_t));// = new SBUN[_outputMaxSize];
		malloc_check(localOutputSet);
		uint32 localOutputCounter(0);
#endif


		for(uint32 Q_index = nljtp.startTupleInclusive; Q_index < nljtp.endTupleExclusive; ++Q_index)
		{
			S_value = nljtp.setQ->tuples[Q_index].key;

			// Work out which partition this chump belongs to
			Q_partition = S_value & nljtp.bitMask;

			// Probe for matches in R
			R_items = _storageR->getHistogram()->getItemCountPostSum(Q_partition);
			if(R_items != 0)
			{
				for(R_index = 0; R_index < R_items; ++R_index)
				{
					// Grab R value (this will be R.key!)
					R_value = _storageR->readDataValue(Q_partition, R_index);

					if(R_value == S_value)
					{
#ifdef MATERIALIZE
						localOutputSet[localOutputCounter].key = _storageR->readOffsetValue(Q_partition, R_index);
						localOutputSet[localOutputCounter].payload = nljtp.setQ->tuples[Q_index].payload;
#endif

						++localMatches;
#ifdef MATERIALIZE
                        ++localOutputCounter;
						if(localOutputCounter == _outputMaxSize)
						{
							timer_restitch.update();
							restitchTuples(nljtp.R_chunkIndex, localOutputSet, localOutputCounter);
							timer_restitch.update();
							_metrics->time_restitch += timer_restitch.getElapsedTime();
							localOutputCounter = 0;
						}
#endif
					}
				}
			}
		}
#ifdef MATERIALIZE
		timer_restitch.update();
		restitchTuples(nljtp.R_chunkIndex, localOutputSet, localOutputCounter);
		timer_restitch.update();
#endif

		_metrics->time_restitch += timer_restitch.getElapsedTime();		
		_metrics->output_cardinality += localMatches;
#ifdef MATERIALIZE
//		delete localOutputSet;
        free(localOutputSet);
#endif
	}

	void restitchTuples(uint32 R_chunkIndex, tuple_t* pOutputSet, uint32 outputCounter)
	{		
		const bool debug(false);

		register uint64_t offset;

		if(debug)
		{
			printf("Flushing output: \n");
		}		

		for(uint64_t index = 0; index < outputCounter; ++index)
		{
			offset = pOutputSet->key;
			pOutputSet->key = _relationR->tuples[R_chunkIndex + offset].payload; // Yes, R.payload is being stored in O.key. I want my final output to be R.payload, S.payload (stored in O.key, O.payload respectively)

			if(debug)
			{				
				printf("R: %d, S: %d \n", pOutputSet->key, pOutputSet->payload);
			}

			++pOutputSet;
		}
	}

	uint64_t memNeededInBytes(const uint32 count, const uint32 bitsToStore)
	{
		uint64_t result(0);

		uint64_t totalBits = count;
		totalBits *= bitsToStore;

		result = totalBits >> 6;

		if(((totalBits) & 63) != 0)
		{
			++result;	// Round up to next 64-bit boundary
		}

		result <<= 3;

		return result;
	}

	uint32 getBitsNeededForOffsetRepresentation(uint32 itemCount)
	{
		double bits = log((double)itemCount) / log(2.0);

		return (uint32)ceil(bits);
	}

private:
	const relation_t* _relationR;
	const relation_t* _relationS;

	CPackedPartitionMemory* _storageR;

	const uint32 _bitRadixLength;
	const uint32 _maxBitsPerFlipFlopPass;
	
	uint32 _outputMaxSize;
	uint32 _maxThreads;

	SMetrics* const _metrics;
};