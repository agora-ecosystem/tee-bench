/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#pragma once
#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "Logger.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

class CFlipFlop
{
public:
	CFlipFlop(): _input(NULL), _output(NULL), _cardinality(0), _histogram(NULL), _numberOfPartitions(0)
	{
	}

	~CFlipFlop()
	{
	}

	__forceinline void setCardinality(uint32 cardinality)
	{
		_cardinality = cardinality;
	}

	__forceinline void setInput(relation_t* input)
	{
		_input = input;
	}

	__forceinline void setOutput(relation_t* output)
	{
		_output = output;
	}

	__forceinline void setRelations(const relation_t* input, relation_t* output, uint32 cardinality)
	{
		_input = input;
		_output = output;

		setCardinality(cardinality);
	}

	void go(uint32 D, uint32 R)
	{
		buildHistogram(D, R);
		buildPrefixSum();
		scatter(D, R);
		dropHistogram();
	}

	void go(uint32 D, uint32 R, uint32 start, uint32 end, uint32 baseIndex, bool retainPayload)
	{	
		buildHistogram(D, R, start, end);
		buildPrefixSum();
		scatter(D, R, start, end, baseIndex, retainPayload);
		dropHistogram();
	}

private:

	__forceinline void dropHistogram()
	{
		if(_histogram != NULL)
		{
			free(_histogram);
			_histogram = NULL;
		}
	}

	__forceinline void buildHistogram(uint32 D, uint32 R)
	{
		_numberOfPartitions = 1 << D;

		_histogram = (uint64_t*)calloc(_numberOfPartitions, sizeof(uint64_t));

		register uint32 M = ( ( 1 << D ) - 1) << R;
		register tuple_t * p = _input->tuples;
		register uint32 idx(0);

		for(register uint32 index = 0; index < _cardinality; ++index)
		{
			idx = (p->key) & M;
			idx >>= R;

			++(_histogram[idx]);

			++p;
		}
	}

	__forceinline void buildHistogram(uint32 D, uint32 R, uint32 start, uint32 end)
	{	
		_numberOfPartitions = 1 << D;	
		_histogram = (uint64_t*)calloc(_numberOfPartitions, sizeof(uint64_t));

		register uint32 M = ( ( 1 << D ) - 1) << R;
		register tuple_t* p = &_input->tuples[start];

		register uint32 idx(0);
		register uint32 itemCount(end - start);

		for(register uint32 index = 0; index < itemCount; ++index)
		{
			idx = (p->key) & M;
			idx >>= R;

			++(_histogram[idx]);

			++p;
		}
	}

	__forceinline void buildPrefixSum()
	{
		uint64_t* h = _histogram;
		uint64_t thisValue(0);
		register uint64_t sum(0);
		
		for(uint32 index = 0; index < _numberOfPartitions; ++index )
		{						
			thisValue = *h;

			*h = sum;

			sum += thisValue;
			++h;	
		}

		uint64_t histValue(0);
		tuple_t* p = _output->tuples;
		for(uint32 index = 0; index < _numberOfPartitions; ++index )
		{		
			histValue = _histogram[index];
			_histogram[index] = (uint64_t)(p+histValue);
		}
	}

	__forceinline void scatter(uint32 D, uint32 R)
	{
		register uint32 M = ( ( 1 << D ) - 1) << R;
		register tuple_t * p = _input->tuples;
		register tuple_t* write(NULL);
		register uint32 idx(0);

		for(register uint32 index = 0; index < _cardinality; ++index)
		{
			idx = (p->key) & M;
			idx >>= R;

			write = (tuple_t*)_histogram[idx];
			*write = *p;
			_histogram[idx] += sizeof(tuple_t);

			++p;
		}
	}

	__forceinline void scatter(uint32 D, uint32 R, uint32 start, uint32 end, uint32 baseIndex, bool retainPayload)
	{
		register uint32 M = ( ( 1 << D ) - 1) << R;
		register tuple_t* p = &_input->tuples[start];
		register tuple_t* write(NULL);
		register uint32 idx(0);

		register uint32 itemCount(end - start);

		for(register uint32 index = 0; index < itemCount; ++index)
		{
			idx = (p->key) & M;
			idx >>= R;

			write = (tuple_t*)_histogram[idx];
			//*write = *p;
			write->key = p->key;
			write->payload = retainPayload?p->payload:((start + index) - baseIndex);

			_histogram[idx] += sizeof(tuple_t);

			++p;
		}
	}


	void showDebug()
	{
		for(uint32 counter = 0; counter < _numberOfPartitions; ++counter)
		{
			//TRACE2("Partition %d : [%d]\n", counter, _histogram[counter]);
// 			cout << "Partition " << counter << " : [" << _histogram[counter] << "]" << endl;
 			logger(DBG, "Partition %d : [%lu]", counter, _histogram[counter]);
		}
	}


private:
	uint32 _numberOfPartitions;
	uint32 _cardinality;
	uint64_t* _histogram;
	const relation_t* _input;
	relation_t* _output;
};