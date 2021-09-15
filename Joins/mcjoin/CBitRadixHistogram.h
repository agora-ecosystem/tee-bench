/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/

#pragma once

#include <stdio.h>
#ifdef NATIVE_COMPILATION
#include <memory.h>
#endif
#include <bitset>
#include "util.h"

using namespace std;

class CBitRadixHistogram
{
public:
	CBitRadixHistogram(const uint32 radixLength) : _radixLength(radixLength)
	{		
		_numberOfPartitions = (1 << radixLength);
		_histogram = (uint32 *)malloc(_numberOfPartitions * sizeof(uint32));
		malloc_check(_histogram)
		reset();

		_radixMask = (1 << (_radixLength)) -  1;
	}

	~CBitRadixHistogram()
	{
		free(_histogram);
	}

	__forceinline uint32 getRadixLength()
	{
		return _radixLength;
	}

	inline void reset()
	{
		memset(_histogram, 0, _numberOfPartitions * sizeof(uint32));
		_itemCount = 0;
	}

/*
	*** If this remains commented: delete it! ***

	inline void buildHistogramPayload(const CRelation* r, const uint32 from, const uint32 to)
	{
		uint32 recordCount(to - from);
		SBUN* p = &(r->getBATPointer()[from]);

		_itemCount = recordCount;

		for(uint32 counter = 0; counter < recordCount; ++counter)
		{
			addValueToHistogram(p->payload);

			++p;
		}
	}
*/

	void buildHistogramKey(const relation_t* r, const uint32 from, const uint32 to)
	{
		uint32 recordCount(to - from);
		tuple_t* p = &(r->tuples[from]);

		_itemCount = recordCount;

		for(uint32 counter = 0; counter < recordCount; ++counter)
		{
			addValueToHistogram(p->key);

			++p;
		}
	}

	inline void buildPrefixSum()
	{
		uint32* h = _histogram;
		uint32 thisValue(0);
		uint32 sum(0);

		for(uint32 index = 0; index < _numberOfPartitions; ++index )
		{						
			thisValue = *h;

			*h = sum;

			sum += thisValue;
			++h;	
		}
	}

	inline void rollbackPrefixSum()
	{
		uint32* h = _histogram;
		uint32 thisValue(0);
		uint32 lastValue(0);

		for(uint32 index = 0; index < _numberOfPartitions; ++index )
		{						
			thisValue = *h;

			*h = lastValue;
			
			++h;	

			lastValue = thisValue;
		}
	}


	__forceinline void addValueToHistogram(const uint32 value)
	{
		uint32 maskedValue = value & _radixMask;
		
		++(_histogram[maskedValue]);
	}


	__forceinline uint32 getNumberOfPartitions()
	{
		return _numberOfPartitions;
	}

	inline uint32 getItemCount()
	{
		return _itemCount;
	}

	inline uint32 getItemCountPostSum(uint32 partition)
	{
		// To calculate how many items exist in this partiton, post prefix-sum operation,
		// I subtract the value at the supplied partition from the next one in sequence.
		// If the partition nominated is the END partition, I subtract the value of that one
		// from the total number of items stored in this histogram.

		uint32 nextValue(0);

		if(partition < (_numberOfPartitions - 1))
		{
			nextValue = _histogram[partition + 1];
		}
		else
		{
			nextValue = _itemCount;
		}

		uint32 result(nextValue - _histogram[partition]);

		return result;		
	}

	void showDebug()
	{
		char* binValue = new char[_radixLength + 1];	

		for(uint32 counter = 0; counter < _numberOfPartitions; ++counter)
		{
			convertIntToBinary(counter, binValue, _radixLength + 1);
			//TRACE3("Partition %3d (%s) : [%d]\n", counter, binValue, _histogram[counter]);
			printf("Partition %3d (%s) : [%d]\n", counter, binValue, _histogram[counter]);
			//cout << "Partition " << counter << "(" <<
		}

		delete [] binValue;
	}


	inline size_t getHistogramMemoryAllocated()
	{
		return _numberOfPartitions * sizeof(uint32);
	}

	inline uint32& operator[] (const int index)
	{
		return _histogram[index];
	}

	__forceinline uint32* getHistogramArray()
	{
		return _histogram;
	}


private:

	void convertIntToBinary(uint32 val, char* p, const uint32 length)
	{
		// Set last character to be \0
		p[length - 1] = '\0';		
		for(uint32 index = length - 2; index >= 0; --index)
		{
			((val & 1) == 1)?p[index] = '1':p[index] = '0';
			val >>= 1;
		}
	}

	uint32 _radixLength;
	uint32 _radixMask;
	uint32 _numberOfPartitions;
	uint32 _itemCount;
	uint32* _histogram;
};