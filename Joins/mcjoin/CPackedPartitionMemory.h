/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#pragma once

#include <math.h>
#include <cstdlib>

#include "SPageManagement.h"
#include "CBitRadixHistogram.h"

class CPackedPartitionMemory
{
public:
	CPackedPartitionMemory(const uint32 maxItemCount, const uint32 dataBitRadixLength) 
	{
		init(dataBitRadixLength);

		_maxItems = maxItemCount;
		resetBuffers(_maxItems);		
	}

	CPackedPartitionMemory(const uint32 dataBitRadixLength) 
	{
		init(dataBitRadixLength);
	}

	__forceinline void resetBuffers(const uint32 maxItemCount)
	{
		if(_dataBuffer != NULL)
		{
			free (_dataBuffer);
		}
		if(_offsetBuffer != NULL)
		{
			free (_offsetBuffer);
		}

		// Data buffer creation
		uint64_t bytesNeeded = memNeededInBytes(maxItemCount, _dataBitsToStore);
		_totalMemoryAllocated = bytesNeeded;
		_dataBuffer = (uint64_t *)calloc((size_t)bytesNeeded, (size_t)sizeof(char));
		
		// Offset buffer creation
		_offsetBitsToStore = getBitsNeededForOffsetRepresentation(maxItemCount);
		bytesNeeded = memNeededInBytes(maxItemCount, _offsetBitsToStore);
		_totalMemoryAllocated += bytesNeeded;
		_offsetBuffer = (uint64_t *)calloc((size_t)bytesNeeded, (size_t)sizeof(char));
		_offsetBitMask = (1 << (_offsetBitsToStore)) -  1;
	}

	~CPackedPartitionMemory()
	{
		if(_dataBuffer != NULL)
		{
			free(_dataBuffer);
			_dataBuffer = NULL;
		}

		if(_offsetBuffer != NULL)
		{
			free(_offsetBuffer);
			_offsetBuffer = NULL;
		}	

		if(_histogram != NULL)
		{
			delete _histogram;
			_histogram = NULL;
		}		
	}


	/*
	__forceinline void scatterPayload(const CRelation* r, const uint32 from, const uint32 to)
	{			
		register uint32 partition(0);
		register uint32 recordCount(to - from);		
				
		register SBUN* p = &(r->getBATPointer()[from]);
		for(register uint32 offset = 0; offset < recordCount; ++offset)
		{
			// Work out which partition this value writes to
			partition = p->payload & _dataRadixMask;

			writeDataValue(p->payload, partition);
			writeOffsetValue(storeKeyInsteadOfOffset?p->key:offset, partition);				
			
			// Update the number of items written to this partition
			(*_histogram)[partition]++;

			++p;
		}				
	}
	*/

	__forceinline void scatterKey(const relation_t* r, const uint32 from, const uint32 to)
	{			
		register uint32 partition(0);
		register uint32 recordCount(to - from);		

		register row_t* p = &(r->tuples[from]);
		for(register uint32 offset = 0; offset < recordCount; ++offset)
		{
			// Work out which partition this value writes to
			partition = p->key & _dataRadixMask;

			writeDataValue(p->key, partition);
			writeOffsetValue(p->payload, partition);				

			// Update the number of items written to this partition
			(*_histogram)[partition]++;

			++p;
		}				
	}

	//__forceinline void scatterOID(const CRelation* r, const uint32 from, const uint32 to, const bool useValuePayload)
	//{				
	//	register uint32 partition(0);
	//	uint32 recordCount(to - from);

	//	SBUN* p = &(r->getBATPointer()[from]);
	//	for(uint32 offset = 0; offset < recordCount; ++offset)
	//	{
	//		// Work out which partition this value writes to
	//		partition = p->value & _dataRadixMask;

	//		writeDataValue(p->OID, partition);

	//		// Update the number of items written to this partition
	//		(*_histogram)[partition]++;

	//		++p;
	//	}
	//	_histogram->rollbackPrefixSum();

	//	p = &(r->getBATPointer()[from]);
	//	for(uint32 offset = 0; offset < recordCount; ++offset)
	//	{
	//		// Work out which partition this value writes to
	//		partition = p->value & _dataRadixMask;

	//		writeOffsetValue(useValuePayload?p->value:offset, partition);

	//		// Update the number of items written to this partition
	//		(*_histogram)[partition]++;

	//		++p;
	//	}
	//	_histogram->rollbackPrefixSum();
	//}

	inline void writeOffsetValue(const uint32 offset, const uint32 partition)
	{
		// Work out which partition this value writes to
		//uint32 partition = value & _dataRadixMask;

		// Write to packed buffer
		writeOffsetBits(offset, partition);	

		// Update the number of items written to this partition
		//(*_histogram)[partition]++;

	}

	inline void writeDataValue(const uint32 value, const uint32 partition)
	{
		// Work out which partition this value writes to
		//register uint32 partition = value & _dataRadixMask;
		
		// Remove the radix portion of the value
		register uint32 bitsToWrite = value >> _dataBitRadixLength;

		// Write to packed buffer
		writeDataBits(bitsToWrite, partition);

		// Update the number of items written to this partition
		//(*_histogram)[partition]++;
	}


	__forceinline uint32 readOffsetValue(uint32 partition,  uint32 itemOffset)
	{

		uint32 itemNumber = (*_histogram)[partition] + itemOffset;
		SPageManagement readTarget;
		readTarget.calculateOffset(itemNumber, _offsetBitsToStore);
		
		uint64_t readBits(0);

		if(readTarget.offset + _offsetBitsToStore <= 64)	// Can read from a single 64-bit page
		{
			readBits = _offsetBuffer[readTarget.page];
			uint32 shift(64 - readTarget.offset - _offsetBitsToStore);
			readBits >>= shift;
			readBits &= _offsetBitMask;			
		}
		else
		{
			// Read the left side
			readBits = _offsetBuffer[readTarget.page];			

			uint32 shift(readTarget.offset + _offsetBitsToStore - 64);
			
			readBits <<= shift;
			uint32 leftSide = (uint32)readBits;

			// Read the right side
			readBits = _offsetBuffer[readTarget.page + 1];			

			shift = 64 - (_offsetBitsToStore - (64 - readTarget.offset));

			readBits >>= shift;	// Should crunch out any superfluous data at the end of the bits of interest	
			uint32 rightSide = (uint32)readBits;	

			readBits = leftSide | rightSide;

			readBits &= _offsetBitMask;
		}

		return (uint32)readBits;
	}


	__forceinline uint32 readDataValue(uint32 partition, uint32 itemOffset)
	{
		uint32 itemNumber = (*_histogram)[partition] + itemOffset;
		SPageManagement readTarget;
		readTarget.calculateOffset(itemNumber, _dataBitsToStore);		
		
		uint64_t readBits(0);

		if(readTarget.offset + _dataBitsToStore <= 64)	// Can read from a single 64-bit page
		{
			readBits = _dataBuffer[readTarget.page];
			uint32 shift(64 - readTarget.offset - _dataBitsToStore);
			readBits >>= shift;
			readBits <<= _dataBitRadixLength;

			readBits |= partition;
		}
		else
		{
			// Read the left side
			readBits = _dataBuffer[readTarget.page];			

			uint32 shift(readTarget.offset + _dataBitsToStore - 64);

			readBits <<= (shift + _dataBitRadixLength);
			uint32 leftSide = (uint32)readBits;

			// Read the right side
			readBits = _dataBuffer[readTarget.page + 1];			

			shift = 64 - (_dataBitsToStore - (64 - readTarget.offset));

			readBits >>= shift;	// Should crunch out any superfluous data at the end of the bits of interest	
			readBits <<= _dataBitRadixLength;
			uint32 rightSide = (uint32)readBits;	

			readBits = leftSide | rightSide;
			readBits |= partition;
		}

		return (uint32)readBits;
	}

	__forceinline void writeOffsetBits(const uint32 bits, const uint32 partition)
	{
		register uint32 itemNumber = (*_histogram)[partition];
		SPageManagement writeTarget;
		writeTarget.calculateOffset(itemNumber, _offsetBitsToStore);	

		register uint64_t writeBits(bits);

		if((writeTarget.offset + _offsetBitsToStore) <= 64)	// can fit into single 64-bit page
		{			
			register uint32 shift(64 - writeTarget.offset - _offsetBitsToStore);

			writeBits <<= shift;

			uint64_t& target = _offsetBuffer[writeTarget.page];

			target |= writeBits;	
		}
		else										// spans two pages
		{

			// Write the left side
			register uint32 shift(_offsetBitsToStore - (64 - writeTarget.offset));
			writeBits >>= shift;

			uint64_t& leftTarget = _offsetBuffer[writeTarget.page];
			leftTarget |= writeBits;

			// Write the right ride
			shift = (64 - _offsetBitsToStore + (_offsetBitsToStore - shift));
			writeBits = bits;
			writeBits <<= shift;

			uint64_t& rightTarget = _offsetBuffer[writeTarget.page + 1];
			rightTarget |= writeBits;			
		}
	}

	__forceinline void writeDataBits(const uint32 bits, const uint32 partition)
	{
		register uint32 itemNumber = (*_histogram)[partition];
		SPageManagement writeTarget;
		writeTarget.calculateOffset(itemNumber, _dataBitsToStore);	

		register uint64_t writeBits(bits);

		if((writeTarget.offset + _dataBitsToStore) <= 64)	// can fit into single 64-bit page
		{			
			register uint32 shift(64 - writeTarget.offset - _dataBitsToStore);

			writeBits <<= shift;

			uint64_t& target = _dataBuffer[writeTarget.page];

			target |= writeBits;	
		}
		else										// spans two pages
		{

			// Write the left side
			register uint32 shift(_dataBitsToStore - (64 - writeTarget.offset));
			writeBits >>= shift;

			uint64_t& leftTarget = _dataBuffer[writeTarget.page];
			leftTarget |= writeBits;

			// Write the right ride
			shift = (64 - _dataBitsToStore + (_dataBitsToStore - shift));
			writeBits = bits;
			writeBits <<= shift;

			uint64_t& rightTarget = _dataBuffer[writeTarget.page + 1];
			rightTarget |= writeBits;			
		}
	}

	__forceinline CBitRadixHistogram* getHistogram()
	{
		return _histogram;
	}

	void showDebug()
	{
		uint32 key(0);
		uint32 payload(0);
		uint32 partitionCount = _histogram->getNumberOfPartitions();

		for(uint32 partitionCounter = 0; partitionCounter < partitionCount; ++partitionCounter )
		{
			uint32 itemCount = _histogram->getItemCountPostSum(partitionCounter);
			for(uint32 itemIndex = 0; itemIndex < itemCount; ++itemIndex)
			{
				key = readDataValue(partitionCounter, itemIndex);
				payload = readOffsetValue(partitionCounter, itemIndex);

				printf("Partition: %d, Key: %d, Payload: %d\n", partitionCounter, key, payload);
			}
		}
	}

	uint32 getBitsNeededForOffsetRepresentation(uint32 itemCount)
	{
		double bits = log((double)itemCount) / log(2.0);

		return (uint32)ceil(bits);
	}

	uint64_t memNeededInBytes(const uint32 count, const uint32 bitsToStore)
	{
		uint64_t result(0);

		//result = (count * bitsToStore / 64);
		uint64_t totalBits = count;
		totalBits *= bitsToStore;
	
		//result = totalBits / 64;
		result = totalBits >> 6;
		
		//if((totalBits) % 64 != 0)
		if(((totalBits) & 63) != 0)
		{
			++result;	// Round up to next 64-bit boundary
		}

		//result *= 8;
		result <<= 3;

		return result;
	}

	inline uint64_t getTotalMemoryAllocated()
	{
		return _totalMemoryAllocated;
	}

private:
	void init(uint32 dataBitRadixLength)
	{
		_maxItems = 0; 
		_dataBuffer = NULL;
		_dataBitRadixLength = dataBitRadixLength;
		_dataBitsToStore = (32 - dataBitRadixLength);
		_dataRadixMask = 0; 
		_offsetBuffer = NULL;
		_offsetBitsToStore = 0; 

		// Histogram and radix mask
		_histogram = new CBitRadixHistogram(dataBitRadixLength);
		_dataRadixMask = (1 << (_dataBitRadixLength)) -  1;
		_offsetBitMask = (1 << (_offsetBitsToStore)) -  1;
	}

private:
	uint64_t* _dataBuffer;
	uint32 _maxItems;
	uint32 _dataBitRadixLength;
	uint32 _dataBitsToStore;
	uint32 _dataRadixMask; 

	uint64_t* _offsetBuffer;
	uint32 _offsetBitsToStore;
	uint32 _offsetBitMask;

	CBitRadixHistogram* _histogram;

	uint64_t _totalMemoryAllocated;
};