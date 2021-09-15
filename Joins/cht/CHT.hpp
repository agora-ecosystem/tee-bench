#include <stddef.h>
#include "Barrier.h"
#include "data-types.h"
#include <stdlib.h>
#include <string.h>
#include "Utils.h"
#include <assert.h>
#include <stdexcept>
#include <sstream>
#include <iostream>

#ifdef NATIVE_COMPILATION
#include "Logger.h"
#include "native_ocalls.h"
#else
#include "Enclave_t.h"
#include "Enclave.h"
#endif

#define PROBE_BATCH_SIZE 16

#ifndef CORES
#define CORES 8
#endif

class CHT {
private:
    struct popCount_t {
    uint32_t bits;
    uint32_t count;
    };
    const int bitsPerBucket=sizeof(popCount_t)*8/2;
    size_t tableSize;
    int nThreads;
    int nPartitions;
    size_t bitMapSize;
    const size_t partitionSize;
    const int log2PartitionSize;
    popCount_t *bitMap;
    tuple_t *tupleArray;

    PThreadLockCVBarrier *barrier;

    bool setIfFree(intkey_t pos);
    tuple_t *findTuplePlace(intkey_t key);

    intkey_t nextInPartition(intkey_t pos);

public:

    CHT(size_t tableSize, int nThreads,int nPartitions);
    void computePopCount(int threadId);
    void computePartPopCount(int partID, uint32_t startCount);
    void init(uint64_t threadId);
    void setBit(intkey_t key);
    void setTuple(tuple_t tuple);
    tuple_t* probe(intkey_t key);
	void batch_probe(tuple_t *probeTuples, uint64_t &matches, uint64_t &checksum);



};



#define _IDHASH_

#if defined(_IDHASH_)
    /** Identity Hashing */
    inline intkey_t hashKey(const intkey_t k) {
        return k;
    }
#elif defined(_FIBHASH_)
	/** Fibonacci Hashing */
	inline intkey_t hashKey(const intkey_t k) const {
		return (k * 11400714819323198485ull) ;
	}
#elif defined(_CRCHASH_)
	/** CRC Hashing */
	inline intkey_t hashKey(const intkey_t k) const {
		return _mm_crc32_u64(0, k) ;
	}
#else

/** MurmurHash64A */
inline intkey_t hashKey(intkey_t k) const {
    const intkey_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    intkey_t h = 0x8445d61a4e774912 ^(8 * m);
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h ;
}
#endif

CHT::CHT(size_t _tableSize,int _nThreads,int _nPartitions) : tableSize(_tableSize) ,nThreads(_nThreads),nPartitions(_nPartitions),bitMapSize(Utils::nextPowerOfTwo(tableSize)),
                                                          partitionSize((bitMapSize)/(unsigned long) _nPartitions),log2PartitionSize(__builtin_ctz((unsigned int)partitionSize)){

    assert(Utils::nextPowerOfTwo(_tableSize)%(unsigned long) bitsPerBucket==0);



    barrier=new PThreadLockCVBarrier(_nThreads);
}


void CHT::init(uint64_t threadId) {

    uint64_t memChunkSize = 1024*2;
    uint64_t numChunksTable = (tableSize * sizeof(tuple_t)+memChunkSize-1) / memChunkSize;
    uint64_t numChunksBitmap = ((bitMapSize/bitsPerBucket)* sizeof(popCount_t)+memChunkSize-1) / memChunkSize;

		if (threadId == 0) {
            tupleArray = static_cast<tuple_t*>(malloc(tableSize * sizeof(tuple_t)+memChunkSize));
            bitMap = static_cast<popCount_t *>(malloc((bitMapSize/bitsPerBucket)* sizeof(popCount_t)+memChunkSize));
		}
		barrier->Arrive();



    //Table should be larger than bitmap therefore use same initorder as for table
		intkey_t * initOrderTable = Utils::generateShuffledNumbers((intkey_t)numChunksTable, (intkey_t)numChunksTable, 19650218ULL);
    intkey_t * initOrderBitmap = Utils::generateShuffledNumbers((intkey_t)numChunksBitmap, (intkey_t)numChunksBitmap, 19650218ULL);

		for (uint64_t i = threadId; i < numChunksTable; i += (unsigned long)nThreads) {
			memset(((char*)tupleArray) + (initOrderTable[i] * memChunkSize), 0, memChunkSize);
		}

    for (uint64_t i = threadId; i < numChunksBitmap; i += (unsigned long)nThreads) {
        memset(((char*)bitMap) + (initOrderBitmap[i] * memChunkSize), 0, memChunkSize);
    }
		delete [] initOrderTable;
    delete [] initOrderBitmap;

}

inline bool CHT::setIfFree(intkey_t pos) {
    //
    uint32_t bits=bitMap[pos>> /*(int)log2(bitsPerBucket)*/ 5].bits;
    uint32_t hashBit=(1<<(pos&(bitsPerBucket-1)));
    if ((bits & hashBit) ==0) {
        bitMap[pos/bitsPerBucket].bits|=hashBit;
        return true;
    }
    else
        return false;
}

inline void CHT::setBit(intkey_t key) {
    intkey_t hash = (intkey_t) (hashKey(key) & (bitMapSize-1));
    if(!setIfFree(hash)&&!setIfFree(nextInPartition(hash))) {
//		std::cerr << "setBit " << key << " failed" << std::endl;
//		logger(DBG, "setBit %d failed", key);
//        throw std::runtime_error("TODO insert into Overflow HT should not happen with dense keys!");
//        ocall_throw("TODO insert into Overflow HT should not happen with dense keys!");
	}

}

inline tuple_t* CHT::findTuplePlace(intkey_t key) {
    intkey_t hash = (intkey_t)(hashKey(key) & (bitMapSize-1));
    return tupleArray+bitMap[hash>>/* (int)log2(bitsPerBucket)*/ 5].count+
            __builtin_popcount(bitMap[hash>>/* (int)log2(bitsPerBucket)*/ 5].bits&
                    ~((~0)<<((hash&(bitsPerBucket-1)))));
}

inline void CHT::setTuple(tuple_t tuple) {
    tuple_t *toInsert=findTuplePlace(tuple.key);
    if (toInsert->key==0)
        *toInsert=tuple;
    else {
        toInsert=tupleArray+ nextInPartition((intkey_t)(toInsert-tupleArray));
        if (toInsert->key==0)
            *toInsert=tuple;

        else {

//            std::stringstream stringstream("TODO insert Tuple into Overflow HT should not happen with dense keys!");
//            stringstream<<" Key: "<<tuple.key<<" toInsert: "<<toInsert->key<<", "<<toInsert->payload;
//            throw std::runtime_error(stringstream.str());
//            logger(ERROR, "Key: %u to Insert: %u, %u", tuple.key, toInsert->key, toInsert->payload);
//            ocall_throw("TODO insert Tuple into Overflow HT should not happen with dense keys!");
        }
    }

}

inline tuple_t *CHT::probe(intkey_t key) {
    tuple_t *toReturn =findTuplePlace(key);
    if (toReturn->key==key)
        return toReturn;
    else if ((++toReturn)->key==key)
        return toReturn;
    else {
//		std::cout << "probe key " << key << " failed" << std::endl;
//		throw std::runtime_error("TODO lookup Tuple into Overflow HT should not happen with dense keys!");
		logger(DBG, "Probe key = %d", key);
		ocall_throw("Probe key failed");
	}
}

void CHT::batch_probe(tuple_t *probeTuples, uint64_t &matches, uint64_t &checksum) {
	tuple_t *tupleBatch[PROBE_BATCH_SIZE];
	for (int i = 0; i < PROBE_BATCH_SIZE; ++i) {
		tupleBatch[i] = findTuplePlace(probeTuples[i].key);
	}
	for (int i = 0; i < PROBE_BATCH_SIZE; ++i) {
		if (tupleBatch[i]->key == probeTuples[i].key) {
			matches++;
			checksum += tupleBatch[i]->payload + probeTuples[i].payload;
		} else if ((++(tupleBatch[i]))->key == probeTuples[i].key) {
			matches++;
			checksum += tupleBatch[i]->payload + probeTuples[i].payload;
		} else {
//			std::cout << "batch probe key " << probeTuples[i].key << " " << tupleBatch[i]->key << " failed" << std::endl;
//		   	throw std::runtime_error("TODO lookup Tuple into Overflow HT should not happen with dense keys!");
//		   	logger(DBG, "Batch probe key %u %u failed", probeTuples[i].key, tupleBatch[i]->key);
//		   	ocall_throw("Batch probe key failed");
		}
	}
}

void CHT::computePopCount(int threadId) {
    //First Version everything done by thread 0:
    if (threadId==0) {
        uint32_t count=0;
        for (size_t i = 0; i < bitMapSize/bitsPerBucket; ++i) {
            bitMap[i].count=count;
            count+=__builtin_popcount(bitMap[i].bits);
        }
    }

    barrier->Arrive();
}

void CHT::computePartPopCount(int partID, uint32_t startCount) {
    uint32_t count = startCount;
    size_t startBuckets = (partitionSize / bitsPerBucket) * partID;
    size_t endBuckets = startBuckets + (partitionSize / bitsPerBucket);
    for (size_t i = startBuckets; i < endBuckets; ++i) {
        bitMap[i].count = count;
        count += __builtin_popcount(bitMap[i].bits);
    }
}

inline intkey_t CHT::nextInPartition(intkey_t pos) {
    return (intkey_t)((pos&(~(partitionSize-1))) | ((pos+1)&(partitionSize-1)));
}
