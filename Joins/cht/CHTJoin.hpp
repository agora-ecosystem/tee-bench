#include <vector>
#include <atomic>
#include <iostream>

#include "data-types.h"
#include "CHT.hpp"
#include "Barrier.h"

#include <cstring> //memset

#ifdef NATIVE_COMPILATION
#include "Logger.h"
#include "native_ocalls.h"
#else
#include "Enclave_t.h"
#include "Enclave.h"
#endif

class CHTPartitionQueue{
private:
    std::atomic<int> counter;
    int nPart;
public:
    CHTPartitionQueue(int _nPart):counter(0),nPart(_nPart) {
    }

    int getPartition() {
        return counter++;
    }

    bool empty() {
        return counter.load() == nPart;
    }
};


const unsigned int TUPLES_PER_CACHELINE= 64 / sizeof(tuple_t);

typedef struct {
    tuple_t tuples[TUPLES_PER_CACHELINE - 1];
    uint64_t target;
} cht_cacheline_t;

#define STREAM_UNIT 4

class CHTJoin {
private:
    int nthreads;
    int npart;
    relation_t *relR;
    relation_t *relS;
    tuple_t *partBuffer;
    uint64_t **hist;
    uint64_t **dst;
    CHTPartitionQueue *partitions;
    CHT ht;
    const intkey_t MASK;
    const intkey_t SHIFT;
    uint64_t *matches;
    uint64_t *checksums;
    uint64_t time_usec;
    uint64_t partitionTime;
    /*
         * timer1 - entire algorithm
         * timer2 - partition phase
         * timer3 - build phase
         * timer4 - probe phase
         * */
//    uint64_t start, end, timer1, timer2, timer3, timer4;
    struct timers_t timers;

    PThreadLockCVBarrier *barrier;

    void radix_partition(const tuple_t *input,
                         tuple_t *output,
                         uint64_t *histogram,
                         size_t numTuples) {

        __attribute__((aligned(64))) cht_cacheline_t buffers[npart];

        // we have to make sure that the size of each partitioning in terms of elements is a multiple of 4 (since 4 tuples fit into 32 bytes = 256 bits).
        for(int i = 0; i < npart; ++i){
            buffers[i].target = histogram[i];
        }

        __attribute__((aligned(64))) uint64_t bucket_num = 0;
        for(uint64_t j = 0; j < numTuples; ++j){
            bucket_num = (input[j].key >> SHIFT) & MASK;
            int slot=buffers[bucket_num].target & (TUPLES_PER_CACHELINE - 1);
            if(slot == TUPLES_PER_CACHELINE - 1) {
                uint64_t targetBkp=buffers[bucket_num].target- (TUPLES_PER_CACHELINE-1);
                buffers[bucket_num].tuples[slot]=input[j];
                for(uint32_t b = 0; b < TUPLES_PER_CACHELINE; b += STREAM_UNIT) {
//                    _mm256_stream_si256(reinterpret_cast<__m256i*>(output + targetBkp), _mm256_load_si256((reinterpret_cast<__m256i*>(buffers[bucket_num].tuples + b))));
                    memcpy(output + targetBkp, buffers[bucket_num].tuples + b, 32);
                    targetBkp += STREAM_UNIT;
                }
                buffers[bucket_num].target=targetBkp;

            } else {
                buffers[bucket_num].tuples[slot] = input[j];
                buffers[bucket_num].target++;
            }
        }

        barrier->Arrive();

        for (int i = npart - 1; i >= 0; i--) {
            uint32_t slot  = (uint32_t)buffers[i].target;
            uint32_t sz    = (slot) & (TUPLES_PER_CACHELINE - 1);
            slot          -= sz;
            uint32_t startPos = (slot < histogram[i]) ? ((uint32_t)histogram[i] - slot) : 0;
            for(uint32_t j = startPos; j < sz; j++) {
                output[slot+j]  = buffers[i].tuples[j];
            }
        }
    }

    void partition(int threadID, relation_t chunkR)
    {
        const tuple_t * tupleR = chunkR.tuples;
        uint64_t * my_hist     = hist[threadID];
        uint64_t * my_dst      = dst[threadID];
        uint64_t sum           = 0;

        for (size_t i = 0; i < chunkR.num_tuples; ++i) {
            intkey_t hk = (hashKey(tupleR[i].key) >> SHIFT) & MASK;
            my_hist[hk]++;
        }

        for (int i = 0; i < npart; ++i) {
            sum += my_hist[i];
            my_hist[i] = sum;
        }

        barrier->Arrive();

        for (int i = 0; i < threadID; ++i) {
            for (int j = 0; j < npart; ++j) {
                my_dst[j] += hist[i][j];
            }
        }

        for (int i = threadID; i < nthreads; ++i) {
            for (int j = 1; j < npart; ++j) {
                my_dst[j] += hist[i][j-1];
            }
        }

        radix_partition(chunkR.tuples, partBuffer, my_dst, chunkR.num_tuples);
    }

    void build(int threadID, int part)
    {
        (void) (threadID);
        tuple_t * tuples= partBuffer + dst[0][part];
        const uint64_t num_tuples = part == npart - 1 ? relR->num_tuples - dst[0][part] : dst[0][part + 1] - dst[0][part];

        for (uint64_t i = 0; i < num_tuples; ++i) {
            ht.setBit(tuples[i].key);
		}

        ht.computePartPopCount(part, (uint32_t)dst[0][part]);

        for (uint64_t i = 0; i < num_tuples; ++i)
            ht.setTuple(tuples[i]);
    }

    void probe(int threadID, relation_t chunkS)
    {
        uint64_t match = 0;
        uint64_t checksum = 0;
 		tuple_t *tupleS = chunkS.tuples;
		const size_t numS = chunkS.num_tuples;
		const size_t batchStartUpperBound = numS - PROBE_BATCH_SIZE;
        for (size_t i = 0; i <= batchStartUpperBound; i += PROBE_BATCH_SIZE)
		{
			ht.batch_probe(tupleS + i, match, checksum);
		}

		const size_t leftOver = numS % PROBE_BATCH_SIZE;
		tupleS += numS - leftOver;
        for (size_t i = 0; i < leftOver; ++i)
        {
            tuple_t *foundTuple = ht.probe(tupleS[i].key);
            if (foundTuple)
            {
                match++;
                checksum += foundTuple->payload + tupleS[i].payload;
            }
        }
        matches[threadID] = match;
        checksums[threadID] = checksum;
    }

public:

    CHTJoin(int _nthreads, int _npart, relation_t *_relR, relation_t *_relS, tuple_t * _partBuffer) : nthreads(_nthreads),
                                                     npart(_npart), relR(_relR) , relS(_relS),partBuffer(_partBuffer),
                                                     partitions(new CHTPartitionQueue(npart)),
                                                     ht(relR->num_tuples, nthreads, npart),
                                                     MASK(npart-1),SHIFT(__builtin_ctz(Utils::nextPowerOfTwo(relR->num_tuples)) - __builtin_ctz(npart)),
                                                     matches(new uint64_t[nthreads]), checksums(new uint64_t[nthreads])
    {
        barrier = new PThreadLockCVBarrier(nthreads);
        hist = (uint64_t **) malloc(nthreads * sizeof(uint64_t *));
        dst = (uint64_t **) malloc(nthreads * sizeof(uint64_t *));
    }


    void join(int threadID) {
        ht.init(threadID);

        hist[threadID] = (uint64_t*) calloc(npart, sizeof(uint64_t));
        dst[threadID] = (uint64_t*) calloc(npart, sizeof(uint64_t));

        barrier->Arrive();

        if (threadID == 0) {
            ocall_get_system_micros(&timers.start);
            ocall_startTimer(&timers.total);
            ocall_startTimer(&timers.timer1);
#ifdef PCM_COUNT
            ocall_set_system_counter_state("Start Partition");
#endif
        }

        // partition phase
        uint64_t numRPerThread = relR->num_tuples / nthreads;
        relation_t myChunkR;
        myChunkR.tuples = relR->tuples + numRPerThread * threadID;
        myChunkR.num_tuples = threadID == nthreads - 1 ? (uint32_t)(relR->num_tuples - numRPerThread * threadID) : (uint32_t)numRPerThread;

        partition(threadID, myChunkR);

        barrier->Arrive();

        if (threadID == 0) {
            ocall_get_system_micros(&timers.end);
            ocall_stopTimer(&timers.timer1);
            ocall_startTimer(&timers.timer2);
            partitionTime=timers.end-timers.start;
		}

        // build phase
        int partID;
        while ((partID = partitions->getPartition()) < npart)
        {
            build(threadID, partID);
        }

        barrier->Arrive();

        if (threadID == 0)
        {
#ifdef PCM_COUNT
            ocall_get_system_counter_state("Partition", 0);
            ocall_set_system_counter_state("Join");
#endif
            ocall_stopTimer(&timers.timer2);
            ocall_startTimer(&timers.timer3);
        }
      //  std::cerr << "build finished" << std::endl;

        // probe phase
        uint64_t numSperThread = relS->num_tuples / nthreads;
        relation_t myChunkS;
        myChunkS.tuples = relS->tuples + numSperThread * threadID;
        myChunkS.num_tuples = threadID == nthreads - 1 ? (uint32_t)(relS->num_tuples - numSperThread * threadID) : (uint32_t)numSperThread;
        probe(threadID, myChunkS);

        barrier->Arrive();

        if (threadID == 0) {
#ifdef PCM_COUNT
            ocall_get_system_counter_state("Join", 0);
#endif
	//		std::cerr << "probe finished" << std::endl;
	        ocall_stopTimer(&timers.timer3);
            ocall_get_system_micros(&timers.end);
            ocall_stopTimer(&timers.total);
            time_usec = timers.end - timers.start;

        }
    }

    join_result_t get_join_result()
    {
        uint64_t m = 0;
        uint64_t c = 0;
        for (int i = 0; i < nthreads; ++i) {
            m += matches[i];
            c += checksums[i];
        }
        join_result_t result;
        result.time_usec = time_usec;
        result.part_usec=partitionTime;
        result.join_usec=time_usec-partitionTime;
        result.matches = m;
        result.checksum = c;
        return result;
    }

    struct timers_t get_timers()
    {
        return timers;
    }

    ~CHTJoin()
    {
		for (int i = 0; i < nthreads; ++i) {
			free(dst[i]);
			free(hist[i]);
		}
		free(dst);
		free(hist);
        delete[] checksums;
        delete[] matches;
        delete 	 barrier;
        delete   partitions;
    }
};
