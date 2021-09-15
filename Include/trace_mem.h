#ifndef TRACE_MEM_H
#define TRACE_MEM_H

#include <vector>
#include <string>
#include <assert.h>
#include <string.h>
#ifdef LOG_ALL
#include <sstream>
#endif
#ifdef LOG_HASH
#include <openssl/sha.h>
#endif
using namespace std;


#define OP_READ 0
#define OP_WRITE 1


template <typename T>
class TraceMem {

    struct LogEntry {
        char opType;
        int i;
    };

    private:
        static int count;

        T *mem;
        long long num_rws = 0;
        #ifdef LOG_HASH
        SHA256_CTX *sha;
        #endif
        vector<LogEntry> log;

    public:
        const int mem_id;
        int size;

        TraceMem(int _size) : mem_id(count++), size(_size) {
            #ifdef LOG_HASH
            sha = new SHA256_CTX();
            SHA256_Init(sha);
            #endif
            mem = (T *)calloc(_size, sizeof(T));
            if (mem == nullptr)
            {
                printf("failed to calloc %d bytes", _size * sizeof(T));
            }
        }

        void resize(int new_size) {
            mem = (T *)realloc(mem, sizeof(T) * new_size);
            if (mem == nullptr)
            {
                printf("failed to realloc size %d bytes", sizeof(T) * new_size);
            }
            if (new_size > size)
                memset((T *)mem + size, 0, sizeof(T) * (new_size - size));
            size = new_size;
        }

        T read(int i) {
            assert(i >= 0 && i < size);
            #ifdef LOG_HASH
            num_rws++;
            SHA256_Update(sha, &i, sizeof(int));
            #endif
            #ifdef LOG_ALL
            num_rws++;
            log.push_back({OP_READ, i});
            #endif
            return mem[i];
        }

        void write(int i, T elt) {
            assert(i >= 0 && i < size);
            #ifdef LOG_HASH
            num_rws++;
            int ii = -1-i;
            SHA256_Update(sha, &ii, sizeof(int));
            #endif
            #ifdef LOG_ALL
            num_rws++;
            log.push_back({OP_WRITE, i});
            #endif
            mem[i] = elt;
        }

        uint32_t getTotalRW() {
            return num_rws;
        }

        #ifdef LOG_HASH
        SHA256_CTX *getHash() {
            return sha;
        }
        #endif

        #ifdef LOG_ALL
        string getTrace() {
            stringstream trace;
            int i = 0;
            for (LogEntry entry : log) {
                trace << (entry.opType == OP_READ ? "R" : "W") <<
                    ":" << mem_id << "[" << entry.i << "] ";
                if (i++ > 0 && i % 8 == 0) trace << "\n";
            }
            return trace.str();
        }
        #endif
};

template<typename T> int TraceMem<T>::count = 0;

#endif