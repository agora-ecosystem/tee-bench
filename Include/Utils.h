#ifndef _UTILS_H_
#define _UTILS_H_

#include "MersenneTwister.h"
//#include <sys/mman.h>
#include <stdio.h>
#include "data-types.h"
#include <stdlib.h>
#include <string.h>

class Utils {
public:
	static inline void* malloc_huge(size_t size) {
#ifdef HUGE_PAGE
		void* p = mmap(NULL, size, PROT_READ | PROT_WRITE,
				MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		madvise(p, size, MADV_HUGEPAGE);
#else
		void* p = malloc(size);
#endif
		return p;
	}

	static inline void dealloc_huge(void* ptr, size_t size) {
#ifdef HUGE_PAGE
		munmap(ptr,size);
#else
        (void)(size);
		free(ptr);
#endif
	}

	static inline void* malloc_huge_and_set(size_t size) {
#ifdef HUGE_PAGE
		void* p = mmap(NULL, size, PROT_READ | PROT_WRITE,
				MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		madvise(p, size, MADV_HUGEPAGE);
#else
		void* p = malloc(size);
#endif
		memset(p, 0, size);
		return p;
	}

	static inline uint64_t unsignedLog2 (uint64_t val) {
		uint64_t ret = -1;
	    while (val != 0) {
	        val >>= 1;
	        ret++;
	    }
	    return ret;
	}

	template<typename T>
	static inline T nextPowerOfTwo(T n) {
	    --n;
	    for(T k=1;!(k&(1<<(sizeof(n)+1)));k<<=1) n|=n>>k;
	    return ++n;
	}

	/** MurmurHash64A */
	static inline uint64_t hashKey(uint64_t k) {
	   const uint64_t m = 0xc6a4a7935bd1e995;
	   const int r = 47;
	   uint64_t h = 0x8445d61a4e774912 ^ (8*m);
	   k *= m;
	   k ^= k >> r;
	   k *= m;
	   h ^= k;
	   h *= m;
	   h ^= h >> r;
	   h *= m;
	   h ^= h >> r;
	   return h;
	}

	static inline double gettime(void) {
//	  struct timeval now_tv;
//	  gettimeofday (&now_tv, NULL);
//	  return ((double)now_tv.tv_sec) + ((double)now_tv.tv_usec)/1000000.0;
        return 0.0;
	}

	template<typename T>
	static inline T createMsbMask(unsigned int nBits) {
		T mask = 0;
		for (unsigned int i = 0; i < nBits; i++) {
			mask <<= 1;
			mask |= 1;
		}
		mask <<= (sizeof(T) * 8 - nBits);
		return mask;
	}

	static intkey_t * generateShuffledNumbers(intkey_t n, intkey_t modulus, uint64_t seed) {
        intkey_t* numbers = new intkey_t[n];
		for (intkey_t i = 0; i < n; i++) {
			numbers[i] = i % modulus;
		}

		MersenneTwister mersenneTwister(seed);

		for (intkey_t i = n - 1; i > 0; i--) {
            intkey_t p = (intkey_t) (mersenneTwister.rnd() % (i + 1));
            intkey_t t = numbers[i];
			numbers[i] = numbers[p];
			numbers[p] = t;
		}
		return numbers;
	}

	static intkey_t* generateShuffledNumbers(intkey_t n, intkey_t distance, intkey_t modulus, uint64_t seed) {
        intkey_t* numbers = new intkey_t[n];
		for (intkey_t i = 0; i < n; i++) {
			numbers[i] = (i * distance) % modulus;
		}

		MersenneTwister mersenneTwister(seed);

		for (intkey_t i = n - 1; i > 0; i--) {
            intkey_t p = (intkey_t) (mersenneTwister.rnd() % (i + 1));
            intkey_t t = numbers[i];
			numbers[i] = numbers[p];
			numbers[p] = t;
		}
		return numbers;
	}

};

#endif // _UTILS_H_
