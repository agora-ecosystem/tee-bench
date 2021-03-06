#ifndef MERSENNETWISTER_H_
#define MERSENNETWISTER_H_

#include <cstdint>

class MersenneTwister {
private:
   static const int NN=312;
   static const int MM=156;
   static const uint64_t MATRIX_A=0xB5026F5AA96619E9ULL;
   static const uint64_t UM=0xFFFFFFFF80000000ULL;
   static const uint64_t LM=0x7FFFFFFFULL;

   uint64_t mt[NN];
   int mti;

   void init(uint64_t seed) {
      mt[0] = seed;
      for (mti=1; mti<NN; mti++)
         mt[mti] = (6364136223846793005ULL * (mt[mti-1] ^ (mt[mti-1] >> 62)) + mti);
   }

public:
   MersenneTwister (uint64_t seed=19650218ULL): mti(NN+1) {
      init(seed);
   }

   uint64_t rnd() {
      int i;
      uint64_t x;
      static uint64_t mag01[2]={0ULL, MATRIX_A};

      if (mti >= NN) { /* generate NN words at one time */

         for (i=0;i<NN-MM;i++) {
            x = (mt[i]&UM)|(mt[i+1]&LM);
            mt[i] = mt[i+MM] ^ (x>>1) ^ mag01[(int)(x&1ULL)];
         }
         for (;i<NN-1;i++) {
            x = (mt[i]&UM)|(mt[i+1]&LM);
            mt[i] = mt[i+(MM-NN)] ^ (x>>1) ^ mag01[(int)(x&1ULL)];
         }
         x = (mt[NN-1]&UM)|(mt[0]&LM);
         mt[NN-1] = mt[MM-1] ^ (x>>1) ^ mag01[(int)(x&1ULL)];

         mti = 0;
      }

      x = mt[mti++];

      x ^= (x >> 29) & 0x5555555555555555ULL;
      x ^= (x << 17) & 0x71D67FFFEDA60000ULL;
      x ^= (x << 37) & 0xFFF7EEE000000000ULL;
      x ^= (x >> 43);

      return x;
   }
};

#endif /* MERSENNETWISTER_H_ */
