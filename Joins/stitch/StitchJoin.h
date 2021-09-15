#ifndef STITCHJOIN_H
#define STITCHJOIN_H

#include "data-types.h"

using namespace std;

class StitchJoin {

public:
    result_t* STJ(relation_t* relR, relation_t* relS, int nthreads);

private:
    static void * stj_thread (void * param);
};

#endif //STITCHJOIN_H
