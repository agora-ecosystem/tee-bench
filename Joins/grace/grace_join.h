#ifndef GRACE_JOIN_H
#define GRACE_JOIN_H


#include "data-types.h"

result_t*
GHT (struct table_t * relR, struct table_t * relS, int nthreads);

#endif //GRACE_JOIN_H
