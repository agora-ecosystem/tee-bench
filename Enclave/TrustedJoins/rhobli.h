#ifndef RHOBLI_H
#define RHOBLI_H

#include "data-types.h"
//#include "radix_join.h"

result_t* rhobli_join (struct table_t *relR, struct table_t *relS, int nthreads);

#endif //RHOBLI_H
