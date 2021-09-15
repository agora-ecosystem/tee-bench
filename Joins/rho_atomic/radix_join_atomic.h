#ifndef RADIX_JOIN_ATOMIC_H
#define RADIX_JOIN_ATOMIC_H

#include <stdlib.h>
#include "data-types.h"

typedef int64_t (*JoinFunction)(const struct table_t * const,
                                const struct table_t * const,
                                struct table_t * const,
                                output_list_t ** output);

result_t*
RHO_atomic (relation_t * relR, relation_t * relS, int nthreads);

#endif //RADIX_JOIN_ATOMIC_H
