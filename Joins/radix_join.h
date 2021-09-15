#ifndef _RADIX_JOIN_H_
#define _RADIX_JOIN_H_

#include <stdlib.h>
#include "data-types.h"

typedef int64_t (*JoinFunction)(const struct table_t * const,
                                const struct table_t * const,
                                struct table_t * const,
                                output_list_t ** output);

void print_timing(uint64_t total, uint64_t build, uint64_t part,
                  uint64_t numtuples, int64_t result,
                  uint64_t start, uint64_t end,
                  uint64_t pass1, uint64_t pass2);

result_t*
RJ (struct table_t * relR, struct table_t * relS, int nthreads);

result_t*
RHO (struct table_t * relR, struct table_t * relS, int nthreads);

result_t*
RHT (relation_t * relR, relation_t * relS, int nthreads);

result_t*
RHO_seal_buffer (relation_t * relR, relation_t * relS, int nthreads);

result_t *
join_init_run(struct table_t * relR, struct table_t * relS, JoinFunction jf, int nthreads);

#endif  //_RADIX_JOIN_H_