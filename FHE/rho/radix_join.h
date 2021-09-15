#ifndef _RADIX_JOIN_H_
#define _RADIX_JOIN_H_

#include <stdlib.h>
#include "../data-types.h"

typedef int64_t (*JoinFunction)(const relation_enc_t * const,
                                const relation_enc_t * const,
                                relation_enc_t * const,
                                output_list_t ** output);


result_t*
RJ_enc (relation_enc_t * relR, relation_enc_t * relS);

result_t*
RHO_enc (relation_enc_t * relR, relation_enc_t * relS, int nthreads);

#endif  //_RADIX_JOIN_H_