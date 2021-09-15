#ifndef CHTJOINWRAPPER_HPP_
#define CHTJOINWRAPPER_HPP_

#include "data-types.h"

template<int numbits>
join_result_t CHTJ(relation_t *, relation_t *, int);

//int64_t CHT(relation_t *relR, relation_t *relS, int nthreads);

#endif
