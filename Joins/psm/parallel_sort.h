#include "utility.h"
#include <utility>
#include <cmath>
#include <iterator>


#ifndef PARALLEL_SORT_STL_H
#define PARALLEL_SORT_STL_H

namespace internal {

    void qsort3w(row_t *_First, row_t *_Last);

    void parallel_sort(row_t *_First, row_t *_Last, int nthreads);

    void psort(relation_t *relation, int nthreads);
}

#endif // PARALLEL_SORT_STL_H