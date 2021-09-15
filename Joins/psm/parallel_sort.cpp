#include "parallel_sort.h"
#include "utility.h"

namespace internal
{
    std::size_t g_depth = 0L;
    const std::size_t cutoff = 1000000L;

    void qsort3w(row_t* _First, row_t* _Last)
    {
        if (_First >= _Last) return;

        std::size_t _Size = 0L; g_depth++;
        if ((_Size = std::distance(_First, _Last)) > 0)
        {
            row_t *_LeftIt = _First, *_RightIt = _Last;
            bool is_swapped_left = false, is_swapped_right = false;
            typename std::iterator_traits<row_t*>::value_type _Pivot = *_First;

            row_t *_FwdIt = _First + 1;
            while (_FwdIt <= _RightIt)
            {
                if (misc::compare(*_FwdIt, _Pivot))
                {
                    is_swapped_left = true;
                    std::iter_swap(_LeftIt, _FwdIt);
                    _LeftIt++; _FwdIt++;
                }

                else if (misc::compare(_Pivot, *_FwdIt)) {
                    is_swapped_right = true;
                    std::iter_swap(_RightIt, _FwdIt);
                    _RightIt--;
                }

                else _FwdIt++;
            }

            if (_Size >= internal::cutoff)
            {
                #pragma omp taskgroup
                {
                    #pragma omp task untied mergeable
                    if ((std::distance(_First, _LeftIt) > 0) && (is_swapped_left))
                        qsort3w(_First, _LeftIt - 1);

                    #pragma omp task untied mergeable
                    if ((std::distance(_RightIt, _Last) > 0) && (is_swapped_right))
                        qsort3w(_RightIt + 1, _Last);
                }
            }

            else
            {
                #pragma omp task untied mergeable
                {
                    if ((std::distance(_First, _LeftIt) > 0) && is_swapped_left)
                        qsort3w(_First, _LeftIt - 1);

                    if ((std::distance(_RightIt, _Last) > 0) && is_swapped_right)
                        qsort3w(_RightIt + 1, _Last);
                }
            }
        }
    }

    void parallel_sort(row_t* _First, row_t* _Last, int nthreads)
    {
//        std::size_t pos = 0L;
        g_depth = 0L;
//		if (!misc::sorted(_First, _Last, pos))
        #pragma omp parallel num_threads(nthreads)
        #pragma omp master
        internal::qsort3w(_First, _Last - 1);

    }

    void psort(relation_t *relation, int nthreads)
    {
        uint64_t size = relation->num_tuples;
        parallel_sort(&relation->tuples[0], &relation->tuples[size], nthreads);
    }
}