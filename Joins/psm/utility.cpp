#include "utility.h"

#include <iterator>

namespace misc
{
    std::size_t _sorted(row_t *_First, row_t *_Last, std::size_t& position)
    {
        bool is_sorted = true;
        row_t *_MidIt = _First + (_Last - _First) / 2;
        if (compare(*_MidIt, *_First) || compare(*(_Last - 1), *_MidIt) ||
            compare(*(_Last - 1), *_First)) return !is_sorted;

        for (auto _FwdIt = _First; _FwdIt != _Last - 1 && is_sorted; _FwdIt++)
        {
            if (compare(*(_FwdIt + 1), *_FwdIt))
            {
                if (is_sorted == true)
                    position = std::distance(_First, _FwdIt);

                is_sorted = false;
            }
        }

        return is_sorted;
    }

    std::size_t sorted(relation_t *rel, std::size_t& position)
    {
        uint64_t size = rel->num_tuples;
        return _sorted(&rel->tuples[0], &rel->tuples[size], position);
    }
}