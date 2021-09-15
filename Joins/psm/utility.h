#include "data-types.h"
#include <cstddef>

#ifndef UTILITY_STL_H
#define UTILITY_STL_H

namespace misc
{
    inline bool compare(row_t first, row_t end)
    {
        return first.key < end.key;
    }

    std::size_t _sorted(row_t *_First, row_t *_Last, std::size_t& position);

    std::size_t sorted(relation_t *rel, std::size_t& position);
}

#endif // UTILITY_STL_H
