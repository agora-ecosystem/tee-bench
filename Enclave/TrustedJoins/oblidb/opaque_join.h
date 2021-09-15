#ifndef OPAQUE_JOIN_H
#define OPAQUE_JOIN_H

#include <stdint.h>
#include "data-types.h"

result_t* opaque_join (struct table_t *relR, struct table_t *relS, int nthreads);

int printTableCheating(char* tableName);

#endif //OPAQUE_JOIN_H
