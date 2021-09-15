#ifndef OBLIDB_JOIN_H
#define OBLIDB_JOIN_H
#include <stdlib.h>

result_t* oblidb_join (struct table_t *relR, struct table_t *relS, int nthreads);

#endif //OBLIDB_JOIN_H
