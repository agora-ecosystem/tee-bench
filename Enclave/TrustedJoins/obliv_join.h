#ifndef OBLIV_JOIN_H
#define OBLIV_JOIN_H

#include "layout.h"
#include "sort.h"
#include "trace_mem.h"
#include "data-types.h"


result_t* OJ_wrapper(struct table_t *relR, struct table_t *relS, int nthreads);

/*
 * t: concatenated input table
 * t0, t1: aligned output tables
 *
 * timers:
 * 1. augment tables
 * 2. expand tables
 * 3. align table
 * 4. collect values
*/
void join(Table &t, Table &t0, Table &t1, struct timers_t &timers);

#endif //OBLIV_JOIN_H