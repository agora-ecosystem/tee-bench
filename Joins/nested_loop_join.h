#ifndef _NESTED_LOO_JOIN_H_
#define _NESTED_LOO_JOIN_H_

result_t* NL (struct table_t* relR, struct table_t* relS, int nthreads);

result_t* INL (struct table_t* relR, struct table_t* relS, int nthreads);

#endif // _NESTED_LOO_JOIN_H_
