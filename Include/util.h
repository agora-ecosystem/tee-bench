#ifndef UTIL_H
#define UTIL_H

#include "data-types.h"
#include <stdlib.h>
#ifdef NATIVE_COMPILATION
#include "Logger.h"
#else
#include "Enclave.h"
#endif

#define malloc_check(ptr) malloc_check_ex(ptr, __FILE__, __FUNCTION__, __LINE__);

void malloc_check_ex(void * ptr, char const * file, char const * function, int line);

void insert_output(output_list_t ** head, type_key key, type_value Rpayload, type_value Spayload);

uint64_t sizeof_output(output_list_t * list);

uint64_t sizeof_result(result_t * result);

#endif
