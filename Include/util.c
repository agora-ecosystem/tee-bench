#include "data-types.h"
#include <stdlib.h>
#ifdef NATIVE_COMPILATION
#include "Logger.h"
#include "native_ocalls.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

void malloc_check_ex(void * ptr, char const * file, char const * function, int line)
{
    if (ptr == nullptr)
    {
        logger(ERROR, "%s:%s:%d Failed to allocate memory", file, function, line);
        ocall_exit(EXIT_FAILURE);
    }
}

void insert_output(output_list_t ** head, type_key key, type_value Rpayload, type_value Spayload)
{
    output_list_t * row = (output_list_t *) malloc(sizeof(output_list_t));
    malloc_check_ex(row, __FILE__, __FUNCTION__, __LINE__);
    row->key = key;
    row->Rpayload = Rpayload;
    row->Spayload = Spayload;
    row->next = *head;
    *head = row;
}

uint64_t sizeof_output(output_list_t * list)
{
    uint64_t size = 0;
    output_list_t * cur = list;
    while (cur != nullptr)
    {
        size++;
        cur = cur->next;
    }
    return size * sizeof(output_list_t);
}

uint64_t sizeof_result(result_t * result)
{
    uint64_t size = sizeof(result->totalresults) +
                    sizeof(result->nthreads);
    for (int i = 0; i < result->nthreads; i++)
    {
        size += sizeof(result->resultlist[i].nresults);
        size += sizeof(result->resultlist[i].threadid);
//        size += sizeof_output(result->resultlist[i].results);
        size += result->resultlist[i].nresults * sizeof(output_list_t);
    }
    return size;
}
