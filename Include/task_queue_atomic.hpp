#ifndef TASK_QUEUE_ATOMIC_HPP
#define TASK_QUEUE_ATOMIC_HPP

#include <stdlib.h>
#include <pthread.h>
#include "data-types.h"
#include <atomic>
#ifdef NATIVE_COMPILATION
#include "Logger.h"
#include "native_ocalls.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

typedef struct task_atomic_t {
    struct table_t relR;
    struct table_t tmpR;
    struct table_t relS;
    struct table_t tmpS;
} task_atomic_t;

typedef struct task_queue_atomic_t {
    task_atomic_t *  tasks;
    int32_t          count;
    std::atomic<int> added;
    std::atomic<int> processed;
} task_queue_atomic_t;

inline task_atomic_t *
task_queue_atomic_get_atomic(struct task_queue_atomic_t * tq)
        __attribute__((always_inline));

inline void
task_queue_atomic_add(task_queue_atomic_t * tq, task_atomic_t * t) __attribute__((always_inline));

/* initialize a task queue with given allocation block size */
task_queue_atomic_t *
task_queue_atomic_init(int alloc_size);

void
task_queue_atomic_free(task_queue_atomic_t * tq);
/****************** Definitions *******************************/

inline task_atomic_t *
task_queue_atomic_get_atomic(struct task_queue_atomic_t * tq)
{
    int part = tq->processed++;
    if (part >= tq->count) {
        return 0;
    }
    return &(tq->tasks[part]);
}

inline void
task_queue_atomic_add(task_queue_atomic_t * tq, task_atomic_t * t)
{
    int part = tq->added++;
    if (part >= tq->count) {
        logger(ERROR, "Task queue add out of range. Part: %d", part);
        ocall_exit(EXIT_FAILURE);
    }
    tq->tasks[part] = *t;
}

inline task_atomic_t *
task_queue_atomic_get_slot(task_queue_atomic_t * tq)
{
    int part = tq->added++;
    if (part >= tq->count) {
        logger(ERROR, "Task queue get slot out of range. Part: %d", part);
        ocall_exit(EXIT_FAILURE);
    }
    return &(tq->tasks[part]);
}

/* initialize a task queue with given allocation block size */
task_queue_atomic_t *
task_queue_atomic_init(int alloc_size)
{
    task_queue_atomic_t * ret = (task_queue_atomic_t*) malloc (sizeof(task_queue_atomic_t));
    ret->tasks = (task_atomic_t*) malloc (alloc_size * sizeof(task_atomic_t));
    ret->count = alloc_size;
    ret->added = 0;
    ret->processed = 0;
    return ret;
}

void
task_queue_atomic_free(task_queue_atomic_t * tq)
{
    free(tq->tasks);
    free(tq);
}
#endif //TASK_QUEUE_ATOMIC_HPP
