/**
 * @file    barrier.h
 * @author  Cagri Balkesen <cagri.balkesen@inf.ethz.ch>
 * @date    Wed Aug  1 14:26:56 2012
 * @version $Id: barrier.h 3017 2012-12-07 10:56:20Z bcagri $
 * 
 * @brief  Barrier implementation, defaults to Pthreads. On Mac custom
 * implementation since barriers are not included in Pthreads.
 *
 * (c) 2012, ETH Zurich, Systems Group
 *
 */
#ifndef BARRIER_H
#define BARRIER_H

#include <pthread.h>            /* pthread_* */
#include <stdlib.h>

#ifndef HAVE_PTHREAD_BARRIER_WAIT /* If POSIX BARRIERS are not supported */

#define pthread_barrier_t           barrier_t
#define pthread_barrier_attr_t      barrier_attr_t
#define pthread_barrier_init(b,a,n) barrier_init(b,n)
//#define pthread_barrier_destroy(b)  barrier_destroy(b)
#define pthread_barrier_wait(b)     barrier_wait(b)

#ifndef PTHREAD_BARRIER_SERIAL_THREAD
#define PTHREAD_BARRIER_SERIAL_THREAD 1
#endif

typedef struct {
    int             needed;
    int             called;
    pthread_mutex_t mutex;
    pthread_cond_t  cond;
} barrier_t;

static int barrier_init(barrier_t *barrier,int needed);
//static int barrier_destroy(barrier_t *barrier);
static int barrier_wait(barrier_t *barrier);
static void barrier_arrive(pthread_barrier_t* b, int rv);

int 
barrier_init(barrier_t *barrier,int needed)
{
    barrier->needed = needed;
    barrier->called = 0;
    pthread_mutex_init(&barrier->mutex,NULL);
    pthread_cond_init(&barrier->cond,NULL);

    return 0;
}

//int
//barrier_destroy(barrier_t *barrier)
//{
//    pthread_mutex_destroy(&barrier->mutex);
//    pthread_cond_destroy(&barrier->cond);
//
//    return 0;
//}

int 
barrier_wait(barrier_t *barrier)
{
    pthread_mutex_lock(&barrier->mutex);
    barrier->called++;

    if (barrier->called == barrier->needed) {
        barrier->called = 0;
        pthread_cond_broadcast(&barrier->cond);
    } else {
        pthread_cond_wait(&barrier->cond,&barrier->mutex);
    }

    pthread_mutex_unlock(&barrier->mutex);

    return 0;
}

void barrier_arrive(pthread_barrier_t* b, int rv) {
    rv = pthread_barrier_wait(b);
    if (rv != 0 && rv != 1) {
        std::cout<<"Couldn't wait on barrier" << std::endl;
        exit(EXIT_FAILURE);
    }
}

#endif

#endif /* BARRIER_H */
