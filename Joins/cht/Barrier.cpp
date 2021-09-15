/*
    Copyright 2011, Dan Gibson.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include "Barrier.h"
#include <stdio.h>

#ifdef NATIVE_COMPILATION
#include "Logger.h"
#include "native_ocalls.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

PThreadLockCVBarrier::PThreadLockCVBarrier( int nThreads ) {

  if( nThreads < 1 ) {
      ocall_throw("Invalid number of threads for barrier (<1)");
  }

  m_nThreads = nThreads;

  int ret;
  ret = pthread_mutex_init(&m_l_SyncLock, NULL);
//  if(ret!=0) printf("pthread_mutex_init failed at barrier creation.");
  if(ret!=0) ocall_throw("pthread_mutex_init failed at barrier creation.");

  ret = pthread_cond_init(&m_cv_SyncCV, NULL);
//  if(ret!=0) printf("pthread_cond_init failed at barrier creation.");
  if(ret!=0) ocall_throw("pthread_cond_init failed at barrier creation.");

  m_nSyncCount = 0;
}

PThreadLockCVBarrier::~PThreadLockCVBarrier() {
  pthread_mutex_destroy(&m_l_SyncLock);
  pthread_cond_destroy(&m_cv_SyncCV);
}

void PThreadLockCVBarrier::Arrive() {
  pthread_mutex_lock(&m_l_SyncLock);
  m_nSyncCount++;
  if(m_nSyncCount == m_nThreads) {
    pthread_cond_broadcast(&m_cv_SyncCV);
    m_nSyncCount = 0;
  } else {
    pthread_cond_wait(&m_cv_SyncCV, &m_l_SyncLock);
  }

  pthread_mutex_unlock(&m_l_SyncLock);
  
}

