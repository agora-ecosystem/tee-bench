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


#ifndef _BARRIER_H_
#define _BARRIER_H_

#include <pthread.h>

/* C++ object-oriented barriers -- use Barrier.C */
class PThreadLockCVBarrier {
public:
  PThreadLockCVBarrier( int nThreads ); 
  ~PThreadLockCVBarrier();

  void Arrive();

private:
  int             m_nThreads;
  pthread_mutex_t m_l_SyncLock;
  pthread_cond_t  m_cv_SyncCV;
  volatile int    m_nSyncCount;
};

#endif

