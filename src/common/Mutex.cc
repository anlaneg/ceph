// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/Mutex.h"
#include "common/perf_counters.h"
#include "common/config.h"
#include "common/Clock.h"
#include "common/valgrind.h"

Mutex::Mutex(const std::string &n, bool r, bool ld,
	     bool bt) :
  name(n), id(-1), recursive(r), lockdep(ld), backtrace(bt), nlock(0),
  locked_by(0)
{
  ANNOTATE_BENIGN_RACE_SIZED(&id, sizeof(id), "Mutex lockdep id");
  ANNOTATE_BENIGN_RACE_SIZED(&nlock, sizeof(nlock), "Mutex nlock");
  ANNOTATE_BENIGN_RACE_SIZED(&locked_by, sizeof(locked_by), "Mutex locked_by");
  if (recursive) {
    // Mutexes of type PTHREAD_MUTEX_RECURSIVE do all the same checks as
    // mutexes of type PTHREAD_MUTEX_ERRORCHECK.
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    //如果一个线程对这种类型的互斥锁重复上锁，不会引起死锁，
    //一个线程对这类互斥锁的多次重复上锁必须由这个线程来重复相同数量的解锁，
    //这样才能解开这个互斥锁，别的线程才能得到这个互斥锁。如果试图解锁一个由别的线程锁定的互斥锁将会返回一个错误代码。
    //如果一个线程试图解锁已经被解锁的互斥锁也将会返回一个错误代码。
    //这种类型的互斥锁只能是进程私有的（作用域属性为PTHREAD_PROCESS_PRIVATE）。
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&_m,&attr);
    pthread_mutexattr_destroy(&attr);
    if (lockdep && g_lockdep)
      _register();
  }
  else if (lockdep) {
    // If the mutex type is PTHREAD_MUTEX_ERRORCHECK, then error checking
    // shall be provided. If a thread attempts to relock a mutex that it
    // has already locked, an error shall be returned. If a thread
    // attempts to unlock a mutex that it has not locked or a mutex which
    // is unlocked, an error shall be returned.
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    //这种类型的互斥锁会自动检测死锁。如果一个线程试图对一个互斥锁重复锁定，
    //将会返回一个错误代码。如果试图解锁一个由别的线程锁定的互斥锁将会返回一个错误代码。
    //如果一个线程试图解锁已经被解锁的互斥锁也将会返回一个错误代码。
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
    pthread_mutex_init(&_m, &attr);
    pthread_mutexattr_destroy(&attr);
    if (g_lockdep)
      _register();
  }
  else {
    // If the mutex type is PTHREAD_MUTEX_DEFAULT, attempting to recursively
    // lock the mutex results in undefined behavior. Attempting to unlock the
    // mutex if it was not locked by the calling thread results in undefined
    // behavior. Attempting to unlock the mutex if it is not locked results in
    // undefined behavior.
    pthread_mutex_init(&_m, NULL);
  }
}

Mutex::~Mutex() {
  ceph_assert(nlock == 0);

  // helgrind gets confused by condition wakeups leading to mutex destruction
  ANNOTATE_BENIGN_RACE_SIZED(&_m, sizeof(_m), "Mutex primitive");
  pthread_mutex_destroy(&_m);

  if (lockdep && g_lockdep) {
    lockdep_unregister(id);
  }
}

void Mutex::lock(bool no_lockdep)
{
  //开启了锁错误检测，并且全局变量容许锁错误检测，并且本函数未明确规定不做
  //错误检测，则进行锁错误检测

  if (lockdep && g_lockdep && !no_lockdep && !recursive) _will_lock();
  int r = pthread_mutex_lock(&_m);
  ceph_assert(r == 0);
  if (lockdep && g_lockdep) _locked();
  _post_lock();
}

void Mutex::unlock()
{
  _pre_unlock();
  if (lockdep && g_lockdep) _will_unlock();
  int r = pthread_mutex_unlock(&_m);
  ceph_assert(r == 0);
}
