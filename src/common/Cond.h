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


#ifndef CEPH_COND_H
#define CEPH_COND_H

#include <time.h>
#include <pthread.h>

#include "include/Context.h"

#include "common/ceph_time.h"
#include "common/Mutex.h"
#include "common/Clock.h"

//实现对pthread_cond_t的封装,未提供多余的功能
//封装的不怎么好,接口存在重复,功能不单一.
class Cond {
  // my bits
  pthread_cond_t _c;

  //条件变量关联的锁，wait时注入
  Mutex *waiter_mutex;

  // don't allow copying.
  //禁用此两者
  void operator=(Cond &C);
  Cond(const Cond &C);

 public:
  //初始化条件变量，注入条件变量对应的锁
  Cond() : waiter_mutex(NULL) {
    int r = pthread_cond_init(&_c,NULL);
    assert(r == 0);
  }

  //销毁条件变量
  virtual ~Cond() { 
    pthread_cond_destroy(&_c); 
  }

  //等待条件达成
  int Wait(Mutex &mutex)  { 
    // make sure this cond is used with one mutex only
	//必须未给值或者必须为同一把锁
    assert(waiter_mutex == NULL || waiter_mutex == &mutex);
    waiter_mutex = &mutex;

    //条件变更等待时，必须是加锁的
    assert(mutex.is_locked());

    mutex._pre_unlock();
    int r = pthread_cond_wait(&_c, &mutex._m);//等待条件
    mutex._post_lock();
    return r;
  }

  //有超时功能的条件等待（采用pthread_cond_timedwait实现）
  int WaitUntil(Mutex &mutex, utime_t when) {
    // make sure this cond is used with one mutex only
    assert(waiter_mutex == NULL || waiter_mutex == &mutex);
    waiter_mutex = &mutex;

    assert(mutex.is_locked());

    struct timespec ts;
    when.to_timespec(&ts);

    mutex._pre_unlock();
    int r = pthread_cond_timedwait(&_c, &mutex._m, &ts);
    mutex._post_lock();

    return r;
  }

  //等待信号量，并设置等待的最大间隔
  int WaitInterval(Mutex &mutex, utime_t interval) {
    utime_t when = ceph_clock_now();
    when += interval;
    return WaitUntil(mutex, when);
  }

  //等待信号量，并设置等待的最大间隔
  template<typename Duration>
  int WaitInterval(Mutex &mutex, Duration interval) {
    ceph::real_time when(ceph::real_clock::now());
    when += interval;

    struct timespec ts = ceph::real_clock::to_timespec(when);

    mutex._pre_unlock();
    int r = pthread_cond_timedwait(&_c, &mutex._m, &ts);
    mutex._post_lock();

    return r;
  }

  //唤醒所有等待此信号量的线程
  int SloppySignal() { 
    int r = pthread_cond_broadcast(&_c);
    return r;
  }

  //确认waiter有锁的情况下唤醒等待此信号量的所有线程
  int Signal() { 
    // make sure signaler is holding the waiter's lock.
    assert(waiter_mutex == NULL ||
	   waiter_mutex->is_locked());

    int r = pthread_cond_broadcast(&_c);
    return r;
  }

  //确认waiter有锁的情况下唤醒等待此信号量的一个线程
  int SignalOne() { 
    // make sure signaler is holding the waiter's lock.
    assert(waiter_mutex == NULL ||
	   waiter_mutex->is_locked());

    int r = pthread_cond_signal(&_c);
    return r;
  }

  //与signal实现相同，属于多余接口（确认waiter有锁的情况下唤醒等待此信号量的所有线程）
  int SignalAll() { 
    // make sure signaler is holding the waiter's lock.
    assert(waiter_mutex == NULL ||
	   waiter_mutex->is_locked());

    int r = pthread_cond_broadcast(&_c);
    return r;
  }
};

/**
 * context to signal a cond
 *
 * Generic context to signal a cond and store the return value.  We
 * assume the caller is holding the appropriate lock.
 */
//注入式的无锁cond-context
//当finish被调用时，对应的信号量，将唤醒所有线程
class C_Cond : public Context {
  Cond *cond;   ///< Cond to signal
  bool *done;   ///< true if finish() has been called
  int *rval;    ///< return value
public:
  C_Cond(Cond *c, bool *d, int *r) : cond(c), done(d), rval(r) {
    *done = false;
  }
  void finish(int r) override {
    *done = true;
    *rval = r;
    cond->Signal();
  }
};

/**
 * context to signal a cond, protected by a lock
 *
 * Generic context to signal a cond under a specific lock. We take the
 * lock in the finish() callback, so the finish() caller must not
 * already hold it.
 */
//注入式的有锁cond-context
class C_SafeCond : public Context {
  Mutex *lock;    ///< Mutex to take
  Cond *cond;     ///< Cond to signal
  bool *done;     ///< true after finish() has been called
  int *rval;      ///< return value (optional)
public:
  C_SafeCond(Mutex *l, Cond *c, bool *d, int *r=0) : lock(l), cond(c), done(d), rval(r) {
    *done = false;
  }
  void finish(int r) override {
    lock->Lock();
    if (rval)
      *rval = r;
    *done = true;
    cond->Signal();
    lock->Unlock();
  }
};

/**
 * Context providing a simple wait() mechanism to wait for completion
 *
 * The context will not be deleted as part of complete and must live
 * until wait() returns.
 */
//非注入式的cond-context
class C_SaferCond : public Context {
  Mutex lock;    ///< Mutex to take
  Cond cond;     ///< Cond to signal
  bool done;     ///< true after finish() has been called
  int rval;      ///< return value
public:
  C_SaferCond() : lock("C_SaferCond"), done(false), rval(0) {}
  void finish(int r) override { complete(r); }

  /// We overload complete in order to not delete the context
  void complete(int r) override {
    Mutex::Locker l(lock);
    done = true;
    rval = r;
    cond.Signal();
  }

  /// Returns rval once the Context is called
  //等待信号量
  int wait() {
    Mutex::Locker l(lock);
    while (!done)
      cond.Wait(lock);
    return rval;
  }
};

#endif
