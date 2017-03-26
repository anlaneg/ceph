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

#ifndef CEPH_MUTEX_H
#define CEPH_MUTEX_H

#include "include/assert.h"
#include "lockdep.h"
#include "common/ceph_context.h"

#include <pthread.h>

using namespace ceph;

class PerfCounters;

enum {
  l_mutex_first = 999082,
  l_mutex_wait,
  l_mutex_last
};

//实现了对pthread_mutex_t加解锁的封装,在封闭的基础上提供了简单的
//调试机制.（支持在启用锁检测机制的情况下进行检测）
class Mutex {
private:
  std::string name;//名称
  int id;//锁对应的id号（错误检测时有用）
  bool recursive;//是否支持递归加锁
  bool lockdep;//是否支持锁错误检测
  //是否收集堆栈
  bool backtrace;  // gather backtrace on lock acquisition

  pthread_mutex_t _m;//锁（被封装的）
  int nlock;//记录被索的次数（当容许递归加锁时有用）
  pthread_t locked_by;//哪个线程拥有这把锁
  CephContext *cct;
  PerfCounters *logger;

  // don't allow copying.
  void operator=(const Mutex &M);
  Mutex(const Mutex &M);

  void _register() {
	//注册当前锁
    id = lockdep_register(name.c_str());
  }

  //开启检测时，在准备拿锁时调用
  void _will_lock() { // about to lock
	//监测入口
    id = lockdep_will_lock(name.c_str(), id, backtrace);
  }

  //开启检测时，在拿到锁时调用
  void _locked() {    // just locked
    id = lockdep_locked(name.c_str(), id, backtrace);
  }

  //开启检测时，在准备解锁时调用
  void _will_unlock() {  // about to unlock
    id = lockdep_will_unlock(name.c_str(), id);
  }

public:
  Mutex(const std::string &n, bool r = false, bool ld=true, bool bt=false,
	CephContext *cct = 0);
  ~Mutex();

  //是否处理被锁状态
  bool is_locked() const {
    return (nlock > 0);
  }

  //是否被当前线程锁住了
  bool is_locked_by_me() const {
    return nlock > 0 && locked_by == pthread_self();
  }

  //尝试获取锁
  bool TryLock() {
    int r = pthread_mutex_trylock(&_m);
    if (r == 0) {
      if (lockdep && g_lockdep) _locked();
      _post_lock();
    }
    return r == 0;
  }

  //加锁
  void Lock(bool no_lockdep=false);

  //加锁后运行
  void _post_lock() {
    if (!recursive) {
      //如果不支持递归加锁，则加锁时，需要
      //更新持有锁的线程
      assert(nlock == 0);
      locked_by = pthread_self();
    };
    //锁数加1
    nlock++;
  }

  //解锁前运行
  void _pre_unlock() {
    assert(nlock > 0);
    --nlock;//锁数减1，用于跟踪递归加锁
    if (!recursive) {
      //如果不支持递归加锁，则解锁时，需要确保
      //一定是本线程持有这把所
      assert(locked_by == pthread_self());
      locked_by = 0;//清空持有人
      assert(nlock == 0);
    }
  }

  //解锁
  void Unlock();

  friend class Cond;


public:
  //定义Locker类，实现在构造函数中加锁，在析构函数中解锁
  class Locker {
    Mutex &mutex;

  public:
    explicit Locker(Mutex& m) : mutex(m) {
      mutex.Lock();
    }
    ~Locker() {
      mutex.Unlock();
    }
  };
};


#endif
