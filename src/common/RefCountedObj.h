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

#ifndef CEPH_REFCOUNTEDOBJ_H
#define CEPH_REFCOUNTEDOBJ_H
 
#include "common/Mutex.h"
#include "common/Cond.h"
#include "include/atomic.h"
#include "common/ceph_context.h"
#include "common/valgrind.h"

//实现简单的引用计数功能,通过继承此对象,可使得子类获得引入计数功能.
struct RefCountedObject {
private:
  mutable atomic_t nref;
  CephContext *cct;//输出log用
public:
  RefCountedObject(CephContext *c = NULL, int n=1) : nref(n), cct(c) {}
  virtual ~RefCountedObject() {
	//断言，销毁时，必须为0
    assert(nref.read() == 0);
  }
  
  //get时增加引用计数
  const RefCountedObject *get() const {
    int v = nref.inc();
    if (cct)
      lsubdout(cct, refs, 1) << "RefCountedObject::get " << this << " "
			     << (v - 1) << " -> " << v
			     << dendl;
    return this;
  }

  //get时增加引用计数
  RefCountedObject *get() {
    int v = nref.inc();
    if (cct)
      lsubdout(cct, refs, 1) << "RefCountedObject::get " << this << " "
			     << (v - 1) << " -> " << v
			     << dendl;
    return this;
  }

  //put时减少引用计数，如果引用计数为0，则释放对象
  void put() const {
    CephContext *local_cct = cct;
    int v = nref.dec();
    if (v == 0) {
      ANNOTATE_HAPPENS_AFTER(&nref);
      ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&nref);
      delete this;//为0，释放
    } else {
      ANNOTATE_HAPPENS_BEFORE(&nref);
    }
    if (local_cct)
      lsubdout(local_cct, refs, 1) << "RefCountedObject::put " << this << " "
				   << (v + 1) << " -> " << v
				   << dendl;
  }

  //这个是为引入计数类添加的多余功能（估计是补救方便的处理）
  void set_cct(CephContext *c) {
    cct = c;
  }

  //获取当前的引用数
  uint64_t get_nref() const {
    return nref.read();
  }
};

/**
 * RefCountedCond
 *
 *  a refcounted condition, will be removed when all references are dropped
 */

//含有引用功能的信号量
struct RefCountedCond : public RefCountedObject {
  bool complete;
  Mutex lock;//信号量关联的锁
  Cond cond;//信号量
  int rval;

  RefCountedCond() : complete(false), lock("RefCountedCond"), rval(0) {}

  int wait() {
	//加锁
    Mutex::Locker l(lock);

    //如果complete不为true,则恒在条件变量上等待
    while (!complete) {
      cond.Wait(lock);
    }
    return rval;
  }

  //加锁指定complete为true,并唤醒所有线程
  void done(int r) {
    Mutex::Locker l(lock);
    rval = r;
    complete = true;
    cond.SignalAll();
  }

  //调用done,并使其返回值为0
  void done() {
    done(0);
  }
};

/**
 * RefCountedWaitObject
 *
 * refcounted object that allows waiting for the object's last reference.
 * Any referrer can either put or put_wait(). A simple put() will return
 * immediately, a put_wait() will return only when the object is destroyed.
 * e.g., useful when we want to wait for a specific event completion. We
 * use RefCountedCond, as the condition can be referenced after the object
 * destruction. 
 *    
 */
struct RefCountedWaitObject {
  atomic_t nref;
  RefCountedCond *c;

  //构造时，创建引用计数功能的条件变量
  RefCountedWaitObject() : nref(1) {
    c = new RefCountedCond;
  }
  virtual ~RefCountedWaitObject() {
    c->put();
  }

  RefCountedWaitObject *get() {
    nref.inc();
    return this;
  }

  //put时，返回true,变更已销毁
  bool put() {
    bool ret = false;
    RefCountedCond *cond = c;
    cond->get();
    //如果是最后一个
    if (nref.dec() == 0) {
      cond->done();//指明完成
      delete this;
      ret = true;
    }
    cond->put();
    return ret;
  }

  //待待信号量唤醒
  void put_wait() {
    RefCountedCond *cond = c;

    cond->get();
    if (nref.dec() == 0) {
      cond->done();
      delete this;
    } else {
      cond->wait();
    }
    cond->put();
  }
};

//增加引用
void intrusive_ptr_add_ref(const RefCountedObject *p);
//减少引用
void intrusive_ptr_release(const RefCountedObject *p);

#endif
