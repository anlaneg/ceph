// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2008-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "lockdep.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/valgrind.h"

/******* Constants **********/
#define lockdep_dout(v) lsubdout(g_lockdep_ceph_ctx, lockdep, v)
#define MAX_LOCKS  4096   // increase me as needed
#define BACKTRACE_SKIP 2

/******* Globals **********/
//锁错误检测变量，用于标记是否全局禁止锁错误检测
bool g_lockdep;
struct lockdep_stopper_t {
  // disable lockdep when this module destructs.
  ~lockdep_stopper_t() {
    g_lockdep = 0;
  }
};

//锁错误检测需要的保护机制（一把保护下面多个结构的锁）
static pthread_mutex_t lockdep_mutex = PTHREAD_MUTEX_INITIALIZER;
static CephContext *g_lockdep_ceph_ctx = NULL;
static lockdep_stopper_t lockdep_stopper;
//锁名称到锁id的映射表
static ceph::unordered_map<std::string, int> lock_ids;
//锁id到锁名称的映射表
static map<int, std::string> lock_names;
//记录锁id（first)被引用的数量(second)
static map<int, int> lock_refs;
//bitmap用一个位来表示指定id是否空闲
static char free_ids[MAX_LOCKS/8]; // bit set = free
//记录每个线程的锁占用情况
static ceph::unordered_map<pthread_t, map<int,BackTrace*> > held;
//follows[a][b]用于记录，a锁在拥有的情况下，请求拥有b*8锁（假设flows[a][b] == 0x1)
static char follows[MAX_LOCKS][MAX_LOCKS/8]; // follows[a][b] means b taken after a
static BackTrace *follows_bt[MAX_LOCKS][MAX_LOCKS];
//记录当前已分配出去的最大id号
unsigned current_maxid;
//为锁分配空闲id用，记录上次发现的空闲id
int last_freed_id = -1;
static bool free_ids_inited;

//检测系统配置是否要求强制收集堆栈
static bool lockdep_force_backtrace()
{
  return (g_lockdep_ceph_ctx != NULL &&
          g_lockdep_ceph_ctx->_conf->lockdep_force_backtrace);
}

/******* Functions **********/
//启用锁检测机制
void lockdep_register_ceph_context(CephContext *cct)
{
  static_assert((MAX_LOCKS > 0) && (MAX_LOCKS % 8 == 0),                   
    "lockdep's MAX_LOCKS needs to be divisible by 8 to operate correctly.");
  pthread_mutex_lock(&lockdep_mutex);
  if (g_lockdep_ceph_ctx == NULL) {
    ANNOTATE_BENIGN_RACE_SIZED(&g_lockdep_ceph_ctx, sizeof(g_lockdep_ceph_ctx),
                               "lockdep cct");
    ANNOTATE_BENIGN_RACE_SIZED(&g_lockdep, sizeof(g_lockdep),
                               "lockdep enabled");
    g_lockdep = true;
    g_lockdep_ceph_ctx = cct;
    lockdep_dout(1) << "lockdep start" << dendl;
    if (!free_ids_inited) {
      free_ids_inited = true;
      memset((void*) &free_ids[0], 255, sizeof(free_ids));
    }
  }
  pthread_mutex_unlock(&lockdep_mutex);
}

//禁用锁检测机制
void lockdep_unregister_ceph_context(CephContext *cct)
{
  pthread_mutex_lock(&lockdep_mutex);
  if (cct == g_lockdep_ceph_ctx) {
    lockdep_dout(1) << "lockdep stop" << dendl;
    // this cct is going away; shut it down!
    g_lockdep = false;
    g_lockdep_ceph_ctx = NULL;

    // blow away all of our state, too, in case it starts up again.
    for (unsigned i = 0; i < current_maxid; ++i) {
      for (unsigned j = 0; j < current_maxid; ++j) {
        delete follows_bt[i][j];
      }
    }

    //清空，以备下次使用
    held.clear();
    lock_names.clear();
    lock_ids.clear();
    memset((void*)&follows[0][0], 0, current_maxid * MAX_LOCKS/8);
    memset((void*)&follows_bt[0][0], 0, sizeof(BackTrace*) * current_maxid * MAX_LOCKS);
  }
  pthread_mutex_unlock(&lockdep_mutex);
}

//显示各线程拥有的锁情况
int lockdep_dump_locks()
{
  pthread_mutex_lock(&lockdep_mutex);
  if (!g_lockdep)
    goto out;

  for (ceph::unordered_map<pthread_t, map<int,BackTrace*> >::iterator p = held.begin();
       p != held.end();
       ++p) {
    lockdep_dout(0) << "--- thread " << p->first << " ---" << dendl;
    for (map<int,BackTrace*>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      lockdep_dout(0) << "  * " << lock_names[q->first] << "\n";
      if (q->second)
	*_dout << *(q->second);
      *_dout << dendl;
    }
  }
out:
  pthread_mutex_unlock(&lockdep_mutex);
  return 0;
}

//分配锁对应的空闲id
int lockdep_get_free_id(void)
{
  // if there's id known to be freed lately, reuse it
  //我们记录有上次空闲的id号，则从它的位置开始，检测是否仍有空闲
  if ((last_freed_id >= 0) && 
     (free_ids[last_freed_id/8] & (1 << (last_freed_id % 8)))) {
    int tmp = last_freed_id;
    last_freed_id = -1;//空闲的被用了
    free_ids[tmp/8] &= 255 - (1 << (tmp % 8));//将指定位清0
    lockdep_dout(1) << "lockdep reusing last freed id " << tmp << dendl;
    return tmp;//返回找到的空闲id
  }
  
  // walk through entire array and locate nonzero char, then find
  // actual bit.
  //遍历查找空闲id
  for (int i = 0; i < MAX_LOCKS / 8; ++i) {
    if (free_ids[i] != 0) {
      for (int j = 0; j < 8; ++j) {
        if (free_ids[i] & (1 << j)) {
          free_ids[i] &= 255 - (1 << j);
          lockdep_dout(1) << "lockdep using id " << i * 8 + j << dendl;
          return i * 8 + j;
        }
      }
    }
  }
  
  // not found
  //无空闲的
  lockdep_dout(0) << "failing miserably..." << dendl;
  return -1;
}

//按名称注册锁
static int _lockdep_register(const char *name)
{
  int id = -1;

  if (!g_lockdep)
    return id;

  //首先用名称查找这把锁对应的id
  ceph::unordered_map<std::string, int>::iterator p = lock_ids.find(name);

  if (p == lock_ids.end()) {
	//没有找到这把锁对应的id,为其分配一个id号
    id = lockdep_get_free_id();
    if (id < 0) {
      //分配id号失败，挂掉
      lockdep_dout(0) << "ERROR OUT OF IDS .. have 0"
		      << " max " << MAX_LOCKS << dendl;
      for (auto& p : lock_names) {
	lockdep_dout(0) << "  lock " << p.first << " " << p.second << dendl;
      }
      ceph_abort();
    }

    //更新当前已分配出去的最大id号
    if (current_maxid <= (unsigned)id) {
        current_maxid = (unsigned)id + 1;
    }

    //更新映射表
    lock_ids[name] = id;
    lock_names[id] = name;
    lockdep_dout(10) << "registered '" << name << "' as " << id << dendl;
  } else {
    id = p->second;
    lockdep_dout(20) << "had '" << name << "' as " << id << dendl;
  }

  //增加其引用数
  ++lock_refs[id];

  return id;
}

int lockdep_register(const char *name)
{
  int id;

  pthread_mutex_lock(&lockdep_mutex);
  id = _lockdep_register(name);
  pthread_mutex_unlock(&lockdep_mutex);
  return id;
}

//去消对指定锁的注册
void lockdep_unregister(int id)
{
  if (id < 0) {
    return;
  }

  pthread_mutex_lock(&lockdep_mutex);

  std::string name;
  map<int, std::string>::iterator p = lock_names.find(id);
  if (p == lock_names.end())
    name = "unknown" ;
  else
    name = p->second;

  int &refs = lock_refs[id];//减少引用计数
  if (--refs == 0) {
    if (p != lock_names.end()) {
      // reset dependency ordering
      memset((void*)&follows[id][0], 0, MAX_LOCKS/8);
      for (unsigned i=0; i<current_maxid; ++i) {
        delete follows_bt[id][i];
        follows_bt[id][i] = NULL;

        delete follows_bt[i][id];
        follows_bt[i][id] = NULL;
        follows[i][id / 8] &= 255 - (1 << (id % 8));
      }

      lockdep_dout(10) << "unregistered '" << name << "' from " << id << dendl;
      lock_ids.erase(p->second);
      lock_names.erase(id);
    }
    lock_refs.erase(id);
    free_ids[id/8] |= (1 << (id % 8));
    last_freed_id = id;
  } else if (g_lockdep) {
    lockdep_dout(20) << "have " << refs << " of '" << name << "' " <<
			"from " << id << dendl;
  }
  pthread_mutex_unlock(&lockdep_mutex);
}


// does b follow a?
//是否a依赖于b
static bool does_follow(int a, int b)
{
  if (follows[a][b/8] & (1 << (b % 8))) {
	//由于a锁依赖b锁，故返回依赖
    lockdep_dout(0) << "\n";
    *_dout << "------------------------------------" << "\n";
    *_dout << "existing dependency " << lock_names[a] << " (" << a << ") -> "
           << lock_names[b] << " (" << b << ") at:\n";
    //显示依赖情况
    if (follows_bt[a][b]) {
      follows_bt[a][b]->print(*_dout);
    }
    *_dout << dendl;
    return true;
  }

  //遍历a锁的所有依赖项，针对每个依赖项i，考虑是否i依赖于b,如果依赖则构成环。
  for (unsigned i=0; i<current_maxid; i++) {
    if ((follows[a][i/8] & (1 << (i % 8))) &&
	does_follow(i, b)) {
      lockdep_dout(0) << "existing intermediate dependency " << lock_names[a]
          << " (" << a << ") -> " << lock_names[i] << " (" << i << ") at:\n";
      if (follows_bt[a][i]) {
        follows_bt[a][i]->print(*_dout);
      }
      *_dout << dendl;
      return true;
    }
  }

  return false;
}

//准备加锁时调用（主要检测1.重复上锁;2.循环依赖)
int lockdep_will_lock(const char *name, int id, bool force_backtrace,
		      bool recursive)
{
  pthread_t p = pthread_self();

  pthread_mutex_lock(&lockdep_mutex);
  if (!g_lockdep) {
    pthread_mutex_unlock(&lockdep_mutex);
    return id;
  }

  if (id < 0)
    id = _lockdep_register(name);

  lockdep_dout(20) << "_will_lock " << name << " (" << id << ")" << dendl;

  // check dependency graph
  map<int, BackTrace *> &m = held[p];
  for (map<int, BackTrace *>::iterator p = m.begin();
       p != m.end();
       ++p) {
	//如果在当前线程的索引用栈里，发现其已拥有这把锁，则显示递归加锁
	//并打log,然后主动挂掉
    if (p->first == id) {
      if (!recursive) {
	lockdep_dout(0) << "\n";
	*_dout << "recursive lock of " << name << " (" << id << ")\n";
	BackTrace *bt = new BackTrace(BACKTRACE_SKIP);
	bt->print(*_dout);
	if (p->second) {
	  *_dout << "\npreviously locked at\n";
	  p->second->print(*_dout);
	}
	delete bt;
	*_dout << dendl;
	ceph_abort();
      }
    }
    else if (!(follows[p->first][id/8] & (1 << (id % 8)))) {
      // new dependency
      //p->first还未占用id锁，等加锁后它就会占用，检查此时是否有环

      // did we just create a cycle?
      //如果id锁现已依赖于p，则构成环，报错
      if (does_follow(id, p->first)) {
        BackTrace *bt = new BackTrace(BACKTRACE_SKIP);
	lockdep_dout(0) << "new dependency " << lock_names[p->first]
		<< " (" << p->first << ") -> " << name << " (" << id << ")"
		<< " creates a cycle at\n";
	bt->print(*_dout);
	*_dout << dendl;

	//附加显示，自身拥有的锁
	lockdep_dout(0) << "btw, i am holding these locks:" << dendl;
	for (map<int, BackTrace *>::iterator q = m.begin();
	     q != m.end();
	     ++q) {
	  lockdep_dout(0) << "  " << lock_names[q->first] << " (" << q->first << ")" << dendl;
	  if (q->second) {
	    lockdep_dout(0) << " ";
	    q->second->print(*_dout);
	    *_dout << dendl;
	  }
	}

	lockdep_dout(0) << "\n" << dendl;

	// don't add this dependency, or we'll get aMutex. cycle in the graph, and
	// does_follow() won't terminate.

	ceph_abort();  // actually, we should just die here.
      } else {
        BackTrace *bt = NULL;
        if (force_backtrace || lockdep_force_backtrace()) {
          bt = new BackTrace(BACKTRACE_SKIP);
        }
        //注明p->first锁依赖于id锁
        follows[p->first][id/8] |= 1 << (id % 8);
        //注明p->first锁依赖于id锁时的位置
        follows_bt[p->first][id] = bt;
	lockdep_dout(10) << lock_names[p->first] << " -> " << name << " at" << dendl;
	//bt->print(*_dout);
      }
    }
  }
  pthread_mutex_unlock(&lockdep_mutex);
  return id;
}

//加锁完成后调用（记录哪个线程占用哪把锁）
int lockdep_locked(const char *name, int id, bool force_backtrace)
{
  pthread_t p = pthread_self();

  pthread_mutex_lock(&lockdep_mutex);
  if (!g_lockdep)
    goto out;
  if (id < 0)
    id = _lockdep_register(name);

  lockdep_dout(20) << "_locked " << name << dendl;

  //记录线程p(我们自已）拥有了锁$id
  if (force_backtrace || lockdep_force_backtrace())
    held[p][id] = new BackTrace(BACKTRACE_SKIP);
  else
    held[p][id] = 0;
out:
  pthread_mutex_unlock(&lockdep_mutex);
  return id;
}

//删除移除项
int lockdep_will_unlock(const char *name, int id)
{
  pthread_t p = pthread_self();

  if (id < 0) {
    //id = lockdep_register(name);
    ceph_assert(id == -1);
    return id;
  }

  pthread_mutex_lock(&lockdep_mutex);
  if (!g_lockdep)
    goto out;
  lockdep_dout(20) << "_will_unlock " << name << dendl;

  // don't assert.. lockdep may be enabled at any point in time
  //assert(held.count(p));
  //assert(held[p].count(id));

  delete held[p][id];//删除移除项
  held[p].erase(id);
out:
  pthread_mutex_unlock(&lockdep_mutex);
  return id;
}


