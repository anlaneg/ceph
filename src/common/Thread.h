// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_THREAD_H
#define CEPH_THREAD_H

#include <system_error>
#include <thread>

#include <pthread.h>
#include <sys/types.h>

#include "include/compat.h"

class Thread {
 private:
  pthread_t thread_id;
  pid_t pid;
  int ioprio_class, ioprio_priority;
  //绑定cpu
  int cpuid;
  //线程名称
  const char *thread_name;

  void *entry_wrapper();

 public:
  Thread(const Thread&) = delete;
  Thread& operator=(const Thread&) = delete;

  Thread();
  virtual ~Thread();

 protected:
  virtual void *entry() = 0;

 private:
  static void *_entry_func(void *arg);

 public:
  const pthread_t &get_thread_id() const;
  pid_t get_pid() const { return pid; }
  bool is_started() const;
  bool am_self() const;
  int kill(int signal);
  int try_create(size_t stacksize);
  void create(const char *name, size_t stacksize = 0);
  int join(void **prval = 0);
  int detach();
  int set_ioprio(int cls, int prio);
  int set_affinity(int cpuid);
};

// Functions for with std::thread

void set_thread_name(std::thread& t, const std::string& s);
std::string get_thread_name(const std::thread& t);
void kill(std::thread& t, int signal);

template<typename Fun, typename... Args>
std::thread make_named_thread(const std::string& s,
			      Fun&& fun,
			      Args&& ...args) {
  auto t = std::thread(std::forward<Fun>(fun),
		       std::forward<Args>(args)...);
  set_thread_name(t, s);
  return t;
}

#endif
