// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
  *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OS_BLUESTORE_BLOCKDEVICE_H
#define CEPH_OS_BLUESTORE_BLOCKDEVICE_H

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "acconfig.h"
#include "os/fs/FS.h"

#define SPDK_PREFIX "spdk:"

/// track in-flight io
struct IOContext {//这个类提供的功能比较简单，仅是一个粘合层，连接两端
  CephContext* cct;
  void *priv;
#ifdef HAVE_SPDK
  void *nvme_task_first = nullptr;
  void *nvme_task_last = nullptr;
#endif

  std::mutex lock;
  std::condition_variable cond;

  list<FS::aio_t> pending_aios;    ///< not yet submitted //还没有提交的aio
  list<FS::aio_t> running_aios;    ///< submitting or submitted //已经提交的或者正在提交的aio
  std::atomic_int num_pending = {0}; //还没有提交的aio有多少个
  std::atomic_int num_running = {0}; //正在提交的aio或者已提提交的aio
  std::atomic_int num_reading = {0};
  std::atomic_int num_waiting = {0};

  explicit IOContext(CephContext* cct, void *p)
    : cct(cct), priv(p)
    {}

  // no copying
  IOContext(const IOContext& other) = delete;
  IOContext &operator=(const IOContext& other) = delete;

  bool has_pending_aios() {//是否有未绝aio
    return num_pending.load();
  }

  void aio_wait();//在此等aio

  void aio_wake() {//尝试着唤醒
    if (num_waiting.load()) {
      std::lock_guard<std::mutex> l(lock);
      cond.notify_all();
    }
  }
};

//基类，向上提供两种kernel的用一写裸设备，
class BlockDevice {
public:
  CephContext* cct;
private:
  std::mutex ioc_reap_lock;
  vector<IOContext*> ioc_reap_queue;
  std::atomic_int ioc_reap_count = {0};

protected:
  bool rotational = true;//是否可转动设备（普通硬盘）,如果是ssd，则此值为false

public:
  BlockDevice(CephContext* cct) : cct(cct) {}
  virtual ~BlockDevice() = default;
  typedef void (*aio_callback_t)(void *handle, void *aio);

  static BlockDevice *create(
    CephContext* cct, const string& path, aio_callback_t cb, void *cbpriv);
  virtual bool supported_bdev_label() { return true; }
  virtual bool is_rotational() { return rotational; }

  virtual void aio_submit(IOContext *ioc) = 0;

  virtual uint64_t get_size() const = 0;
  virtual uint64_t get_block_size() const = 0;

  virtual int read(
    uint64_t off,
    uint64_t len,
    bufferlist *pbl,
    IOContext *ioc,
    bool buffered) = 0;
  virtual int read_random(
    uint64_t off,
    uint64_t len,
    char *buf,
    bool buffered) = 0;

  virtual int aio_read(
    uint64_t off,
    uint64_t len,
    bufferlist *pbl,
    IOContext *ioc) = 0;
  virtual int aio_write(
    uint64_t off,
    bufferlist& bl,
    IOContext *ioc,
    bool buffered) = 0;
  virtual int flush() = 0;

  void queue_reap_ioc(IOContext *ioc);
  void reap_ioc();

  // for managing buffered readers/writers
  virtual int invalidate_cache(uint64_t off, uint64_t len) = 0;
  virtual int open(const string& path) = 0;
  virtual void close() = 0;
};

#endif //CEPH_OS_BLUESTORE_BLOCKDEVICE_H
