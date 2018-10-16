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
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "acconfig.h"
#ifdef HAVE_LIBAIO
#include "aio.h"
#endif
#include "include/ceph_assert.h"
#include "include/buffer.h"
#include "include/interval_set.h"
#define SPDK_PREFIX "spdk:"

class CephContext;

/// track in-flight io
struct IOContext {//这个类提供的功能比较简单，仅是一个粘合层，连接两端
private:
  std::mutex lock;
  std::condition_variable cond;
  int r = 0;

public:
  CephContext* cct;
  void *priv;
#ifdef HAVE_SPDK
  void *nvme_task_first = nullptr;
  void *nvme_task_last = nullptr;
  std::atomic_int total_nseg = {0};
#endif

#ifdef HAVE_LIBAIO
  std::list<aio_t> pending_aios;    ///< not yet submitted//还没有提交的aio
  std::list<aio_t> running_aios;    ///< submitting or submitted//已经提交的或者正在提交的aio
#endif
  std::atomic_int num_pending = {0};//还没有提交的aio有多少个
  std::atomic_int num_running = {0};//正在提交的aio或者已提提交的aio
  bool allow_eio;

  explicit IOContext(CephContext* cct, void *p, bool allow_eio = false)
    : cct(cct), priv(p), allow_eio(allow_eio)
    {}

  // no copying
  IOContext(const IOContext& other) = delete;
  IOContext &operator=(const IOContext& other) = delete;

  bool has_pending_aios() {//是否有未绝aio
    return num_pending.load();
  }
  void release_running_aios();
  void aio_wait();//在此等aio
  uint64_t get_num_ios() const;

  void try_aio_wake() {//尝试着唤醒
    assert(num_running >= 1);
    if (num_running.fetch_sub(1) == 1) {

      // we might have some pending IOs submitted after the check
      // as there is no lock protection for aio_submit.
      // Hence we might have false conditional trigger.
      // aio_wait has to handle that hence do not care here.
      std::lock_guard<std::mutex> l(lock);
      cond.notify_all();
    }
  }

  void set_return_value(int _r) {
    r = _r;
  }

  int get_return_value() const {
    return r;
  }
};

//基类，向上提供两种kernel的用一写裸设备，
class BlockDevice {
public:
  CephContext* cct;
  typedef void (*aio_callback_t)(void *handle, void *aio);
private:
  std::mutex ioc_reap_lock;
  std::vector<IOContext*> ioc_reap_queue;
  std::atomic_int ioc_reap_count = {0};

protected:
  uint64_t size;
  uint64_t block_size;
  bool rotational = true;//是否可转动设备（普通硬盘）,如果是ssd，则此值为false

public:
  aio_callback_t aio_callback;
  void *aio_callback_priv;
  BlockDevice(CephContext* cct, aio_callback_t cb, void *cbpriv)
  : cct(cct),
    size(0),
    block_size(0),
    aio_callback(cb),
    aio_callback_priv(cbpriv)
 {}
  virtual ~BlockDevice() = default;

  static BlockDevice *create(
    CephContext* cct, const std::string& path, aio_callback_t cb, void *cbpriv, aio_callback_t d_cb, void *d_cbpriv);
  virtual bool supported_bdev_label() { return true; }
  virtual bool is_rotational() { return rotational; }

  virtual void aio_submit(IOContext *ioc) = 0;

  uint64_t get_size() const { return size; }
  uint64_t get_block_size() const { return block_size; }

  /// hook to provide utilization of thinly-provisioned device
  virtual bool get_thin_utilization(uint64_t *total, uint64_t *avail) const {
    return false;
  }

  virtual int collect_metadata(const std::string& prefix, std::map<std::string,std::string> *pm) const = 0;

  virtual int get_devname(std::string *out) {
    return -ENOENT;
  }
  virtual int get_devices(std::set<std::string> *ls) {
    std::string s;
    if (get_devname(&s) == 0) {
      ls->insert(s);
    }
    return 0;
  }

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
  virtual int write(
    uint64_t off,
    bufferlist& bl,
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
  virtual int discard(uint64_t offset, uint64_t len) { return 0; }
  virtual int queue_discard(interval_set<uint64_t> &to_release) { return -1; }
  virtual void discard_drain() { return; }

  void queue_reap_ioc(IOContext *ioc);
  void reap_ioc();

  // for managing buffered readers/writers
  virtual int invalidate_cache(uint64_t off, uint64_t len) = 0;
  virtual int open(const std::string& path) = 0;
  virtual void close() = 0;

protected:
  bool is_valid_io(uint64_t off, uint64_t len) const {
    return (off % block_size == 0 &&
            len % block_size == 0 &&
            len > 0 &&
            off < size &&
            off + len <= size);
  }
};

#endif //CEPH_OS_BLUESTORE_BLOCKDEVICE_H
