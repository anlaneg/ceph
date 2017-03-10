// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OS_BLUESTORE_KERNELDEVICE_H
#define CEPH_OS_BLUESTORE_KERNELDEVICE_H

#include <atomic>

#include "os/fs/FS.h"
#include "include/interval_set.h"

#include "BlockDevice.h"

class KernelDevice : public BlockDevice {
  int fd_direct, fd_buffered;
  uint64_t size;//这个磁盘大小
  uint64_t block_size;//块大小
  string path;//磁盘对应的路径
  FS *fs;//依据不同的类型，例如xfs文件系统生成对应的XFS类
  bool aio, dio;//是否开启aio,dio

  Mutex debug_lock;
  interval_set<uint64_t> debug_inflight;

  Mutex flush_lock;
  atomic_t io_since_flush;

  FS::aio_queue_t aio_queue;//(仅向kernel提交aio,暂没有队列)
  aio_callback_t aio_callback;//aio完成时的回调函数
  void *aio_callback_priv;//aio完成时回主财函数的第一个参数
  bool aio_stop;//aio线程停止标记

  struct AioCompletionThread : public Thread {
    KernelDevice *bdev;
    explicit AioCompletionThread(KernelDevice *b) : bdev(b) {}
    void *entry() {
      bdev->_aio_thread();//实现aio写
      return NULL;
    }
  } aio_thread;//aio线程

  std::atomic_int injecting_crash;

  void _aio_thread();
  int _aio_start();
  void _aio_stop();

  void _aio_log_start(IOContext *ioc, uint64_t offset, uint64_t length);
  void _aio_log_finish(IOContext *ioc, uint64_t offset, uint64_t length);

  int _lock();

  int direct_read_unaligned(uint64_t off, uint64_t len, char *buf);

  // stalled aio debugging
  FS::aio_list_t debug_queue;
  std::mutex debug_queue_lock;
  FS::aio_t *debug_oldest = nullptr;
  utime_t debug_stall_since;
  void debug_aio_link(FS::aio_t& aio);
  void debug_aio_unlink(FS::aio_t& aio);

public:
  KernelDevice(CephContext* cct, aio_callback_t cb, void *cbpriv);

  void aio_submit(IOContext *ioc) override;

  uint64_t get_size() const override {
    return size;
  }
  uint64_t get_block_size() const override {
    return block_size;
  }

  int read(uint64_t off, uint64_t len, bufferlist *pbl,
	   IOContext *ioc,
	   bool buffered) override;
  int aio_read(uint64_t off, uint64_t len, bufferlist *pbl,
	       IOContext *ioc) override;
  int read_random(uint64_t off, uint64_t len, char *buf, bool buffered) override;

  int aio_write(uint64_t off, bufferlist& bl,
		IOContext *ioc,
		bool buffered) override;
  int flush() override;

  // for managing buffered readers/writers
  int invalidate_cache(uint64_t off, uint64_t len) override;
  int open(const string& path) override;
  void close() override;
};

#endif
