// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_LOG_LOG_H
#define __CEPH_LOG_LOG_H

#include <boost/circular_buffer.hpp>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <string_view>

#include "common/Thread.h"
#include "common/likely.h"

#include "log/Entry.h"

namespace ceph {
namespace logging {

class Graylog;
class SubsystemMap;

//没有看出这个log实现的有多好，感觉非常普通，且不高效
class Log : private Thread
{
  using EntryRing = boost::circular_buffer<ConcreteEntry>;
  using EntryVector = std::vector<ConcreteEntry>;

  static const std::size_t DEFAULT_MAX_NEW = 100;
  static const std::size_t DEFAULT_MAX_RECENT = 10000;

  Log **m_indirect_this;
  log_clock clock;

  const SubsystemMap *m_subs;

  std::mutex m_queue_mutex;
  std::mutex m_flush_mutex;
  std::condition_variable m_cond_loggers;
  std::condition_variable m_cond_flusher;//需要flush的信号量

  //哪个线程拥有queue mutex
  pthread_t m_queue_mutex_holder;
  //哪个线程拥有flush mutex
  pthread_t m_flush_mutex_holder;

  EntryVector m_new;    ///< new entries
  EntryRing m_recent; ///< recent (less new) entries we've already written at low detail
  EntryVector m_flush; ///< entries to be flushed (here to optimize heap allocations)

  //日志文件名称
  std::string m_log_file;
  //日志文件fd（>=0时有效）
  int m_fd = -1;
  //日志文件的owner
  uid_t m_uid = 0;
  gid_t m_gid = 0;

  int m_fd_last_error = 0;  ///< last error we say writing to fd (if any)

  int m_syslog_log = -2, m_syslog_crash = -2;//写syslog时的级别
  int m_stderr_log = -1, m_stderr_crash = -1;
  int m_graylog_log = -3, m_graylog_crash = -3;

  std::string m_log_stderr_prefix;

  std::shared_ptr<Graylog> m_graylog;

  std::vector<char> m_log_buf;

  bool m_stop = false;//标记线程是否需要停止

  std::size_t m_max_new = DEFAULT_MAX_NEW;
  std::size_t m_max_recent = DEFAULT_MAX_RECENT;

  bool m_inject_segv = false;//断错误注入用

  void *entry() override;

  void _log_safe_write(std::string_view sv);
  void _flush_logbuf();
  void _flush(EntryVector& q, bool requeue, bool crash);

  void _log_message(const char *s, bool crash);

public:
  Log(const SubsystemMap *s);
  ~Log() override;

  void set_flush_on_exit();

  void set_coarse_timestamps(bool coarse);
  void set_max_new(std::size_t n);
  void set_max_recent(std::size_t n);
  void set_log_file(std::string_view fn);
  void reopen_log_file();
  void chown_log_file(uid_t uid, gid_t gid);
  void set_log_stderr_prefix(std::string_view p);

  void flush();

  void dump_recent();

  void set_syslog_level(int log, int crash);
  void set_stderr_level(int log, int crash);
  void set_graylog_level(int log, int crash);

  void start_graylog();
  void stop_graylog();

  std::shared_ptr<Graylog> graylog() { return m_graylog; }

  void submit_entry(Entry&& e);

  void start();
  void stop();

  /// true if the log lock is held by our thread
  bool is_inside_log_lock();

  /// induce a segv on the next log event
  void inject_segv();
  void reset_segv();
};

}
}

#endif
