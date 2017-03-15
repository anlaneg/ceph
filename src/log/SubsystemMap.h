// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LOG_SUBSYSTEMS
#define CEPH_LOG_SUBSYSTEMS

#include <string>
#include <vector>

#include "include/assert.h"

namespace ceph {
namespace logging {

struct Subsystem {
  //log级别，收集级别（越小越容易被收集）
  int log_level, gather_level;
  std::string name;//子系统名称(none时指默认模块）
  
  Subsystem() : log_level(0), gather_level(0) {}     
};

class SubsystemMap {
  std::vector<Subsystem> m_subsys;//所有子系统的日志收集情况
  unsigned m_max_name_len;//子系统中名称最大长度

  friend class Log;

public:
  SubsystemMap() : m_max_name_len(0) {}

  //获取子系统数目
  int get_num() const {
    return m_subsys.size();
  }

  //获取subsys中子系统名称的最大长度
  int get_max_subsys_len() const {
    return m_max_name_len;
  }

  void add(unsigned subsys, std::string name, int log, int gather);  
  void set_log_level(unsigned subsys, int log);
  void set_gather_level(unsigned subsys, int gather);

  //获取某个subsys的log_level
  int get_log_level(unsigned subsys) const {
    if (subsys >= m_subsys.size())
      subsys = 0;
    return m_subsys[subsys].log_level;
  }

  //获取某个subsys的gather_level
  int get_gather_level(unsigned subsys) const {
    if (subsys >= m_subsys.size())
      subsys = 0;
    return m_subsys[subsys].gather_level;
  }

  //获取某个subsys的name
  const std::string& get_name(unsigned subsys) const {
    if (subsys >= m_subsys.size())
      subsys = 0;
    return m_subsys[subsys].name;
  }

  //检查sub是否可收集(level小于gather_level或者log_level)时可收集
  bool should_gather(unsigned sub, int level) {
    assert(sub < m_subsys.size());
    return level <= m_subsys[sub].gather_level ||
      level <= m_subsys[sub].log_level;
  }
};

}
}

#endif
