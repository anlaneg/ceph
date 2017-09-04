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

#ifndef CEPH_MONMAP_H
#define CEPH_MONMAP_H

#include "include/err.h"

#include "msg/Message.h"
#include "include/types.h"
#include "mon/mon_types.h"

namespace ceph {
  class Formatter;
}

struct mon_info_t {
  /**
   * monitor name
   *
   * i.e., 'foo' in 'mon.foo'
   */
  string name;
  /**
   * monitor's public address
   *
   * public facing address, traditionally used to communicate with all clients
   * and other monitors.
   */
  entity_addr_t public_addr;
  /**
   * the priority of the mon, the lower value the more preferred
   */
  uint16_t priority{0};

  mon_info_t(const string& n, const entity_addr_t& p_addr, uint16_t p)
    : name(n), public_addr(p_addr), priority(p)
  {}
  mon_info_t(const string &n, const entity_addr_t& p_addr)
    : name(n), public_addr(p_addr)
  { }

  mon_info_t() { }


  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& p);
  void print(ostream& out) const;
};
WRITE_CLASS_ENCODER_FEATURES(mon_info_t)

inline ostream& operator<<(ostream& out, const mon_info_t& mon) {
  mon.print(out);
  return out;
}

//monmap　这张表来源于配置属于静态表。每个mon启动后，都会去读取，并产生这张表。
class MonMap {
 public:
  epoch_t epoch;       // what epoch/version of the monmap　//版本号
  uuid_d fsid;
  utime_t last_changed;
  utime_t created;

  map<string, mon_info_t> mon_info;//名称到地址
  map<entity_addr_t, string> addr_mons;//地址到mons信息

  vector<string> ranks;//所有mon名称

  /**
   * Persistent Features are all those features that once set on a
   * monmap cannot, and should not, be removed. These will define the
   * non-negotiable features that a given monitor must support to
   * properly operate in a given quorum.
   *
   * Should be reserved for features that we really want to make sure
   * are sticky, and are important enough to tolerate not being able
   * to downgrade a monitor.
   */
  mon_feature_t persistent_features;
  /**
   * Optional Features are all those features that can be enabled or
   * disabled following a given criteria -- e.g., user-mandated via the
   * cli --, and act much like indicators of what the cluster currently
   * supports.
   *
   * They are by no means "optional" in the sense that monitors can
   * ignore them. Just that they are not persistent.
   */
  mon_feature_t optional_features;

  /**
   * Returns the set of features required by this monmap.
   *
   * The features required by this monmap is the union of all the
   * currently set persistent features and the currently set optional
   * features.
   *
   * @returns the set of features required by this monmap
   */
  mon_feature_t get_required_features() const {
    return (persistent_features | optional_features);
  }

public:
  void sanitize_mons(map<string,entity_addr_t>& o);
  void calc_ranks();

  MonMap()
    : epoch(0) {
    memset(&fsid, 0, sizeof(fsid));
  }

  uuid_d& get_fsid() { return fsid; }

  unsigned size() const {
    return mon_info.size();
  }

  epoch_t get_epoch() const { return epoch; }
  void set_epoch(epoch_t e) { epoch = e; }

  /**
   * Obtain list of public facing addresses
   *
   * @param ls list to populate with the monitors' addresses
   */
  void list_addrs(list<entity_addr_t>& ls) const {//列出mon_info中的所有public_addr
    for (map<string,mon_info_t>::const_iterator p = mon_info.begin();
         p != mon_info.end();
         ++p) {
      ls.push_back(p->second.public_addr);
    }
  }

  /**
   * Add new monitor to the monmap
   *
   * @param m monitor info of the new monitor
   */
  void add(mon_info_t &&m) {
    assert(mon_info.count(m.name) == 0);
    assert(addr_mons.count(m.public_addr) == 0);
    mon_info[m.name] = std::move(m);
    calc_ranks();
  }

  /**
   * Add new monitor to the monmap
   *
   * @param name Monitor name (i.e., 'foo' in 'mon.foo')
   * @param addr Monitor's public address
   */
  void add(const string &name, const entity_addr_t &addr) {
    add(mon_info_t(name, addr));
  }

  /**
   * Remove monitor from the monmap
   *
   * @param name Monitor name (i.e., 'foo' in 'mon.foo')
   */
  void remove(const string &name) {
    assert(mon_info.count(name));
    mon_info.erase(name);
    assert(mon_info.count(name) == 0);
    calc_ranks();
  }

  /**
   * Rename monitor from @p oldname to @p newname
   *
   * @param oldname monitor's current name (i.e., 'foo' in 'mon.foo')
   * @param newname monitor's new name (i.e., 'bar' in 'mon.bar')
   */
  void rename(string oldname, string newname) {
    assert(contains(oldname));
    assert(!contains(newname));
    mon_info[newname] = mon_info[oldname];
    mon_info.erase(oldname);
    mon_info[newname].name = newname;
    calc_ranks();
  }

  bool contains(const string& name) const {
    return mon_info.count(name);
  }

  /**
   * Check if monmap contains a monitor with address @p a
   *
   * @note checks for all addresses a monitor may have, public or otherwise.
   *
   * @param a monitor address
   * @returns true if monmap contains a monitor with address @p;
   *          false otherwise.
   */
  bool contains(const entity_addr_t &a) const {
    for (map<string,mon_info_t>::const_iterator p = mon_info.begin();
         p != mon_info.end();
         ++p) {
      if (p->second.public_addr == a)
        return true;
    }
    return false;
  }

  string get_name(unsigned n) const {//给定id取对应的Ｍon名称
    assert(n < ranks.size());
    return ranks[n];
  }
  string get_name(const entity_addr_t& a) const {//给定地址，取mon名称
    map<entity_addr_t,string>::const_iterator p = addr_mons.find(a);
    if (p == addr_mons.end())
      return string();
    else
      return p->second;
  }

  int get_rank(const string& n) {//给定mon名称，取此mon对应的id,没有找到返回－１
    for (unsigned i = 0; i < ranks.size(); i++)
      if (ranks[i] == n)
	return i;
    return -1;
  }
  int get_rank(const entity_addr_t& a) {//给定地址，取mon对应的id,找不到返回-1
    string n = get_name(a);
    if (n.empty())
      return -1;

    for (unsigned i = 0; i < ranks.size(); i++)
      if (ranks[i] == n)
	return i;
    return -1;
  }
  bool get_addr_name(const entity_addr_t& a, string& name) {//给定地址，取mon以应名称
    if (addr_mons.count(a) == 0)
      return false;
    name = addr_mons[a];
    return true;
  }

  const entity_addr_t& get_addr(const string& n) const {//给定mon取ip地址
    assert(mon_info.count(n));
    map<string,mon_info_t>::const_iterator p = mon_info.find(n);
    return p->second.public_addr;
  }
  const entity_addr_t& get_addr(unsigned m) const {//给定mon编号，取对应ip地址
    assert(m < ranks.size());
    return get_addr(ranks[m]);
  }
  void set_addr(const string& n, const entity_addr_t& a) {//给定名称，取地址
    assert(mon_info.count(n));
    mon_info[n].public_addr = a;
    calc_ranks();
  }
  entity_inst_t get_inst(const string& n) {//给定名称，获取entity_inst_t类型
    assert(mon_info.count(n));
    int m = get_rank(n);
    assert(m >= 0); // vector can't take negative indicies
    return get_inst(m);
  }
  entity_inst_t get_inst(unsigned m) const {//给定id取entity_inst_t类型
    assert(m < ranks.size());
    entity_inst_t i;
    i.addr = get_addr(m);
    i.name = entity_name_t::MON(m);
    return i;
  }

  void encode(bufferlist& blist, uint64_t con_features) const;
  void decode(bufferlist& blist) {
    bufferlist::iterator p = blist.begin();
    decode(p);
  }
  void decode(bufferlist::iterator &p);

  void generate_fsid() {//随机生成fsid
    fsid.generate_random();
  }

  // read from/write to a file
  int write(const char *fn);
  int read(const char *fn);

  /**
   * build an initial bootstrap monmap from conf
   *
   * Build an initial bootstrap monmap from the config.  This will
   * try, in this order:
   *
   *   1 monmap   -- an explicitly provided monmap
   *   2 mon_host -- list of monitors
   *   3 config [mon.*] sections, and 'mon addr' fields in those sections
   *
   * @param cct context (and associated config)
   * @param errout ostream to send error messages too
   */
  int build_initial(CephContext *cct, ostream& errout);//自配置文件等中获取mon列表

  /**
   * build a monmap from a list of hosts or ips
   *
   * Resolve dns as needed.  Give mons dummy names.
   *
   * @param hosts  list of hosts, space or comma separated
   * @param prefix prefix to prepend to generated mon names
   * @return 0 for success, -errno on error
   */
  int build_from_host_list(std::string hosts, std::string prefix);//给一组列表，构造mon列表

  /**
   * filter monmap given a set of initial members.
   *
   * Remove mons that aren't in the initial_members list.  Add missing
   * mons and give them dummy IPs (blank IPv4, with a non-zero
   * nonce). If the name matches my_name, then my_addr will be used in
   * place of a dummy addr.
   *
   * @param initial_members list of initial member names
   * @param my_name name of self, can be blank
   * @param my_addr my addr
   * @param removed optional pointer to set to insert removed mon addrs to
   */
  void set_initial_members(CephContext *cct,
			   list<std::string>& initial_members,
			   string my_name, const entity_addr_t& my_addr,
			   set<entity_addr_t> *removed);

  void print(ostream& out) const;
  void print_summary(ostream& out) const;
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(list<MonMap*>& o);//测试代码不看
};
WRITE_CLASS_ENCODER_FEATURES(MonMap)

inline ostream& operator<<(ostream &out, const MonMap &m) {
  m.print_summary(out);
  return out;
}

#endif
