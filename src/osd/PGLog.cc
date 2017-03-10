// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "PGLog.h"
#include "include/unordered_map.h"
#include "common/ceph_context.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

static ostream& _prefix(std::ostream *_dout, const PGLog *pglog)
{
  return *_dout << pglog->gen_prefix();
}

//////////////////// PGLog::IndexedLog ////////////////////

void PGLog::IndexedLog::split_out_child(
  pg_t child_pgid,
  unsigned split_bits,
  PGLog::IndexedLog *target)
{
  unindex();
  *target = pg_log_t::split_out_child(child_pgid, split_bits);
  index();
  target->index();
  reset_rollback_info_trimmed_to_riter();
}

void PGLog::IndexedLog::trim(
  CephContext* cct,
  eversion_t s,
  set<eversion_t> *trimmed)
{
  if (complete_to != log.end() &&
      complete_to->version <= s) {
    generic_dout(0) << " bad trim to " << s << " when complete_to is "
		    << complete_to->version
		    << " on " << *this << dendl;
  }

  assert(s <= can_rollback_to);

  while (!log.empty()) {
    pg_log_entry_t &e = *log.begin();
    if (e.version > s)
      break;//如果队首版本大于s则停止
    generic_dout(20) << "trim " << e << dendl;
    if (trimmed)
      trimmed->insert(e.version);//加入到trimmed队列

    unindex(e);         // remove from index,//自索引中移除此entity

    //移除队列中的entity(另外需要查看riter是否需要更新,防止其空指向)
    if (rollback_info_trimmed_to_riter == log.rend() ||
	e.version == rollback_info_trimmed_to_riter->version) {
      log.pop_front();
      rollback_info_trimmed_to_riter = log.rend();//更新为下一个需要trime的节点
    } else {
      log.pop_front();
    }
  }

  // raise tail?
  if (tail < s)
    tail = s;//更新tail
}

ostream& PGLog::IndexedLog::print(ostream& out) const
{
  out << *this << std::endl;
  for (list<pg_log_entry_t>::const_iterator p = log.begin();
       p != log.end();
       ++p) {
    out << *p << " " << (logged_object(p->soid) ? "indexed":"NOT INDEXED") << std::endl;
    assert(!p->reqid_is_indexed() || logged_req(p->reqid));
  }
  return out;
}

//////////////////// PGLog ////////////////////

void PGLog::reset_backfill()
{
  missing.clear();
}

void PGLog::clear() {
  missing.clear();
  log.clear();
  log_keys_debug.clear();
  undirty();
}

void PGLog::clear_info_log(
  spg_t pgid,
  ObjectStore::Transaction *t) {
  coll_t coll(pgid);
  t->remove(coll, pgid.make_pgmeta_oid());
}

//更新tail
//PGlog修剪,修到trim_to版本之前.
//将要修剪的log放在trimmed集合里
//log减小了,所以log.tail要移动
//其它一些指针可能会发生问题,所以也要移动.
void PGLog::trim(
  eversion_t trim_to,//可以trim到这个版本
  pg_info_t &info)
{
  // trim?
  //如果trim_to比log.tail小,则不必trim
  if (trim_to > log.tail) {
    // We shouldn't be trimming the log past last_complete
    assert(trim_to <= info.last_complete);

    dout(10) << "trim " << log << " to " << trim_to << dendl;
    log.trim(cct, trim_to, &trimmed);
    info.log_tail = log.tail;//更新info.log_tail
  }
}

void PGLog::proc_replica_log(
  ObjectStore::Transaction& t,
  pg_info_t &oinfo,
  const pg_log_t &olog,
  pg_missing_t& omissing,
  pg_shard_t from) const
{
  dout(10) << "proc_replica_log for osd." << from << ": "
	   << oinfo << " " << olog << " " << omissing << dendl;

  if (olog.head < log.tail) {//olog与当前无重合
    dout(10) << __func__ << ": osd." << from << " does not overlap, not looking "
	     << "for divergent objects" << dendl;
    return;//无法合并
  }
  if (olog.head == log.head) {//如果head相等，则不可能有分歧
    dout(10) << __func__ << ": osd." << from << " same log head, not looking "
	     << "for divergent objects" << dendl;
    return;
  }
  assert(olog.head >= log.tail);

  /*
    basically what we're doing here is rewinding the remote log,
    dropping divergent entries, until we find something that matches
    our master log.  we then reset last_update to reflect the new
    point up to which missing is accurate.

    later, in activate(), missing will get wound forward again and
    we will send the peer enough log to arrive at the same state.
  */

  //显示下,副本提交上来的missing集
  for (map<hobject_t, pg_missing_item>::const_iterator i = omissing.get_items().begin();
       i != omissing.get_items().end();
       ++i) {
    dout(20) << " before missing " << i->first << " need " << i->second.need
	     << " have " << i->second.have << dendl;
  }

  //由于本函数是处理副本的log合并,而这里olog实际上是副本发送过来的log
  //由于我们在getLog阶段已经将权威log合并到自身的log成员里了,
  //所以这里log.head一定是大于olog.head的

  //从log中找出大于olog.head的点(此循环结束时,first_non_divergent-1指向的节点大于olog.head
  list<pg_log_entry_t>::const_reverse_iterator first_non_divergent =
    log.log.rbegin();
  while (1) {
    if (first_non_divergent == log.log.rend())
      break;
    if (first_non_divergent->version <= olog.head) {//从这个位置向tail方向是没有分歧的
      dout(20) << "merge_log point (usually last shared) is "
	       << *first_non_divergent << dendl;
      break;
    }
    ++first_non_divergent;
  }

  /* Because olog.head >= log.tail, we know that both pgs must at least have
   * the event represented by log.tail.  Similarly, because log.head >= olog.tail,
   * we know that the even represented by olog.tail must be common to both logs.
   * Furthermore, the event represented by a log tail was necessarily trimmed,
   * thus neither olog.tail nor log.tail can be divergent. It's
   * possible that olog/log contain no actual events between olog.head and
   * MAX(log.tail, olog.tail), however, since they might have been split out.
   * Thus, if we cannot find an event e such that
   * log.tail <= e.version <= log.head, the last_update must actually be
   * MAX(log.tail, olog.tail).
   */
  eversion_t limit = MAX(olog.tail, log.tail);
  eversion_t lu =
    (first_non_divergent == log.log.rend() ||
     first_non_divergent->version < limit) ?
    limit :
    first_non_divergent->version;

  IndexedLog folog(olog);
  auto divergent = folog.rewind_from_head(lu);
  _merge_divergent_entries(
    folog,
    divergent,
    oinfo,
    olog.get_can_rollback_to(),
    omissing,
    0,
    this);

  if (lu < oinfo.last_update) {
    dout(10) << " peer osd." << from << " last_update now " << lu << dendl;
    oinfo.last_update = lu;
  }

  if (omissing.have_missing()) {
    eversion_t first_missing =
      omissing.get_items().at(omissing.get_rmissing().begin()->second).need;
    oinfo.last_complete = eversion_t();
    list<pg_log_entry_t>::const_iterator i = olog.log.begin();
    for (;
	 i != olog.log.end();
	 ++i) {
      if (i->version < first_missing)
	oinfo.last_complete = i->version;
      else
	break;
    }
  } else {
    oinfo.last_complete = oinfo.last_update;
  }
}

/**
 * rewind divergent entries at the head of the log
 *
 * This rewinds entries off the head of our log that are divergent.
 * This is used by replicas during activation.
 *
 * @param t transaction
 * @param newhead new head to rewind to
 */
void PGLog::rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead,
				 pg_info_t &info, LogEntryHandler *rollbacker,
				 bool &dirty_info, bool &dirty_big_info)
{
  dout(10) << "rewind_divergent_log truncate divergent future " << newhead << dendl;


  if (info.last_complete > newhead)
    info.last_complete = newhead;

  auto divergent = log.rewind_from_head(newhead);
  if (!divergent.empty()) {
    mark_dirty_from(divergent.front().version);
  }
  for (auto &&entry: divergent) {
    dout(10) << "rewind_divergent_log future divergent " << entry << dendl;
  }
  info.last_update = newhead;

  _merge_divergent_entries(
    log,
    divergent,
    info,
    log.get_can_rollback_to(),
    missing,
    rollbacker,
    this);

  dirty_info = true;
  dirty_big_info = true;
}

void PGLog::merge_log(ObjectStore::Transaction& t,
                      pg_info_t &oinfo, pg_log_t &olog, pg_shard_t fromosd,
                      pg_info_t &info, LogEntryHandler *rollbacker,
                      bool &dirty_info, bool &dirty_big_info)
{
  dout(10) << "merge_log " << olog << " from osd." << fromosd
           << " into " << log << dendl;

  // Check preconditions

  // If our log is empty, the incoming log needs to have not been trimmed.
  assert(!log.null() || olog.tail == eversion_t());
  // The logs must overlap.
  assert(log.head >= olog.tail && olog.head >= log.tail);//日志一定是有交集才能走到这个流程(前面选权威时来保证这点)

  //missing拿出来显示下.
  for (map<hobject_t, pg_missing_item>::const_iterator i = missing.get_items().begin();
       i != missing.get_items().end();
       ++i) {
    dout(20) << "pg_missing_t sobject: " << i->first << dendl;
  }

  bool changed = false;

  // extend on tail?
  //  this is just filling in history.  it does not affect our
  //  missing set, as that should already be consistent with our
  //  current log.
  //护展我们集合中tail
  eversion_t orig_tail = log.tail;
  if (olog.tail < log.tail) {
    dout(10) << "merge_log extending tail to " << olog.tail << dendl;
    list<pg_log_entry_t>::iterator from = olog.log.begin();
    list<pg_log_entry_t>::iterator to;
    eversion_t last;
    for (to = from;
	 to != olog.log.end();
	 ++to) {
      if (to->version > log.tail)
	break;
      log.index(*to);
      dout(15) << *to << dendl;
      last = to->version;
    }
    mark_dirty_to(last);

    // splice into our log.
    log.log.splice(log.log.begin(),
		   olog.log, from, to);
      
    info.log_tail = log.tail = olog.tail;
    changed = true;
  }

  if (oinfo.stats.reported_seq < info.stats.reported_seq ||   // make sure reported always increases
      oinfo.stats.reported_epoch < info.stats.reported_epoch) {
    oinfo.stats.reported_seq = info.stats.reported_seq;
    oinfo.stats.reported_epoch = info.stats.reported_epoch;
  }
  if (info.last_backfill.is_max())
    info.stats = oinfo.stats;
  info.hit_set = oinfo.hit_set;

  // do we have divergent entries to throw out?
  //一直觉得这种情况不应出现,但epoch是一个u32_t类型,所以可能发生环回.
  if (olog.head < log.head) {
    rewind_divergent_log(t, olog.head, info, rollbacker, dirty_info, dirty_big_info);
    changed = true;
  }

  // extend on head?
  //如果权威日志的版本号head比我们的大,这说明我们缺少了东西,
  //由于我们与olog之间是有重叠的,所以我们首先要进行"首部合并"

  //"首部合并"就是将olog.head到log.head之间的给我们先加进来
  //我们的做法是在olog.head到olog.tail之间找出一个位置,这个位置采用from指向,它用来说明
  //从from开始到olog.head之间,都比我们的log.head大.
  //lower_bound是我们在olog中能找到的一个比log.head首个小的版本号.
  if (olog.head > log.head) {
    dout(10) << "merge_log extending head to " << olog.head << dendl;
      
    // find start point in olog
    list<pg_log_entry_t>::iterator to = olog.log.end();
    list<pg_log_entry_t>::iterator from = olog.log.end();//从from开始我们有不一样的
    eversion_t lower_bound = MAX(olog.tail, orig_tail);
    while (1) {
      if (from == olog.log.begin())
	break;
      --from;
      dout(20) << "  ? " << *from << dendl;
      if (from->version <= log.head) {
	lower_bound = MAX(lower_bound, from->version);
	++from;
	break;
      }
    }
    dout(20) << "merge_log cut point (usually last shared) is "
	     << lower_bound << dendl;
    mark_dirty_from(lower_bound);

    auto divergent = log.rewind_from_head(lower_bound);
    // move aside divergent items
    for (auto &&oe: divergent) {
      dout(10) << "merge_log divergent " << oe << dendl;
    }
    log.roll_forward_to(log.head, rollbacker);

    mempool::osd::list<pg_log_entry_t> new_entries;
    new_entries.splice(new_entries.end(), olog.log, from, to);
    append_log_entries_update_missing(
      info.last_backfill,
      info.last_backfill_bitwise,
      new_entries,
      false,
      &log,
      missing,
      rollbacker,
      this);

    _merge_divergent_entries(
      log,
      divergent,
      info,
      log.get_can_rollback_to(),
      missing,
      rollbacker,
      this);

    info.last_update = log.head = olog.head;

    // We cannot rollback into the new log entries
    log.skip_can_rollback_to_to_head();

    info.last_user_version = oinfo.last_user_version;
    info.purged_snaps = oinfo.purged_snaps;

    changed = true;
  }
  
  dout(10) << "merge_log result " << log << " " << missing << " changed=" << changed << dendl;

  if (changed) {
    dirty_info = true;
    dirty_big_info = true;
  }
}

void PGLog::check() {
  if (!pg_log_debug)
    return;
  if (log.log.size() != log_keys_debug.size()) {
    derr << "log.log.size() != log_keys_debug.size()" << dendl;
    derr << "actual log:" << dendl;
    for (list<pg_log_entry_t>::iterator i = log.log.begin();
	 i != log.log.end();
	 ++i) {
      derr << "    " << *i << dendl;
    }
    derr << "log_keys_debug:" << dendl;
    for (set<string>::const_iterator i = log_keys_debug.begin();
	 i != log_keys_debug.end();
	 ++i) {
      derr << "    " << *i << dendl;
    }
  }
  assert(log.log.size() == log_keys_debug.size());
  for (list<pg_log_entry_t>::iterator i = log.log.begin();
       i != log.log.end();
       ++i) {
    assert(log_keys_debug.count(i->get_key_name()));
  }
}

void PGLog::write_log_and_missing(
  ObjectStore::Transaction& t,
  map<string,bufferlist> *km,
  const coll_t& coll, const ghobject_t &log_oid,
  bool require_rollback)
{
  if (is_dirty()) {
    dout(5) << "write_log_and_missing with: "
	     << "dirty_to: " << dirty_to
	     << ", dirty_from: " << dirty_from
	     << ", writeout_from: " << writeout_from
	     << ", trimmed: " << trimmed
	     << ", clear_divergent_priors: " << clear_divergent_priors
	     << dendl;
    _write_log_and_missing(
      t, km, log, coll, log_oid,
      dirty_to,
      dirty_from,
      writeout_from,
      trimmed,
      missing,
      !touched_log,
      require_rollback,
      clear_divergent_priors,
      (pg_log_debug ? &log_keys_debug : 0));
    undirty();
  } else {
    dout(10) << "log is not dirty" << dendl;
  }
}

void PGLog::write_log_and_missing_wo_missing(
    ObjectStore::Transaction& t,
    map<string,bufferlist> *km,
    pg_log_t &log,
    const coll_t& coll, const ghobject_t &log_oid,
    map<eversion_t, hobject_t> &divergent_priors,
    bool require_rollback)
{
  _write_log_and_missing_wo_missing(
    t, km, log, coll, log_oid,
    divergent_priors, eversion_t::max(), eversion_t(), eversion_t(),
    set<eversion_t>(),
    true, true, require_rollback, 0);
}

void PGLog::write_log_and_missing(
    ObjectStore::Transaction& t,
    map<string,bufferlist> *km,
    pg_log_t &log,
    const coll_t& coll,
    const ghobject_t &log_oid,
    const pg_missing_tracker_t &missing,
    bool require_rollback)
{
  _write_log_and_missing(
    t, km, log, coll, log_oid,
    eversion_t::max(),
    eversion_t(),
    eversion_t(),
    set<eversion_t>(),
    missing,
    true, require_rollback, false, 0);
}

void PGLog::_write_log_and_missing_wo_missing(
  ObjectStore::Transaction& t,
  map<string,bufferlist> *km,
  pg_log_t &log,
  const coll_t& coll, const ghobject_t &log_oid,
  map<eversion_t, hobject_t> &divergent_priors,
  eversion_t dirty_to,
  eversion_t dirty_from,
  eversion_t writeout_from,
  const set<eversion_t> &trimmed,
  bool dirty_divergent_priors,
  bool touch_log,
  bool require_rollback,
  set<string> *log_keys_debug
  )
{
  //将trimmed中的数据加入to_remove中,一会构造删除数据到transaction
  set<string> to_remove;
  for (set<eversion_t>::const_iterator i = trimmed.begin();
       i != trimmed.end();
       ++i) {
    to_remove.insert(i->get_key_name());
    if (log_keys_debug) {
      assert(log_keys_debug->count(i->get_key_name()));
      log_keys_debug->erase(i->get_key_name());
    }
  }

//dout(10) << "write_log_and_missing, clearing up to " << dirty_to << dendl;
  if (touch_log)
    t.touch(coll, log_oid);
  if (dirty_to != eversion_t()) {
    t.omap_rmkeyrange(
      coll, log_oid,
      eversion_t().get_key_name(), dirty_to.get_key_name());
    clear_up_to(log_keys_debug, dirty_to.get_key_name());
  }
  if (dirty_to != eversion_t::max() && dirty_from != eversion_t::max()) {
    //   dout(10) << "write_log_and_missing, clearing from " << dirty_from << dendl;
    t.omap_rmkeyrange(
      coll, log_oid,
      dirty_from.get_key_name(), eversion_t::max().get_key_name());
    clear_after(log_keys_debug, dirty_from.get_key_name());
  }

  for (list<pg_log_entry_t>::iterator p = log.log.begin();
       p != log.log.end() && p->version <= dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    (*km)[p->get_key_name()].claim(bl);
  }

  for (list<pg_log_entry_t>::reverse_iterator p = log.log.rbegin();
       p != log.log.rend() &&
	 (p->version >= dirty_from || p->version >= writeout_from) &&
	 p->version >= dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    (*km)[p->get_key_name()].claim(bl);
  }

  if (log_keys_debug) {
    for (map<string, bufferlist>::iterator i = (*km).begin();
	 i != (*km).end();
	 ++i) {
      if (i->first[0] == '_')
	continue;
      assert(!log_keys_debug->count(i->first));
      log_keys_debug->insert(i->first);
    }
  }

  if (dirty_divergent_priors) {
    //dout(10) << "write_log_and_missing: writing divergent_priors" << dendl;
    ::encode(divergent_priors, (*km)["divergent_priors"]);
  }
  if (require_rollback) {
    ::encode(
      log.get_can_rollback_to(),
      (*km)["can_rollback_to"]);
    ::encode(
      log.get_rollback_info_trimmed_to(),
      (*km)["rollback_info_trimmed_to"]);
  }

  if (!to_remove.empty())
    t.omap_rmkeys(coll, log_oid, to_remove);
}

void PGLog::_write_log_and_missing(
  ObjectStore::Transaction& t,
  map<string,bufferlist>* km,
  pg_log_t &log,
  const coll_t& coll, const ghobject_t &log_oid,
  eversion_t dirty_to,
  eversion_t dirty_from,
  eversion_t writeout_from,
  const set<eversion_t> &trimmed,
  const pg_missing_tracker_t &missing,
  bool touch_log,
  bool require_rollback,
  bool clear_divergent_priors,
  set<string> *log_keys_debug
  ) {
  set<string> to_remove;
  for (set<eversion_t>::const_iterator i = trimmed.begin();
       i != trimmed.end();
       ++i) {
    to_remove.insert(i->get_key_name());
    if (log_keys_debug) {
      assert(log_keys_debug->count(i->get_key_name()));
      log_keys_debug->erase(i->get_key_name());
    }
  }

  if (touch_log)
    t.touch(coll, log_oid);
  if (dirty_to != eversion_t()) {
    t.omap_rmkeyrange(
      coll, log_oid,
      eversion_t().get_key_name(), dirty_to.get_key_name());
    clear_up_to(log_keys_debug, dirty_to.get_key_name());
  }
  if (dirty_to != eversion_t::max() && dirty_from != eversion_t::max()) {
    //   dout(10) << "write_log_and_missing, clearing from " << dirty_from << dendl;
    t.omap_rmkeyrange(
      coll, log_oid,
      dirty_from.get_key_name(), eversion_t::max().get_key_name());
    clear_after(log_keys_debug, dirty_from.get_key_name());
  }

  //将log.log中的dirty_to版本以下的,编码进bl
  for (list<pg_log_entry_t>::iterator p = log.log.begin();
       p != log.log.end() && p->version <= dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    (*km)[p->get_key_name()].claim(bl);
  }

  for (list<pg_log_entry_t>::reverse_iterator p = log.log.rbegin();
       p != log.log.rend() &&
	 (p->version >= dirty_from || p->version >= writeout_from) &&
	 p->version >= dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    (*km)[p->get_key_name()].claim(bl);
  }

  if (log_keys_debug) {
    for (map<string, bufferlist>::iterator i = (*km).begin();
	 i != (*km).end();
	 ++i) {
      if (i->first[0] == '_')
	continue;
      assert(!log_keys_debug->count(i->first));
      log_keys_debug->insert(i->first);
    }
  }

  if (clear_divergent_priors) {
    //dout(10) << "write_log_and_missing: writing divergent_priors" << dendl;
    to_remove.insert("divergent_priors");
  }
  missing.get_changed(
    [&](const hobject_t &obj) {
      string key = string("missing/") + obj.to_str();
      pg_missing_item item;
      if (!missing.is_missing(obj, &item)) {
	to_remove.insert(key);
      } else {
	::encode(make_pair(obj, item), (*km)[key]);
      }
    });
  if (require_rollback) {
    ::encode(
      log.get_can_rollback_to(),
      (*km)["can_rollback_to"]);
    ::encode(
      log.get_rollback_info_trimmed_to(),
      (*km)["rollback_info_trimmed_to"]);
  }

  if (!to_remove.empty())
    t.omap_rmkeys(coll, log_oid, to_remove);//将rm删除合入事务
}
