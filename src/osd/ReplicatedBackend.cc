// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/errno.h"
#include "ReplicatedBackend.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MOSDRepOpReply.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPushReply.h"

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, ReplicatedBackend *pgb) {
  return *_dout << pgb->get_parent()->gen_dbg_prefix();
}

namespace {
class PG_SendMessageOnConn: public Context {
  PGBackend::Listener *pg;
  Message *reply;
  ConnectionRef conn;
  public:
  PG_SendMessageOnConn(
    PGBackend::Listener *pg,
    Message *reply,
    ConnectionRef conn) : pg(pg), reply(reply), conn(conn) {}
  void finish(int) {
    pg->send_message_osd_cluster(reply, conn.get());
  }
};

class PG_RecoveryQueueAsync : public Context {
  PGBackend::Listener *pg;
  GenContext<ThreadPool::TPHandle&> *c;
  public:
  PG_RecoveryQueueAsync(
    PGBackend::Listener *pg,
    GenContext<ThreadPool::TPHandle&> *c) : pg(pg), c(c) {}
  void finish(int) {
    pg->schedule_recovery_work(c);
  }
};
}

struct ReplicatedBackend::C_OSD_RepModifyApply : public Context {
  ReplicatedBackend *pg;
  RepModifyRef rm;
  C_OSD_RepModifyApply(ReplicatedBackend *pg, RepModifyRef r)
    : pg(pg), rm(r) {}
  void finish(int r) {
    pg->sub_op_modify_applied(rm);
  }
};

struct ReplicatedBackend::C_OSD_RepModifyCommit : public Context {
  ReplicatedBackend *pg;
  RepModifyRef rm;
  C_OSD_RepModifyCommit(ReplicatedBackend *pg, RepModifyRef r)
    : pg(pg), rm(r) {}
  void finish(int r) {
    pg->sub_op_modify_commit(rm);
  }
};

static void log_subop_stats(
  PerfCounters *logger,
  OpRequestRef op, int subop)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t latency = now;
  latency -= op->get_req()->get_recv_stamp();


  logger->inc(l_osd_sop);
  logger->tinc(l_osd_sop_lat, latency);
  logger->inc(subop);

  if (subop != l_osd_sop_pull) {
    uint64_t inb = op->get_req()->get_data().length();
    logger->inc(l_osd_sop_inb, inb);
    if (subop == l_osd_sop_w) {
      logger->inc(l_osd_sop_w_inb, inb);
      logger->tinc(l_osd_sop_w_lat, latency);
    } else if (subop == l_osd_sop_push) {
      logger->inc(l_osd_sop_push_inb, inb);
      logger->tinc(l_osd_sop_push_lat, latency);
    } else
      assert("no support subop" == 0);
  } else {
    logger->tinc(l_osd_sop_pull_lat, latency);
  }
}

ReplicatedBackend::ReplicatedBackend(
  PGBackend::Listener *pg,
  coll_t coll,
  ObjectStore::CollectionHandle &c,
  ObjectStore *store,
  CephContext *cct) :
  PGBackend(pg, store, coll, c),
  cct(cct) {}

//按h中的参数或发pushes,或发pulls
void ReplicatedBackend::run_recovery_op(
  PGBackend::RecoveryHandle *_h,
  int priority)
{
  RPGHandle *h = static_cast<RPGHandle *>(_h);
  send_pushes(priority, h->pushes);
  send_pulls(priority, h->pulls);
  delete h;
}

//针对pull,构造请求,针对push构造响应.(如果主上有,优先从主上拿,主上没有从别的上面拿)
void ReplicatedBackend::recover_object(
  const hobject_t &hoid,
  eversion_t v,
  ObjectContextRef head,
  ObjectContextRef obc,
  RecoveryHandle *_h
  )
{
  dout(10) << __func__ << ": " << hoid << dendl;
  RPGHandle *h = static_cast<RPGHandle *>(_h);//两个函数均将数据封闭在_h中
  if (get_parent()->get_local_missing().is_missing(hoid)) {//如果自已pglog中缺这个对象
    assert(!obc);
    // pull
    prepare_pull(//准备拉过来
      v,
      hoid,
      head,
      h);
    return;
  } else {
    assert(obc);
    int started = start_pushes(//自已有,推过去
      hoid,
      obc,
      h);
    assert(started > 0);
  }
}

void ReplicatedBackend::check_recovery_sources(const OSDMapRef& osdmap)
{
  for(map<pg_shard_t, set<hobject_t, hobject_t::BitwiseComparator> >::iterator i = pull_from_peer.begin();
      i != pull_from_peer.end();
      ) {
    if (osdmap->is_down(i->first.osd)) {
      dout(10) << "check_recovery_sources resetting pulls from osd." << i->first
	       << ", osdmap has it marked down" << dendl;
      for (set<hobject_t, hobject_t::BitwiseComparator>::iterator j = i->second.begin();
	   j != i->second.end();
	   ++j) {
	assert(pulling.count(*j) == 1);
	get_parent()->cancel_pull(*j);
	pulling.erase(*j);
      }
      pull_from_peer.erase(i++);
    } else {
      ++i;
    }
  }
}

bool ReplicatedBackend::can_handle_while_inactive(OpRequestRef op)
{
  dout(10) << __func__ << ": " << op << dendl;
  switch (op->get_req()->get_type()) {
  case MSG_OSD_PG_PULL:
    return true;
  case MSG_OSD_SUBOP: {
    MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
    if (m->ops.size() >= 1) {
      OSDOp *first = &m->ops[0];
      switch (first->op.op) {
      case CEPH_OSD_OP_PULL:
	return true;
      default:
	return false;
      }
    } else {
      return false;
    }
  }
  default:
    return false;
  }
}

//幅本形式的处理消息
bool ReplicatedBackend::handle_message(
  OpRequestRef op
  )
{
  dout(10) << __func__ << ": " << op << dendl;
  switch (op->get_req()->get_type()) {
  case MSG_OSD_PG_PUSH:
    do_push(op);//收到主(从)发送过来的push操作
    return true;

  case MSG_OSD_PG_PULL://处理pull
    do_pull(op);
    return true;

  case MSG_OSD_PG_PUSH_REPLY://push_reply
    do_push_reply(op);
    return true;

  case MSG_OSD_SUBOP: {
    MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
    if (m->ops.size() >= 1) {
      OSDOp *first = &m->ops[0];
      switch (first->op.op) {
      case CEPH_OSD_OP_PULL:
	sub_op_pull(op);
	return true;
      case CEPH_OSD_OP_PUSH:
	sub_op_push(op);
	return true;
      default:
	break;
      }
    } else {
      sub_op_modify(op);
      return true;
    }
    break;
  }

  case MSG_OSD_REPOP: {
    sub_op_modify(op);//幅本将在这里处理完成
    return true;
  }

  case MSG_OSD_SUBOPREPLY: {
    MOSDSubOpReply *r = static_cast<MOSDSubOpReply*>(op->get_req());
    if (r->ops.size() >= 1) {
      OSDOp &first = r->ops[0];
      switch (first.op.op) {
      case CEPH_OSD_OP_PUSH:
	// continue peer recovery
	sub_op_push_reply(op);
	return true;
      }
    }
    break;
  }

  case MSG_OSD_REPOPREPLY: {
    sub_op_modify_reply(op);//幅本发送过来的响应消息
    return true;
  }

  default:
    break;
  }
  return false;
}

void ReplicatedBackend::clear_recovery_state()
{
  // clear pushing/pulling maps
  pushing.clear();
  pulling.clear();
  pull_from_peer.clear();
}

void ReplicatedBackend::on_change()
{
  dout(10) << __func__ << dendl;
  for (map<ceph_tid_t, InProgressOp>::iterator i = in_progress_ops.begin();
       i != in_progress_ops.end();
       in_progress_ops.erase(i++)) {
    if (i->second.on_commit)
      delete i->second.on_commit;
    if (i->second.on_applied)
      delete i->second.on_applied;
  }
  clear_recovery_state();
}

void ReplicatedBackend::on_flushed()
{
}

int ReplicatedBackend::objects_read_sync(
  const hobject_t &hoid,
  uint64_t off,
  uint64_t len,
  uint32_t op_flags,
  bufferlist *bl)
{
  return store->read(ch, ghobject_t(hoid), off, len, *bl, op_flags);
}

struct AsyncReadCallback : public GenContext<ThreadPool::TPHandle&> {
  int r;
  Context *c;
  AsyncReadCallback(int r, Context *c) : r(r), c(c) {}
  void finish(ThreadPool::TPHandle&) {
    c->complete(r);
    c = NULL;
  }
  ~AsyncReadCallback() {
    delete c;
  }
};
void ReplicatedBackend::objects_read_async(
  const hobject_t &hoid,
  const list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		  pair<bufferlist*, Context*> > > &to_read,
  Context *on_complete,
  bool fast_read)
{
  // There is no fast read implementation for replication backend yet
  assert(!fast_read);

  int r = 0;
  for (list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		 pair<bufferlist*, Context*> > >::const_iterator i =
	   to_read.begin();
       i != to_read.end() && r >= 0;
       ++i) {
    int _r = store->read(ch, ghobject_t(hoid), i->first.get<0>(),
			 i->first.get<1>(), *(i->second.first),
			 i->first.get<2>());
    if (i->second.second) {
      get_parent()->schedule_recovery_work(
	get_parent()->bless_gencontext(
	  new AsyncReadCallback(_r, i->second.second)));
    }
    if (_r < 0)
      r = _r;
  }
  get_parent()->schedule_recovery_work(
    get_parent()->bless_gencontext(
      new AsyncReadCallback(r, on_complete)));
}

class C_OSD_OnOpCommit : public Context {
  ReplicatedBackend *pg;
  ReplicatedBackend::InProgressOp *op;
public:
  C_OSD_OnOpCommit(ReplicatedBackend *pg, ReplicatedBackend::InProgressOp *op) 
    : pg(pg), op(op) {}
  void finish(int) {
    pg->op_commit(op);
  }
};

class C_OSD_OnOpApplied : public Context {
  ReplicatedBackend *pg;
  ReplicatedBackend::InProgressOp *op;
public:
  C_OSD_OnOpApplied(ReplicatedBackend *pg, ReplicatedBackend::InProgressOp *op) 
    : pg(pg), op(op) {}
  void finish(int) {
    pg->op_applied(op);
  }
};

void generate_transaction(
  PGTransactionUPtr &pgt,
  const coll_t &coll,
  bool legacy_log_entries,
  vector<pg_log_entry_t> &log_entries,
  ObjectStore::Transaction *t,
  set<hobject_t, hobject_t::BitwiseComparator> *added,
  set<hobject_t, hobject_t::BitwiseComparator> *removed)
{
  assert(t);
  assert(added);
  assert(removed);

  for (auto &&le: log_entries) {
    le.mark_unrollbackable();
    auto oiter = pgt->op_map.find(le.soid);
    if (oiter != pgt->op_map.end() && oiter->second.updated_snaps) {
      vector<snapid_t> snaps(
	oiter->second.updated_snaps->second.begin(),
	oiter->second.updated_snaps->second.end());
      ::encode(snaps, le.snaps);
    }
  }

  pgt->safe_create_traverse(
    [&](pair<const hobject_t, PGTransaction::ObjectOperation> &obj_op) {
      const hobject_t &oid = obj_op.first;
      const ghobject_t goid =
	ghobject_t(oid, ghobject_t::NO_GEN, shard_id_t::NO_SHARD);
      const PGTransaction::ObjectOperation &op = obj_op.second;

      if (oid.is_temp()) {
	if (op.is_fresh_object()) {
	  added->insert(oid);
	} else if (op.is_delete()) {
	  removed->insert(oid);
	}
      }

      if (op.delete_first) {
	t->remove(coll, goid);
      }

      match(
	op.init_type,
	[&](const PGTransaction::ObjectOperation::Init::None &) {
	},
	[&](const PGTransaction::ObjectOperation::Init::Create &op) {
	  t->touch(coll, goid);
	},
	[&](const PGTransaction::ObjectOperation::Init::Clone &op) {
	  t->clone(
	    coll,
	    ghobject_t(
	      op.source, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
	    goid);
	},
	[&](const PGTransaction::ObjectOperation::Init::Rename &op) {
	  assert(op.source.is_temp());
	  t->collection_move_rename(
	    coll,
	    ghobject_t(
	      op.source, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
	    coll,
	    goid);
	});

      if (op.truncate) {
	t->truncate(coll, goid, op.truncate->first);
	if (op.truncate->first != op.truncate->second)
	  t->truncate(coll, goid, op.truncate->second);
      }

      if (!op.attr_updates.empty()) {
	map<string, bufferlist> attrs;
	for (auto &&p: op.attr_updates) {
	  if (p.second)
	    attrs[p.first] = *(p.second);
	  else
	    t->rmattr(coll, goid, p.first);
	}
	t->setattrs(coll, goid, attrs);
      }

      if (op.clear_omap)
	t->omap_clear(coll, goid);
      if (op.omap_header)
	t->omap_setheader(coll, goid, *(op.omap_header));

      for (auto &&up: op.omap_updates) {
	using UpdateType = PGTransaction::ObjectOperation::OmapUpdateType;
	switch (up.first) {
	case UpdateType::Remove:
	  t->omap_rmkeys(coll, goid, up.second);
	  break;
	case UpdateType::Insert:
	  t->omap_setkeys(coll, goid, up.second);
	  break;
	}
      }

      // updated_snaps doesn't matter since we marked unrollbackable

      if (op.alloc_hint) {
	auto &hint = *(op.alloc_hint);
	t->set_alloc_hint(
	  coll,
	  goid,
	  hint.expected_object_size,
	  hint.expected_write_size,
	  hint.flags);
      }

      for (auto &&extent: op.buffer_updates) {
	using BufferUpdate = PGTransaction::ObjectOperation::BufferUpdate;
	match(
	  extent.get_val(),
	  [&](const BufferUpdate::Write &op) {
	    t->write(
	      coll,
	      goid,
	      extent.get_off(),
	      extent.get_len(),
	      op.buffer);
	  },
	  [&](const BufferUpdate::Zero &op) {
	    t->zero(
	      coll,
	      goid,
	      extent.get_off(),
	      extent.get_len());
	  },
	  [&](const BufferUpdate::CloneRange &op) {
	    assert(op.len == extent.get_len());
	    t->clone_range(
	      coll,
	      ghobject_t(op.from, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
	      goid,
	      op.offset,
	      extent.get_len(),
	      extent.get_off());
	  });
      }
    });
}

void ReplicatedBackend::submit_transaction(
  const hobject_t &soid,//操作哪个对象
  const object_stat_sum_t &delta_stats,
  const eversion_t &at_version,
  PGTransactionUPtr &&_t,//事务
  const eversion_t &trim_to,
  const eversion_t &roll_forward_to,
  const vector<pg_log_entry_t> &_log_entries,//本次操作对应的pglog
  boost::optional<pg_hit_set_history_t> &hset_history,
  Context *on_local_applied_sync,
  Context *on_all_acked,
  Context *on_all_commit,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef orig_op)
{
  parent->apply_stats(
    soid,
    delta_stats);

  vector<pg_log_entry_t> log_entries(_log_entries);
  ObjectStore::Transaction op_t;
  PGTransactionUPtr t(std::move(_t));
  set<hobject_t, hobject_t::BitwiseComparator> added, removed;
  generate_transaction(
    t,
    coll,
    !get_osdmap()->test_flag(CEPH_OSDMAP_REQUIRE_KRAKEN),
    log_entries,
    &op_t,
    &added,
    &removed);
  assert(added.size() <= 1);
  assert(removed.size() <= 1);

  assert(!in_progress_ops.count(tid));
  assert(!in_progress_ops.count(tid));//tid一定没有在处理中

  //向in_progress_ops中加入当前操作,用于接受其它幅本的reply
  InProgressOp &op = in_progress_ops.insert(
    make_pair(
      tid,
      InProgressOp(
	tid, on_all_commit, on_all_acked,
	orig_op, at_version)
      )
    ).first->second;

  //我们需要等待这些个幅本的commit,applied的ack消息,故这里把它们信息先注册到相应的列表.
  //含primary节点
  op.waiting_for_applied.insert(
    parent->get_actingbackfill_shards().begin(),
    parent->get_actingbackfill_shards().end());
  op.waiting_for_commit.insert(
    parent->get_actingbackfill_shards().begin(),
    parent->get_actingbackfill_shards().end());

  issue_op(
    soid,
    at_version,
    tid,
    reqid,
    trim_to,
    at_version,
    added.size() ? *(added.begin()) : hobject_t(),
    removed.size() ? *(removed.begin()) : hobject_t(),
    log_entries,
    hset_history,
    &op,
    op_t);

  add_temp_objs(added);
  clear_temp_objs(removed);

  //primary osd记录日志
  parent->log_operation(
    log_entries,
    hset_history,
    trim_to,
    at_version,
    true,
    op_t);
  
  op_t.register_on_applied_sync(on_local_applied_sync);
  op_t.register_on_applied(
    parent->bless_context(
      new C_OSD_OnOpApplied(this, &op)));
  op_t.register_on_commit(
    parent->bless_context(
      new C_OSD_OnOpCommit(this, &op)));

  //这里将op_t构造成vector,然后传入queue_transactions
  vector<ObjectStore::Transaction> tls;
  tls.push_back(std::move(op_t));

  parent->queue_transactions(tls, op.op);//事务入队（primary处理，三个回调会被调起）
}

//on_applied回调处理
void ReplicatedBackend::op_applied(
  InProgressOp *op)
{
  dout(10) << __func__ << ": " << op->tid << dendl;
  if (op->op)
    op->op->mark_event("op_applied");

  op->waiting_for_applied.erase(get_parent()->whoami_shard());
  parent->op_applied(op->v);//清洗相关?

  if (op->waiting_for_applied.empty()) {
    op->on_applied->complete(0);
    op->on_applied = 0;
  }
  if (op->done()) {
    assert(!op->on_commit && !op->on_applied);
    in_progress_ops.erase(op->tid);
  }
}

//on_commit事件处理
void ReplicatedBackend::op_commit(
  InProgressOp *op)
{
  dout(10) << __func__ << ": " << op->tid << dendl;
  if (op->op)
    op->op->mark_event("op_commit");

  op->waiting_for_commit.erase(get_parent()->whoami_shard());

  if (op->waiting_for_commit.empty()) {
    op->on_commit->complete(0);//如果所有人均回应,则触发回调
    op->on_commit = 0;
  }
  //如果所有人均已处理,回收in_progress_ops中的资源
  if (op->done()) {
    assert(!op->on_commit && !op->on_applied);
    in_progress_ops.erase(op->tid);
  }
}

//osd_repopreply消息处理
void ReplicatedBackend::sub_op_modify_reply(OpRequestRef op)
{
  MOSDRepOpReply *r = static_cast<MOSDRepOpReply *>(op->get_req());
  r->finish_decode();
  assert(r->get_header().type == MSG_OSD_REPOPREPLY);

  op->mark_started();

  // must be replication.
  ceph_tid_t rep_tid = r->get_tid();
  pg_shard_t from = r->from;

  //如果响应消息自in_progress_ops中找不到,则忽略
  if (in_progress_ops.count(rep_tid)) {
    map<ceph_tid_t, InProgressOp>::iterator iter =
      in_progress_ops.find(rep_tid);
    InProgressOp &ip_op = iter->second;
    MOSDOp *m = NULL;
    if (ip_op.op)
      m = static_cast<MOSDOp *>(ip_op.op->get_req());

    if (m)
      dout(7) << __func__ << ": tid " << ip_op.tid << " op " //<< *m
	      << " ack_type " << (int)r->ack_type
	      << " from " << from
	      << dendl;
    else
      dout(7) << __func__ << ": tid " << ip_op.tid << " (no op) "
	      << " ack_type " << (int)r->ack_type
	      << " from " << from
	      << dendl;

    // oh, good.

    if (r->ack_type & CEPH_OSD_FLAG_ONDISK) {//commit阶段的响应(ONDISK标记)
      assert(ip_op.waiting_for_commit.count(from));
      ip_op.waiting_for_commit.erase(from);//移除对此osd的等待,因为已经回来了.
      if (ip_op.op) {
        ostringstream ss;
        ss << "sub_op_commit_rec from " << from;
	ip_op.op->mark_event(ss.str());
      }
    } else {//applied阶段的响应
      assert(ip_op.waiting_for_applied.count(from));
      if (ip_op.op) {
        ostringstream ss;
        ss << "sub_op_applied_rec from " << from;
	ip_op.op->mark_event(ss.str());
      }
    }
    //这个位置是不是放的不对?不应在else内部吗?
    //猜测:只要有一个commit回来,说明已提交到日志,接下来即便applied不回来,
    //也需要重试使数据保证写入完成.原因commit时,需要向用户返回write完成.
    //将waiting_for_applied移至此处,可相应的减少读的廷迟,由于读仍是从主
    //进行读取的,如果主发生切换?会被加入等待恢复.
    ip_op.waiting_for_applied.erase(from);

    parent->update_peer_last_complete_ondisk(
      from,
      r->get_last_complete_ondisk());//记录回应信息

    //以下两个回调的顺序是不是反了?(这样会不会有问题?)
    if (ip_op.waiting_for_applied.empty() &&
        ip_op.on_applied) {
      ip_op.on_applied->complete(0);//执行on_applied回调
      ip_op.on_applied = 0;//表示调用过了
    }
    if (ip_op.waiting_for_commit.empty() &&
        ip_op.on_commit) {
      ip_op.on_commit->complete(0);//如果是最后一个commit,则执行commit
      ip_op.on_commit= 0;
    }
    if (ip_op.done()) {
      assert(!ip_op.on_commit && !ip_op.on_applied);
      in_progress_ops.erase(iter);//移除
    }
  }
}

void ReplicatedBackend::be_deep_scrub(
  const hobject_t &poid,
  uint32_t seed,
  ScrubMap::object &o,
  ThreadPool::TPHandle &handle)
{
  dout(10) << __func__ << " " << poid << " seed " 
	   << std::hex << seed << std::dec << dendl;
  bufferhash h(seed), oh(seed);
  bufferlist bl, hdrbl;
  int r;
  __u64 pos = 0;

  uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL | CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;

  //读对象
  while (true) {
    handle.reset_tp_timeout();
    r = store->read(//一次读512k
	  ch,
	  ghobject_t(
	    poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
	  pos,
	  cct->_conf->osd_deep_scrub_stride, bl,
	  fadvise_flags, true);
    if (r <= 0)
      break;

    h << bl;//生成crc
    pos += bl.length();
    bl.clear();
  }
  if (r == -EIO) {//读错.
    dout(25) << __func__ << "  " << poid << " got "
	     << r << " on read, read_error" << dendl;
    o.read_error = true;
    return;
  }
  o.digest = h.digest();//填充crc
  o.digest_present = true;//标记crc有效

  bl.clear();//刚才读的对象那些数据,丢掉了.
  r = store->omap_get_header(//读取omap头
    coll,
    ghobject_t(
      poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    &hdrbl, true);
  // NOTE: bobtail to giant, we would crc the head as (len, head).
  // that changes at the same time we start using a non-zero seed.
  if (r == 0 && hdrbl.length()) {
    dout(25) << "CRC header " << string(hdrbl.c_str(), hdrbl.length())
             << dendl;
    if (seed == 0) {
      // legacy
      bufferlist bl;
      ::encode(hdrbl, bl);
      oh << bl;
    } else {
      oh << hdrbl;//对omap执行crc运算{重载成<<运算符,真的很好吗?}
    }
  } else if (r == -EIO) {
    dout(25) << __func__ << "  " << poid << " got "
	     << r << " on omap header read, read_error" << dendl;
    o.read_error = true;
    return;
  }

  ObjectMap::ObjectMapIterator iter = store->get_omap_iterator(
    coll,
    ghobject_t(
      poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  assert(iter);
  uint64_t keys_scanned = 0;
  for (iter->seek_to_first(); iter->status() == 0 && iter->valid();
    iter->next(false)) {
    if (cct->_conf->osd_scan_list_ping_tp_interval &&
	(keys_scanned % cct->_conf->osd_scan_list_ping_tp_interval == 0)) {
      handle.reset_tp_timeout();
    }
    ++keys_scanned;

    dout(25) << "CRC key " << iter->key() << " value:\n";
    iter->value().hexdump(*_dout);//每一个属性
    *_dout << dendl;

    ::encode(iter->key(), bl);
    ::encode(iter->value(), bl);
    oh << bl;
    bl.clear();
  }

  if (iter->status() < 0) {
    dout(25) << __func__ << "  " << poid
             << " on omap scan, db status error" << dendl;
    o.read_error = true;
    return;
  }

  //Store final calculated CRC32 of omap header & key/values
  o.omap_digest = oh.digest();
  o.omap_digest_present = true;//标记crc的有效.
  dout(20) << __func__ << "  " << poid << " omap_digest "
	   << std::hex << o.omap_digest << std::dec << dendl;
}

void ReplicatedBackend::_do_push(OpRequestRef op)
{
  MOSDPGPush *m = static_cast<MOSDPGPush *>(op->get_req());
  assert(m->get_type() == MSG_OSD_PG_PUSH);
  pg_shard_t from = m->from;

  op->mark_started();

  vector<PushReplyOp> replies;
  ObjectStore::Transaction t;
  for (vector<PushOp>::iterator i = m->pushes.begin();
       i != m->pushes.end();
       ++i) {
    replies.push_back(PushReplyOp());
    handle_push(from, *i, &(replies.back()), &t);//针对每个push响应PushReplyOp,暂时加入
  }

  MOSDPGPushReply *reply = new MOSDPGPushReply;
  reply->from = get_parent()->whoami_shard();
  reply->set_priority(m->get_priority());
  reply->pgid = get_info().pgid;
  reply->map_epoch = m->map_epoch;
  reply->replies.swap(replies);
  reply->compute_cost(cct);

  t.register_on_complete(
    new PG_SendMessageOnConn(
      get_parent(), reply, m->get_connection()));

  get_parent()->queue_transaction(std::move(t));//push操作事务入队处
}

struct C_ReplicatedBackend_OnPullComplete : GenContext<ThreadPool::TPHandle&> {
  ReplicatedBackend *bc;
  list<hobject_t> to_continue;
  int priority;
  C_ReplicatedBackend_OnPullComplete(ReplicatedBackend *bc, int priority)
    : bc(bc), priority(priority) {}

  void finish(ThreadPool::TPHandle &handle) {
    ReplicatedBackend::RPGHandle *h = bc->_open_recovery_op();
    for (list<hobject_t>::iterator i =
	   to_continue.begin();
	 i != to_continue.end();
	 ++i) {
      map<hobject_t, ReplicatedBackend::PullInfo, hobject_t::BitwiseComparator>::iterator j =
	bc->pulling.find(*i);
      assert(j != bc->pulling.end());
      if (!bc->start_pushes(*i, j->second.obc, h)) {
	bc->get_parent()->on_global_recover(
	  *i, j->second.stat);
      }
      bc->pulling.erase(*i);
      handle.reset_tp_timeout();
    }
    bc->run_recovery_op(h, priority);
  }
};

void ReplicatedBackend::_do_pull_response(OpRequestRef op)
{
  MOSDPGPush *m = static_cast<MOSDPGPush *>(op->get_req());
  assert(m->get_type() == MSG_OSD_PG_PUSH);
  pg_shard_t from = m->from;

  op->mark_started();

  vector<PullOp> replies(1);
  ObjectStore::Transaction t;
  list<hobject_t> to_continue;
  for (vector<PushOp>::iterator i = m->pushes.begin();
       i != m->pushes.end();
       ++i) {
    bool more = handle_pull_response(from, *i, &(replies.back()), &to_continue, &t);
    if (more)
      replies.push_back(PullOp());
  }
  if (!to_continue.empty()) {
    C_ReplicatedBackend_OnPullComplete *c =
      new C_ReplicatedBackend_OnPullComplete(
	this,
	m->get_priority());
    c->to_continue.swap(to_continue);
    t.register_on_complete(
      new PG_RecoveryQueueAsync(
	get_parent(),
	get_parent()->bless_gencontext(c)));
  }
  replies.erase(replies.end() - 1);

  if (replies.size()) {
    MOSDPGPull *reply = new MOSDPGPull;
    reply->from = parent->whoami_shard();
    reply->set_priority(m->get_priority());
    reply->pgid = get_info().pgid;
    reply->map_epoch = m->map_epoch;
    reply->pulls.swap(replies);
    reply->compute_cost(cct);

    t.register_on_complete(
      new PG_SendMessageOnConn(
	get_parent(), reply, m->get_connection()));
  }

  get_parent()->queue_transaction(std::move(t));
}

//pull处理
void ReplicatedBackend::do_pull(OpRequestRef op)
{
  MOSDPGPull *m = static_cast<MOSDPGPull *>(op->get_req());
  assert(m->get_type() == MSG_OSD_PG_PULL);
  pg_shard_t from = m->from;

  map<pg_shard_t, vector<PushOp> > replies;
  for (vector<PullOp>::iterator i = m->pulls.begin();
       i != m->pulls.end();
       ++i) {
    replies[from].push_back(PushOp());//针对每一个pull操作,构造成push消息
    handle_pull(from, *i, &(replies[from].back()));//处理pull方法
  }
  send_pushes(m->get_priority(), replies);
}

void ReplicatedBackend::do_push_reply(OpRequestRef op)
{
  MOSDPGPushReply *m = static_cast<MOSDPGPushReply *>(op->get_req());
  assert(m->get_type() == MSG_OSD_PG_PUSH_REPLY);
  pg_shard_t from = m->from;

  vector<PushOp> replies(1);
  for (vector<PushReplyOp>::iterator i = m->replies.begin();
       i != m->replies.end();
       ++i) {
    bool more = handle_push_reply(from, *i, &(replies.back()));
    if (more)
      replies.push_back(PushOp());
  }
  replies.erase(replies.end() - 1);

  map<pg_shard_t, vector<PushOp> > _replies;
  _replies[from].swap(replies);
  send_pushes(m->get_priority(), _replies);
}

Message * ReplicatedBackend::generate_subop(
  const hobject_t &soid,
  const eversion_t &at_version,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  eversion_t pg_trim_to,
  eversion_t pg_roll_forward_to,
  hobject_t new_temp_oid,
  hobject_t discard_temp_oid,
  const vector<pg_log_entry_t> &log_entries,
  boost::optional<pg_hit_set_history_t> &hset_hist,
  ObjectStore::Transaction &op_t,
  pg_shard_t peer,
  const pg_info_t &pinfo)
{
  int acks_wanted = CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK;
  // forward the write/update/whatever
  MOSDRepOp *wr = new MOSDRepOp(
    reqid, parent->whoami_shard(),
    spg_t(get_info().pgid.pgid, peer.shard),
    soid, acks_wanted,
    get_osdmap()->get_epoch(),
    tid, at_version);

  // ship resulting transaction, log entries, and pg_stats
  if (!parent->should_send_op(peer, soid)) {
    dout(10) << "issue_repop shipping empty opt to osd." << peer
	     <<", object " << soid
	     << " beyond MAX(last_backfill_started "
	     << ", pinfo.last_backfill "
	     << pinfo.last_backfill << ")" << dendl;
    ObjectStore::Transaction t;
    ::encode(t, wr->get_data());
  } else {
    ::encode(op_t, wr->get_data());
    wr->get_header().data_off = op_t.get_data_alignment();
  }

  ::encode(log_entries, wr->logbl);

  if (pinfo.is_incomplete())
    wr->pg_stats = pinfo.stats;  // reflects backfill progress
  else
    wr->pg_stats = get_info().stats;

  wr->pg_trim_to = pg_trim_to;
  wr->pg_roll_forward_to = pg_roll_forward_to;

  wr->new_temp_oid = new_temp_oid;
  wr->discard_temp_oid = discard_temp_oid;
  wr->updated_hit_set_history = hset_hist;
  return wr;
}

void ReplicatedBackend::issue_op(
  const hobject_t &soid,
  const eversion_t &at_version,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  eversion_t pg_trim_to,
  eversion_t pg_roll_forward_to,
  hobject_t new_temp_oid,
  hobject_t discard_temp_oid,
  const vector<pg_log_entry_t> &log_entries,
  boost::optional<pg_hit_set_history_t> &hset_hist,
  InProgressOp *op,
  ObjectStore::Transaction &op_t)
{

  if (parent->get_actingbackfill_shards().size() > 1) {
    ostringstream ss;
    set<pg_shard_t> replicas = parent->get_actingbackfill_shards();
    replicas.erase(parent->whoami_shard());
    ss << "waiting for subops from " << replicas;
    if (op->op)
      op->op->mark_sub_op_sent(ss.str());
  }
  //遍历当前pg的其它幅本
  for (set<pg_shard_t>::const_iterator i =
	 parent->get_actingbackfill_shards().begin();
       i != parent->get_actingbackfill_shards().end();
       ++i) {
	  //一不小心就遇到了自已,呵呵,忽略
    if (*i == parent->whoami_shard()) continue;
    pg_shard_t peer = *i;
    const pg_info_t &pinfo = parent->get_shard_info().find(peer)->second;

    Message *wr;
    wr = generate_subop(
      soid,
      at_version,
      tid,
      reqid,
      pg_trim_to,
      pg_roll_forward_to,
      new_temp_oid,
      discard_temp_oid,
      log_entries,
      hset_hist,
      op_t,
      peer,
      pinfo);

    //前面构造了消息,现在把消息发给它(peer.osd)
    get_parent()->send_message_osd_cluster(
      peer.osd, wr, get_osdmap()->get_epoch());
  }
}

// sub op modify
//幅本操作入口
void ReplicatedBackend::sub_op_modify(OpRequestRef op)
{
  MOSDRepOp *m = static_cast<MOSDRepOp *>(op->get_req());
  m->finish_decode();
  int msg_type = m->get_type();
  assert(MSG_OSD_REPOP == msg_type);

  const hobject_t& soid = m->poid;

  dout(10) << "sub_op_modify trans"
           << " " << soid
           << " v " << m->version
	   << (m->logbl.length() ? " (transaction)" : " (parallel exec")
	   << " " << m->logbl.length()
	   << dendl;

  // sanity checks
  assert(m->map_epoch >= get_info().history.same_interval_since);

  // we better not be missing this.
  assert(!parent->get_log().get_missing().is_missing(soid));

  int ackerosd = m->get_source().num();

  op->mark_started();

  RepModifyRef rm(std::make_shared<RepModify>());
  rm->op = op;
  rm->ackerosd = ackerosd;
  rm->last_complete = get_info().last_complete;
  rm->epoch_started = get_osdmap()->get_epoch();

  assert(m->logbl.length());
  // shipped transaction and log entries
  vector<pg_log_entry_t> log;

  bufferlist::iterator p = m->get_data().begin();
  ::decode(rm->opt, p);

  if (m->new_temp_oid != hobject_t()) {
    dout(20) << __func__ << " start tracking temp " << m->new_temp_oid << dendl;
    add_temp_obj(m->new_temp_oid);
  }
  if (m->discard_temp_oid != hobject_t()) {
    dout(20) << __func__ << " stop tracking temp " << m->discard_temp_oid << dendl;
    if (rm->opt.empty()) {
      dout(10) << __func__ << ": removing object " << m->discard_temp_oid
	       << " since we won't get the transaction" << dendl;
      rm->localt.remove(coll, ghobject_t(m->discard_temp_oid));
    }
    clear_temp_obj(m->discard_temp_oid);
  }

  p = m->logbl.begin();
  ::decode(log, p);//将pglog解出来
  rm->opt.set_fadvise_flag(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);

  bool update_snaps = false;
  if (!rm->opt.empty()) {
    // If the opt is non-empty, we infer we are before
    // last_backfill (according to the primary, not our
    // not-quite-accurate value), and should update the
    // collections now.  Otherwise, we do it later on push.
    update_snaps = true;
  }
  parent->update_stats(m->pg_stats);
  //replacate osd记录日志
  parent->log_operation(
    log,
    m->updated_hit_set_history,
    m->pg_trim_to,
    m->pg_roll_forward_to,
    update_snaps,
    rm->localt);//记录log

  rm->opt.register_on_commit(
    parent->bless_context(
      new C_OSD_RepModifyCommit(this, rm)));
  rm->localt.register_on_applied(
    parent->bless_context(
      new C_OSD_RepModifyApply(this, rm)));
  vector<ObjectStore::Transaction> tls;
  tls.reserve(2);
  tls.push_back(std::move(rm->localt));
  tls.push_back(std::move(rm->opt));
  parent->queue_transactions(tls, op);//事务入队（幅本处理）
  // op is cleaned up by oncommit/onapply when both are executed
}

void ReplicatedBackend::sub_op_modify_applied(RepModifyRef rm)
{
  rm->op->mark_event("sub_op_applied");
  rm->applied = true;

  dout(10) << "sub_op_modify_applied on " << rm << " op "
	   << *rm->op->get_req() << dendl;
  Message *m = rm->op->get_req();

  Message *ack = NULL;
  eversion_t version;

  if (m->get_type() == MSG_OSD_SUBOP) {
    // doesn't have CLIENT SUBOP feature ,use Subop
    MOSDSubOp *req = static_cast<MOSDSubOp*>(m);
    version = req->version;
    if (!rm->committed)
      ack = new MOSDSubOpReply(
	req, parent->whoami_shard(),
	0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
  } else if (m->get_type() == MSG_OSD_REPOP) {
	  //幅本操作
    MOSDRepOp *req = static_cast<MOSDRepOp*>(m);
    version = req->version;
    if (!rm->committed)
      ack = new MOSDRepOpReply(
	static_cast<MOSDRepOp*>(m), parent->whoami_shard(),
	0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
  } else {
    ceph_abort();
  }

  // send ack to acker only if we haven't sent a commit already
  if (ack) {
    ack->set_priority(CEPH_MSG_PRIO_HIGH); // this better match commit priority!
    get_parent()->send_message_osd_cluster(
      rm->ackerosd, ack, get_osdmap()->get_epoch());
  }

  parent->op_applied(version);
}

void ReplicatedBackend::sub_op_modify_commit(RepModifyRef rm)
{
  rm->op->mark_commit_sent();
  rm->committed = true;

  // send commit.
  dout(10) << "sub_op_modify_commit on op " << *rm->op->get_req()
	   << ", sending commit to osd." << rm->ackerosd
	   << dendl;

  assert(get_osdmap()->is_up(rm->ackerosd));
  get_parent()->update_last_complete_ondisk(rm->last_complete);

  Message *m = rm->op->get_req();
  Message *commit = NULL;
  if (m->get_type() == MSG_OSD_SUBOP) {
    // doesn't have CLIENT SUBOP feature ,use Subop
    MOSDSubOpReply  *reply = new MOSDSubOpReply(
      static_cast<MOSDSubOp*>(m),
      get_parent()->whoami_shard(),
      0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ONDISK);
    reply->set_last_complete_ondisk(rm->last_complete);
    commit = reply;
  } else if (m->get_type() == MSG_OSD_REPOP) {
	  //如果是幅本操作消息,则向那个谁回应opReply
    MOSDRepOpReply *reply = new MOSDRepOpReply(
      static_cast<MOSDRepOp*>(m),
      get_parent()->whoami_shard(),
      0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ONDISK);
    reply->set_last_complete_ondisk(rm->last_complete);
    commit = reply;
  }
  else {
    ceph_abort();
  }

  commit->set_priority(CEPH_MSG_PRIO_HIGH); // this better match ack priority!
  get_parent()->send_message_osd_cluster(
    rm->ackerosd, commit, get_osdmap()->get_epoch());

  log_subop_stats(get_parent()->get_logger(), rm->op, l_osd_sop_w);
}


// ===========================================================

void ReplicatedBackend::calc_head_subsets(
  ObjectContextRef obc, SnapSet& snapset, const hobject_t& head,
  const pg_missing_t& missing,
  const hobject_t &last_backfill,
  interval_set<uint64_t>& data_subset,
  map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator>& clone_subsets)
{
  dout(10) << "calc_head_subsets " << head
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = obc->obs.oi.size;
  if (size)
    data_subset.insert(0, size);

  if (get_parent()->get_pool().allow_incomplete_clones()) {
    dout(10) << __func__ << ": caching (was) enabled, skipping clone subsets" << dendl;
    return;
  }

  if (!cct->_conf->osd_recover_clone_overlap) {
    dout(10) << "calc_head_subsets " << head << " -- osd_recover_clone_overlap disabled" << dendl;
    return;
  }


  interval_set<uint64_t> cloning;
  interval_set<uint64_t> prev;
  if (size)
    prev.insert(0, size);

  for (int j=snapset.clones.size()-1; j>=0; j--) {
    hobject_t c = head;
    c.snap = snapset.clones[j];
    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);
    if (!missing.is_missing(c) &&
	cmp(c, last_backfill, get_parent()->sort_bitwise()) < 0) {
      dout(10) << "calc_head_subsets " << head << " has prev " << c
	       << " overlap " << prev << dendl;
      clone_subsets[c] = prev;
      cloning.union_of(prev);
      break;
    }
    dout(10) << "calc_head_subsets " << head << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }


  if (cloning.num_intervals() > cct->_conf->osd_recover_clone_overlap_limit) {
    dout(10) << "skipping clone, too many holes" << dendl;
    clone_subsets.clear();
    cloning.clear();
  }

  // what's left for us to push?
  data_subset.subtract(cloning);

  dout(10) << "calc_head_subsets " << head
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}

void ReplicatedBackend::calc_clone_subsets(
  SnapSet& snapset, const hobject_t& soid,
  const pg_missing_t& missing,
  const hobject_t &last_backfill,
  interval_set<uint64_t>& data_subset,
  map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator>& clone_subsets)
{
  dout(10) << "calc_clone_subsets " << soid
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = snapset.clone_size[soid.snap];
  if (size)
    data_subset.insert(0, size);

  if (get_parent()->get_pool().allow_incomplete_clones()) {
    dout(10) << __func__ << ": caching (was) enabled, skipping clone subsets" << dendl;
    return;
  }

  if (!cct->_conf->osd_recover_clone_overlap) {
    dout(10) << "calc_clone_subsets " << soid << " -- osd_recover_clone_overlap disabled" << dendl;
    return;
  }

  unsigned i;
  for (i=0; i < snapset.clones.size(); i++)
    if (snapset.clones[i] == soid.snap)
      break;

  // any overlap with next older clone?
  interval_set<uint64_t> cloning;
  interval_set<uint64_t> prev;
  if (size)
    prev.insert(0, size);
  for (int j=i-1; j>=0; j--) {
    hobject_t c = soid;
    c.snap = snapset.clones[j];
    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);
    if (!missing.is_missing(c) &&
	cmp(c, last_backfill, get_parent()->sort_bitwise()) < 0) {
      dout(10) << "calc_clone_subsets " << soid << " has prev " << c
	       << " overlap " << prev << dendl;
      clone_subsets[c] = prev;
      cloning.union_of(prev);
      break;
    }
    dout(10) << "calc_clone_subsets " << soid << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }

  // overlap with next newest?
  interval_set<uint64_t> next;
  if (size)
    next.insert(0, size);
  for (unsigned j=i+1; j<snapset.clones.size(); j++) {
    hobject_t c = soid;
    c.snap = snapset.clones[j];
    next.intersection_of(snapset.clone_overlap[snapset.clones[j-1]]);
    if (!missing.is_missing(c) &&
	cmp(c, last_backfill, get_parent()->sort_bitwise()) < 0) {
      dout(10) << "calc_clone_subsets " << soid << " has next " << c
	       << " overlap " << next << dendl;
      clone_subsets[c] = next;
      cloning.union_of(next);
      break;
    }
    dout(10) << "calc_clone_subsets " << soid << " does not have next " << c
	     << " overlap " << next << dendl;
  }

  if (cloning.num_intervals() > cct->_conf->osd_recover_clone_overlap_limit) {
    dout(10) << "skipping clone, too many holes" << dendl;
    clone_subsets.clear();
    cloning.clear();
  }


  // what's left for us to push?
  data_subset.subtract(cloning);

  dout(10) << "calc_clone_subsets " << soid
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}

//查找最哪个osd上取,这里会遇到要的版本恰好在找的osd上也缺少的情况.
//原因是我们在search_for_missing中仅解决了oid的缺少查找
//将结果存放在h->pulls中
void ReplicatedBackend::prepare_pull(
  eversion_t v,
  const hobject_t& soid,
  ObjectContextRef headctx,
  RPGHandle *h)
{
  assert(get_parent()->get_local_missing().get_items().count(soid));//本地一定有,否则不执行pull
  eversion_t _v = get_parent()->get_local_missing().get_items().find(
    soid)->second.need;
  assert(_v == v);
  const map<hobject_t, set<pg_shard_t>, hobject_t::BitwiseComparator> &missing_loc(
    get_parent()->get_missing_loc_shards());//取所有已知位置的对象位置
  const map<pg_shard_t, pg_missing_t > &peer_missing(
    get_parent()->get_shard_missing());//取missing集
  map<hobject_t, set<pg_shard_t>, hobject_t::BitwiseComparator>::const_iterator q = missing_loc.find(soid);//找谁上有它
  assert(q != missing_loc.end());
  assert(!q->second.empty());

  // pick a pullee
  vector<pg_shard_t> shuffle(q->second.begin(), q->second.end());
  random_shuffle(shuffle.begin(), shuffle.end());//随机打乱
  vector<pg_shard_t>::iterator p = shuffle.begin();
  assert(get_osdmap()->is_up(p->osd));
  pg_shard_t fromshard = *p;

  dout(7) << "pull " << soid
	  << " v " << v
	  << " on osds " << q->second
	  << " from osd." << fromshard
	  << dendl;

  assert(peer_missing.count(fromshard));
  const pg_missing_t &pmissing = peer_missing.find(fromshard)->second;
  if (pmissing.is_missing(soid, v)) {//如果peer也缺少此对象的v版本
    assert(pmissing.get_items().find(soid)->second.have != v);
    dout(10) << "pulling soid " << soid << " from osd " << fromshard
	     << " at version " << pmissing.get_items().find(soid)->second.have
	     << " rather than at version " << v << dendl;
    v = pmissing.get_items().find(soid)->second.have;//更新版本为其拥有的版本v
    assert(get_parent()->get_log().get_log().objects.count(soid) &&
	   (get_parent()->get_log().get_log().objects.find(soid)->second->op ==
	    pg_log_entry_t::LOST_REVERT) &&
	   (get_parent()->get_log().get_log().objects.find(
	     soid)->second->reverting_to ==
	    v));
  }

  ObjectRecoveryInfo recovery_info;

  if (soid.is_snap()) {
    assert(!get_parent()->get_local_missing().is_missing(
	     soid.get_head()) ||
	   !get_parent()->get_local_missing().is_missing(
	     soid.get_snapdir()));
    assert(headctx);
    // check snapset
    SnapSetContext *ssc = headctx->ssc;
    assert(ssc);
    dout(10) << " snapset " << ssc->snapset << dendl;
    calc_clone_subsets(ssc->snapset, soid, get_parent()->get_local_missing(),
		       get_info().last_backfill,
		       recovery_info.copy_subset,
		       recovery_info.clone_subset);
    // FIXME: this may overestimate if we are pulling multiple clones in parallel...
    dout(10) << " pulling " << recovery_info << dendl;

    assert(ssc->snapset.clone_size.count(soid.snap));
    recovery_info.size = ssc->snapset.clone_size[soid.snap];
  } else {
    // pulling head or unversioned object.
    // always pull the whole thing.
    recovery_info.copy_subset.insert(0, (uint64_t)-1);
    recovery_info.size = ((uint64_t)-1);
  }

  h->pulls[fromshard].push_back(PullOp());//指定从哪个osd拉取
  PullOp &op = h->pulls[fromshard].back();
  op.soid = soid;//对象

  op.recovery_info = recovery_info;
  op.recovery_info.soid = soid;
  op.recovery_info.version = v;//版本
  op.recovery_progress.data_complete = false;
  op.recovery_progress.omap_complete = false;
  op.recovery_progress.data_recovered_to = 0;
  op.recovery_progress.first = true;

  assert(!pulling.count(soid));
  pull_from_peer[fromshard].insert(soid);
  PullInfo &pi = pulling[soid];
  pi.head_ctx = headctx;
  pi.recovery_info = op.recovery_info;
  pi.recovery_progress = op.recovery_progress;
  pi.cache_dont_need = h->cache_dont_need;
}

/*
 * intelligently push an object to a replica.  make use of existing
 * clones/heads and dup data ranges where possible.
 */
void ReplicatedBackend::prep_push_to_replica(
  ObjectContextRef obc, const hobject_t& soid, pg_shard_t peer,
  PushOp *pop, bool cache_dont_need)
{
  const object_info_t& oi = obc->obs.oi;
  uint64_t size = obc->obs.oi.size;

  dout(10) << __func__ << ": " << soid << " v" << oi.version
	   << " size " << size << " to osd." << peer << dendl;

  map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator> clone_subsets;
  interval_set<uint64_t> data_subset;

  // are we doing a clone on the replica?
  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;

    // try to base push off of clones that succeed/preceed poid
    // we need the head (and current SnapSet) locally to do that.
    if (get_parent()->get_local_missing().is_missing(head)) {
      dout(15) << "push_to_replica missing head " << head << ", pushing raw clone" << dendl;
      return prep_push(obc, soid, peer, pop, cache_dont_need);
    }
    hobject_t snapdir = head;
    snapdir.snap = CEPH_SNAPDIR;
    if (get_parent()->get_local_missing().is_missing(snapdir)) {
      dout(15) << "push_to_replica missing snapdir " << snapdir
	       << ", pushing raw clone" << dendl;
      return prep_push(obc, soid, peer, pop, cache_dont_need);
    }

    SnapSetContext *ssc = obc->ssc;
    assert(ssc);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    map<pg_shard_t, pg_missing_t>::const_iterator pm =
      get_parent()->get_shard_missing().find(peer);
    assert(pm != get_parent()->get_shard_missing().end());
    map<pg_shard_t, pg_info_t>::const_iterator pi =
      get_parent()->get_shard_info().find(peer);
    assert(pi != get_parent()->get_shard_info().end());
    calc_clone_subsets(ssc->snapset, soid,
		       pm->second,
		       pi->second.last_backfill,
		       data_subset, clone_subsets);
  } else if (soid.snap == CEPH_NOSNAP) {
    // pushing head or unversioned object.
    // base this on partially on replica's clones?
    SnapSetContext *ssc = obc->ssc;
    assert(ssc);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    calc_head_subsets(
      obc,
      ssc->snapset, soid, get_parent()->get_shard_missing().find(peer)->second,
      get_parent()->get_shard_info().find(peer)->second.last_backfill,
      data_subset, clone_subsets);
  }

  prep_push(obc, soid, peer, oi.version, data_subset, clone_subsets, pop, cache_dont_need);
}

void ReplicatedBackend::prep_push(ObjectContextRef obc,
			     const hobject_t& soid, pg_shard_t peer,
			     PushOp *pop, bool cache_dont_need)
{
  interval_set<uint64_t> data_subset;
  if (obc->obs.oi.size)
    data_subset.insert(0, obc->obs.oi.size);
  map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator> clone_subsets;

  prep_push(obc, soid, peer,
	    obc->obs.oi.version, data_subset, clone_subsets,
	    pop, cache_dont_need);
}

void ReplicatedBackend::prep_push(
  ObjectContextRef obc,
  const hobject_t& soid, pg_shard_t peer,
  eversion_t version,
  interval_set<uint64_t> &data_subset,
  map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator>& clone_subsets,
  PushOp *pop,
  bool cache_dont_need)
{
  get_parent()->begin_peer_recover(peer, soid);
  // take note.
  PushInfo &pi = pushing[soid][peer];
  pi.obc = obc;
  pi.recovery_info.size = obc->obs.oi.size;
  pi.recovery_info.copy_subset = data_subset;
  pi.recovery_info.clone_subset = clone_subsets;
  pi.recovery_info.soid = soid;
  pi.recovery_info.oi = obc->obs.oi;
  pi.recovery_info.version = version;
  pi.recovery_progress.first = true;
  pi.recovery_progress.data_recovered_to = 0;
  pi.recovery_progress.data_complete = 0;
  pi.recovery_progress.omap_complete = 0;

  ObjectRecoveryProgress new_progress;
  int r = build_push_op(pi.recovery_info,
			pi.recovery_progress,
			&new_progress,
			pop,
			&(pi.stat), cache_dont_need);
  assert(r == 0);
  pi.recovery_progress = new_progress;
}

int ReplicatedBackend::send_pull_legacy(int prio, pg_shard_t peer,
					const ObjectRecoveryInfo &recovery_info,
					ObjectRecoveryProgress progress)
{
  // send op
  ceph_tid_t tid = get_parent()->get_tid();
  osd_reqid_t rid(get_parent()->get_cluster_msgr_name(), 0, tid);

  dout(10) << "send_pull_op " << recovery_info.soid << " "
	   << recovery_info.version
	   << " first=" << progress.first
	   << " data " << recovery_info.copy_subset
	   << " from osd." << peer
	   << " tid " << tid << dendl;

  MOSDSubOp *subop = new MOSDSubOp(
    rid, parent->whoami_shard(),
    get_info().pgid, recovery_info.soid,
    CEPH_OSD_FLAG_ACK,
    get_osdmap()->get_epoch(), tid,
    recovery_info.version);
  subop->set_priority(prio);
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PULL;
  subop->ops[0].op.extent.length = cct->_conf->osd_recovery_max_chunk;
  subop->recovery_info = recovery_info;
  subop->recovery_progress = progress;

  get_parent()->send_message_osd_cluster(
    peer.osd, subop, get_osdmap()->get_epoch());

  get_parent()->get_logger()->inc(l_osd_pull);
  return 0;
}

//写对象到本地文件系统
void ReplicatedBackend::submit_push_data(
  ObjectRecoveryInfo &recovery_info,
  bool first,
  bool complete,
  bool cache_dont_need,
  const interval_set<uint64_t> &intervals_included,
  bufferlist data_included,
  bufferlist omap_header,
  map<string, bufferlist> &attrs,
  map<string, bufferlist> &omap_entries,
  ObjectStore::Transaction *t)
{
  hobject_t target_oid;
  if (first && complete) {
    target_oid = recovery_info.soid;
  } else {
    target_oid = get_parent()->get_temp_recovery_object(recovery_info.version,
							recovery_info.soid.snap);
    if (first) {
      dout(10) << __func__ << ": Adding oid "
	       << target_oid << " in the temp collection" << dendl;
      add_temp_obj(target_oid);
    }
  }

  if (first) {
    t->remove(coll, ghobject_t(target_oid));//移除
    t->touch(coll, ghobject_t(target_oid));
    t->truncate(coll, ghobject_t(target_oid), recovery_info.size);
    t->omap_setheader(coll, ghobject_t(target_oid), omap_header);

    bufferlist bv = attrs[OI_ATTR];
    object_info_t oi(bv);
    t->set_alloc_hint(coll, ghobject_t(target_oid),
		      oi.expected_object_size,
		      oi.expected_write_size,
		      oi.alloc_hint_flags);
  }
  uint64_t off = 0;
  uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL;
  if (cache_dont_need)
    fadvise_flags |= CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;
  for (interval_set<uint64_t>::const_iterator p = intervals_included.begin();
       p != intervals_included.end();
       ++p) {
    bufferlist bit;
    bit.substr_of(data_included, off, p.get_len());
    t->write(coll, ghobject_t(target_oid),
	     p.get_start(), p.get_len(), bit, fadvise_flags);//写文件
    off += p.get_len();
  }

  if (!omap_entries.empty())
    t->omap_setkeys(coll, ghobject_t(target_oid), omap_entries);
  if (!attrs.empty())
    t->setattrs(coll, ghobject_t(target_oid), attrs);

  if (complete) {
    if (!first) {
      dout(10) << __func__ << ": Removing oid "
	       << target_oid << " from the temp collection" << dendl;
      clear_temp_obj(target_oid);
      t->remove(coll, ghobject_t(recovery_info.soid));
      t->collection_move_rename(coll, ghobject_t(target_oid),
				coll, ghobject_t(recovery_info.soid));
    }

    submit_push_complete(recovery_info, t);
  }
}

void ReplicatedBackend::submit_push_complete(ObjectRecoveryInfo &recovery_info,
					     ObjectStore::Transaction *t)
{
  for (map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator>::const_iterator p =
	 recovery_info.clone_subset.begin();
       p != recovery_info.clone_subset.end();
       ++p) {
    for (interval_set<uint64_t>::const_iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(15) << " clone_range " << p->first << " "
	       << q.get_start() << "~" << q.get_len() << dendl;
      t->clone_range(coll, ghobject_t(p->first), ghobject_t(recovery_info.soid),
		     q.get_start(), q.get_len(), q.get_start());
    }
  }
}

ObjectRecoveryInfo ReplicatedBackend::recalc_subsets(
  const ObjectRecoveryInfo& recovery_info,
  SnapSetContext *ssc)
{
  if (!recovery_info.soid.snap || recovery_info.soid.snap >= CEPH_NOSNAP)
    return recovery_info;
  ObjectRecoveryInfo new_info = recovery_info;
  new_info.copy_subset.clear();
  new_info.clone_subset.clear();
  assert(ssc);
  calc_clone_subsets(ssc->snapset, new_info.soid, get_parent()->get_local_missing(),
		     get_info().last_backfill,
		     new_info.copy_subset, new_info.clone_subset);
  return new_info;
}

bool ReplicatedBackend::handle_pull_response(
  pg_shard_t from, PushOp &pop, PullOp *response,
  list<hobject_t> *to_continue,
  ObjectStore::Transaction *t
  )
{
  interval_set<uint64_t> data_included = pop.data_included;
  bufferlist data;
  data.claim(pop.data);
  dout(10) << "handle_pull_response "
	   << pop.recovery_info
	   << pop.after_progress
	   << " data.size() is " << data.length()
	   << " data_included: " << data_included
	   << dendl;
  if (pop.version == eversion_t()) {//对空版本的处理
    // replica doesn't have it!
    _failed_push(from, pop.soid);
    return false;
  }

  hobject_t &hoid = pop.soid;
  assert((data_included.empty() && data.length() == 0) ||
	 (!data_included.empty() && data.length() > 0));

  if (!pulling.count(hoid)) {
    return false;
  }

  PullInfo &pi = pulling[hoid];
  if (pi.recovery_info.size == (uint64_t(-1))) {
    pi.recovery_info.size = pop.recovery_info.size;
    pi.recovery_info.copy_subset.intersection_of(
      pop.recovery_info.copy_subset);
  }

  bool first = pi.recovery_progress.first;
  if (first) {
    // attrs only reference the origin bufferlist (decode from MOSDPGPush message)
    // whose size is much greater than attrs in recovery. If obc cache it (get_obc maybe
    // cache the attr), this causes the whole origin bufferlist would not be free until
    // obc is evicted from obc cache. So rebuild the bufferlist before cache it.
    for (map<string, bufferlist>::iterator it = pop.attrset.begin();
         it != pop.attrset.end();
         ++it) {
      it->second.rebuild();
    }
    pi.obc = get_parent()->get_obc(pi.recovery_info.soid, pop.attrset);
    pi.recovery_info.oi = pi.obc->obs.oi;
    pi.recovery_info = recalc_subsets(pi.recovery_info, pi.obc->ssc);
  }


  interval_set<uint64_t> usable_intervals;
  bufferlist usable_data;
  trim_pushed_data(pi.recovery_info.copy_subset,
		   data_included,
		   data,
		   &usable_intervals,
		   &usable_data);
  data_included = usable_intervals;
  data.claim(usable_data);


  pi.recovery_progress = pop.after_progress;

  dout(10) << "new recovery_info " << pi.recovery_info
	   << ", new progress " << pi.recovery_progress
	   << dendl;

  bool complete = pi.is_complete();

  submit_push_data(pi.recovery_info, first,
		   complete, pi.cache_dont_need,
		   data_included, data,
		   pop.omap_header,
		   pop.attrset,
		   pop.omap_entries,
		   t);

  pi.stat.num_keys_recovered += pop.omap_entries.size();
  pi.stat.num_bytes_recovered += data.length();

  if (complete) {
    pi.stat.num_objects_recovered++;
    to_continue->push_back(hoid);
    get_parent()->on_local_recover(
      hoid, pi.recovery_info, pi.obc, t);
    pull_from_peer[from].erase(hoid);
    if (pull_from_peer[from].empty())
      pull_from_peer.erase(from);
    return false;
  } else {
    response->soid = pop.soid;
    response->recovery_info = pi.recovery_info;
    response->recovery_progress = pi.recovery_progress;
    return true;
  }
}

void ReplicatedBackend::handle_push(
  pg_shard_t from, PushOp &pop, PushReplyOp *response,
  ObjectStore::Transaction *t)
{
  dout(10) << "handle_push "
	   << pop.recovery_info
	   << pop.after_progress
	   << dendl;
  bufferlist data;
  data.claim(pop.data);
  bool first = pop.before_progress.first;
  bool complete = pop.after_progress.data_complete &&
    pop.after_progress.omap_complete;

  response->soid = pop.recovery_info.soid;
  submit_push_data(pop.recovery_info,
		   first,
		   complete,
		   true, // must be replicate
		   pop.data_included,
		   data,
		   pop.omap_header,
		   pop.attrset,
		   pop.omap_entries,
		   t);

  if (complete)
    get_parent()->on_local_recover(
      pop.recovery_info.soid,
      pop.recovery_info,
      ObjectContextRef(), // ok, is replica
      t);
}

//逐个构造消息并发送pushes中的数据
void ReplicatedBackend::send_pushes(int prio, map<pg_shard_t, vector<PushOp> > &pushes)
{
  for (map<pg_shard_t, vector<PushOp> >::iterator i = pushes.begin();
       i != pushes.end();
       ++i) {
	//1.拿到连接
    ConnectionRef con = get_parent()->get_con_osd_cluster(
      i->first.osd,
      get_osdmap()->get_epoch());
    if (!con)
      continue;
    //2.构造消息并发送
    vector<PushOp>::iterator j = i->second.begin();
    while (j != i->second.end()) {
      uint64_t cost = 0;
      uint64_t pushes = 0;
      MOSDPGPush *msg = new MOSDPGPush();
      msg->from = get_parent()->whoami_shard();
      msg->pgid = get_parent()->primary_spg_t();
      msg->map_epoch = get_osdmap()->get_epoch();
      msg->set_priority(prio);
      for (;
           (j != i->second.end() &&
	    cost < cct->_conf->osd_max_push_cost &&
	    pushes < cct->_conf->osd_max_push_objects) ;
	   ++j) {
	dout(20) << __func__ << ": sending push " << *j
		 << " to osd." << i->first << dendl;
	cost += j->cost(cct);
	pushes += 1;
	msg->pushes.push_back(*j);
      }
      msg->set_cost(cost);
      get_parent()->send_message_osd_cluster(msg, con);
    }
  }
}

//逐个发送pull
void ReplicatedBackend::send_pulls(int prio, map<pg_shard_t, vector<PullOp> > &pulls)
{
  for (map<pg_shard_t, vector<PullOp> >::iterator i = pulls.begin();
       i != pulls.end();
       ++i) {
    ConnectionRef con = get_parent()->get_con_osd_cluster(
      i->first.osd,
      get_osdmap()->get_epoch());
    if (!con)
      continue;
    dout(20) << __func__ << ": sending pulls " << i->second
	     << " to osd." << i->first << dendl;
    MOSDPGPull *msg = new MOSDPGPull();
    msg->from = parent->whoami_shard();
    msg->set_priority(prio);
    msg->pgid = get_parent()->primary_spg_t();
    msg->map_epoch = get_osdmap()->get_epoch();
    msg->pulls.swap(i->second);
    msg->compute_cost(cct);
    get_parent()->send_message_osd_cluster(msg, con);
  }
}

//获取object信息,并填充进pushOp
int ReplicatedBackend::build_push_op(const ObjectRecoveryInfo &recovery_info,
				     const ObjectRecoveryProgress &progress,
				     ObjectRecoveryProgress *out_progress,
				     PushOp *out_op,
				     object_stat_sum_t *stat,
                                     bool cache_dont_need)
{
  ObjectRecoveryProgress _new_progress;
  if (!out_progress)
    out_progress = &_new_progress;
  ObjectRecoveryProgress &new_progress = *out_progress;
  new_progress = progress;

  dout(7) << "send_push_op " << recovery_info.soid
	  << " v " << recovery_info.version
	  << " size " << recovery_info.size
	  << " recovery_info: " << recovery_info
          << dendl;

  if (progress.first) {
    int r = store->omap_get_header(coll, ghobject_t(recovery_info.soid), &out_op->omap_header);//读omap_head
    if(r < 0) {
      dout(1) << __func__ << " get omap header failed: " << cpp_strerror(-r) << dendl; 
      return r;
    }
    r = store->getattrs(ch, ghobject_t(recovery_info.soid), out_op->attrset);//读对象属性
    if(r < 0) {
      dout(1) << __func__ << " getattrs failed: " << cpp_strerror(-r) << dendl;
      return r;
    }

    // Debug
    bufferlist bv = out_op->attrset[OI_ATTR];
    object_info_t oi(bv);

    if (oi.version != recovery_info.version) {//本地对象版本与请求的版本不一致.
      get_parent()->clog_error() << get_info().pgid << " push "
				 << recovery_info.soid << " v "
				 << recovery_info.version
				 << " failed because local copy is "
				 << oi.version << "\n";
      return -EINVAL;
    }

    new_progress.first = false;
  }

  uint64_t available = cct->_conf->osd_recovery_max_chunk;
  if (!progress.omap_complete) {
    ObjectMap::ObjectMapIterator iter =
      store->get_omap_iterator(coll,
			       ghobject_t(recovery_info.soid));
    assert(iter);
    for (iter->lower_bound(progress.omap_recovered_to);
	 iter->valid();
	 iter->next(false)) {
      if (!out_op->omap_entries.empty() &&
	  ((cct->_conf->osd_recovery_max_omap_entries_per_chunk > 0 &&
	    out_op->omap_entries.size() >= cct->_conf->osd_recovery_max_omap_entries_per_chunk) ||
	   available <= iter->key().size() + iter->value().length()))
	break;
      out_op->omap_entries.insert(make_pair(iter->key(), iter->value()));

      if ((iter->key().size() + iter->value().length()) <= available)
	available -= (iter->key().size() + iter->value().length());
      else
	available = 0;
    }
    if (!iter->valid())
      new_progress.omap_complete = true;
    else
      new_progress.omap_recovered_to = iter->key();
  }

  if (available > 0) {
    if (!recovery_info.copy_subset.empty()) {
      interval_set<uint64_t> copy_subset = recovery_info.copy_subset;
      bufferlist bl;
      int r = store->fiemap(ch, ghobject_t(recovery_info.soid), 0,
                            copy_subset.range_end(), bl);
      if (r >= 0)  {
        interval_set<uint64_t> fiemap_included;
        map<uint64_t, uint64_t> m;
        bufferlist::iterator iter = bl.begin();
        ::decode(m, iter);
        map<uint64_t, uint64_t>::iterator miter;
        for (miter = m.begin(); miter != m.end(); ++miter) {
          fiemap_included.insert(miter->first, miter->second);
        }

        copy_subset.intersection_of(fiemap_included);
      }
      out_op->data_included.span_of(copy_subset, progress.data_recovered_to,
                                    available);
      if (out_op->data_included.empty()) // zero filled section, skip to end!
        new_progress.data_recovered_to = recovery_info.copy_subset.range_end();
      else
        new_progress.data_recovered_to = out_op->data_included.range_end();
    }
  } else {
    out_op->data_included.clear();
  }

  for (interval_set<uint64_t>::iterator p = out_op->data_included.begin();
       p != out_op->data_included.end();
       ++p) {
    bufferlist bit;
    store->read(ch, ghobject_t(recovery_info.soid),
		p.get_start(), p.get_len(), bit,
                cache_dont_need ? CEPH_OSD_OP_FLAG_FADVISE_DONTNEED: 0);
    if (p.get_len() != bit.length()) {
      dout(10) << " extent " << p.get_start() << "~" << p.get_len()
	       << " is actually " << p.get_start() << "~" << bit.length()
	       << dendl;
      interval_set<uint64_t>::iterator save = p++;
      if (bit.length() == 0)
        out_op->data_included.erase(save);     //Remove this empty interval
      else
        save.set_len(bit.length());
      // Remove any other intervals present
      while (p != out_op->data_included.end()) {
        interval_set<uint64_t>::iterator save = p++;
        out_op->data_included.erase(save);
      }
      new_progress.data_complete = true;
      out_op->data.claim_append(bit);
      break;
    }
    out_op->data.claim_append(bit);
  }

  if (new_progress.is_complete(recovery_info)) {
    new_progress.data_complete = true;
    if (stat)
      stat->num_objects_recovered++;
  }

  if (stat) {
    stat->num_keys_recovered += out_op->omap_entries.size();
    stat->num_bytes_recovered += out_op->data.length();
  }

  get_parent()->get_logger()->inc(l_osd_push);
  get_parent()->get_logger()->inc(l_osd_push_outb, out_op->data.length());

  // send
  out_op->version = recovery_info.version;
  out_op->soid = recovery_info.soid;
  out_op->recovery_info = recovery_info;
  out_op->after_progress = new_progress;
  out_op->before_progress = progress;
  return 0;
}

int ReplicatedBackend::send_push_op_legacy(int prio, pg_shard_t peer, PushOp &pop)
{
  ceph_tid_t tid = get_parent()->get_tid();
  osd_reqid_t rid(get_parent()->get_cluster_msgr_name(), 0, tid);
  MOSDSubOp *subop = new MOSDSubOp(
    rid, parent->whoami_shard(),
    spg_t(get_info().pgid.pgid, peer.shard), pop.soid,
    0, get_osdmap()->get_epoch(),
    tid, pop.recovery_info.version);
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PUSH;

  subop->set_priority(prio);
  subop->version = pop.version;
  subop->ops[0].indata.claim(pop.data);
  subop->data_included.swap(pop.data_included);
  subop->omap_header.claim(pop.omap_header);
  subop->omap_entries.swap(pop.omap_entries);
  subop->attrset.swap(pop.attrset);
  subop->recovery_info = pop.recovery_info;
  subop->current_progress = pop.before_progress;
  subop->recovery_progress = pop.after_progress;

  get_parent()->send_message_osd_cluster(peer.osd, subop, get_osdmap()->get_epoch());
  return 0;
}

void ReplicatedBackend::prep_push_op_blank(const hobject_t& soid, PushOp *op)
{
  op->recovery_info.version = eversion_t();
  op->version = eversion_t();
  op->soid = soid;
}

void ReplicatedBackend::sub_op_push_reply(OpRequestRef op)
{
  MOSDSubOpReply *reply = static_cast<MOSDSubOpReply*>(op->get_req());
  const hobject_t& soid = reply->get_poid();
  assert(reply->get_type() == MSG_OSD_SUBOPREPLY);
  dout(10) << "sub_op_push_reply from " << reply->get_source() << " " << *reply << dendl;
  pg_shard_t peer = reply->from;

  op->mark_started();

  PushReplyOp rop;
  rop.soid = soid;
  PushOp pop;
  bool more = handle_push_reply(peer, rop, &pop);
  if (more)
    send_push_op_legacy(op->get_req()->get_priority(), peer, pop);
}

bool ReplicatedBackend::handle_push_reply(pg_shard_t peer, PushReplyOp &op, PushOp *reply)
{
  const hobject_t &soid = op.soid;
  if (pushing.count(soid) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << ", or anybody else"
	     << dendl;
    return false;
  } else if (pushing[soid].count(peer) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << dendl;
    return false;
  } else {
    PushInfo *pi = &pushing[soid][peer];

    if (!pi->recovery_progress.data_complete) {
      dout(10) << " pushing more from, "
	       << pi->recovery_progress.data_recovered_to
	       << " of " << pi->recovery_info.copy_subset << dendl;
      ObjectRecoveryProgress new_progress;
      int r = build_push_op(
	pi->recovery_info,
	pi->recovery_progress, &new_progress, reply,
	&(pi->stat));
      assert(r == 0);
      pi->recovery_progress = new_progress;
      return true;
    } else {
      // done!
      get_parent()->on_peer_recover(
	peer, soid, pi->recovery_info,
	pi->stat);

      object_stat_sum_t stat;
      stat.num_bytes_recovered = pi->recovery_info.size;
      stat.num_keys_recovered = reply->omap_entries.size();
      stat.num_objects_recovered = 1;

      pushing[soid].erase(peer);
      pi = NULL;


      if (pushing[soid].empty()) {
	get_parent()->on_global_recover(soid, stat);
	pushing.erase(soid);
      } else {
	dout(10) << "pushed " << soid << ", still waiting for push ack from "
		 << pushing[soid].size() << " others" << dendl;
      }
      return false;
    }
  }
}

/** op_pull
 * process request to pull an entire object.
 * NOTE: called from opqueue.
 */
void ReplicatedBackend::sub_op_pull(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
  assert(m->get_type() == MSG_OSD_SUBOP);

  op->mark_started();

  const hobject_t soid = m->poid;

  dout(7) << "pull" << soid << " v " << m->version
          << " from " << m->get_source()
          << dendl;

  assert(!is_primary());  // we should be a replica or stray.

  PullOp pop;
  pop.soid = soid;
  pop.recovery_info = m->recovery_info;
  pop.recovery_progress = m->recovery_progress;

  PushOp reply;
  handle_pull(m->from, pop, &reply);
  send_push_op_legacy(
    m->get_priority(),
    m->from,
    reply);

  log_subop_stats(get_parent()->get_logger(), op, l_osd_sop_pull);
}

void ReplicatedBackend::handle_pull(pg_shard_t peer, PullOp &op, PushOp *reply)
{
  const hobject_t &soid = op.soid;
  struct stat st;
  int r = store->stat(ch, ghobject_t(soid), &st);
  if (r != 0) {
    get_parent()->clog_error() << get_info().pgid << " "
			       << peer << " tried to pull " << soid
			       << " but got " << cpp_strerror(-r) << "\n";
    prep_push_op_blank(soid, reply);//空的,有错误,填充reply
  } else {
    ObjectRecoveryInfo &recovery_info = op.recovery_info;
    ObjectRecoveryProgress &progress = op.recovery_progress;
    if (progress.first && recovery_info.size == ((uint64_t)-1)) {
      // Adjust size and copy_subset
      recovery_info.size = st.st_size;
      recovery_info.copy_subset.clear();
      if (st.st_size)
        recovery_info.copy_subset.insert(0, st.st_size);
      assert(recovery_info.clone_subset.empty());
    }

    r = build_push_op(recovery_info, progress, 0, reply);
    if (r < 0)
      prep_push_op_blank(soid, reply);
  }
}

/**
 * trim received data to remove what we don't want
 *
 * @param copy_subset intervals we want
 * @param data_included intervals we got
 * @param data_recieved data we got
 * @param intervals_usable intervals we want to keep
 * @param data_usable matching data we want to keep
 */
void ReplicatedBackend::trim_pushed_data(
  const interval_set<uint64_t> &copy_subset,
  const interval_set<uint64_t> &intervals_received,
  bufferlist data_received,
  interval_set<uint64_t> *intervals_usable,
  bufferlist *data_usable)
{
  if (intervals_received.subset_of(copy_subset)) {
    *intervals_usable = intervals_received;
    *data_usable = data_received;
    return;
  }

  intervals_usable->intersection_of(copy_subset,
				    intervals_received);

  uint64_t off = 0;
  for (interval_set<uint64_t>::const_iterator p = intervals_received.begin();
       p != intervals_received.end();
       ++p) {
    interval_set<uint64_t> x;
    x.insert(p.get_start(), p.get_len());
    x.intersection_of(copy_subset);
    for (interval_set<uint64_t>::const_iterator q = x.begin();
	 q != x.end();
	 ++q) {
      bufferlist sub;
      uint64_t data_off = off + (q.get_start() - p.get_start());
      sub.substr_of(data_received, data_off, q.get_len());
      data_usable->claim_append(sub);
    }
    off += p.get_len();
  }
}

/** op_push
 * NOTE: called from opqueue.
 */
void ReplicatedBackend::sub_op_push(OpRequestRef op)
{
  op->mark_started();
  MOSDSubOp *m = static_cast<MOSDSubOp *>(op->get_req());

  PushOp pop;
  pop.soid = m->recovery_info.soid;
  pop.version = m->version;
  m->claim_data(pop.data);
  pop.data_included.swap(m->data_included);
  pop.omap_header.swap(m->omap_header);
  pop.omap_entries.swap(m->omap_entries);
  pop.attrset.swap(m->attrset);
  pop.recovery_info = m->recovery_info;
  pop.before_progress = m->current_progress;
  pop.after_progress = m->recovery_progress;
  ObjectStore::Transaction t;

  if (is_primary()) {
    PullOp resp;
    RPGHandle *h = _open_recovery_op();
    list<hobject_t> to_continue;
    bool more = handle_pull_response(
      m->from, pop, &resp,
      &to_continue, &t);
    if (more) {
      send_pull_legacy(
	m->get_priority(),
	m->from,
	resp.recovery_info,
	resp.recovery_progress);
    } else {
      C_ReplicatedBackend_OnPullComplete *c =
	new C_ReplicatedBackend_OnPullComplete(
	  this,
	  op->get_req()->get_priority());
      c->to_continue.swap(to_continue);
      t.register_on_complete(
	new PG_RecoveryQueueAsync(
	  get_parent(),
	  get_parent()->bless_gencontext(c)));
    }
    run_recovery_op(h, op->get_req()->get_priority());
  } else {
    PushReplyOp resp;
    MOSDSubOpReply *reply = new MOSDSubOpReply(
      m, parent->whoami_shard(), 0,
      get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
    reply->set_priority(m->get_priority());
    assert(entity_name_t::TYPE_OSD == m->get_connection()->peer_type);
    handle_push(m->from, pop, &resp, &t);
    t.register_on_complete(new PG_SendMessageOnConn(
			      get_parent(), reply, m->get_connection()));
  }
  get_parent()->queue_transaction(std::move(t));
  return;
}

void ReplicatedBackend::_failed_push(pg_shard_t from, const hobject_t &soid)
{
  list<pg_shard_t> fl = { from };
  get_parent()->failed_push(fl, soid);
  pull_from_peer[from].erase(soid);
  if (pull_from_peer[from].empty())
    pull_from_peer.erase(from);
  pulling.erase(soid);
}

int ReplicatedBackend::start_pushes(
  const hobject_t &soid,
  ObjectContextRef obc,
  RPGHandle *h)
{
  int pushes = 0;
  // who needs it?
  assert(get_parent()->get_actingbackfill_shards().size() > 0);
  for (set<pg_shard_t>::iterator i =
	 get_parent()->get_actingbackfill_shards().begin();
       i != get_parent()->get_actingbackfill_shards().end();
       ++i) {//遍历peer
    if (*i == get_parent()->whoami_shard()) continue;
    pg_shard_t peer = *i;
    map<pg_shard_t, pg_missing_t>::const_iterator j =
      get_parent()->get_shard_missing().find(peer);//找peer的missing表项
    assert(j != get_parent()->get_shard_missing().end());
    if (j->second.is_missing(soid)) {//如果peer j 也缺少此对象
      ++pushes;
      h->pushes[peer].push_back(PushOp());//定义pushes
      prep_push_to_replica(obc, soid, peer,
			   &(h->pushes[peer].back()), h->cache_dont_need);//需要从本地读取并加入pushes
    }
  }
  return pushes;
}
