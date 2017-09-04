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

#include "Elector.h"
#include "Monitor.h"

#include "common/Timer.h"
#include "MonitorDBStore.h"
#include "messages/MMonElection.h"

#include "common/config.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, epoch)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, epoch_t epoch) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").elector(" << epoch << ") ";
}


void Elector::init()
{
  epoch = mon->store->get(Monitor::MONITOR_NAME, "election_epoch");
  if (!epoch) {
    dout(1) << "init, first boot, initializing epoch at 1 " << dendl;
    epoch = 1;
  } else if (epoch % 2) {
    dout(1) << "init, last seen epoch " << epoch
	    << ", mid-election, bumping" << dendl;
    ++epoch;
    auto t(std::make_shared<MonitorDBStore::Transaction>());
    t->put(Monitor::MONITOR_NAME, "election_epoch", epoch);
    mon->store->apply_transaction(t);
  } else {
    dout(1) << "init, last seen epoch " << epoch << dendl;
  }
}

void Elector::shutdown()
{
  cancel_timer();
}

void Elector::bump_epoch(epoch_t e) 
{
  dout(10) << "bump_epoch " << epoch << " to " << e << dendl;
  assert(epoch <= e);
  epoch = e;
  auto t(std::make_shared<MonitorDBStore::Transaction>());
  t->put(Monitor::MONITOR_NAME, "election_epoch", epoch);
  mon->store->apply_transaction(t);

  mon->join_election();

  // clear up some state
  electing_me = false;
  acked_me.clear();
}


void Elector::start()//开始选举
{
  if (!participating) {
    dout(0) << "not starting new election -- not participating" << dendl;
    return;
  }
  dout(5) << "start -- can i be leader?" << dendl;

  acked_me.clear();
  init();
  
  // start by trying to elect me
  if (epoch % 2 == 0) {
    bump_epoch(epoch+1);  // odd == election cycle
  } else {
    // do a trivial db write just to ensure it is writeable.
    auto t(std::make_shared<MonitorDBStore::Transaction>());
    t->put(Monitor::MONITOR_NAME, "election_writeable_test", rand());
    int r = mon->store->apply_transaction(t);
    assert(r >= 0);
  }
  start_stamp = ceph_clock_now();
  electing_me = true;
  acked_me[mon->rank].cluster_features = CEPH_FEATURES_ALL;
  acked_me[mon->rank].mon_features = ceph::features::mon::get_supported();
  mon->collect_metadata(&acked_me[mon->rank].metadata);
  leader_acked = -1;

  // bcast to everyone else
  for (unsigned i=0; i<mon->monmap->size(); ++i) {//给monmap中的每个人发送propose消息（计划选自已）
    if ((int)i == mon->rank) continue;
    MMonElection *m =
      new MMonElection(MMonElection::OP_PROPOSE, epoch, mon->monmap);
    m->mon_features = ceph::features::mon::get_supported();
    mon->messenger->send_message(m, mon->monmap->get_inst(i));
  }
  
  reset_timer();
}

void Elector::defer(int who)//向对方响应ack
{
  dout(5) << "defer to " << who << dendl;

  if (electing_me) {
    // drop out
    acked_me.clear();
    electing_me = false;
  }

  // ack them
  leader_acked = who;//记录我们向应响应了ack
  ack_stamp = ceph_clock_now();
  MMonElection *m = new MMonElection(MMonElection::OP_ACK, epoch, mon->monmap);
  m->mon_features = ceph::features::mon::get_supported();
  mon->collect_metadata(&m->metadata);

  // This field is unused completely in luminous, but jewel uses it to
  // determine whether we are a dumpling mon due to some crufty old
  // code.  It only needs to see this buffer non-empty, so put
  // something useless there.
  m->sharing_bl = mon->get_local_commands_bl(mon->get_required_mon_features());

  mon->messenger->send_message(m, mon->monmap->get_inst(who));
  
  // set a timer
  reset_timer(1.0);  // give the leader some extra time to declare victory
}


void Elector::reset_timer(double plus)
{
  // set the timer
  cancel_timer();
  /**
   * This class is used as the callback when the expire_event timer fires up.
   *
   * If the expire_event is fired, then it means that we had an election going,
   * either started by us or by some other participant, but it took too long,
   * thus expiring.
   *
   * When the election expires, we will check if we were the ones who won, and
   * if so we will declare victory. If that is not the case, then we assume
   * that the one we defered to didn't declare victory quickly enough (in fact,
   * as far as we know, we may even be dead); so, just propose ourselves as the
   * Leader.
   */
  expire_event = mon->timer.add_event_after(
    g_conf->mon_election_timeout + plus,
    new C_MonContext(mon, [this](int) {
	expire();//如果无人长时间响应，则自然当选
      }));
}


void Elector::cancel_timer()
{
  if (expire_event) {
    mon->timer.cancel_event(expire_event);
    expire_event = 0;
  }
}

void Elector::expire()//选举过程中超时
{
  dout(5) << "election timer expired" << dendl;
  
  // did i win?
  //当前选的是我，且半数通过
  if (electing_me &&
      acked_me.size() > (unsigned)(mon->monmap->size() / 2)) {
    // i win
    victory();//选举获胜
  } else {
    // whoever i deferred to didn't declare victory quickly enough.
    if (mon->has_ever_joined)
      start();
    else
      mon->bootstrap();
  }
}


void Elector::victory()//选举获胜
{
  leader_acked = -1;
  electing_me = false;

  uint64_t cluster_features = CEPH_FEATURES_ALL;
  mon_feature_t mon_features = ceph::features::mon::get_supported();
  set<int> quorum;
  map<int,Metadata> metadata;
  for (map<int, elector_info_t>::iterator p = acked_me.begin();
       p != acked_me.end();//遍历acked_me
       ++p) {
    quorum.insert(p->first);
    cluster_features &= p->second.cluster_features;
    mon_features &= p->second.mon_features;
    metadata[p->first] = p->second.metadata;
  }

  cancel_timer();
  
  assert(epoch % 2 == 1);  // election
  bump_epoch(epoch+1);     // is over!

  // tell everyone!
  //通其其它人，我选举成功，让他们执行选举失败函数
  for (set<int>::iterator p = quorum.begin();
       p != quorum.end();
       ++p) {
    if (*p == mon->rank) continue;
    MMonElection *m = new MMonElection(MMonElection::OP_VICTORY, epoch,
				       mon->monmap);//选举成功消息
    m->quorum = quorum;
    m->quorum_features = cluster_features;
    m->mon_features = mon_features;
    m->sharing_bl = mon->get_local_commands_bl(mon_features);
    mon->messenger->send_message(m, mon->monmap->get_inst(*p));
  }

  // tell monitor
  //选举成功
  mon->win_election(epoch, quorum,
                    cluster_features, mon_features, metadata);
}


void Elector::handle_propose(MonOpRequestRef op)//收到别人提名并主持的选举
{
  op->mark_event("elector:handle_propose");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(5) << "handle_propose from " << m->get_source() << dendl;
  int from = m->get_source().num();//从哪个monitor来

  assert(m->epoch % 2 == 1); // election
  uint64_t required_features = mon->get_required_features();
  mon_feature_t required_mon_features = mon->get_required_mon_features();

  dout(10) << __func__ << " required features " << required_features
           << " " << required_mon_features
           << ", peer features " << m->get_connection()->get_features()
           << " " << m->mon_features
           << dendl;

  if ((required_features ^ m->get_connection()->get_features()) &
      required_features) {
    dout(5) << " ignoring propose from mon" << from
	    << " without required features" << dendl;
    nak_old_peer(op);
    return;
  } else if (!m->mon_features.contains_all(required_mon_features)) {
    // all the features in 'required_mon_features' not in 'm->mon_features'
    mon_feature_t missing = required_mon_features.diff(m->mon_features);
    dout(5) << " ignoring propose from mon." << from
            << " without required mon_features " << missing
            << dendl;
    nak_old_peer(op);
  } else if (m->epoch > epoch) {
    bump_epoch(m->epoch);
  } else if (m->epoch < epoch) {
    // got an "old" propose,
    if (epoch % 2 == 0 &&    // in a non-election cycle
	mon->quorum.count(from) == 0) {  // from someone outside the quorum
      // a mon just started up, call a new election so they can rejoin!
      dout(5) << " got propose from old epoch, quorum is " << mon->quorum 
	      << ", " << m->get_source() << " must have just started" << dendl;
      // we may be active; make sure we reset things in the monitor appropriately.
      mon->start_election();
    } else {
      dout(5) << " ignoring old propose" << dendl;
      return;
    }
  }

  if (mon->rank < from) {//选举获胜的标准是比大小，现在from比我们大，那我们胜
    // i would win over them.
    if (leader_acked >= 0) {        // we already acked someone
      assert(leader_acked < from);  // and they still win, of course
      dout(5) << "no, we already acked " << leader_acked << dendl;
    } else {
    //如果我们没有参选，则我们要提名自已并主持选举
      // wait, i should win!
      if (!electing_me) {
	mon->start_election();//开始选举
      }
    }
  } else {
    // they would win over me
    if (leader_acked < 0 ||      // haven't acked anyone yet, or　//我们之前没有向任何人回复过ack或者
	leader_acked > from ||   // they would win over who you did ack, or　//之前回复ack的id较大
	leader_acked == from) {  // this is the guy we're already deferring to　//之前给此人已回复过ack
      defer(from);
    } else {
      // ignore them!
      dout(5) << "no, we already acked " << leader_acked << dendl;　//
    }
  }
}

void Elector::handle_ack(MonOpRequestRef op)//处理ack消息，此消息为选举响应
{
  op->mark_event("elector:handle_ack");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(5) << "handle_ack from " << m->get_source() << dendl;
  int from = m->get_source().num();

  assert(m->epoch % 2 == 1); // election，消息中的epoch是奇数
  if (m->epoch > epoch) {
    dout(5) << "woah, that's a newer epoch, i must have rebooted.  bumping and re-starting!" << dendl;
    bump_epoch(m->epoch);
    start();
    return;
  }
  assert(m->epoch == epoch);
  uint64_t required_features = mon->get_required_features();
  if ((required_features ^ m->get_connection()->get_features()) &
      required_features) {
    dout(5) << " ignoring ack from mon" << from
	    << " without required features" << dendl;
    return;
  }

  mon_feature_t required_mon_features = mon->get_required_mon_features();
  if (!m->mon_features.contains_all(required_mon_features)) {
    mon_feature_t missing = required_mon_features.diff(m->mon_features);
    dout(5) << " ignoring ack from mon." << from
            << " without required mon_features " << missing
            << dendl;
    return;
  }

  if (electing_me) {//加入acted_me记录中，对方响应了我
    // thanks
    acked_me[from].cluster_features = m->get_connection()->get_features();
    acked_me[from].mon_features = m->mon_features;
    acked_me[from].metadata = m->metadata;
    dout(5) << " so far i have {";
    for (map<int, elector_info_t>::const_iterator p = acked_me.begin();
         p != acked_me.end();
         ++p) {
      if (p != acked_me.begin())
        *_dout << ",";
      *_dout << " mon." << p->first << ":"
             << " features " << p->second.cluster_features
             << " " << p->second.mon_features;
    }
    *_dout << " }" << dendl;

    // is that _everyone_?
    if (acked_me.size() == mon->monmap->size()) {//如果monmap中规定的数量，都回复我了，我获胜
      // if yes, shortcut to election finish
      victory();//选举获胜
    }
  } else {
    // ignore, i'm deferring already.
    assert(leader_acked >= 0);
  }
}


void Elector::handle_victory(MonOpRequestRef op)//收到获胜消息
{
  op->mark_event("elector:handle_victory");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(5) << "handle_victory from " << m->get_source()
          << " quorum_features " << m->quorum_features
          << " " << m->mon_features
          << dendl;
  int from = m->get_source().num();

  assert(from < mon->rank);
  assert(m->epoch % 2 == 0);  

  leader_acked = -1;

  // i should have seen this election if i'm getting the victory.
  if (m->epoch != epoch + 1) { 
    dout(5) << "woah, that's a funny epoch, i must have rebooted.  bumping and re-starting!" << dendl;
    bump_epoch(m->epoch);
    start();
    return;
  }

  bump_epoch(m->epoch);

  // they win
  //选举失败
  mon->lose_election(epoch, m->quorum, from,
                     m->quorum_features, m->mon_features);

  // cancel my timer
  cancel_timer();

  // stash leader's commands
  assert(m->sharing_bl.length());
  vector<MonCommand> new_cmds;
  bufferlist::iterator bi = m->sharing_bl.begin();
  MonCommand::decode_vector(new_cmds, bi);
  mon->set_leader_commands(new_cmds);
}

void Elector::nak_old_peer(MonOpRequestRef op)
{
  op->mark_event("elector:nak_old_peer");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  uint64_t supported_features = m->get_connection()->get_features();
  uint64_t required_features = mon->get_required_features();
  mon_feature_t required_mon_features = mon->get_required_mon_features();
  dout(10) << "sending nak to peer " << m->get_source()
    << " that only supports " << supported_features
    << " " << m->mon_features
    << " of the required " << required_features
    << " " << required_mon_features
    << dendl;

  MMonElection *reply = new MMonElection(MMonElection::OP_NAK, m->epoch,
                                         mon->monmap);
  reply->quorum_features = required_features;
  reply->mon_features = required_mon_features;
  mon->features.encode(reply->sharing_bl);
  m->get_connection()->send_message(reply);
}

void Elector::handle_nak(MonOpRequestRef op)
{
  op->mark_event("elector:handle_nak");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(1) << "handle_nak from " << m->get_source()
	  << " quorum_features " << m->quorum_features
          << " " << m->mon_features
          << dendl;

  CompatSet other;
  bufferlist::iterator bi = m->sharing_bl.begin();
  other.decode(bi);
  CompatSet diff = Monitor::get_supported_features().unsupported(other);

  mon_feature_t mon_supported = ceph::features::mon::get_supported();
  // all features in 'm->mon_features' not in 'mon_supported'
  mon_feature_t mon_diff = m->mon_features.diff(mon_supported);

  derr << "Shutting down because I do not support required monitor features: { "
       << diff << " } " << mon_diff << dendl;

  exit(0);
  // the end!
}

void Elector::dispatch(MonOpRequestRef op)
{
  op->mark_event("elector:dispatch");
  assert(op->is_type_election());

  switch (op->get_req()->get_type()) {
    
  case MSG_MON_ELECTION:
    {
      if (!participating) {
        return;
      }
      if (op->get_req()->get_source().num() >= mon->monmap->size()) {
	dout(5) << " ignoring bogus election message with bad mon rank " 
		<< op->get_req()->get_source() << dendl;
	return;
      }

      MMonElection *em = static_cast<MMonElection*>(op->get_req());

      // assume an old message encoding would have matched
      if (em->fsid != mon->monmap->fsid) {
	dout(0) << " ignoring election msg fsid " 
		<< em->fsid << " != " << mon->monmap->fsid << dendl;
	return;
      }

      if (!mon->monmap->contains(em->get_source_addr())) {
	dout(1) << "discarding election message: " << em->get_source_addr()
		<< " not in my monmap " << *mon->monmap << dendl;
	return;
      }

      MonMap peermap;
      peermap.decode(em->monmap_bl);
      if (peermap.epoch > mon->monmap->epoch) {
	dout(0) << em->get_source_inst() << " has newer monmap epoch " << peermap.epoch
		<< " > my epoch " << mon->monmap->epoch 
		<< ", taking it"
		<< dendl;
	mon->monmap->decode(em->monmap_bl);
        auto t(std::make_shared<MonitorDBStore::Transaction>());
        t->put("monmap", mon->monmap->epoch, em->monmap_bl);
        t->put("monmap", "last_committed", mon->monmap->epoch);
        mon->store->apply_transaction(t);
	//mon->monmon()->paxos->stash_latest(mon->monmap->epoch, em->monmap_bl);
	cancel_timer();
	mon->bootstrap();
	return;
      }
      if (peermap.epoch < mon->monmap->epoch) {
	dout(0) << em->get_source_inst() << " has older monmap epoch " << peermap.epoch
		<< " < my epoch " << mon->monmap->epoch 
		<< dendl;
      } 

      switch (em->op) {
      case MMonElection::OP_PROPOSE://收到某一个mon的提议
	handle_propose(op);
	return;
      }

      if (em->epoch < epoch) {
	dout(5) << "old epoch, dropping" << dendl;
	break;
      }

      switch (em->op) {
      case MMonElection::OP_ACK://选举响应
	handle_ack(op);
	return;
      case MMonElection::OP_VICTORY:
	handle_victory(op);
	return;
      case MMonElection::OP_NAK:
	handle_nak(op);
	return;
      default:
	ceph_abort();
      }
    }
    break;
    
  default: 
    ceph_abort();
  }
}

void Elector::start_participating()
{
  if (!participating) {
    participating = true;
  }
}
