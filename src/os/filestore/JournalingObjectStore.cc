// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "JournalingObjectStore.h"

#include "common/errno.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_journal
#undef dout_prefix
#define dout_prefix *_dout << "journal "



void JournalingObjectStore::journal_start()
{
  dout(10) << "journal_start" << dendl;
  finisher.start();
}

void JournalingObjectStore::journal_stop()
{
  dout(10) << "journal_stop" << dendl;
  finisher.wait_for_empty();
  finisher.stop();
}

// A journal_replay() makes journal writeable, this closes that out.
void JournalingObjectStore::journal_write_close()
{
  if (journal) {
    journal->close();
    delete journal;
    journal = 0;
  }
  apply_manager.reset();
}

int JournalingObjectStore::journal_replay(uint64_t fs_op_seq)
{
  dout(10) << "journal_replay fs op_seq " << fs_op_seq << dendl;

  if (cct->_conf->journal_replay_from) {//默认肯定是0
    dout(0) << "journal_replay forcing replay from "
	    << cct->_conf->journal_replay_from
	    << " instead of " << fs_op_seq << dendl;
    // the previous op is the last one committed
    fs_op_seq = cct->_conf->journal_replay_from - 1;
  }

  uint64_t op_seq = fs_op_seq;
  apply_manager.init_seq(fs_op_seq);

  if (!journal) {
    submit_manager.set_op_seq(op_seq);
    return 0;
  }

  int err = journal->open(op_seq);
  if (err < 0) {
    dout(3) << "journal_replay open failed with "
	    << cpp_strerror(err) << dendl;
    delete journal;
    journal = 0;
    return err;
  }

  replaying = true;

  int count = 0;
  while (1) {//哈哈,读取每个journal的实例,把其中的内容调用do_transactions完成实例化.
    bufferlist bl;
    uint64_t seq = op_seq + 1;
    if (!journal->read_entry(bl, seq)) {
      dout(3) << "journal_replay: end of journal, done." << dendl;
      break;
    }

    if (seq <= op_seq) {
      dout(3) << "journal_replay: skipping old op seq " << seq << " <= " << op_seq << dendl;
      continue;
    }
    assert(op_seq == seq-1);

    dout(3) << "journal_replay: applying op seq " << seq << dendl;
    bufferlist::iterator p = bl.begin();
    vector<ObjectStore::Transaction> tls;
    while (!p.end()) {
      tls.emplace_back(Transaction(p));
    }

    apply_manager.op_apply_start(seq);
    int r = do_transactions(tls, seq);//直接落盘(不执行sync)
    apply_manager.op_apply_finish(seq);

    op_seq = seq;
    count++;

    dout(3) << "journal_replay: r = " << r << ", op_seq now " << op_seq << dendl;
  }

  if (count)
    dout(3) << "journal_replay: total = " << count << dendl;

  replaying = false;

  submit_manager.set_op_seq(op_seq);

  // done reading, make writeable.
  err = journal->make_writeable();
  if (err < 0)
    return err;

  if (!count)
    journal->committed_thru(fs_op_seq);

  return count;
}


// ------------------------------------

//标记当前有操作在执行,如果被commit阻塞,则等待.
uint64_t JournalingObjectStore::ApplyManager::op_apply_start(uint64_t op)
{
  Mutex::Locker l(apply_lock);
  //如果被置为阻塞,则等待阻塞解除信号
  while (blocked) {
    dout(10) << "op_apply_start blocked, waiting" << dendl;
    blocked_cond.Wait(apply_lock);
  }
  dout(10) << "op_apply_start " << op << " open_ops " << open_ops << " -> "
	   << (open_ops+1) << dendl;
  assert(!blocked);
  assert(op > committed_seq);
  open_ops++;
  return op;
}

//标记向kenerl写盘操作完成.
void JournalingObjectStore::ApplyManager::op_apply_finish(uint64_t op)
{
  Mutex::Locker l(apply_lock);
  dout(10) << "op_apply_finish " << op << " open_ops " << open_ops << " -> "
	   << (open_ops-1) << ", max_applied_seq " << max_applied_seq << " -> "
	   << MAX(op, max_applied_seq) << dendl;
  --open_ops;//标记正在执行的操作减少
  assert(open_ops >= 0);

  // signal a blocked commit_start
  //如果标记为blocked此时,按照实现约定,commit_start会阻塞(因为open_ops不为0)
  //走到此时,检测下,如果commit_start存在,则唤醒它.
  if (blocked) {
    blocked_cond.Signal();
  }

  // there can be multiple applies in flight; track the max value we
  // note.  note that we can't _read_ this value and learn anything
  // meaningful unless/until we've quiesced all in-flight applies.
  if (op > max_applied_seq)
    max_applied_seq = op;//如果我发现我的这个seq比当前记录的seq要打,则更新
}

//增加op_seq,并返回op_seq,实现seq分配.(注,op_submit_start与op_submit_finish是一对,一个负责
//加锁,一个负责解锁)
uint64_t JournalingObjectStore::SubmitManager::op_submit_start()
{
  lock.Lock();
  uint64_t op = ++op_seq;
  dout(10) << "op_submit_start " << op << dendl;
  return op;
}

//增加op_sumbitted,要求传入的op必须按序进行submitted,有序号按序提交的检查.
void JournalingObjectStore::SubmitManager::op_submit_finish(uint64_t op)
{
  dout(10) << "op_submit_finish " << op << dendl;
  if (op != op_submitted + 1) {
    dout(0) << "op_submit_finish " << op << " expected " << (op_submitted + 1)
	    << ", OUT OF ORDER" << dendl;
    assert(0 == "out of order op_submit_finish");
  }
  op_submitted = op;
  lock.Unlock();
}


// ------------------------------------------

//向commit_waiters中添加等待.
void JournalingObjectStore::ApplyManager::add_waiter(uint64_t op, Context *c)
{
  Mutex::Locker l(com_lock);
  assert(c);
  commit_waiters[op].push_back(c);
}

//更新committing_seq,开始准备提交,在此过程中,应用层的落盘操作,需要停止.
bool JournalingObjectStore::ApplyManager::commit_start()
{
  bool ret = false;

  {
    Mutex::Locker l(apply_lock);
    dout(10) << "commit_start max_applied_seq " << max_applied_seq
	     << ", open_ops " << open_ops << dendl;
    blocked = true;
    while (open_ops > 0) {
      dout(10) << "commit_start waiting for " << open_ops
	       << " open ops to drain" << dendl;
      blocked_cond.Wait(apply_lock);//有op_apply_finish未调用完成.等待
    }
    assert(open_ops == 0);
    dout(10) << "commit_start blocked, all open_ops have completed" << dendl;
    {
      Mutex::Locker l(com_lock);
      if (max_applied_seq == committed_seq) {
	dout(10) << "commit_start nothing to do" << dendl;
	blocked = false;
	assert(commit_waiters.empty());
	goto out;
      }

      //更新已applied_seq为当前正在committing的seq
      committing_seq = max_applied_seq;

      dout(10) << "commit_start committing " << committing_seq
	       << ", still blocked" << dendl;
    }
  }
  ret = true;

  if (journal)
    journal->commit_start(committing_seq);  // tell the journal too
 out:
  return ret;
}

//放通阻塞,使其它操作可以继续进行
void JournalingObjectStore::ApplyManager::commit_started()
{
  Mutex::Locker l(apply_lock);
  // allow new ops. (underlying fs should now be committing all prior ops)
  dout(10) << "commit_started committing " << committing_seq << ", unblocking"
	   << dendl;
  blocked = false;
  blocked_cond.Signal();
}

//更新已committed_seq为committing_seq
//commit时可以添加waiters,这里将其出队.
void JournalingObjectStore::ApplyManager::commit_finish()
{
  Mutex::Locker l(com_lock);
  dout(10) << "commit_finish thru " << committing_seq << dendl;

  if (journal)
    journal->committed_thru(committing_seq);

  committed_seq = committing_seq;

  map<version_t, vector<Context*> >::iterator p = commit_waiters.begin();
  while (p != commit_waiters.end() &&
    p->first <= committing_seq) {
    finisher.queue(p->second);//处理commit_waiters上等待的.
    commit_waiters.erase(p++);
  }
}

//如果有journal,且journal可写,则journal->submit_entry
//否则如果onjournal不为null,则apply_manager.add_waiter
void JournalingObjectStore::_op_journal_transactions(
  bufferlist& tbl, uint32_t orig_len, uint64_t op,
  Context *onjournal, TrackedOpRef osd_op)
{
  if (osd_op.get())
    dout(10) << "op_journal_transactions " << op << " reqid_t "
             << (static_cast<OpRequest *>(osd_op.get()))->get_reqid() << dendl;
  else
    dout(10) << "op_journal_transactions " << op  << dendl;

  if (journal && journal->is_writeable()) {
    journal->submit_entry(op, tbl, orig_len, onjournal, osd_op);//提交事务
  } else if (onjournal) {
    apply_manager.add_waiter(op, onjournal);//等待提交事务. ???
  }
}
