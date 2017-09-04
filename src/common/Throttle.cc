// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/scope_guard.h"

#include "common/Throttle.h"
#include "common/ceph_time.h"
#include "common/perf_counters.h"
#include "common/Throttle.h"


// re-include our assert to clobber the system one; fix dout:
#include "include/assert.h"

#define dout_subsys ceph_subsys_throttle

#undef dout_prefix
#define dout_prefix *_dout << "throttle(" << name << " " << (void*)this << ") "

using ceph::mono_clock;
using ceph::mono_time;
using ceph::uniquely_lock;

enum {
  l_throttle_first = 532430,
  l_throttle_val,
  l_throttle_max,
  l_throttle_get_started,
  l_throttle_get,
  l_throttle_get_sum,
  l_throttle_get_or_fail_fail,
  l_throttle_get_or_fail_success,
  l_throttle_take,
  l_throttle_take_sum,
  l_throttle_put,
  l_throttle_put_sum,
  l_throttle_wait,
  l_throttle_last,
};

Throttle::Throttle(CephContext *cct, const std::string& n, int64_t m,
		   bool _use_perf)
  : cct(cct), name(n), max(m), use_perf(_use_perf)
{
  assert(m >= 0);

  if (!use_perf)
    return;

  if (cct->_conf->throttler_perf_counter) {
    PerfCountersBuilder b(cct, string("throttle-") + name, l_throttle_first, l_throttle_last);
    b.add_u64(l_throttle_val, "val", "Currently available throttle");
    b.add_u64(l_throttle_max, "max", "Max value for throttle");
    b.add_u64_counter(l_throttle_get_started, "get_started", "Number of get calls, increased before wait");
    b.add_u64_counter(l_throttle_get, "get", "Gets");
    b.add_u64_counter(l_throttle_get_sum, "get_sum", "Got data");
    b.add_u64_counter(l_throttle_get_or_fail_fail, "get_or_fail_fail", "Get blocked during get_or_fail");
    b.add_u64_counter(l_throttle_get_or_fail_success, "get_or_fail_success", "Successful get during get_or_fail");
    b.add_u64_counter(l_throttle_take, "take", "Takes");
    b.add_u64_counter(l_throttle_take_sum, "take_sum", "Taken data");
    b.add_u64_counter(l_throttle_put, "put", "Puts");
    b.add_u64_counter(l_throttle_put_sum, "put_sum", "Put data");
    b.add_time_avg(l_throttle_wait, "wait", "Waiting latency");

    logger = { b.create_perf_counters(), cct };
    cct->get_perfcounters_collection()->add(logger.get());
    logger->set(l_throttle_max, max);
  }
}

Throttle::~Throttle()
{
  auto l = uniquely_lock(lock);
  assert(conds.empty());
}

//重置max,如果max发生变化,则原有cond中首个将被缓醒.(唤醒后锁处理?)
void Throttle::_reset_max(int64_t m)
{
  // lock must be held.
  //检查是否有必要变更max取值
  if (static_cast<int64_t>(max) == m)
    return;
  //cond队列不为空，取第一个条件变量，并唤醒第一个等待此条件变量的
  if (!conds.empty())
    conds.front().notify_one();
  if (logger)
    logger->set(l_throttle_max, m);
  //重置max
  max = m;
}

//尝试着检查c是否可通过，如果不能通过，就等待，返回值用于表示
//是否经历了等待
bool Throttle::_wait(int64_t c, UNIQUE_LOCK_T(lock)& l)
{
  mono_time start;
  bool waited = false;
  //检查c是否需要等待，如果不需要等待，但cond不为空，也认为需要等待
  if (_should_wait(c) || !conds.empty()) { // always wait behind other waiters.
    {
      auto cv = conds.emplace(conds.end());
      auto w = make_scope_guard([this, cv]() {
	  conds.erase(cv);
	});
      waited = true;//指明需要等待
      ldout(cct, 2) << "_wait waiting..." << dendl;
      if (logger)
          start = mono_clock::now();

      //阻塞，等待被唤醒
      //被唤配，再检查是否仍需要阻塞（需要等或者自已需要等排在自已前面的人出队后再处理）
      cv->wait(l, [this, c, cv]() { return (!_should_wait(c) &&
					    cv == conds.begin()); });
      ldout(cct, 2) << "_wait finished waiting" << dendl;
      if (logger) {
	logger->tinc(l_throttle_wait, mono_clock::now() - start);
      }
    }
    // wake up the next guy
    //给后面的兄弟知会一声，让它也去检查，看能不能出
    if (!conds.empty())
      conds.front().notify_one();
  }
  //返回是否进行过等待
  return waited;
}

//重置m,并尝试着唤醒等待者，采用_wait(0)
//检查自身是否等待（防止在自已前面有人等待）
bool Throttle::wait(int64_t m)
{
  if (0 == max && 0 == m) {
    return false;
  }

  auto l = uniquely_lock(lock);
  if (m) {
    assert(m > 0);
    _reset_max(m);
  }
  ldout(cct, 10) << "wait" << dendl;
  return _wait(0, l);
}

//增加占用数
int64_t Throttle::take(int64_t c)
{
  if (0 == max) {
    return 0;
  }
  assert(c >= 0);
  ldout(cct, 10) << "take " << c << dendl;
  {
    auto l = uniquely_lock(lock);
    count += c;//增加
  }
  if (logger) {
    logger->inc(l_throttle_take);
    logger->inc(l_throttle_take_sum, c);
    logger->set(l_throttle_val, count);
  }
  //返回增加后的计数
  return count;
}

//检查c是否可以支出,如果可以支出,则更新当前count值.(m用于更新max)
//返回是否经历了等待，true经历了，false未经历
bool Throttle::get(int64_t c, int64_t m)
{
  if (0 == max && 0 == m) {
    return false;
  }

  assert(c >= 0);
  ldout(cct, 10) << "get " << c << " (" << count.load() << " -> " << (count.load() + c) << ")" << dendl;
  if (logger) {
    logger->inc(l_throttle_get_started);
  }
  bool waited = false;
  {
    auto l = uniquely_lock(lock);
    //检查是否要变更max
    if (m) {
      assert(m > 0);
      _reset_max(m);
    }
    waited = _wait(c, l);
    count += c;//增加占用值
  }
  if (logger) {
    logger->inc(l_throttle_get);
    logger->inc(l_throttle_get_sum, c);
    logger->set(l_throttle_val, count);
  }
  return waited;
}

/* Returns true if it successfully got the requested amount,
 * or false if it would block.
 */
//如果c可满足,则返回true,否则不阻塞并返回false
bool Throttle::get_or_fail(int64_t c)
{
  //0表示禁用此功能
  if (0 == max) {
    return true;
  }

  assert (c >= 0);
  auto l = uniquely_lock(lock);
  //如果需要等待，则返回false
  if (_should_wait(c) || !conds.empty()) {
    ldout(cct, 10) << "get_or_fail " << c << " failed" << dendl;
    if (logger) {
      logger->inc(l_throttle_get_or_fail_fail);
    }
    return false;
  } else {
	//不需要等待，增加计数
    ldout(cct, 10) << "get_or_fail " << c << " success (" << count.load()
		   << " -> " << (count.load() + c) << ")" << dendl;
    count += c;
    if (logger) {
      logger->inc(l_throttle_get_or_fail_success);
      logger->inc(l_throttle_get);
      logger->inc(l_throttle_get_sum, c);
      logger->set(l_throttle_val, count);
    }
    return true;
  }
}

int64_t Throttle::put(int64_t c)
{
  //为0时，表示禁用Throttle
  if (0 == max) {
    return 0;
  }

  assert(c >= 0);
  ldout(cct, 10) << "put " << c << " (" << count.load() << " -> "
		 << (count.load()-c) << ")" << dendl;
  auto l = uniquely_lock(lock);
  if (c) {
    if (!conds.empty())
      //因为每个cond只有一个等待者，故只需要唤醒一个
      conds.front().notify_one();
    // if count goes negative, we failed somewhere!
    //count一定大于等于c
    assert(static_cast<int64_t>(count) >= c);
    count -= c;//归还占用量
    if (logger) {
      logger->inc(l_throttle_put);
      logger->inc(l_throttle_put_sum, c);
      logger->set(l_throttle_val, count);
    }
  }
  return count;//返回当前占用量
}

//唤醒并清空所有等待者
void Throttle::reset()
{
  auto l = uniquely_lock(lock);
  if (!conds.empty())
    conds.front().notify_one();
  count = 0;
  if (logger) {
    logger->set(l_throttle_val, 0);
  }
}

enum {
  l_backoff_throttle_first = l_throttle_last + 1,
  l_backoff_throttle_val,
  l_backoff_throttle_max,
  l_backoff_throttle_get,
  l_backoff_throttle_get_sum,
  l_backoff_throttle_take,
  l_backoff_throttle_take_sum,
  l_backoff_throttle_put,
  l_backoff_throttle_put_sum,
  l_backoff_throttle_wait,
  l_backoff_throttle_last,
};

BackoffThrottle::BackoffThrottle(CephContext *cct, const std::string& n,
				 unsigned expected_concurrency, bool _use_perf)
  : cct(cct), name(n),
    conds(expected_concurrency),///< [in] determines size of conds
    use_perf(_use_perf)
{
  if (!use_perf)
    return;

  if (cct->_conf->throttler_perf_counter) {
    PerfCountersBuilder b(cct, string("throttle-") + name,
			  l_backoff_throttle_first, l_backoff_throttle_last);
    b.add_u64(l_backoff_throttle_val, "val", "Currently available throttle");
    b.add_u64(l_backoff_throttle_max, "max", "Max value for throttle");
    b.add_u64_counter(l_backoff_throttle_get, "get", "Gets");
    b.add_u64_counter(l_backoff_throttle_get_sum, "get_sum", "Got data");
    b.add_u64_counter(l_backoff_throttle_take, "take", "Takes");
    b.add_u64_counter(l_backoff_throttle_take_sum, "take_sum", "Taken data");
    b.add_u64_counter(l_backoff_throttle_put, "put", "Puts");
    b.add_u64_counter(l_backoff_throttle_put_sum, "put_sum", "Put data");
    b.add_time_avg(l_backoff_throttle_wait, "wait", "Waiting latency");

    logger = { b.create_perf_counters(), cct };
    cct->get_perfcounters_collection()->add(logger.get());
    logger->set(l_backoff_throttle_max, max);
  }
}

BackoffThrottle::~BackoffThrottle()
{
  auto l = uniquely_lock(lock);
  assert(waiters.empty());
}

bool BackoffThrottle::set_params(
  double _low_threshhold,
  double _high_threshhold,
  double _expected_throughput,
  double _high_multiple,
  double _max_multiple,
  uint64_t _throttle_max,
  ostream *errstream)
{
  bool valid = true;
  if (_low_threshhold > _high_threshhold) {
    valid = false;
    if (errstream) {
      *errstream << "low_threshhold (" << _low_threshhold
		 << ") > high_threshhold (" << _high_threshhold
		 << ")" << std::endl;
    }
  }

  if (_high_multiple > _max_multiple) {
    valid = false;
    if (errstream) {
      *errstream << "_high_multiple (" << _high_multiple
		 << ") > _max_multiple (" << _max_multiple
		 << ")" << std::endl;
    }
  }

  if (_low_threshhold > 1 || _low_threshhold < 0) {
    valid = false;
    if (errstream) {
      *errstream << "invalid low_threshhold (" << _low_threshhold << ")"
		 << std::endl;
    }
  }

  if (_high_threshhold > 1 || _high_threshhold < 0) {
    valid = false;
    if (errstream) {
      *errstream << "invalid high_threshhold (" << _high_threshhold << ")"
		 << std::endl;
    }
  }

  if (_max_multiple < 0) {
    valid = false;
    if (errstream) {
      *errstream << "invalid _max_multiple ("
		 << _max_multiple << ")"
		 << std::endl;
    }
  }

  if (_high_multiple < 0) {
    valid = false;
    if (errstream) {
      *errstream << "invalid _high_multiple ("
		 << _high_multiple << ")"
		 << std::endl;
    }
  }

  if (_expected_throughput < 0) {
    valid = false;
    if (errstream) {
      *errstream << "invalid _expected_throughput("
		 << _expected_throughput << ")"
		 << std::endl;
    }
  }

  if (!valid)
    return false;

  locker l(lock);
  low_threshhold = _low_threshhold;
  high_threshhold = _high_threshhold;
  high_delay_per_count = _high_multiple / _expected_throughput;
  max_delay_per_count = _max_multiple / _expected_throughput;
  max = _throttle_max;

  if (logger)
    logger->set(l_backoff_throttle_max, max);

  if (high_threshhold - low_threshhold > 0) {
    s0 = high_delay_per_count / (high_threshhold - low_threshhold);
  } else {
    low_threshhold = high_threshhold;
    s0 = 0;
  }

  if (1 - high_threshhold > 0) {
    s1 = (max_delay_per_count - high_delay_per_count)
      / (1 - high_threshhold);
  } else {
    high_threshhold = 1;
    s1 = 0;
  }

  _kick_waiters();
  return true;
}

std::chrono::duration<double> BackoffThrottle::_get_delay(uint64_t c) const
{
  if (max == 0)
    return std::chrono::duration<double>(0);

  double r = ((double)current) / ((double)max);
  if (r < low_threshhold) {
    return std::chrono::duration<double>(0);
  } else if (r < high_threshhold) {
    return c * std::chrono::duration<double>(
      (r - low_threshhold) * s0);
  } else {
    return c * std::chrono::duration<double>(
      high_delay_per_count + ((r - high_threshhold) * s1));
  }
}

std::chrono::duration<double> BackoffThrottle::get(uint64_t c)
{
  locker l(lock);
  auto delay = _get_delay(c);

  if (logger) {
    logger->inc(l_backoff_throttle_get);
    logger->inc(l_backoff_throttle_get_sum, c);
  }

  // fast path
  if (delay == std::chrono::duration<double>(0) &&
      waiters.empty() &&
      ((max == 0) || (current == 0) || ((current + c) <= max))) {
    current += c;

    if (logger) {
      logger->set(l_backoff_throttle_val, current);
    }

    return std::chrono::duration<double>(0);
  }

  auto ticket = _push_waiter();
  auto wait_from = mono_clock::now();
  bool waited = false;

  while (waiters.begin() != ticket) {
    (*ticket)->wait(l);
    waited = true;
  }

  auto start = std::chrono::system_clock::now();
  delay = _get_delay(c);
  while (true) {
    if (!((max == 0) || (current == 0) || (current + c) <= max)) {
      (*ticket)->wait(l);
      waited = true;
    } else if (delay > std::chrono::duration<double>(0)) {
      (*ticket)->wait_for(l, delay);
      waited = true;
    } else {
      break;
    }
    assert(ticket == waiters.begin());
    delay = _get_delay(c) - (std::chrono::system_clock::now() - start);
  }
  waiters.pop_front();
  _kick_waiters();

  current += c;

  if (logger) {
    logger->set(l_backoff_throttle_val, current);
    if (waited) {
      logger->tinc(l_backoff_throttle_wait, mono_clock::now() - wait_from);
    }
  }

  return std::chrono::system_clock::now() - start;
}

uint64_t BackoffThrottle::put(uint64_t c)
{
  locker l(lock);
  assert(current >= c);
  current -= c;
  _kick_waiters();

  if (logger) {
    logger->inc(l_backoff_throttle_put);
    logger->inc(l_backoff_throttle_put_sum, c);
    logger->set(l_backoff_throttle_val, current);
  }

  return current;
}

uint64_t BackoffThrottle::take(uint64_t c)
{
  locker l(lock);
  current += c;

  if (logger) {
    logger->inc(l_backoff_throttle_take);
    logger->inc(l_backoff_throttle_take_sum, c);
    logger->set(l_backoff_throttle_val, current);
  }

  return current;
}

uint64_t BackoffThrottle::get_current()
{
  locker l(lock);
  return current;
}

uint64_t BackoffThrottle::get_max()
{
  locker l(lock);
  return max;
}

SimpleThrottle::SimpleThrottle(uint64_t max, bool ignore_enoent)
  : m_max(max), m_ignore_enoent(ignore_enoent) {}

SimpleThrottle::~SimpleThrottle()
{
  auto l = uniquely_lock(m_lock);
  assert(m_current == 0);
  assert(waiters == 0);
}

void SimpleThrottle::start_op()
{
  auto l = uniquely_lock(m_lock);
  waiters++;
  m_cond.wait(l, [this]() { return m_max != m_current; });
  waiters--;
  ++m_current;
}

void SimpleThrottle::end_op(int r)
{
  auto l = uniquely_lock(m_lock);
  --m_current;
  if (r < 0 && !m_ret && !(r == -ENOENT && m_ignore_enoent))
    m_ret = r;
  m_cond.notify_all();
}

bool SimpleThrottle::pending_error() const
{
  auto l = uniquely_lock(m_lock);
  return (m_ret < 0);
}

int SimpleThrottle::wait_for_ret()
{
  auto l = uniquely_lock(m_lock);
  waiters++;
  m_cond.wait(l, [this]() { return m_current == 0; });
  waiters--;
  return m_ret;
}

void C_OrderedThrottle::finish(int r) {
  m_ordered_throttle->finish_op(m_tid, r);
}

OrderedThrottle::OrderedThrottle(uint64_t max, bool ignore_enoent)
  : m_max(max), m_ignore_enoent(ignore_enoent) {}

OrderedThrottle::~OrderedThrottle() {
  auto l  = uniquely_lock(m_lock);
  assert(waiters == 0);
}

C_OrderedThrottle *OrderedThrottle::start_op(Context *on_finish) {
  assert(on_finish);

  auto l = uniquely_lock(m_lock);
  uint64_t tid = m_next_tid++;
  m_tid_result[tid] = Result(on_finish);
  auto ctx = make_unique<C_OrderedThrottle>(this, tid);

  complete_pending_ops(l);
  while (m_max == m_current) {
    ++waiters;
    m_cond.wait(l);
    --waiters;
    complete_pending_ops(l);
  }
  ++m_current;

  return ctx.release();
}

void OrderedThrottle::end_op(int r) {
  auto l = uniquely_lock(m_lock);
  assert(m_current > 0);

  if (r < 0 && m_ret_val == 0 && (r != -ENOENT || !m_ignore_enoent)) {
    m_ret_val = r;
  }
  --m_current;
  m_cond.notify_all();
}

void OrderedThrottle::finish_op(uint64_t tid, int r) {
  auto l = uniquely_lock(m_lock);

  auto it = m_tid_result.find(tid);
  assert(it != m_tid_result.end());

  it->second.finished = true;
  it->second.ret_val = r;
  m_cond.notify_all();
}

bool OrderedThrottle::pending_error() const {
  auto l = uniquely_lock(m_lock);
  return (m_ret_val < 0);
}

int OrderedThrottle::wait_for_ret() {
  auto l = uniquely_lock(m_lock);
  complete_pending_ops(l);

  while (m_current > 0) {
    ++waiters;
    m_cond.wait(l);
    --waiters;
    complete_pending_ops(l);
  }
  return m_ret_val;
}

void OrderedThrottle::complete_pending_ops(UNIQUE_LOCK_T(m_lock)& l) {
  while (true) {
    auto it = m_tid_result.begin();
    if (it == m_tid_result.end() || it->first != m_complete_tid ||
        !it->second.finished) {
      break;
    }

    Result result = it->second;
    m_tid_result.erase(it);

    l.unlock();
    result.on_finish->complete(result.ret_val);
    l.lock();

    ++m_complete_tid;
  }
}
