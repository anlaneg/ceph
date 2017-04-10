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

#ifndef PRIORITY_QUEUE_H
#define PRIORITY_QUEUE_H

#include "common/Formatter.h"
#include "common/OpQueue.h"

#include <map>
#include <list>

/**
 * Manages queue for normal and strict priority items
 * 支持普通和严格优先级队列
 *
 * On dequeue, the queue will select the lowest priority queue
 * such that the q has bucket > cost of front queue item.
 *
 * 在出队时，首先选择最小优先级的队列，在其中先择token值大于花费的第一个元素
 *
 * If there is no such queue, we choose the next queue item for
 * the highest priority queue.
 * 如果没有这样的队列，我们换下一个队列，直到最高优先级队列
 *
 * Before returning a dequeued item, we place into each bucket
 * cost * (priority/total_priority) tokens.
 * 在返回出队的元素前，我们增加每个桶中的token修正
 *
 * enqueue_strict and enqueue_strict_front queue items into queues
 * which are serviced in strict priority order before items queued
 * with enqueue and enqueue_front
 * 严格入队，可以放在队前或队后。
 *
 * Within a priority class, we schedule round robin based on the class
 * of type K used to enqueue items.  e.g. you could use entity_inst_t
 * to provide fairness for different clients.
 *
 * PrioritizedQueue是一个优先级队列，按优先级划分成组，每个组称之为SubQueue
 * SubQueue按类型划分成多个子队列，称之为ListPairs，ListPairs的每项由花费与item构成
 * 当我们入队时，会指定优先级，类型，故可以定位到一个ListPairs,在ListPairs中记录cost,
 * 及入队的项
 * 当我们出队时，分两种high_queue按优先级出队,queue按token值出队
 */
template <typename T, typename K>
class PrioritizedQueue : public OpQueue <T, K> {
  int64_t total_priority;//所有优先级对列的优先级值之和
  int64_t max_tokens_per_subqueue;//每子队列的最大tokens
  int64_t min_cost;//最小花费

  typedef std::list<std::pair<unsigned, T> > ListPairs;
  //在l中移除被f匹配的项，返回移除的项总数目
  static unsigned filter_list_pairs(
    ListPairs *l,
    std::function<bool (T)> f) {
    unsigned ret = 0;
    //逆序遍历并移除
    for (typename ListPairs::iterator i = l->end();
	 i != l->begin();
      ) {
      auto next = i;
      --next;
      if (f(next->second)) {
    	  //被匹配，增加计数，移除next
	++ret;
	l->erase(next);
      } else {
	i = next;
      }
    }
    return ret;
  }

  struct SubQueue {
  private:
	//将map类型定义为Classes类型
    typedef std::map<K, ListPairs> Classes;
    Classes q;
    unsigned tokens, max_tokens;
    int64_t size;//队列长度
    typename Classes::iterator cur;
  public:
    SubQueue(const SubQueue &other)
      : q(other.q),
	tokens(other.tokens),
	max_tokens(other.max_tokens),
	size(other.size),
	cur(q.begin()) {}
    SubQueue()
      : tokens(0),
	max_tokens(0),
	size(0), cur(q.begin()) {}
    void set_max_tokens(unsigned mt) {
      max_tokens = mt;
    }
    unsigned get_max_tokens() const {
      return max_tokens;
    }
    unsigned num_tokens() const {
      return tokens;
    }

    //增加token
    void put_tokens(unsigned t) {
      tokens += t;
      if (tokens > max_tokens) {
	tokens = max_tokens;
      }
    }

    //减少token
    void take_tokens(unsigned t) {
      if (tokens > t) {
	tokens -= t;
      } else {
	tokens = 0;
      }
    }

    //按cl进行分类，分类后形成一个list,将花费与item一起保存（加入到队尾）
    void enqueue(K cl, unsigned cost, T item) {
      q[cl].push_back(std::make_pair(cost, item));
      if (cur == q.end())
	cur = q.begin();
      size++;
    }

    //按cl进行分类，分类后形成一个list,将花费与item一起保存（加入到队头）
    void enqueue_front(K cl, unsigned cost, T item) {
      q[cl].push_front(std::make_pair(cost, item));
      if (cur == q.end())
	cur = q.begin();
      size++;
    }

    std::pair<unsigned, T> front() const {
      assert(!(q.empty()));
      assert(cur != q.end());
      return cur->second.front();
    }

    void pop_front() {
    	  //断言，一定不为空
      assert(!(q.empty()));
      assert(cur != q.end());
      cur->second.pop_front();
      if (cur->second.empty()) {
	q.erase(cur++);
      } else {
	++cur;
      }
      if (cur == q.end()) {
	cur = q.begin();
      }
      size--;
    }

    //返回队列长度
    unsigned length() const {
      assert(size >= 0);
      return (unsigned)size;
    }

    //返回队列是否为空
    bool empty() const {
      return q.empty();
    }

    //满足函数f的，移除
    void remove_by_filter(
      std::function<bool (T)> f) {
      for (typename Classes::iterator i = q.begin();
	   i != q.end();
	   ) {
    	  //在list中执行f函数进行过滤，减少总size
	size -= filter_list_pairs(&(i->second), f);
	if (i->second.empty()) {
	  if (cur == i) {
	    ++cur;
	  }
	  q.erase(i++);
	} else {
	  ++i;
	}
      }
      if (cur == q.end())
	cur = q.begin();
    }

    //移除指定分类
    void remove_by_class(K k, std::list<T> *out) {
      typename Classes::iterator i = q.find(k);
      if (i == q.end()) {
    	  //没有找到k,不需要移除
	return;
      }

      //找到，减队列大小
      size -= i->second.size();
      if (i == cur) {
	++cur;
      }
      if (out) {
	for (typename ListPairs::reverse_iterator j =
	       i->second.rbegin();
	     j != i->second.rend();
	     ++j) {
		//要移除i,将i队列中所有元素，逆序加入到out中
	  out->push_front(j->second);
	}
      }
      //移除i
      q.erase(i);
      if (cur == q.end()) {
	cur = q.begin();
      }
    }

    void dump(ceph::Formatter *f) const {
      f->dump_int("tokens", tokens);
      f->dump_int("max_tokens", max_tokens);
      f->dump_int("size", size);
      f->dump_int("num_keys", q.size());
      if (!empty()) {
	f->dump_int("first_item_cost", front().first);
      }
    }
  };

  //将map类型定义为SubQueues类型,此类型first为优先级，SubQueue是此优先级对应的queue
  typedef std::map<unsigned, SubQueue> SubQueues;
  SubQueues high_queue;
  SubQueues queue;

  //生成优先级为priority的队列
  SubQueue *create_queue(unsigned priority) {
	//如果此优先队列已存在，则直接返回
    typename SubQueues::iterator p = queue.find(priority);
    if (p != queue.end()) {
      return &p->second;
    }

    total_priority += priority;
    SubQueue *sq = &queue[priority];//创建此优先级队列
    sq->set_max_tokens(max_tokens_per_subqueue);
    return sq;
  }

  //移除优先级值为priority的队列
  void remove_queue(unsigned priority) {
	//断言必须存在
    assert(queue.count(priority));
    queue.erase(priority);//移除
    total_priority -= priority;//优先级之后排除
    assert(total_priority >= 0);
  }

  void distribute_tokens(unsigned cost) {
    if (total_priority == 0) {
      return;
    }
    for (typename SubQueues::iterator i = queue.begin();
	 i != queue.end();
	 ++i) {
    	  //为每一个subqueue增加token
      i->second.put_tokens(((i->first * cost) / total_priority) + 1);
    }
  }

public:
  PrioritizedQueue(unsigned max_per, unsigned min_c)
    : total_priority(0),
      max_tokens_per_subqueue(max_per),
      min_cost(min_c)
  {}

  //返回队列长度
  unsigned length() const final {
    unsigned total = 0;
    //枚举queue，对second.length进行计数
    for (typename SubQueues::const_iterator i = queue.begin();
	 i != queue.end();
	 ++i) {
      assert(i->second.length());
      total += i->second.length();
    }

    //枚举high_queue,对second.length进行计数
    for (typename SubQueues::const_iterator i = high_queue.begin();
	 i != high_queue.end();
	 ++i) {
      assert(i->second.length());
      total += i->second.length();
    }
    return total;
  }

  //移除被f函数匹配的项
  void remove_by_filter(
      std::function<bool (T)> f) final {
	//枚举queue,针对每个分队列，按函数f要求移除
    for (typename SubQueues::iterator i = queue.begin();
	 i != queue.end();
	 ) {
      unsigned priority = i->first;
      
      i->second.remove_by_filter(f);
      if (i->second.empty()) {
    	  //如果分队列已为空，则跳过此队列，并将其移除
	++i;
	remove_queue(priority);
      } else {
	++i;//由于上一句if，导致++i没法放在for语句里
      }
    }

    //枚举high_queue队列，进行filter
    for (typename SubQueues::iterator i = high_queue.begin();
	 i != high_queue.end();
	 ) {
      i->second.remove_by_filter(f);
      if (i->second.empty()) {
	high_queue.erase(i++);
      } else {
	++i;
      }
    }
  }

  //移除k指定的队列，将其中所有元素加入到out中
  void remove_by_class(K k, std::list<T> *out = 0) final {

	//处理queue队列
    for (typename SubQueues::iterator i = queue.begin();
	 i != queue.end();
	 ) {
      i->second.remove_by_class(k, out);
      if (i->second.empty()) {
	unsigned priority = i->first;
	++i;
	remove_queue(priority);//移除优先队列
      } else {
	++i;
      }
    }

    //处理high_queue队列
    for (typename SubQueues::iterator i = high_queue.begin();
	 i != high_queue.end();
	 ) {
      i->second.remove_by_class(k, out);
      if (i->second.empty()) {
	high_queue.erase(i++);
      } else {
	++i;
      }
    }
  }

  //入队到high_queue队列
  void enqueue_strict(K cl, unsigned priority, T item) final {
    high_queue[priority].enqueue(cl, 0, item);
  }

  //入队到high_queue队列，且入队至front
  void enqueue_strict_front(K cl, unsigned priority, T item) final {
    high_queue[priority].enqueue_front(cl, 0, item);
  }

  //入队到queue队列
  void enqueue(K cl, unsigned priority, unsigned cost, T item) final {
    if (cost < min_cost)
      cost = min_cost;//规范最小花费
    if (cost > max_tokens_per_subqueue)
      cost = max_tokens_per_subqueue;//规范最大花费
    //创建指定优先队列，并入队
    create_queue(priority)->enqueue(cl, cost, item);
  }

  //入队queue,且在队头位置
  void enqueue_front(K cl, unsigned priority, unsigned cost, T item) final {
    if (cost < min_cost)
      cost = min_cost;
    if (cost > max_tokens_per_subqueue)
      cost = max_tokens_per_subqueue;
    create_queue(priority)->enqueue_front(cl, cost, item);
  }

  //队列是否为空
  bool empty() const final {
    assert(total_priority >= 0);
    assert((total_priority == 0) || !(queue.empty()));
    return queue.empty() && high_queue.empty();
  }

  //出队
  T dequeue() final {
    assert(!empty());

    //如果high_queue不为空时，取优先级最高的SubQueue的中的第一个
    //每个SubQueue中的花费是一样的
    if (!(high_queue.empty())) {
    	  //返回优先级最高的
      T ret = high_queue.rbegin()->second.front().second;
      high_queue.rbegin()->second.pop_front();//移除优先级最高的
      //如果此SubQueue已为空，则移除此优先级队列
      if (high_queue.rbegin()->second.empty()) {
	high_queue.erase(high_queue.rbegin()->first);
      }
      //返回
      return ret;
    }

    // if there are multiple buckets/subqueues with sufficient tokens,
    // we behave like a strict priority queue among all subqueues that
    // are eligible to run.
    for (typename SubQueues::iterator i = queue.begin();
	 i != queue.end();
	 ++i) {
    	  //出队断言，一定不为空，为空的都被移除掉了
      assert(!(i->second.empty()));
      //如果花费比token值小，则可以出队
      if (i->second.front().first < i->second.num_tokens()) {
	T ret = i->second.front().second;
	unsigned cost = i->second.front().first;
	i->second.take_tokens(cost);//消费cost
	i->second.pop_front();//弹出

	//考虑出队后队列为空的情况
	if (i->second.empty()) {
	  remove_queue(i->first);
	}

	//提高token值
	distribute_tokens(cost);
	return ret;
      }
    }

    // if no subqueues have sufficient tokens, we behave like a strict
    // priority queue.
    //按token没有可以出队的，改变方式，按优先级出队
    T ret = queue.rbegin()->second.front().second;
    unsigned cost = queue.rbegin()->second.front().first;
    queue.rbegin()->second.pop_front();
    if (queue.rbegin()->second.empty()) {
      remove_queue(queue.rbegin()->first);
    }
    distribute_tokens(cost);//增加token
    return ret;
  }

  void dump(ceph::Formatter *f) const final {
    f->dump_int("total_priority", total_priority);
    f->dump_int("max_tokens_per_subqueue", max_tokens_per_subqueue);
    f->dump_int("min_cost", min_cost);
    f->open_array_section("high_queues");
    for (typename SubQueues::const_iterator p = high_queue.begin();
	 p != high_queue.end();
	 ++p) {
      f->open_object_section("subqueue");
      f->dump_int("priority", p->first);
      p->second.dump(f);
      f->close_section();
    }
    f->close_section();
    f->open_array_section("queues");
    for (typename SubQueues::const_iterator p = queue.begin();
	 p != queue.end();
	 ++p) {
      f->open_object_section("subqueue");
      f->dump_int("priority", p->first);
      p->second.dump(f);
      f->close_section();
    }
    f->close_section();
  }
};

#endif
