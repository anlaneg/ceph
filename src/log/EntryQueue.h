// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_LOG_ENTRYQUEUE_H
#define __CEPH_LOG_ENTRYQUEUE_H

#include "Entry.h"

namespace ceph {
namespace logging {

struct EntryQueue {
  //队列长度
  int m_len;
  struct Entry *m_head, *m_tail;

  bool empty() const {
    return m_len == 0;
  }

  void swap(EntryQueue& other) {
	//保存自已的成员取值
    int len = m_len;
    struct Entry *h = m_head, *t = m_tail;

    //将other的成员取值设置到自已身上
    m_len = other.m_len;
    m_head = other.m_head;
    m_tail = other.m_tail;

    //将先前保存的自身的值，赋给other
    other.m_len = len;
    other.m_head = h;
    other.m_tail = t;
  }

  void enqueue(Entry *e) {
	//如果m_tail，说明队列有元素，则将e放入结尾，并更新结尾
    if (m_tail) {
      m_tail->m_next = e;
      m_tail = e;
    } else {
    	  //首个元素时，初始化head
      m_head = m_tail = e;
    }

    //长度增长
    m_len++;
  }

  Entry *dequeue() {
    if (!m_head)
      return NULL;//一个元素也没有
    Entry *e = m_head;
    m_head = m_head->m_next;//更新头
    if (!m_head)
      m_tail = NULL;//出队后，队列为空了
    m_len--;
    e->m_next = NULL;
    return e;
  }

  EntryQueue()
    : m_len(0),
      m_head(NULL),
      m_tail(NULL)
  {}
  ~EntryQueue() {
    Entry *t;
    //组团销毁
    while (m_head) {
      t = m_head->m_next;
      delete m_head;
      m_head = t;
    }      
  }
};

}
}

#endif
