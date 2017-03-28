// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in memory allocator.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */

#ifndef  CEPH_OS_BLUESTORE_BITALLOCATOR_H
#define CEPH_OS_BLUESTORE_BITALLOCATOR_H


#include <assert.h>
#include <stdint.h>
#include <pthread.h>
#include <mutex>
#include <atomic>
#include <vector>
#include "include/intarith.h"
#include "os/bluestore/bluestore_types.h"

#define alloc_assert assert

#ifdef BIT_ALLOCATOR_DEBUG
#define alloc_dbg_assert(x) assert(x)
#else
#define alloc_dbg_assert(x) (static_cast<void> (0))
#endif


class BitAllocatorStats {
public:
  std::atomic<int64_t> m_total_alloc_calls;
  std::atomic<int64_t> m_total_free_calls;
  std::atomic<int64_t> m_total_allocated;
  std::atomic<int64_t> m_total_freed;
  std::atomic<int64_t> m_total_serial_scans;
  std::atomic<int64_t> m_total_concurrent_scans;
  std::atomic<int64_t> m_total_node_scanned;

  BitAllocatorStats() {
    m_total_alloc_calls = 0;
    m_total_free_calls = 0;
    m_total_allocated = 0;
    m_total_freed = 0;
    m_total_serial_scans = 0;
    m_total_concurrent_scans = 0;
    m_total_node_scanned = 0;
  }

  void add_alloc_calls(int64_t val) {
    std::atomic_fetch_add(&m_total_alloc_calls, val);
  }
  void add_free_calls(int64_t val) {
    std::atomic_fetch_add(&m_total_free_calls, val);
  }
  void add_allocated(int64_t val) {
    std::atomic_fetch_add(&m_total_allocated, val);
  }
  void add_freed(int64_t val) {
    std::atomic_fetch_add(&m_total_freed, val);
  }
  void add_serial_scans(int64_t val) {
    std::atomic_fetch_add(&m_total_serial_scans, val);
  }
  void add_concurrent_scans(int64_t val) {
    std::atomic_fetch_add(&m_total_concurrent_scans, val);
  }
  void add_node_scanned(int64_t val) {
    std::atomic_fetch_add(&m_total_node_scanned, val);
  }
};

template <class BitMapEntity>
class BitMapEntityIter {
  typedef mempool::bluestore_alloc::vector<BitMapEntity> BitMapEntityVector;
  BitMapEntityVector *m_list;
  int64_t m_start_idx;
  int64_t m_cur_idx;
  bool m_wrap;
  bool m_wrapped;
  bool m_end;
public:

  void init(BitMapEntityVector *list, bool wrap, int64_t start_idx) {
    m_list = list;
    m_wrap = wrap;
    m_start_idx = start_idx;
    m_cur_idx = m_start_idx;
    m_wrapped = false;
    m_end = false;
  }

  BitMapEntityIter(BitMapEntityVector *list, int64_t start_idx) {
    init(list, false, start_idx);
  }
  BitMapEntityIter(BitMapEntityVector *list, int64_t start_idx, bool wrap) {
    init(list, wrap, start_idx);
  }

  BitMapEntity *next() {
    int64_t cur_idx = m_cur_idx;

    if (m_wrapped &&
      cur_idx == m_start_idx) {
      /*
       * End of wrap cycle + 1
       */
      if (!m_end) {
        m_end = true;
        return &(*m_list)[cur_idx];
      }
      return NULL;
    }
    m_cur_idx++;

    if (m_cur_idx == (int64_t)m_list->size() &&
        m_wrap) {
      m_cur_idx = 0;
      m_wrapped = true;
    }

    if (cur_idx == (int64_t)m_list->size()) {
      /*
       * End of list
       */
      return NULL;
    }

    alloc_assert(cur_idx < (int64_t)m_list->size());
    return &(*m_list)[cur_idx];
  }

  int64_t index() {
    return m_cur_idx;
  }
};

typedef unsigned long bmap_t;
typedef mempool::bluestore_alloc::vector<bmap_t> bmap_mask_vec_t;

class BmapEntry {
private:
  bmap_t m_bits;

public:
  MEMPOOL_CLASS_HELPERS();
  static bmap_t full_bmask() {
    return (bmap_t) -1;
  }
  static int64_t size() {
    return (sizeof(bmap_t) * 8);
  }
  static bmap_t empty_bmask() {
    return (bmap_t) 0;
  }
  static bmap_t align_mask(int x) {
    return ((x) >= BmapEntry::size()? (bmap_t) -1 : (~(((bmap_t) -1) >> (x))));
  }
  static bmap_t bit_mask(int bit_num) {
    return (bmap_t) 0x1 << ((BmapEntry::size() - 1) - bit_num);
  }
  bmap_t atomic_fetch() {
    return m_bits;
  }
  BmapEntry(CephContext*, bool val);
  BmapEntry(CephContext*) {
    m_bits = 0;
  }
  BmapEntry(const BmapEntry& bmap) {
    m_bits = bmap.m_bits;
  }

  void clear_bit(int bit);
  void clear_bits(int offset, int num_bits);
  void set_bits(int offset, int num_bits);
  bool check_n_set_bit(int bit);
  bool check_bit(int bit);
  bool is_allocated(int64_t start_bit, int64_t num_bits);

  int find_n_cont_bits(int start_offset, int64_t num_bits);
  int find_n_free_bits(int start_idx, int64_t max_bits,
           int *free_bit, int *end_idx);
  int find_first_set_bits(int64_t required_blocks, int bit_offset,
          int *start_offset, int64_t *scanned);

  void dump_state(CephContext* cct, const int& count);
  ~BmapEntry();

};

typedef enum bmap_area_type {
  UNDEFINED = 0,
  ZONE = 1,
  LEAF = 2,
  NON_LEAF = 3
} bmap_area_type_t;

class BitMapArea {
public:
  CephContext* cct;

protected:
  int16_t m_area_index;
  bmap_area_type_t m_type;

public:
  MEMPOOL_CLASS_HELPERS();
  static int64_t get_zone_size(CephContext* cct);
  static int64_t get_span_size(CephContext* cct);
  bmap_area_type_t level_to_type(int level);
  static int get_level(CephContext* cct, int64_t total_blocks);
  static int64_t get_level_factor(CephContext* cct, int level);
  virtual bool is_allocated(int64_t start_block, int64_t num_blocks) = 0;
  virtual bool is_exhausted() = 0;
  virtual bool child_check_n_lock(BitMapArea *child, int64_t required) {
      ceph_abort();
      return true;
  }
  virtual bool child_check_n_lock(BitMapArea *child, int64_t required, bool lock) {
      ceph_abort();
      return true;
  }
  virtual void child_unlock(BitMapArea *child) {
    ceph_abort();
  }

  virtual void lock_excl() = 0;
  virtual bool lock_excl_try() {
    ceph_abort();
    return false;
  }
  virtual void lock_shared() {
    ceph_abort();
    return;
  }
  virtual void unlock() = 0;

  virtual int64_t sub_used_blocks(int64_t num_blocks) = 0;
  virtual int64_t add_used_blocks(int64_t num_blocks) = 0;
  virtual bool reserve_blocks(int64_t num_blocks) = 0;
  virtual void unreserve(int64_t num_blocks, int64_t allocated) = 0;
  virtual int64_t get_reserved_blocks() = 0;
  virtual int64_t get_used_blocks() = 0;

  virtual void shutdown() = 0;

  virtual int64_t alloc_blocks_dis(int64_t num_blocks, int64_t min_alloc,
             int64_t hint, int64_t blk_off, ExtentList *block_list) {
    ceph_abort();
    return 0;
  }

  virtual void set_blocks_used(int64_t start_block, int64_t num_blocks) = 0;
  virtual void free_blocks(int64_t start_block, int64_t num_blocks) = 0;
  virtual int64_t size() = 0;

  int64_t child_count();
  int64_t get_index();
  int64_t get_level();
  bmap_area_type_t get_type();
  virtual void dump_state(int& count) = 0;
  BitMapArea(CephContext* cct) : cct(cct), m_type(UNDEFINED) {}
  virtual ~BitMapArea() { }
};

class BitMapAreaList {

private:
  BitMapArea **m_items;
  int64_t m_num_items;//m_items指针数组大小
  std::mutex m_marker_mutex;

public:
  BitMapArea *get_nth_item(int64_t idx) {//返回对应元素
    return m_items[idx];
  }

   BitMapArea ** get_item_list() {//返回item数组(与get_list重复，这两个需要被重命名）
    return m_items;
  }

  int64_t size() {//返回数组大小
    return m_num_items;
  }
  BitMapAreaList(BitMapArea **list, int64_t len);//列表注入
  BitMapAreaList(BitMapArea **list, int64_t len, int64_t marker);//列表，互斥量注入

  BitMapArea **get_list() {//返回item数组
    return m_items;
  }
};

/* Intensionally inlined for the sake of BitMapAreaLeaf::alloc_blocks_dis_int. */
class BmapEntityListIter {
  BitMapAreaList *m_list;//遍历哪个链
  int64_t m_start_idx;//标记我们遍历的起点
  int64_t m_cur_idx;//记录我们当前的位置
  bool m_wrap;//用户是否要求回绕
  bool m_wrapped;//当前是否已进行了回绕
  bool m_end;//是否到达结尾

public:
  BmapEntityListIter(BitMapAreaList* const list,
                     const int64_t start_idx,
                     const bool wrap = false)
    : m_list(list),
      m_start_idx(start_idx),
      m_cur_idx(start_idx),
      m_wrap(wrap),
      m_wrapped(false),
      m_end(false) {
  }

  BitMapArea* next() {
    int64_t cur_idx = m_cur_idx;

    //如果已回绕，且当前已等于起始idx
    if (m_wrapped &&
      cur_idx == m_start_idx) {
      /*
       * End of wrap cycle + 1
       */
      if (!m_end) {
    	//如果是第一次到达，则标记为已结束
        m_end = true;
        return m_list->get_nth_item(cur_idx);
      }
      return NULL;//不是第一次到达结尾了，直接返回NULL
    }
    m_cur_idx++;//自m_cur_idx返回（这样才能在end时，使m_list->get_nth_item有效）

    //到达数组结尾，如果需要回绕，则置为0
    if (m_cur_idx == m_list->size() &&
        m_wrap) {
      m_cur_idx = 0;
      m_wrapped = true;//标记为已回绕
    }

    //不容许回绕时，直接在结尾返回NULL
    if (cur_idx == m_list->size()) {
      /*
       * End of list
       */
      return NULL;
    }

    /* This method should be *really* fast as it's being executed over
     * and over during traversal of allocators indexes. */
    alloc_dbg_assert(cur_idx < m_list->size());
    return m_list->get_nth_item(cur_idx);//未达到结尾，返回指定位置元素
  }

  int64_t index();//返回当前位置
};

typedef mempool::bluestore_alloc::vector<BmapEntry> BmapEntryVector;

class BitMapZone: public BitMapArea{

private:
  std::atomic<int32_t> m_used_blocks;
  BmapEntryVector m_bmap_vec;
  std::mutex m_lock;

public:
  MEMPOOL_CLASS_HELPERS();
  static int64_t count;
  static int64_t total_blocks;
  static void incr_count() { count++;}
  static int64_t get_total_blocks() {return total_blocks;}
  bool is_allocated(int64_t start_block, int64_t num_blocks) override;
  bool is_exhausted() override;
  void reset_marker();

  int64_t sub_used_blocks(int64_t num_blocks) override;
  int64_t add_used_blocks(int64_t num_blocks) override;
  bool reserve_blocks(int64_t num_blocks) override;
  void unreserve(int64_t num_blocks, int64_t allocated) override;
  int64_t get_reserved_blocks() override;
  int64_t get_used_blocks() override;
  int64_t size() override {
    return get_total_blocks();
  }

  void lock_excl() override;
  bool lock_excl_try() override;
  void unlock() override;
  bool check_locked();

  void free_blocks_int(int64_t start_block, int64_t num_blocks);
  void init(int64_t zone_num, int64_t total_blocks, bool def);

  BitMapZone(CephContext* cct, int64_t total_blocks, int64_t zone_num);
  BitMapZone(CephContext* cct, int64_t total_blocks, int64_t zone_num, bool def);

  ~BitMapZone() override;
  void shutdown() override;
  int64_t alloc_blocks_dis(int64_t num_blocks, int64_t min_alloc, int64_t hint,
        int64_t blk_off, ExtentList *block_list) override;  
  void set_blocks_used(int64_t start_block, int64_t num_blocks) override;

  void free_blocks(int64_t start_block, int64_t num_blocks) override;
  void dump_state(int& count) override;
};

class BitMapAreaIN: public BitMapArea{

protected:
  int64_t m_child_size_blocks;
  int64_t m_total_blocks;//总块数
  int16_t m_level;
  int16_t m_num_child;

  int64_t m_used_blocks;//已用的块
  int64_t m_reserved_blocks;//预留的块
  std::mutex m_blocks_lock;
  BitMapAreaList *m_child_list;

  bool is_allocated(int64_t start_block, int64_t num_blocks) override;
  bool is_exhausted() override;

  bool child_check_n_lock(BitMapArea *child, int64_t required, bool lock) override {
    ceph_abort();
    return false;
  }

  bool child_check_n_lock(BitMapArea *child, int64_t required) override;
  void child_unlock(BitMapArea *child) override;

  void lock_excl() override {
    return;
  }
  void lock_shared() override {
    return;
  }
  void unlock() override {
    return;
  }

  void init(int64_t total_blocks, int64_t zone_size_block, bool def);
  void init_common(int64_t total_blocks, int64_t zone_size_block, bool def);
  int64_t alloc_blocks_dis_int_work(bool wrap, int64_t num_blocks, int64_t min_alloc, int64_t hint,
        int64_t blk_off, ExtentList *block_list);  

  int64_t alloc_blocks_int_work(bool wait, bool wrap,
                         int64_t num_blocks, int64_t hint, int64_t *start_block);

public:
  MEMPOOL_CLASS_HELPERS();
  BitMapAreaIN(CephContext* cct);
  BitMapAreaIN(CephContext* cct, int64_t zone_num, int64_t total_blocks);
  BitMapAreaIN(CephContext* cct, int64_t zone_num, int64_t total_blocks,
	       bool def);

  ~BitMapAreaIN() override;
  void shutdown() override;
  int64_t sub_used_blocks(int64_t num_blocks) override;
  int64_t add_used_blocks(int64_t num_blocks) override;
  bool reserve_blocks(int64_t num_blocks) override;
  void unreserve(int64_t num_blocks, int64_t allocated) override;
  int64_t get_reserved_blocks() override;
  int64_t get_used_blocks() override;
  virtual int64_t get_used_blocks_adj();
  int64_t size() override {
    return m_total_blocks;
  }
  using BitMapArea::alloc_blocks_dis; //non-wait version

  virtual int64_t alloc_blocks_dis_int(int64_t num_blocks, int64_t min_alloc, int64_t hint,
                                       int64_t blk_off, ExtentList *block_list);  
  int64_t alloc_blocks_dis(int64_t num_blocks, int64_t min_alloc, int64_t hint,
                           int64_t blk_off, ExtentList *block_list) override;  
  virtual void set_blocks_used_int(int64_t start_block, int64_t num_blocks);
  void set_blocks_used(int64_t start_block, int64_t num_blocks) override;

  virtual void free_blocks_int(int64_t start_block, int64_t num_blocks);
  void free_blocks(int64_t start_block, int64_t num_blocks) override;
  void dump_state(int& count) override;
};

class BitMapAreaLeaf: public BitMapAreaIN{

private:
  void init(int64_t total_blocks, int64_t zone_size_block,
            bool def);

public:
  MEMPOOL_CLASS_HELPERS();
  static int64_t count;
  static void incr_count() { count++;}
  BitMapAreaLeaf(CephContext* cct) : BitMapAreaIN(cct) { }
  BitMapAreaLeaf(CephContext* cct, int64_t zone_num, int64_t total_blocks);
  BitMapAreaLeaf(CephContext* cct, int64_t zone_num, int64_t total_blocks,
		 bool def);

  using BitMapAreaIN::child_check_n_lock;
  bool child_check_n_lock(BitMapArea *child, int64_t required) override {
    ceph_abort();
    return false;
  }

  bool child_check_n_lock(BitMapZone* child, int64_t required, bool lock);

  int64_t alloc_blocks_int(int64_t num_blocks, int64_t hint, int64_t *start_block);
  int64_t alloc_blocks_dis_int(int64_t num_blocks, int64_t min_alloc, int64_t hint,
        int64_t blk_off, ExtentList *block_list) override;  
  void free_blocks_int(int64_t start_block, int64_t num_blocks) override;

  ~BitMapAreaLeaf() override;
};


typedef enum bmap_alloc_mode {
  SERIAL = 1,
  CONCURRENT = 2,
} bmap_alloc_mode_t;

class BitAllocator:public BitMapAreaIN{
private:
  bmap_alloc_mode_t m_alloc_mode;
  std::mutex m_serial_mutex;
  pthread_rwlock_t m_rw_lock;
  BitAllocatorStats *m_stats;
  bool m_is_stats_on;
  int64_t m_extra_blocks;//记录的额外块数

  bool is_stats_on() {
    return m_is_stats_on;
  }

  using BitMapArea::child_check_n_lock;
  bool child_check_n_lock(BitMapArea *child, int64_t required) override;
  void child_unlock(BitMapArea *child) override;

  void serial_lock();
  bool try_serial_lock();
  void serial_unlock();
  void lock_excl() override;
  void lock_shared() override;
  bool try_lock();
  void unlock() override;

  bool check_input(int64_t num_blocks);
  bool check_input_dis(int64_t num_blocks);
  void init_check(int64_t total_blocks, int64_t zone_size_block,
                 bmap_alloc_mode_t mode, bool def, bool stats_on);
  int64_t alloc_blocks_dis_work(int64_t num_blocks, int64_t min_alloc, int64_t hint, ExtentList *block_list, bool reserved);

  int64_t alloc_blocks_dis_int(int64_t num_blocks, int64_t min_alloc, 
           int64_t hint, int64_t area_blk_off, ExtentList *block_list) override;

public:
  MEMPOOL_CLASS_HELPERS();

  BitAllocator(CephContext* cct, int64_t total_blocks,
	       int64_t zone_size_block, bmap_alloc_mode_t mode);
  BitAllocator(CephContext* cct, int64_t total_blocks, int64_t zone_size_block,
	       bmap_alloc_mode_t mode, bool def);
  BitAllocator(CephContext* cct, int64_t total_blocks, int64_t zone_size_block,
               bmap_alloc_mode_t mode, bool def, bool stats_on);
  ~BitAllocator() override;
  void shutdown() override;
  using BitMapAreaIN::alloc_blocks_dis; //Wait version

  void free_blocks(int64_t start_block, int64_t num_blocks) override;
  void set_blocks_used(int64_t start_block, int64_t num_blocks) override;
  void unreserve_blocks(int64_t blocks);

  int64_t alloc_blocks_dis_res(int64_t num_blocks, int64_t min_alloc, int64_t hint, ExtentList *block_list);

  void free_blocks_dis(int64_t num_blocks, ExtentList *block_list);
  bool is_allocated_dis(ExtentList *blocks, int64_t num_blocks);

  int64_t total_blocks() const {
    return m_total_blocks - m_extra_blocks;
  }
  int64_t get_used_blocks() override {
    return (BitMapAreaIN::get_used_blocks_adj() - m_extra_blocks);
  }

  BitAllocatorStats *get_stats() {
      return m_stats;
  }
  void dump();
};

#endif //End of file
