// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_BLUESTORE_BLUESTORE_TYPES_H
#define CEPH_OSD_BLUESTORE_BLUESTORE_TYPES_H

#include <ostream>
#include <bitset>
#include "include/types.h"
#include "include/interval_set.h"
#include "include/utime.h"
#include "include/small_encoding.h"
#include "common/hobject.h"
#include "compressor/Compressor.h"
#include "common/Checksummer.h"
#include "include/mempool.h"

namespace ceph {
  class Formatter;
}

/// label for block device
struct bluestore_bdev_label_t {
  uuid_d osd_uuid;     ///< osd uuid
  uint64_t size;       ///< device size
  utime_t btime;       ///< birth time
  string description;  ///< device description

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_bdev_label_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_bdev_label_t)

ostream& operator<<(ostream& out, const bluestore_bdev_label_t& l);

/// collection metadata
//集合元数据
struct bluestore_cnode_t {
  uint32_t bits;   ///< how many bits of coll pgid are significant

  explicit bluestore_cnode_t(int b=0) : bits(b) {}

  DENC(bluestore_cnode_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.bits, p);
    DENC_FINISH(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_cnode_t*>& o);
};
WRITE_CLASS_DENC(bluestore_cnode_t)

class AllocExtent;
typedef mempool::bluestore_alloc::vector<AllocExtent> AllocExtentVector;
class AllocExtent {
public:
  uint64_t offset;//本块数据起始位置在物理磁盘上的偏移量
  uint32_t length;//本块数据长度

  AllocExtent() { 
    offset = 0;
    length = 0;
  }

  AllocExtent(int64_t off, int32_t len) : offset(off), length(len) { }

  //本块数据结束时，在物理磁盘上的偏移量
  uint64_t end() const {
    return offset + length;
  }

  //是否为同一范围
  bool operator==(const AllocExtent& other) const {
    return offset == other.offset && length == other.length;
  }
};

inline static ostream& operator<<(ostream& out, const AllocExtent& e) {
  return out << "0x" << std::hex << e.offset << "~" << e.length << std::dec;
}

class ExtentList {
  AllocExtentVector *m_extents;//存储结果用
  int64_t m_block_size;//块大小
  int64_t m_max_blocks;//最多需要申请多少块

public:
  void init(AllocExtentVector *extents, int64_t block_size,
	    uint64_t max_alloc_size) {
    m_extents = extents;
    m_block_size = block_size;
    m_max_blocks = max_alloc_size / block_size;
    assert(m_extents->empty());//存储用的结果集一定为空
  }

  ExtentList(AllocExtentVector *extents, int64_t block_size) {
    init(extents, block_size, 0);
  }

  ExtentList(AllocExtentVector *extents, int64_t block_size,
	     uint64_t max_alloc_size) {
    init(extents, block_size, max_alloc_size);
  }

  void reset() {
    m_extents->clear();
  }

  void add_extents(int64_t start, int64_t count);

  AllocExtentVector *get_extents() {
    return m_extents;
  }

  std::pair<int64_t, int64_t> get_nth_extent(int index) {
      return std::make_pair
            ((*m_extents)[index].offset / m_block_size,
             (*m_extents)[index].length / m_block_size);
  }

  int64_t get_extent_count() {
    return m_extents->size();
  }
};


/// pextent: physical extent
struct bluestore_pextent_t : public AllocExtent {
  const static uint64_t INVALID_OFFSET = ~0ull;

  bluestore_pextent_t() : AllocExtent() {}
  bluestore_pextent_t(uint64_t o, uint64_t l) : AllocExtent(o, l) {}
  bluestore_pextent_t(AllocExtent &ext) : AllocExtent(ext.offset, ext.length) { }

  //如果已设置值，则为有效的
  bool is_valid() const {
    return offset != INVALID_OFFSET;
  }

  DENC(bluestore_pextent_t, v, p) {
    denc_lba(v.offset, p);
    denc_varint_lowz(v.length, p);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_pextent_t*>& ls);
};
WRITE_CLASS_DENC(bluestore_pextent_t)

ostream& operator<<(ostream& out, const bluestore_pextent_t& o);

//这个定义就实现了一个blob对应多个pextent的方式
typedef mempool::bluestore_meta_other::vector<bluestore_pextent_t> PExtentVector;

template<>
struct denc_traits<PExtentVector> {
  static constexpr bool supported = true;
  static constexpr bool bounded = false;
  static constexpr bool featured = false;
  static void bound_encode(const PExtentVector& v, size_t& p) {
    p += sizeof(uint32_t);
    const auto size = v.size();
    if (size) {
      size_t per = 0;
      denc(v.front(), per);
      p +=  per * size;
    }
  }
  static void encode(const PExtentVector& v,
		     bufferlist::contiguous_appender& p) {
    denc_varint(v.size(), p);
    for (auto& i : v) {
      denc(i, p);
    }
  }
  static void decode(PExtentVector& v, bufferptr::iterator& p) {
    unsigned num;
    denc_varint(num, p);
    v.clear();
    v.resize(num);
    for (unsigned i=0; i<num; ++i) {
      denc(v[i], p);
    }
  }
};


/// extent_map: a map of reference counted extents
//按注释的意思，一组引用计数范围的映射
//实际用处，blob在共享情况下需要知道哪些段没人用了，哪些段只有一个人用，
//哪些段有多个人用，用这些信息来完成物理磁盘空的释放，改写，copy
struct bluestore_extent_ref_map_t {
  struct record_t {
    uint32_t length;//长度
    uint32_t refs;//引用计数
    record_t(uint32_t l=0, uint32_t r=0) : length(l), refs(r) {}
    DENC(bluestore_extent_ref_map_t::record_t, v, p) {
      denc_varint_lowz(v.length, p);
      denc_varint(v.refs, p);
    }
  };

  typedef mempool::bluestore_meta_other::map<uint64_t,record_t> map_t;
  //key是offset,value是指从key指明的offset开始有length字节长的段，这个段
  //这个段被引用的次数是refs次
  //这个数据结构的目的是在clone情况下，在写时，哪些段需要copy，那些可以在原样上修改
  //这个结构比较别脚
  map_t ref_map;

  void _check() const;
  void _maybe_merge_left(map_t::iterator& p);

  void clear() {
    ref_map.clear();
  }

  bool empty() const {
    return ref_map.empty();
  }

  //划分共享段
  void get(uint64_t offset, uint32_t len);
  //释放共享段
  void put(uint64_t offset, uint32_t len, PExtentVector *release);

  bool contains(uint64_t offset, uint32_t len) const;
  bool intersects(uint64_t offset, uint32_t len) const;

  void bound_encode(size_t& p) const {
    denc_varint((uint32_t)0, p);
    if (!ref_map.empty()) {
      size_t elem_size = 0;
      denc_varint_lowz((uint64_t)0, elem_size);
      ref_map.begin()->second.bound_encode(elem_size);
      p += elem_size * ref_map.size();
    }
  }

  //编码
  void encode(bufferlist::contiguous_appender& p) const {
    uint32_t n = ref_map.size();
    denc_varint(n, p);
    if (n) {
      auto i = ref_map.begin();
      denc_varint_lowz(i->first, p);
      i->second.encode(p);
      int64_t pos = i->first;
      while (--n) {
	++i;
	denc_varint_lowz((int64_t)i->first - pos, p);
	i->second.encode(p);
	pos = i->first;
      }
    }
  }

  //解码
  void decode(bufferptr::iterator& p) {
    uint32_t n;
    denc_varint(n, p);
    if (n) {
      int64_t pos;
      denc_varint_lowz(pos, p);
      ref_map[pos].decode(p);
      while (--n) {
	int64_t delta;
	denc_varint_lowz(delta, p);
	pos += delta;
	ref_map[pos].decode(p);
      }
    }
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_extent_ref_map_t*>& o);
};
WRITE_CLASS_DENC(bluestore_extent_ref_map_t)


ostream& operator<<(ostream& out, const bluestore_extent_ref_map_t& rm);
static inline bool operator==(const bluestore_extent_ref_map_t::record_t& l,
			      const bluestore_extent_ref_map_t::record_t& r) {
  return l.length == r.length && l.refs == r.refs;
}
static inline bool operator==(const bluestore_extent_ref_map_t& l,
			      const bluestore_extent_ref_map_t& r) {
  return l.ref_map == r.ref_map;
}
static inline bool operator!=(const bluestore_extent_ref_map_t& l,
			      const bluestore_extent_ref_map_t& r) {
  return !(l == r);
}

/// blob_use_tracker: a set of per-alloc unit ref counters to track blob usage
struct bluestore_blob_use_tracker_t {
  // N.B.: There is no need to minimize au_size/num_au
  //   as much as possible (e.g. have just a single byte for au_size) since:
  //   1) Struct isn't packed hence it's padded. And even if it's packed see 2)
  //   2) Mem manager has its own granularity, most probably >= 8 bytes
  //
  uint32_t au_size; // Allocation (=tracking) unit size,
                    // == 0 if uninitialized
  uint32_t num_au;  // Amount of allocation units tracked
                    // == 0 if single unit or the whole blob is tracked
                       
  union {
    uint32_t* bytes_per_au;
    uint32_t total_bytes;
  };
  
  bluestore_blob_use_tracker_t()
    : au_size(0), num_au(0), bytes_per_au(nullptr) {
  }
  ~bluestore_blob_use_tracker_t() {
    clear();
  }

  void clear() {
    if (num_au != 0) {
      delete[] bytes_per_au;
    }
    bytes_per_au = 0;
    au_size = 0;
    num_au = 0;
  }

  uint32_t get_referenced_bytes() const {
    uint32_t total = 0;
    if (!num_au) {
      total = total_bytes;
    } else {
      for (size_t i = 0; i < num_au; ++i) {
	total += bytes_per_au[i];
      }
    }
    return total;
  }
  bool is_not_empty() const {
    if (!num_au) {
      return total_bytes != 0;
    } else {
      for (size_t i = 0; i < num_au; ++i) {
	if (bytes_per_au[i]) {
	  return true;
	}
      }
    }
    return false;
  }
  bool is_empty() const {
    return !is_not_empty();
  }
  void prune_tail(uint32_t new_len) {
    if (num_au) {
      new_len = ROUND_UP_TO(new_len, au_size);
      uint32_t _num_au = new_len / au_size;
      assert(_num_au <= num_au);
      if (_num_au) {
        num_au = _num_au; // bytes_per_au array is left unmodified
      } else {
        clear();
      }
    }
  }
  
  void init(
    uint32_t full_length,
    uint32_t _au_size);

  void get(
    uint32_t offset,
    uint32_t len);

  /// put: return true if the blob has no references any more after the call,
  /// no release_units is filled for the sake of performance.
  /// return false if there are some references to the blob,
  /// in this case release_units contains pextents
  /// (identified by their offsets relative to the blob start)
  //  that are not used any more and can be safely deallocated. 
  bool put(
    uint32_t offset,
    uint32_t len,
    PExtentVector *release);

  bool can_split() const;
  bool can_split_at(uint32_t blob_offset) const;
  void split(
    uint32_t blob_offset,
    bluestore_blob_use_tracker_t* r);

  bool equal(
    const bluestore_blob_use_tracker_t& other) const;
    
  void bound_encode(size_t& p) const {
    denc_varint(au_size, p);
    if (au_size) {
      denc_varint(num_au, p);
      if (!num_au) {
        denc_varint(total_bytes, p);
      } else {
        size_t elem_size = 0;
        denc_varint((uint32_t)0, elem_size);
        p += elem_size * num_au;
      }
    }
  }
  void encode(bufferlist::contiguous_appender& p) const {
    denc_varint(au_size, p);
    if (au_size) {
      denc_varint(num_au, p);
      if (!num_au) {
        denc_varint(total_bytes, p);
      } else {
        size_t elem_size = 0;
        denc_varint((uint32_t)0, elem_size);
        for (size_t i = 0; i < num_au; ++i) {
          denc_varint(bytes_per_au[i], p);
        }
      }
    }
  }
  void decode(bufferptr::iterator& p) {
    clear();
    denc_varint(au_size, p);
    if (au_size) {
      denc_varint(num_au, p);
      if (!num_au) {
        denc_varint(total_bytes, p);
      } else {
        allocate();
        for (size_t i = 0; i < num_au; ++i) {
	  denc_varint(bytes_per_au[i], p);
        }
      }
    }
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_blob_use_tracker_t*>& o);
private:
  void allocate();
  void fall_back_to_per_au(uint32_t _num_au, uint32_t _au_size);
};
WRITE_CLASS_DENC(bluestore_blob_use_tracker_t)
ostream& operator<<(ostream& out, const bluestore_blob_use_tracker_t& rm);

/// blob: a piece of data on disk
//blob:磁盘上的一段数据
//数据本身存在磁盘上，这个结构体仅保存了这个blob在磁盘中的不同段的（偏移量，长度）
//
struct bluestore_blob_t {
  enum {
	//标记为可overwrite，可分割
    FLAG_MUTABLE = 1,         ///< blob can be overwritten or split
	//标记为已压缩
    FLAG_COMPRESSED = 2,      ///< blob is compressed
	//标记为有checksum
    FLAG_CSUM = 4,            ///< blob has checksums
    FLAG_HAS_UNUSED = 8,      ///< blob has unused map
	//标记为已共享
    FLAG_SHARED = 16,         ///< blob is shared; see external SharedBlob
  };
  //数字标记转字符串
  static string get_flags_string(unsigned flags);


  //占用的物理磁盘范围{（offset,length),(offset,length),。。。}
  //其中offset是用于物理磁盘定位的，非文件本身的offset
  //物理的每个范围，从下标0开始，表示文件实际占用的磁盘，...
  PExtentVector extents;              ///< raw data position on device

  //压缩前数据长度，压缩后长度
  uint32_t compressed_length_orig = 0;///< original length of compressed blob if any
  uint32_t compressed_length = 0;     ///< compressed length if any

  //标记
  uint32_t flags = 0;                 ///< FLAG_*

  //标记哪些位置未用（只有16位，故每一位表示blob_len/16字节）
  uint16_t unused = 0;     ///< portion that has never been written to (bitmap)

  //记录checksum 类型，及checksum块大小
  uint8_t csum_type = Checksummer::CSUM_NONE;      ///< CSUM_*
  uint8_t csum_chunk_order = 0;       ///< csum block size is 1<<block_order bytes

  //保存计算的check sum
  bufferptr csum_data;                ///< opaque vector of csum data

  bluestore_blob_t(uint32_t f = 0) : flags(f) {}

  DENC_HELPERS;
  //计算编码长度
  void bound_encode(size_t& p, uint64_t struct_v) const {
    assert(struct_v == 1 || struct_v == 2);
    denc(extents, p);
    denc_varint(flags, p);
    denc_varint_lowz(compressed_length_orig, p);
    denc_varint_lowz(compressed_length, p);
    denc(csum_type, p);
    denc(csum_chunk_order, p);
    denc_varint(csum_data.length(), p);
    p += csum_data.length();
    p += sizeof(unsigned long long);
  }

  //进行编码
  void encode(bufferlist::contiguous_appender& p, uint64_t struct_v) const {
    assert(struct_v == 1 || struct_v == 2);
    denc(extents, p);
    denc_varint(flags, p);
    if (is_compressed()) {
      denc_varint_lowz(compressed_length_orig, p);
      denc_varint_lowz(compressed_length, p);
    }
    if (has_csum()) {
      denc(csum_type, p);
      denc(csum_chunk_order, p);
      denc_varint(csum_data.length(), p);
      memcpy(p.get_pos_add(csum_data.length()), csum_data.c_str(),
	     csum_data.length());
    }
    if (has_unused()) {
      denc(unused, p);
    }
  }

  //可以解码
  void decode(bufferptr::iterator& p, uint64_t struct_v) {
    assert(struct_v == 1 || struct_v == 2);
    denc(extents, p);
    denc_varint(flags, p);
    if (is_compressed()) {
      denc_varint_lowz(compressed_length_orig, p);
      denc_varint_lowz(compressed_length, p);
    }
    if (has_csum()) {
      denc(csum_type, p);
      denc(csum_chunk_order, p);
      int len;
      denc_varint(len, p);
      csum_data = p.get_ptr(len);
    }
    if (has_unused()) {
      denc(unused, p);
    }
  }

  //是否可分裂
  bool can_split() const {
    return
      !has_flag(FLAG_SHARED) &&
      !has_flag(FLAG_COMPRESSED) &&
      !has_flag(FLAG_HAS_UNUSED);     // splitting unused set is complex
  }

  //非checksum启用下，任意位置可split,启用情况下，仅以chunk_size对齐位置可split
  bool can_split_at(uint32_t blob_offset) const {
    return !has_csum() || blob_offset % get_csum_chunk_size() == 0;
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_blob_t*>& ls);

  bool has_flag(unsigned f) const {
    return flags & f;
  }
  void set_flag(unsigned f) {
    flags |= f;
  }
  void clear_flag(unsigned f) {
    flags &= ~f;
  }

  //显示flags对应的文字描述
  string get_flags_string() const {
    return get_flags_string(flags);
  }

  void set_compressed(uint64_t clen_orig, uint64_t clen) {
    set_flag(FLAG_COMPRESSED);
    compressed_length_orig = clen_orig;
    compressed_length = clen;
  }

  //检查是否有易变标记
  bool is_mutable() const {
    return has_flag(FLAG_MUTABLE);
  }

  //标查是否有压缩标记
  bool is_compressed() const {
    return has_flag(FLAG_COMPRESSED);
  }

  //检查是否有checksum标记
  bool has_csum() const {
    return has_flag(FLAG_CSUM);
  }

  //标查是否有未用标记
  bool has_unused() const {
    return has_flag(FLAG_HAS_UNUSED);
  }

  //是否已共享
  bool is_shared() const {
    return has_flag(FLAG_SHARED);
  }

  /// return chunk (i.e. min readable block) size for the blob
  //默认为dev_block_size ,考虑了需要计算checksum的情况
  uint64_t get_chunk_size(uint64_t dev_block_size) const {
    return has_csum() ?
      MAX(dev_block_size, get_csum_chunk_size()) : dev_block_size;
  }

  //check sum块大小
  uint32_t get_csum_chunk_size() const {
    return 1 << csum_chunk_order;
  }
  uint32_t get_compressed_payload_length() const {
    return is_compressed() ? compressed_length : 0;
  }
  uint32_t get_compressed_payload_original_length() const {
    return is_compressed() ? compressed_length_orig : 0;
  }

  //计算x_off在物理上的偏移量，以及可以自此偏移量开始，最多读取多长数据
  uint64_t calc_offset(uint64_t x_off, uint64_t *plen) const {
    auto p = extents.begin();
    assert(p != extents.end());
    //从第一块物理范围开始
    while (x_off >= p->length) {
      x_off -= p->length;
      ++p;
      assert(p != extents.end());
    }

    //*plen表示，从x_off起始位置到物理块结束，还有多长
    if (plen)
      *plen = p->length - x_off;
    //物理上的偏移量
    return p->offset + x_off;
  }

  /// return true if the entire range is allocated (mapped to extents on disk)
  //如果给定段已完成在磁盘上的映射，则返回true,否则false
  bool is_allocated(uint64_t b_off, uint64_t b_len) const {
	//定位b_off在哪个范围内
    auto p = extents.begin();
    assert(p != extents.end());
    while (b_off >= p->length) {
      b_off -= p->length;
      ++p;
      assert(p != extents.end());
    }
    //完成起始位置定位
    b_len += b_off;
    while (b_len) {
      assert(p != extents.end());
      if (!p->is_valid()) {
	return false;
      }
      if (p->length >= b_len) {
	return true;
      }
      b_len -= p->length;
      ++p;
    }
    assert(0 == "we should not get here");
  }

  /// return true if the logical range has never been used
  //检查offset起始到length之间是否unused
  bool is_unused(uint64_t offset, uint64_t length) const {
    if (!has_unused()) {
      return false;
    }
    uint64_t blob_len = get_logical_length();
    assert((blob_len % (sizeof(unused)*8)) == 0);
    assert(offset + length <= blob_len);
    uint64_t chunk_size = blob_len / (sizeof(unused)*8);
    uint64_t start = offset / chunk_size;
    uint64_t end = ROUND_UP_TO(offset + length, chunk_size) / chunk_size;
    auto i = start;
    //将blob_len构造成 start,end两个chunk位置
    while (i < end && (unused & (1u << i))) {
      i++;
    }
    return i >= end;//如果unused相应的位为1，则返回True
  }

  /// mark a range that has never been used
  //将offset起始长度为length的一段标记为未用
  void add_unused(uint64_t offset, uint64_t length) {
    uint64_t blob_len = get_logical_length();
    assert((blob_len % (sizeof(unused)*8)) == 0);
    //offset指定的范围必须在blob_len以内
    assert(offset + length <= blob_len);
    uint64_t chunk_size = blob_len / (sizeof(unused)*8);
    uint64_t start = ROUND_UP_TO(offset, chunk_size) / chunk_size;
    uint64_t end = (offset + length) / chunk_size;
    for (auto i = start; i < end; ++i) {
      unused |= (1u << i);
    }
    if (start != end) {
      set_flag(FLAG_HAS_UNUSED);
    }
  }

  /// indicate that a range has (now) been used.
  //将offset起始长度为length的一段标记为已用
  void mark_used(uint64_t offset, uint64_t length) {
    if (has_unused()) {
      uint64_t blob_len = get_logical_length();
      assert((blob_len % (sizeof(unused)*8)) == 0);
      assert(offset + length <= blob_len);
      uint64_t chunk_size = blob_len / (sizeof(unused)*8);
      uint64_t start = offset / chunk_size;
      uint64_t end = ROUND_UP_TO(offset + length, chunk_size) / chunk_size;
      for (auto i = start; i < end; ++i) {
        unused &= ~(1u << i);
      }
      if (unused == 0) {
        clear_flag(FLAG_HAS_UNUSED);
      }
    }
  }

  //用f函数访问x_off,x_len之间的数据，由于数据可能分段存储，故f可能会调用多次
  int map(uint64_t x_off, uint64_t x_len,
	   std::function<int(uint64_t,uint64_t)> f) const {
    auto p = extents.begin();
    assert(p != extents.end());
    while (x_off >= p->length) {//找到x_off对应的p
      x_off -= p->length;
      ++p;
      assert(p != extents.end());
    }
    while (x_len > 0) {
      //从x_off开始一直向前走x_len长度，将这段数据，按每个extents进行遍历，访问函数由f定义
      assert(p != extents.end());
      uint64_t l = MIN(p->length - x_off, x_len);
      int r = f(p->offset + x_off, l);
      if (r < 0)
        return r;
      x_off = 0;
      x_len -= l;
      ++p;
    }
    return 0;
  }


  //bl传入了一段数据，我们在当前的extents中查找x_off开始，到(x_off + bl.length())
  //将这么长的数据，沿extents指明的物理范围，使得每一个范围调用一次f,并直接所有的bl数据
  //都被遍历完。
  void map_bl(uint64_t x_off,
	      bufferlist& bl,
	      std::function<void(uint64_t,bufferlist&)> f) const {
    auto p = extents.begin();
    assert(p != extents.end());
    //找x_off在extents中位于那个pe上
    while (x_off >= p->length) {
      //找到合适的p
      x_off -= p->length;
      ++p;
      assert(p != extents.end());
    }
    bufferlist::iterator it = bl.begin();
    uint64_t x_len = bl.length();
    while (x_len > 0) {
      assert(p != extents.end());
      uint64_t l = MIN(p->length - x_off, x_len);
      bufferlist t;
      it.copy(l, t);//准备足够长度的数据
      f(p->offset + x_off, t);//调用回调
      x_off = 0;
      x_len -= l;
      ++p;//处理可能未完，向下一个p移动
    }
  }

  //磁盘占用大小（取每一个磁盘段的物理大小）
  uint32_t get_ondisk_length() const {
    uint32_t len = 0;
    for (auto &p : extents) {
      len += p.length;
    }
    return len;
  }

  //逻辑大小（比如磁盘占用大小）
  uint32_t get_logical_length() const {
    if (is_compressed()) {
    	//压缩模式下，返回原长度
      return compressed_length_orig;
    } else {
      return get_ondisk_length();
    }
  }

  //当前类型，checksum值占用多少字节
  size_t get_csum_value_size() const;

  //计算有多少个checksum值
  size_t get_csum_count() const {
    size_t vs = get_csum_value_size();
    if (!vs)
      return 0;
    return csum_data.length() / vs;
  }

  //按checksum不同结果集字节宽度，提取索引值对应的数据，并返回
  uint64_t get_csum_item(unsigned i) const {
    size_t cs = get_csum_value_size();
    const char *p = csum_data.c_str();
    switch (cs) {
    case 0:
      assert(0 == "no csum data, bad index");
    case 1:
      return reinterpret_cast<const uint8_t*>(p)[i];
    case 2:
      return reinterpret_cast<const __le16*>(p)[i];
    case 4:
      return reinterpret_cast<const __le32*>(p)[i];
    case 8:
      return reinterpret_cast<const __le64*>(p)[i];
    default:
      assert(0 == "unrecognized csum word size");
    }
  }

  //得到checksum值中的第n块校验结果
  const char *get_csum_item_ptr(unsigned i) const {
    size_t cs = get_csum_value_size();
    return csum_data.c_str() + (cs * i);
  }

  //得到checksum值中的第n块校验的存放位置
  char *get_csum_item_ptr(unsigned i) {
    size_t cs = get_csum_value_size();
    return csum_data.c_str() + (cs * i);
  }

  //checksum初始化（设置类型，计算初始值，设置checksum单元块大小）
  void init_csum(unsigned type, unsigned order, unsigned len) {
    flags |= FLAG_CSUM;
    csum_type = type;
    csum_chunk_order = order;
    csum_data = buffer::create(get_csum_value_size() * len / get_csum_chunk_size());
    csum_data.zero();
  }

  /// calculate csum for the buffer at the given b_off
  void calc_csum(uint64_t b_off, const bufferlist& bl);

  /// verify csum: return -EOPNOTSUPP for unsupported checksum type;
  /// return -1 and valid(nonnegative) b_bad_off for checksum error;
  /// return 0 if all is well.
  int verify_csum(uint64_t b_off, const bufferlist& bl, int* b_bad_off,
		  uint64_t *bad_csum) const;

  //extents长度大于1，且最后一块无效，且此块未用，则返回true
  bool can_prune_tail() const {
    return
      extents.size() > 1 &&  // if it's all invalid it's not pruning.
      !extents.back().is_valid() &&
      !has_unused();
  }

  //丢弃掉最后一块物理块
  void prune_tail() {
	//先扔掉最后一个范围
    extents.pop_back();
    if (has_csum()) {
      bufferptr t;
      t.swap(csum_data);
      //由于是按csum chunk size计算checksum的，故对于指定数据，其checksum的内容大小是
      //一定的，按下面式子可计算。（扔掉最后一个范围后，get_logical_length大小会变化。
      //构造csum_data数据
      csum_data = bufferptr(t.c_str(),
			    get_logical_length() / get_csum_chunk_size() *
			    get_csum_value_size());
    }
  }

  uint32_t get_release_size(uint32_t min_alloc_size) const {
    if (is_compressed()) {
    	//压缩情况下，返回压缩源大小
      return get_logical_length();
    }

    //如果没有启用checksum,则返回min_alloc_size
    uint32_t res = get_csum_chunk_size();
    if (!has_csum() || res < min_alloc_size) {
      res = min_alloc_size;
    }

    //如果启用checksum以check sum的大于min_alloc_size为准
    return res;
  }
};
WRITE_CLASS_DENC_FEATURED(bluestore_blob_t)

ostream& operator<<(ostream& out, const bluestore_blob_t& o);


/// shared blob state
struct bluestore_shared_blob_t {
  //blob 编号
  uint64_t sbid;                       ///> shared blob id
  //记录共享的范围及共享计数
  bluestore_extent_ref_map_t ref_map;  ///< shared blob extents

  bluestore_shared_blob_t(uint64_t _sbid) : sbid(_sbid) {}

  DENC(bluestore_shared_blob_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.ref_map, p);
    DENC_FINISH(p);
  }


  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_shared_blob_t*>& ls);

  bool empty() const {
    return ref_map.empty();
  }
};
WRITE_CLASS_DENC(bluestore_shared_blob_t)

ostream& operator<<(ostream& out, const bluestore_shared_blob_t& o);

/// onode: per-object metadata
struct bluestore_onode_t {
  //编号
  uint64_t nid = 0;                    ///< numeric id (locally unique)
  //文件尺寸大小
  uint64_t size = 0;                   ///< object size
  //属性，对象的attribute
  map<mempool::bluestore_meta_other::string, bufferptr> attrs;        ///< attrs //属性，对象的attribute

  struct shard_info {
    uint32_t offset = 0;  ///< logical offset for start of shard
    uint32_t bytes = 0;   ///< encoded bytes
    DENC(shard_info, v, p) {
      denc_varint(v.offset, p);
      denc_varint(v.bytes, p);
    }
    void dump(Formatter *f) const;
  };
  vector<shard_info> extent_map_shards; ///< extent map shards (if any)

  uint32_t expected_object_size = 0;
  uint32_t expected_write_size = 0;
  uint32_t alloc_hint_flags = 0;

  uint8_t flags = 0;

  enum {
    FLAG_OMAP = 1,
  };

  string get_flags_string() const {
    string s;
    if (flags & FLAG_OMAP) {
      s = "omap";
    }
    return s;
  }

  bool has_flag(unsigned f) const {
    return flags & f;
  }

  void set_flag(unsigned f) {
    flags |= f;
  }

  void clear_flag(unsigned f) {
    flags &= ~f;
  }

  bool has_omap() const {
    return has_flag(FLAG_OMAP);
  }

  void set_omap_flag() {
    set_flag(FLAG_OMAP);
  }

  void clear_omap_flag() {
    clear_flag(FLAG_OMAP);
  }

  DENC(bluestore_onode_t, v, p) {
    DENC_START(1, 1, p);
    denc_varint(v.nid, p);
    denc_varint(v.size, p);
    denc(v.attrs, p);
    denc(v.flags, p);
    denc(v.extent_map_shards, p);
    denc_varint(v.expected_object_size, p);
    denc_varint(v.expected_write_size, p);
    denc_varint(v.alloc_hint_flags, p);
    DENC_FINISH(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_onode_t*>& o);
};
WRITE_CLASS_DENC(bluestore_onode_t::shard_info)
WRITE_CLASS_DENC(bluestore_onode_t)

ostream& operator<<(ostream& out, const bluestore_onode_t::shard_info& si);

/// writeahead-logged op
struct bluestore_deferred_op_t {
  typedef enum {
    OP_WRITE = 1,
  } type_t;
  __u8 op = 0;//操作

  PExtentVector extents;//写的范围
  bufferlist data;//要写入的数据

  DENC(bluestore_deferred_op_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.op, p);
    denc(v.extents, p);
    denc(v.data, p);
    DENC_FINISH(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_deferred_op_t*>& o);
};
WRITE_CLASS_DENC(bluestore_deferred_op_t)


/// writeahead-logged transaction
struct bluestore_deferred_transaction_t {
  uint64_t seq = 0;//wal事务编号
  list<bluestore_deferred_op_t> ops;//记录事务需要的操作
  interval_set<uint64_t> released;  ///< allocations to release after tx

  bluestore_deferred_transaction_t() : seq(0) {}

  DENC(bluestore_deferred_transaction_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.seq, p);
    denc(v.ops, p);
    denc(v.released, p);
    DENC_FINISH(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_deferred_transaction_t*>& o);
};
WRITE_CLASS_DENC(bluestore_deferred_transaction_t)

struct bluestore_compression_header_t {
  uint8_t type = Compressor::COMP_ALG_NONE;
  uint32_t length = 0;

  bluestore_compression_header_t() {}
  bluestore_compression_header_t(uint8_t _type)
    : type(_type) {}

  DENC(bluestore_compression_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    denc(v.length, p);
    DENC_FINISH(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_compression_header_t*>& o);
};
WRITE_CLASS_DENC(bluestore_compression_header_t)


#endif
