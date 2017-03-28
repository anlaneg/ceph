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

#include "bluestore_types.h"
#include "common/Formatter.h"
#include "common/Checksummer.h"
#include "include/stringify.h"

//添加新申请到的物理范围
void ExtentList::add_extents(int64_t start, int64_t count) {
  AllocExtent *last_extent = NULL;
  bool can_merge = false;

  if (!m_extents->empty()) {//只有多于一个范围时，才有可合并的可能，检查能否合并
    last_extent = &(m_extents->back());
    uint64_t last_offset = last_extent->end() / m_block_size;
    uint32_t last_length = last_extent->length / m_block_size;
    if ((last_offset == (uint64_t) start) &&
        (!m_max_blocks || (last_length + count) <= m_max_blocks)) {
      can_merge = true;
    }
  }

  if (can_merge) {//可以合并时，直接更新最后一个段
    last_extent->length += (count * m_block_size);
  } else {//不能合并，直接新起一个段
    m_extents->emplace_back(AllocExtent(start * m_block_size,
					count * m_block_size));
  }
}

// bluestore_bdev_label_t

void bluestore_bdev_label_t::encode(bufferlist& bl) const
{
  // be slightly friendly to someone who looks at the device
  bl.append("bluestore block device\n");
  bl.append(stringify(osd_uuid));
  bl.append("\n");
  ENCODE_START(1, 1, bl);
  ::encode(osd_uuid, bl);
  ::encode(size, bl);
  ::encode(btime, bl);
  ::encode(description, bl);
  ENCODE_FINISH(bl);
}

void bluestore_bdev_label_t::decode(bufferlist::iterator& p)
{
  p.advance(60); // see above
  DECODE_START(1, p);
  ::decode(osd_uuid, p);
  ::decode(size, p);
  ::decode(btime, p);
  ::decode(description, p);
  DECODE_FINISH(p);
}

void bluestore_bdev_label_t::dump(Formatter *f) const
{
  f->dump_stream("osd_uuid") << osd_uuid;
  f->dump_unsigned("size", size);
  f->dump_stream("btime") << btime;
  f->dump_string("description", description);
}

void bluestore_bdev_label_t::generate_test_instances(
  list<bluestore_bdev_label_t*>& o)
{
  o.push_back(new bluestore_bdev_label_t);
  o.push_back(new bluestore_bdev_label_t);
  o.back()->size = 123;
  o.back()->btime = utime_t(4, 5);
  o.back()->description = "fakey";
}

ostream& operator<<(ostream& out, const bluestore_bdev_label_t& l)
{
  return out << "bdev(osd_uuid " << l.osd_uuid
	     << " size 0x" << std::hex << l.size << std::dec
	     << " btime " << l.btime
	     << " desc " << l.description << ")";
}

// cnode_t

void bluestore_cnode_t::dump(Formatter *f) const
{
  f->dump_unsigned("bits", bits);
}

void bluestore_cnode_t::generate_test_instances(list<bluestore_cnode_t*>& o)
{
  o.push_back(new bluestore_cnode_t());
  o.push_back(new bluestore_cnode_t(0));
  o.push_back(new bluestore_cnode_t(123));
}

// bluestore_extent_ref_map_t

void bluestore_extent_ref_map_t::_check() const
{
  uint64_t pos = 0;
  unsigned refs = 0;
  for (const auto &p : ref_map) {
    if (p.first < pos)
      assert(0 == "overlap");
    if (p.first == pos && p.second.refs == refs)
      assert(0 == "unmerged");
    pos = p.first + p.second.length;
    refs = p.second.refs;
  }
}

//ref_map可以合并的前提是:1.引用计数相同;2.区间重合
void bluestore_extent_ref_map_t::_maybe_merge_left(
  map<uint64_t,record_t>::iterator& p)
{
  if (p == ref_map.begin())
    return;
  auto q = p;
  --q;
  if (q->second.refs == p->second.refs &&
      q->first + q->second.length == p->first) {
    q->second.length += p->second.length;
    ref_map.erase(p);
    p = q;
  }
}

void bluestore_extent_ref_map_t::get(uint64_t offset, uint32_t length)
{
	//找第一个包含大于等于offset的段
  auto p = ref_map.lower_bound(offset);
  if (p != ref_map.begin()) {
    --p;
    if (p->first + p->second.length <= offset) {
      ++p;
    }
  }

  while (length > 0) {
    if (p == ref_map.end()) {
      // nothing after offset; add the whole thing.
    	//没有找到比offset大的段，所以这里需要将offset起始的段加入引用表，
    	//引用数为1
      p = ref_map.insert(
	map<uint64_t,record_t>::value_type(offset, record_t(length, 1))).first;
      break;
    }

    if (p->first > offset) {
      //这种情况下当前的位置p指向的offset大于offset,则说明offset起始的这一段没有被
      //加入引用表，我们需要将这没重叠的这一段加入，且引用数为1
      // gap
      uint64_t newlen = MIN(p->first - offset, length);
      p = ref_map.insert(
	map<uint64_t,record_t>::value_type(offset,
					   record_t(newlen, 1))).first;

      //更新offset,length
      offset += newlen;
      length -= newlen;

      //检查我们是否可以和左侧进行合并，如果可以合并的话，就并成一个record
      //合并的前提是1.引用计数相同，2左侧的offset+length == 我们的offset
      _maybe_merge_left(p);
      ++p;//我们可能有一部分和后面的是重叠的，这里采用continue继续处理
      continue;
    }

    //p里记录的offset比现在offset要小，这种情况下，我们有一部分和p是重合的
    //这种需要拆分，原因是我们加入后引用计数就不相等了(p->first,offset- p->length)
    if (p->first < offset) {
      // split off the portion before offset
      //拆分，第一部分直接更新即可，将长度更新为offset-p->first
      assert(p->first + p->second.length > offset);
      uint64_t left = p->first + p->second.length - offset;
      p->second.length = offset - p->first;

      //第二部分，从offset开始，到left,引用计数暂不变
      p = ref_map.insert(map<uint64_t,record_t>::value_type(
			   offset, record_t(left, p->second.refs))).first;
      // continue below
      //然后我们就可以和p->first == offset的变成同一类型了
    }

    //处理offset且与某一段相同的情况
    assert(p->first == offset);
    if (length < p->second.length) {
      //我们的长度小于p->length,需要拆分，一部分从offset+length起始，引用计数不变
      ref_map.insert(make_pair(offset + length,
			       record_t(p->second.length - length,
					p->second.refs)));

      //第二部分，缩小长度为length，引用计数加1
      //感觉这里有个bug，引用计数增加了，没有考虑和left的合并问题
      //作者统一改在循环外进行检查了
      p->second.length = length;
      ++p->second.refs;
      break;
    }

    //我们的长度大于p->length,需要先增加这部分的引用计数，然后更新
    //offset,length
    //注：由于引用计数更新,需要考虑和left的合并问题
    ++p->second.refs;
    offset += p->second.length;
    length -= p->second.length;
    _maybe_merge_left(p);
    ++p;//继续尝试后面的段
  }


  //检查是否可以和左侧合并
  if (p != ref_map.end())
    _maybe_merge_left(p);
  //_check();
}

//get的反操作，不分析了，本意是要看哪些段没人用了，就释放物理段
void bluestore_extent_ref_map_t::put(
  uint64_t offset, uint32_t length,
  PExtentVector *release)
{
  //NB: existing entries in 'release' container must be preserved!

  auto p = ref_map.lower_bound(offset);
  if (p == ref_map.end() || p->first > offset) {
    if (p == ref_map.begin()) {
      assert(0 == "put on missing extent (nothing before)");
    }
    --p;
    if (p->first + p->second.length <= offset) {
      assert(0 == "put on missing extent (gap)");
    }
  }
  if (p->first < offset) {
    uint64_t left = p->first + p->second.length - offset;
    p->second.length = offset - p->first;
    p = ref_map.insert(map<uint64_t,record_t>::value_type(
			 offset, record_t(left, p->second.refs))).first;
  }
  while (length > 0) {
    assert(p->first == offset);
    if (length < p->second.length) {
      ref_map.insert(make_pair(offset + length,
			       record_t(p->second.length - length,
					p->second.refs)));
      if (p->second.refs > 1) {
	p->second.length = length;
	--p->second.refs;
	_maybe_merge_left(p);
      } else {
	if (release)
	  release->push_back(bluestore_pextent_t(p->first, length));
	ref_map.erase(p);
      }
      return;
    }
    offset += p->second.length;
    length -= p->second.length;
    if (p->second.refs > 1) {
      --p->second.refs;
      _maybe_merge_left(p);
      ++p;
    } else {
      if (release)
	release->push_back(bluestore_pextent_t(p->first, p->second.length));
      ref_map.erase(p++);
    }
  }
  if (p != ref_map.end())
    _maybe_merge_left(p);
  //_check();
}

//检查offset到offset+length段是否在引用表中存在（有空隙也不行）
bool bluestore_extent_ref_map_t::contains(uint64_t offset, uint32_t length) const
{
  auto p = ref_map.lower_bound(offset);
  if (p == ref_map.end() || p->first > offset) {
    if (p == ref_map.begin()) {
      return false; // nothing before
    }
    --p;
    if (p->first + p->second.length <= offset) {
      return false; // gap
    }
  }
  while (length > 0) {
    if (p == ref_map.end())
      return false;
    if (p->first > offset)
      return false;
    if (p->first + p->second.length >= offset + length)
      return true;
    uint64_t overlap = p->first + p->second.length - offset;
    offset += overlap;
    length -= overlap;
    ++p;
  }
  return true;
}

//检查ref_map是否部分或者全部包含(offset,offset+length),如果是返回True
bool bluestore_extent_ref_map_t::intersects(
  uint64_t offset,
  uint32_t length) const
{
  //找一个包含offset的
  auto p = ref_map.lower_bound(offset);
  if (p != ref_map.begin()) {
    --p;
    if (p->first + p->second.length <= offset) {
      ++p;
    }
  }

  //如果没找到，返回false
  if (p == ref_map.end())
    return false;

  //如果不包含，返回false
  if (p->first >= offset + length)
    return false;

  //肯定全部包含或者部分包含
  return true;  // intersects p!
}

void bluestore_extent_ref_map_t::dump(Formatter *f) const
{
  f->open_array_section("ref_map");
  for (auto& p : ref_map) {
    f->open_object_section("ref");
    f->dump_unsigned("offset", p.first);
    f->dump_unsigned("length", p.second.length);
    f->dump_unsigned("refs", p.second.refs);
    f->close_section();
  }
  f->close_section();
}

void bluestore_extent_ref_map_t::generate_test_instances(
  list<bluestore_extent_ref_map_t*>& o)
{
  o.push_back(new bluestore_extent_ref_map_t);
  o.push_back(new bluestore_extent_ref_map_t);
  o.back()->get(10, 10);
  o.back()->get(18, 22);
  o.back()->get(20, 20);
  o.back()->get(10, 25);
  o.back()->get(15, 20);
}

ostream& operator<<(ostream& out, const bluestore_extent_ref_map_t& m)
{
  out << "ref_map(";
  for (auto p = m.ref_map.begin(); p != m.ref_map.end(); ++p) {
    if (p != m.ref_map.begin())
      out << ",";
    out << std::hex << "0x" << p->first << "~" << p->second.length << std::dec
	<< "=" << p->second.refs;
  }
  out << ")";
  return out;
}

// bluestore_blob_use_tracker_t

void bluestore_blob_use_tracker_t::allocate()
{
  assert(num_au != 0);
  bytes_per_au = new uint32_t[num_au];
  for (uint32_t i = 0; i < num_au; ++i) {
    bytes_per_au[i] = 0;
  }
}

void bluestore_blob_use_tracker_t::init(
  uint32_t full_length, uint32_t _au_size) {
  assert(!au_size || is_empty()); 
  assert(_au_size > 0);
  assert(full_length > 0);
  clear();  
  uint32_t _num_au = ROUND_UP_TO(full_length, _au_size) / _au_size;
  au_size = _au_size;
  if( _num_au > 1 ) {
    num_au = _num_au;
    allocate();
  }
}

void bluestore_blob_use_tracker_t::get(
  uint32_t offset, uint32_t length)
{
  assert(au_size);
  if (!num_au) {
    total_bytes += length;
  }else {
    auto end = offset + length;

    while (offset < end) {
      auto phase = offset % au_size;
      bytes_per_au[offset / au_size] += 
	MIN(au_size - phase, end - offset);
      offset += (phase ? au_size - phase : au_size);
    }
  }
}

bool bluestore_blob_use_tracker_t::put(
  uint32_t offset, uint32_t length,
  PExtentVector *release_units)
{
  assert(au_size);
  if (release_units) {
    release_units->clear();
  }
  bool maybe_empty = true;
  if (!num_au) {
    assert(total_bytes >= length);
    total_bytes -= length;
  } else {
    auto end = offset + length;
    uint64_t next_offs = 0;
    while (offset < end) {
      auto phase = offset % au_size;
      size_t pos = offset / au_size;
      auto diff = MIN(au_size - phase, end - offset);
      assert(diff <= bytes_per_au[pos]);
      bytes_per_au[pos] -= diff;
      offset += (phase ? au_size - phase : au_size);
      if (bytes_per_au[pos] == 0) {
	if (release_units) {
          if (release_units->empty() || next_offs != pos * au_size) {
  	    release_units->emplace_back(pos * au_size, au_size);
          } else {
            release_units->back().length += au_size;
          }
          next_offs += au_size;
	}
      } else {
	maybe_empty = false; // micro optimization detecting we aren't empty 
	                     // even in the affected extent
      }
    }
  }
  bool empty = maybe_empty ? !is_not_empty() : false;
  if (empty && release_units) {
    release_units->clear();
  }
  return empty;
}

bool bluestore_blob_use_tracker_t::can_split() const
{
  return num_au > 0;
}

bool bluestore_blob_use_tracker_t::can_split_at(uint32_t blob_offset) const
{
  assert(au_size);
  return (blob_offset % au_size) == 0 &&
         blob_offset < num_au * au_size;
}

void bluestore_blob_use_tracker_t::split(
  uint32_t blob_offset,
  bluestore_blob_use_tracker_t* r)
{
  assert(au_size);
  assert(can_split());
  assert(can_split_at(blob_offset));
  assert(r->is_empty());
  
  uint32_t new_num_au = blob_offset / au_size;
  r->init( (num_au - new_num_au) * au_size, au_size);

  for (auto i = new_num_au; i < num_au; i++) {
    r->get((i - new_num_au) * au_size, bytes_per_au[i]);
    bytes_per_au[i] = 0;
  }
  if (new_num_au == 0) {
    clear();
  } else if (new_num_au == 1) {
    uint32_t tmp = bytes_per_au[0];
    uint32_t _au_size = au_size;
    clear();
    au_size = _au_size;
    total_bytes = tmp;
  } else {
    num_au = new_num_au;
  }
}

bool bluestore_blob_use_tracker_t::equal(
  const bluestore_blob_use_tracker_t& other) const
{
  if (!num_au && !other.num_au) {
    return total_bytes == other.total_bytes && au_size == other.au_size;
  } else if (num_au && other.num_au) {
    if (num_au != other.num_au || au_size != other.au_size) {
      return false;
    }
    for (size_t i = 0; i < num_au; i++) {
      if (bytes_per_au[i] != other.bytes_per_au[i]) {
        return false;
      }
    }
    return true;
  }

  uint32_t n = num_au ? num_au : other.num_au;
  uint32_t referenced = 
    num_au ? other.get_referenced_bytes() : get_referenced_bytes();
   auto bytes_per_au_tmp = num_au ? bytes_per_au : other.bytes_per_au;
  uint32_t my_referenced = 0;
  for (size_t i = 0; i < n; i++) {
    my_referenced += bytes_per_au_tmp[i];
    if (my_referenced > referenced) {
      return false;
    }
  }
  return my_referenced == referenced;
}

void bluestore_blob_use_tracker_t::dump(Formatter *f) const
{
  f->dump_unsigned("num_au", num_au);
  f->dump_unsigned("au_size", au_size);
  if (!num_au) {
    f->dump_unsigned("total_bytes", total_bytes);
  } else {
    f->open_array_section("bytes_per_au");
    for (size_t i = 0; i < num_au; ++i) {
      f->dump_unsigned("", bytes_per_au[i]);
    }
    f->close_section();
  }
}

void bluestore_blob_use_tracker_t::generate_test_instances(
  list<bluestore_blob_use_tracker_t*>& o)
{
  o.push_back(new bluestore_blob_use_tracker_t());
  o.back()->init(16, 16);
  o.back()->get(10, 10);
  o.back()->get(10, 5);
  o.push_back(new bluestore_blob_use_tracker_t());
  o.back()->init(60, 16);
  o.back()->get(18, 22);
  o.back()->get(20, 20);
  o.back()->get(15, 20);
}

ostream& operator<<(ostream& out, const bluestore_blob_use_tracker_t& m)
{
  out << "use_tracker(" << std::hex;
  if (!m.num_au) {
    out << "0x" << m.au_size 
        << " "
        << "0x" << m.total_bytes;
  } else {
    out << "0x" << m.num_au 
        << "*0x" << m.au_size 
	<< " 0x[";
    for (size_t i = 0; i < m.num_au; ++i) {
      if (i != 0)
	out << ",";
      out << m.bytes_per_au[i];
    }
    out << "]";
  }
  out << std::dec << ")";
  return out;
}

// bluestore_pextent_t

void bluestore_pextent_t::dump(Formatter *f) const
{
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
}

ostream& operator<<(ostream& out, const bluestore_pextent_t& o) {
  if (o.is_valid())
    return out << "0x" << std::hex << o.offset << "~" << o.length << std::dec;
  else
    return out << "!~" << std::hex << o.length << std::dec;
}

void bluestore_pextent_t::generate_test_instances(list<bluestore_pextent_t*>& ls)
{
  ls.push_back(new bluestore_pextent_t);
  ls.push_back(new bluestore_pextent_t(1, 2));
}

// bluestore_blob_t

string bluestore_blob_t::get_flags_string(unsigned flags)
{
  string s;
  if (flags & FLAG_MUTABLE) {
    s = "mutable";
  }
  if (flags & FLAG_COMPRESSED) {
    if (s.length())
      s += '+';
    s += "compressed";
  }
  if (flags & FLAG_CSUM) {
    if (s.length())
      s += '+';
    s += "csum";
  }
  if (flags & FLAG_HAS_UNUSED) {
    if (s.length())
      s += '+';
    s += "has_unused";
  }
  if (flags & FLAG_SHARED) {
    if (s.length())
      s += '+';
    s += "shared";
  }

  return s;
}

//按checksum类型计算checksum值占用的内存大小
size_t bluestore_blob_t::get_csum_value_size() const 
{
  return Checksummer::get_csum_value_size(csum_type);
}

void bluestore_blob_t::dump(Formatter *f) const
{
  f->open_array_section("extents");
  for (auto& p : extents) {
    f->dump_object("extent", p);
  }
  f->close_section();
  f->dump_unsigned("compressed_length_original", compressed_length_orig);
  f->dump_unsigned("compressed_length", compressed_length);
  f->dump_unsigned("flags", flags);
  f->dump_unsigned("csum_type", csum_type);
  f->dump_unsigned("csum_chunk_order", csum_chunk_order);
  f->open_array_section("csum_data");
  size_t n = get_csum_count();
  for (unsigned i = 0; i < n; ++i)
    f->dump_unsigned("csum", get_csum_item(i));
  f->close_section();
  f->dump_unsigned("unused", unused);
}

void bluestore_blob_t::generate_test_instances(list<bluestore_blob_t*>& ls)
{
  ls.push_back(new bluestore_blob_t);
  ls.push_back(new bluestore_blob_t(0));
  ls.push_back(new bluestore_blob_t);
  ls.back()->extents.push_back(bluestore_pextent_t(111, 222));
  ls.push_back(new bluestore_blob_t);
  ls.back()->init_csum(Checksummer::CSUM_XXHASH32, 16, 65536);
  ls.back()->csum_data = buffer::claim_malloc(4, strdup("abcd"));
  ls.back()->extents.emplace_back(bluestore_pextent_t(0x40100000, 0x10000));
  ls.back()->extents.emplace_back(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x1000));
  ls.back()->extents.emplace_back(bluestore_pextent_t(0x40120000, 0x10000));
  ls.back()->add_unused(0, 3);
  ls.back()->add_unused(8, 8);
}

ostream& operator<<(ostream& out, const bluestore_blob_t& o)
{
  out << "blob(" << o.extents;
  if (o.is_compressed()) {
    out << " clen 0x" << std::hex
	<< o.compressed_length_orig
	<< " -> 0x"
	<< o.compressed_length
	<< std::dec;
  }
  if (o.flags) {
    out << " " << o.get_flags_string();
  }
  if (o.csum_type) {
    out << " " << Checksummer::get_csum_type_string(o.csum_type)
	<< "/0x" << std::hex << (1ull << o.csum_chunk_order) << std::dec;
  }
  if (o.has_unused())
    out << " unused=0x" << std::hex << o.unused << std::dec;
  out << ")";
  return out;
}

//计算checksum
void bluestore_blob_t::calc_csum(uint64_t b_off, const bufferlist& bl)
{
  switch (csum_type) {
  case Checksummer::CSUM_XXHASH32:
    Checksummer::calculate<Checksummer::xxhash32>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;
  case Checksummer::CSUM_XXHASH64:
    Checksummer::calculate<Checksummer::xxhash64>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;;
  case Checksummer::CSUM_CRC32C:
    Checksummer::calculate<Checksummer::crc32c>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;
  case Checksummer::CSUM_CRC32C_16:
    Checksummer::calculate<Checksummer::crc32c_16>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;
  case Checksummer::CSUM_CRC32C_8:
    Checksummer::calculate<Checksummer::crc32c_8>(
      get_csum_chunk_size(), b_off, bl.length(), bl, &csum_data);
    break;
  }
}

//校验checksum
int bluestore_blob_t::verify_csum(uint64_t b_off, const bufferlist& bl,
				  int* b_bad_off, uint64_t *bad_csum) const
{
  int r = 0;

  *b_bad_off = -1;
  switch (csum_type) {
  case Checksummer::CSUM_NONE:
    break;
  case Checksummer::CSUM_XXHASH32:
    *b_bad_off = Checksummer::verify<Checksummer::xxhash32>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  case Checksummer::CSUM_XXHASH64:
    *b_bad_off = Checksummer::verify<Checksummer::xxhash64>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  case Checksummer::CSUM_CRC32C:
    *b_bad_off = Checksummer::verify<Checksummer::crc32c>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  case Checksummer::CSUM_CRC32C_16:
    *b_bad_off = Checksummer::verify<Checksummer::crc32c_16>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  case Checksummer::CSUM_CRC32C_8:
    *b_bad_off = Checksummer::verify<Checksummer::crc32c_8>(
      get_csum_chunk_size(), b_off, bl.length(), bl, csum_data, bad_csum);
    break;
  default:
    r = -EOPNOTSUPP;
    break;
  }

  if (r < 0)
    return r;
  else if (*b_bad_off >= 0)
    return -1; // bad checksum
  else
    return 0;
}

// bluestore_shared_blob_t

void bluestore_shared_blob_t::dump(Formatter *f) const
{
  f->dump_int("sbid", sbid);
  f->dump_object("ref_map", ref_map);
}

void bluestore_shared_blob_t::generate_test_instances(
  list<bluestore_shared_blob_t*>& ls)
{
  ls.push_back(new bluestore_shared_blob_t(1));
}

ostream& operator<<(ostream& out, const bluestore_shared_blob_t& sb)
{
  out << " sbid 0x" << std::hex << sb.sbid << std::dec;
  out << " ref_map(" << sb.ref_map << ")";
  return out;
}

// bluestore_onode_t

void bluestore_onode_t::shard_info::dump(Formatter *f) const
{
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("bytes", bytes);
}

ostream& operator<<(ostream& out, const bluestore_onode_t::shard_info& si)
{
  return out << std::hex << "0x" << si.offset << "(0x" << si.bytes << " bytes"
	     << std::dec << ")";
}

void bluestore_onode_t::dump(Formatter *f) const
{
  f->dump_unsigned("nid", nid);
  f->dump_unsigned("size", size);
  f->open_object_section("attrs");
  for (auto p = attrs.begin(); p != attrs.end(); ++p) {
    f->open_object_section("attr");
    f->dump_string("name", p->first.c_str());  // it's not quite std::string
    f->dump_unsigned("len", p->second.length());
    f->close_section();
  }
  f->close_section();
  f->dump_string("flags", get_flags_string());
  f->open_array_section("extent_map_shards");
  for (auto si : extent_map_shards) {
    f->dump_object("shard", si);
  }
  f->close_section();
  f->dump_unsigned("expected_object_size", expected_object_size);
  f->dump_unsigned("expected_write_size", expected_write_size);
  f->dump_unsigned("alloc_hint_flags", alloc_hint_flags);
}

void bluestore_onode_t::generate_test_instances(list<bluestore_onode_t*>& o)
{
  o.push_back(new bluestore_onode_t());
  // FIXME
}

// bluestore_deferred_op_t

void bluestore_deferred_op_t::dump(Formatter *f) const
{
  f->dump_unsigned("op", (int)op);
  f->dump_unsigned("data_len", data.length());
  f->open_array_section("extents");
  for (auto& e : extents) {
    f->dump_object("extent", e);
  }
  f->close_section();
}

void bluestore_deferred_op_t::generate_test_instances(list<bluestore_deferred_op_t*>& o)
{
  o.push_back(new bluestore_deferred_op_t);
  o.push_back(new bluestore_deferred_op_t);
  o.back()->op = OP_WRITE;
  o.back()->extents.push_back(bluestore_pextent_t(1, 2));
  o.back()->extents.push_back(bluestore_pextent_t(100, 5));
  o.back()->data.append("my data");
}

void bluestore_deferred_transaction_t::dump(Formatter *f) const
{
  f->dump_unsigned("seq", seq);
  f->open_array_section("ops");
  for (list<bluestore_deferred_op_t>::const_iterator p = ops.begin(); p != ops.end(); ++p) {
    f->dump_object("op", *p);
  }
  f->close_section();

  f->open_array_section("released extents");
  for (interval_set<uint64_t>::const_iterator p = released.begin(); p != released.end(); ++p) {
    f->open_object_section("extent");
    f->dump_unsigned("offset", p.get_start());
    f->dump_unsigned("length", p.get_len());
    f->close_section();
  }
  f->close_section();
}

void bluestore_deferred_transaction_t::generate_test_instances(list<bluestore_deferred_transaction_t*>& o)
{
  o.push_back(new bluestore_deferred_transaction_t());
  o.push_back(new bluestore_deferred_transaction_t());
  o.back()->seq = 123;
  o.back()->ops.push_back(bluestore_deferred_op_t());
  o.back()->ops.push_back(bluestore_deferred_op_t());
  o.back()->ops.back().op = bluestore_deferred_op_t::OP_WRITE;
  o.back()->ops.back().extents.push_back(bluestore_pextent_t(1,7));
  o.back()->ops.back().data.append("foodata");
}

void bluestore_compression_header_t::dump(Formatter *f) const
{
  f->dump_unsigned("type", type);
  f->dump_unsigned("length", length);
}

void bluestore_compression_header_t::generate_test_instances(
  list<bluestore_compression_header_t*>& o)
{
  o.push_back(new bluestore_compression_header_t);
  o.push_back(new bluestore_compression_header_t(1));
  o.back()->length = 1234;
}
