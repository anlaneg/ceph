// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in-memory allocator.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 *
 */

#include "BitAllocator.h"

#include "BitMapAllocator.h"
#include "bluestore_types.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "bitmapalloc:"


BitMapAllocator::BitMapAllocator(int64_t device_size, int64_t block_size)
{
  assert(ISP2(block_size));
  if (!ISP2(block_size)) {//如果块大小不是2的n次方，报错
    derr << __func__ << " block_size " << block_size
         << " not power of 2 aligned!"
         << dendl;
    return;
  }

  int64_t zone_size_blks = g_conf->bluestore_bitmapallocator_blocks_per_zone;
  assert(ISP2(zone_size_blks));
  if (!ISP2(zone_size_blks)) {//如果zone大小不是2的n次
    derr << __func__ << " zone_size " << zone_size_blks
         << " not power of 2 aligned!"
         << dendl;
    return;
  }

  int64_t span_size = g_conf->bluestore_bitmapallocator_span_size;
  assert(ISP2(span_size));
  if (!ISP2(span_size)) {//如果span大小不是2的n次方（仅检查配置）
    derr << __func__ << " span_size " << span_size
         << " not power of 2 aligned!"
         << dendl;
    return;
  }

  m_block_size = block_size;
  m_bit_alloc = new BitAllocator(device_size / block_size,
        zone_size_blks, CONCURRENT, true);
  assert(m_bit_alloc);
  if (!m_bit_alloc) {//没有申请成功
    derr << __func__ << " Unable to intialize Bit Allocator" << dendl;
  }
  dout(10) << __func__ << " instance " << (uint64_t) this
           << " size 0x" << std::hex << device_size << std::dec
           << dendl;
}

BitMapAllocator::~BitMapAllocator()
{
  delete m_bit_alloc;
}

void BitMapAllocator::insert_free(uint64_t off, uint64_t len)
{
  dout(20) << __func__ << " instance " << (uint64_t) this
           << " off 0x" << std::hex << off
           << " len 0x" << len << std::dec
           << dendl;

  assert(!(off % m_block_size));
  assert(!(len % m_block_size));

  m_bit_alloc->free_blocks(off / m_block_size,//起始块
             len / m_block_size);//块数量
}

int BitMapAllocator::reserve(uint64_t need)
{
  int nblks = need / m_block_size; // apply floor
  assert(!(need % m_block_size));
  dout(10) << __func__ << " instance " << (uint64_t) this
           << " num_used " << m_bit_alloc->get_used_blocks()
           << " total " << m_bit_alloc->total_blocks()
           << dendl;

  if (!m_bit_alloc->reserve_blocks(nblks)) {
    return -ENOSPC;
  }
  return 0;
}

void BitMapAllocator::unreserve(uint64_t unused)
{
  int nblks = unused / m_block_size;
  assert(!(unused % m_block_size));

  dout(10) << __func__ << " instance " << (uint64_t) this
           << " unused " << nblks
           << " num used " << m_bit_alloc->get_used_blocks()
           << " total " << m_bit_alloc->total_blocks()
           << dendl;

  m_bit_alloc->unreserve_blocks(nblks);
}

int BitMapAllocator::allocate(
  uint64_t want_size, uint64_t alloc_unit, int64_t hint,
  uint64_t *offset, uint32_t *length)
{
  if (want_size == 0)
    return 0;

  assert(!(alloc_unit % m_block_size));
  int64_t nblks = (want_size + m_block_size - 1) / m_block_size;

  assert(alloc_unit);

  int64_t start_blk = 0;
  int64_t count = 0;

  dout(10) << __func__ << " instance " << (uint64_t) this
           << " want_size 0x" << std::hex << want_size
           << " alloc_unit 0x" << alloc_unit
           << " hint 0x" << hint << std::dec
           << dendl;

  *offset = 0;
  *length = 0;

  count = m_bit_alloc->alloc_blocks_res(nblks, hint / m_block_size, &start_blk);
  if (count == 0) {
    return -ENOSPC;
  }
  *offset = start_blk * m_block_size;
  *length = count * m_block_size;

  dout(20) << __func__ <<" instance "<< (uint64_t) this
           << " offset 0x" << std::hex << *offset
           << " length 0x" << *length << std::dec
           << dendl;

  return 0;
}

int BitMapAllocator::alloc_extents(
  uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
  int64_t hint, mempool::bluestore_alloc::vector<AllocExtent> *extents, int *count)
{
  assert(!(alloc_unit % m_block_size));
  assert(alloc_unit);

  assert(!max_alloc_size || max_alloc_size >= alloc_unit);

  dout(10) << __func__ <<" instance "<< (uint64_t) this
     << " want_size " << want_size
     << " alloc_unit " << alloc_unit
     << " hint " << hint
     << dendl;

  if (alloc_unit > (uint64_t) m_block_size) {
    return alloc_extents_cont(want_size, alloc_unit, 
                              max_alloc_size, hint / m_block_size, extents, count);     
  } else {
    return alloc_extents_dis(want_size, alloc_unit,
                             max_alloc_size, hint / m_block_size, extents, count); 
  }
}

/*
 * Allocator extents with min alloc unit > bitmap block size.
 */
int BitMapAllocator::alloc_extents_cont(
  uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size, int64_t hint,
  mempool::bluestore_alloc::vector<AllocExtent> *extents, int *count)
{
  *count = 0;
  assert(alloc_unit);
  assert(!(alloc_unit % m_block_size));
  assert(!(max_alloc_size % m_block_size));

  int64_t nblks = (want_size + m_block_size - 1) / m_block_size;
  int64_t start_blk = 0;
  int64_t need_blks = nblks;
  int64_t max_blks = max_alloc_size / m_block_size;

  ExtentList block_list = ExtentList(extents, m_block_size, max_alloc_size);

  while (need_blks > 0) {
    int64_t count = 0;
    count = m_bit_alloc->alloc_blocks_res(
      (max_blks && need_blks > max_blks) ? max_blks : need_blks, hint, &start_blk);
    if (count == 0) {
      break;
    }
    dout(30) << __func__ <<" instance "<< (uint64_t) this
      << " offset " << start_blk << " length " << count << dendl;
    need_blks -= count;
    block_list.add_extents(start_blk, count);
  }

  if (need_blks > 0) {
    m_bit_alloc->free_blocks_dis(nblks - need_blks, &block_list);
    return -ENOSPC;
  }
  *count = block_list.get_extent_count();

  return 0;
}

int BitMapAllocator::alloc_extents_dis(
  uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
  int64_t hint, mempool::bluestore_alloc::vector<AllocExtent> *extents, int *count)
{
  ExtentList block_list = ExtentList(extents, m_block_size, max_alloc_size);
  int64_t nblks = (want_size + m_block_size - 1) / m_block_size;
  int64_t num = 0;
  *count = 0;

  num = m_bit_alloc->alloc_blocks_dis_res(nblks, hint, &block_list);
  if (num < nblks) {
    m_bit_alloc->free_blocks_dis(num, &block_list);
    return -ENOSPC;
  }
  *count = block_list.get_extent_count();

  return 0;
}

int BitMapAllocator::release(
  uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(m_lock);
  dout(10) << __func__ << " 0x"
           << std::hex << offset << "~" << length << std::dec
           << dendl;
  insert_free(offset, length);
  return 0;
}

uint64_t BitMapAllocator::get_free()
{
  assert(m_bit_alloc->total_blocks() >= m_bit_alloc->get_used_blocks());
  return ((
    m_bit_alloc->total_blocks() - m_bit_alloc->get_used_blocks()) *
    m_block_size);
}

void BitMapAllocator::dump()
{
  std::lock_guard<std::mutex> l(m_lock);
  dout(0) << __func__ << " instance " << this << dendl;
  m_bit_alloc->dump();
}

void BitMapAllocator::init_add_free(uint64_t offset, uint64_t length)
{
  dout(10) << __func__ << " instance " << (uint64_t) this
           << " offset 0x" << std::hex << offset
           << " length 0x" << length << std::dec
           << dendl;
  uint64_t size = m_bit_alloc->size() * m_block_size;

  uint64_t offset_adj = ROUND_UP_TO(offset, m_block_size);//调整offset
  uint64_t length_adj = ((length - (offset_adj - offset)) / //获得调整后的len
                         m_block_size) * m_block_size;

  if ((offset_adj + length_adj) > size) {//检查是否足够分配
    assert(((offset_adj + length_adj) - m_block_size) < size);
    length_adj = size - offset_adj;//缩小
  }

  insert_free(offset_adj, length_adj);
}

void BitMapAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  dout(10) << __func__ << " instance " << (uint64_t) this
           << " offset 0x" << std::hex << offset
           << " length 0x" << length << std::dec
           << dendl;

  // we use the same adjustment/alignment that init_add_free does
  // above so that we can yank back some of the space.
  uint64_t offset_adj = ROUND_UP_TO(offset, m_block_size);
  uint64_t length_adj = ((length - (offset_adj - offset)) /
                         m_block_size) * m_block_size;

  assert(!(offset_adj % m_block_size));
  assert(!(length_adj % m_block_size));

  int64_t first_blk = offset_adj / m_block_size;
  int64_t count = length_adj / m_block_size;

  if (count)
    m_bit_alloc->set_blocks_used(first_blk, count);
}


void BitMapAllocator::shutdown()
{
  dout(10) << __func__ << " instance " << (uint64_t) this << dendl;
  m_bit_alloc->shutdown();
}

