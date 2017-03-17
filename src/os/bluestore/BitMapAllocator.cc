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

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "bitmapalloc:"

////device_size 总大小，block_size 每个块大小
BitMapAllocator::BitMapAllocator(CephContext* cct, int64_t device_size,
				 int64_t block_size)
  : cct(cct)
{
  assert(ISP2(block_size));
  if (!ISP2(block_size)) {//如果块大小不是2的n次方，报错
    derr << __func__ << " block_size " << block_size
         << " not power of 2 aligned!"
         << dendl;
    return;
  }

  int64_t zone_size_blks = cct->_conf->bluestore_bitmapallocator_blocks_per_zone;
  assert(ISP2(zone_size_blks));
  if (!ISP2(zone_size_blks)) {//如果zone大小不是2的n次
    derr << __func__ << " zone_size " << zone_size_blks
         << " not power of 2 aligned!"
         << dendl;
    return;
  }

  int64_t span_size = cct->_conf->bluestore_bitmapallocator_span_size;
  assert(ISP2(span_size));
  if (!ISP2(span_size)) {//如果span大小不是2的n次方（仅检查配置）
    derr << __func__ << " span_size " << span_size
         << " not power of 2 aligned!"
         << dendl;
    return;
  }

  m_block_size = block_size;
  m_bit_alloc = new BitAllocator(cct, device_size / block_size,
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
  delete m_bit_alloc;//释放
}

//释放从off起始，长度为len的区域
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

//预留need字节
int BitMapAllocator::reserve(uint64_t need)
{
  //换算成块，并按块进行预留
  int nblks = need / m_block_size; // apply floor
  assert(!(need % m_block_size));//need必须是整数倍
  dout(10) << __func__ << " instance " << (uint64_t) this
           << " num_used " << m_bit_alloc->get_used_blocks()
           << " total " << m_bit_alloc->total_blocks()
           << dendl;

  if (!m_bit_alloc->reserve_blocks(nblks)) {
    return -ENOSPC;
  }
  return 0;
}

//释放预留长度
void BitMapAllocator::unreserve(uint64_t unused)
{
  int nblks = unused / m_block_size;
  assert(!(unused % m_block_size));//unused必须是整数倍

  dout(10) << __func__ << " instance " << (uint64_t) this
           << " unused " << nblks
           << " num used " << m_bit_alloc->get_used_blocks()
           << " total " << m_bit_alloc->total_blocks()
           << dendl;

  m_bit_alloc->unreserve_blocks(nblks);
}

int64_t BitMapAllocator::allocate(
  uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
  int64_t hint, mempool::bluestore_alloc::vector<AllocExtent> *extents)
{

  assert(!(alloc_unit % m_block_size));//最小块必须是block的整数倍
  assert(alloc_unit);//不能是0

  assert(!max_alloc_size || max_alloc_size >= alloc_unit);//最大块不能为0，且必须比期待的最小块大

  dout(10) << __func__ <<" instance "<< (uint64_t) this
     << " want_size " << want_size
     << " alloc_unit " << alloc_unit
     << " hint " << hint
     << dendl;

  //请求want_size字节，最小需要alloc_unit/m_block_size块，最大申请max_alloc_size，hint更新为块编号
  return allocate_dis(want_size, alloc_unit / m_block_size,
                      max_alloc_size, hint / m_block_size, extents);
}

//磁盘空间申请
int64_t BitMapAllocator::allocate_dis(
  uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
  int64_t hint, mempool::bluestore_alloc::vector<AllocExtent> *extents)
{
  //构造list
  ExtentList block_list = ExtentList(extents, m_block_size, max_alloc_size);
  int64_t nblks = (want_size + m_block_size - 1) / m_block_size;//将want_size规范为需要多少的块。
  int64_t num = 0;

  //申请nblks个块，申请的单元大小为alloc_unit,&block_list用于存放结果集
  num = m_bit_alloc->alloc_blocks_dis_res(nblks, alloc_unit, hint, &block_list);
  if (num == 0) {
    return -ENOSPC;
  }

  //返回申请了多少字节
  return num * m_block_size;
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

