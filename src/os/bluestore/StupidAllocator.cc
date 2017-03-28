// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "StupidAllocator.h"
#include "bluestore_types.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "stupidalloc "

StupidAllocator::StupidAllocator(CephContext* cct)
  : cct(cct), num_free(0),
    num_reserved(0),
    free(10),//free大小被硬编码成10，目的减少b树的大小
    last_alloc(0)
{
}

StupidAllocator::~StupidAllocator()
{
}

//根据长度选择一个树，不明白，为何命名为_choose_free_tree ？更直白些，或者_choose_btree也行啊！
unsigned StupidAllocator::_choose_bin(uint64_t orig_len)
{
  uint64_t len = orig_len / cct->_conf->bdev_block_size;
  int bin = std::min((int)cbits(len), (int)free.size() - 1);
  dout(30) << __func__ << " len 0x" << std::hex << orig_len << std::dec
	   << " -> " << bin << dendl;
  return bin;
}

//实际空闲范围的插入
void StupidAllocator::_insert_free(uint64_t off, uint64_t len)
{
  unsigned bin = _choose_bin(len);
  dout(30) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
	   << " in bin " << bin << dendl;
  while (true) {
    free[bin].insert(off, len, &off, &len);//先向bin对应的树里插入
    unsigned newbin = _choose_bin(len);//用返回的len检查，是否要换一棵树
    if (newbin == bin)//不需要，插入完成
      break;
    //需要换一棵树插入，先从旧的树里删除掉，再向新选的树里插入。
    dout(30) << __func__ << " promoting 0x" << std::hex << off << "~" << len
	     << std::dec << " to bin " << newbin << dendl;
    free[bin].erase(off, len);
    bin = newbin;
  }
}

//预留机制是一个提前检查的办法，如果预留可以成功，说明空闲量还足以分配。
//但预留成功不是可以分配成功的标志。属于一种优化
int StupidAllocator::reserve(uint64_t need)//增加预留
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " need 0x" << std::hex << need
	   << " num_free 0x" << num_free
	   << " num_reserved 0x" << num_reserved << std::dec << dendl;
  if ((int64_t)need > num_free - num_reserved)//检查是否足够预留，如果不能，返失败，否则增加预留数
    return -ENOSPC;
  num_reserved += need;
  return 0;
}

void StupidAllocator::unreserve(uint64_t unused)//取消预留
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " unused 0x" << std::hex << unused
	   << " num_free 0x" << num_free
	   << " num_reserved 0x" << num_reserved << std::dec << dendl;
  assert(num_reserved >= (int64_t)unused);//这里不是检查，而是直接断言
  num_reserved -= unused;
}

/// return the effective length of the extent if we align to alloc_unit
//返回p接alloc_unit对齐后的实际大小
static uint64_t aligned_len(btree_interval_set<uint64_t>::iterator p,
			    uint64_t alloc_unit)
{
  uint64_t skew = p.get_start() % alloc_unit;
  if (skew)
    skew = alloc_unit - skew;
  if (skew > p.get_len())
    return 0;
  else
    return p.get_len() - skew;
}

int64_t StupidAllocator::allocate_int(
  uint64_t want_size, uint64_t alloc_unit, int64_t hint,
  uint64_t *offset, uint32_t *length)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " want_size 0x" << std::hex << want_size
	   << " alloc_unit 0x" << alloc_unit
	   << " hint 0x" << hint << std::dec
	   << dendl;
  uint64_t want = MAX(alloc_unit, want_size);//检查要多大
  int bin = _choose_bin(want);
  int orig_bin = bin;

  auto p = free[0].begin();

  if (!hint)
    hint = last_alloc;

  // search up (from hint)
  if (hint) {
    for (bin = orig_bin; bin < (int)free.size(); ++bin) {
      p = free[bin].lower_bound(hint);
      while (p != free[bin].end()) {//尝试着从上次分配的位置进行分配
	if (aligned_len(p, alloc_unit) >= want_size) {//对齐后，大小足够，搞定
	  goto found;
	}
	++p;//大小不够，换下一个。进行检查"按行遍历“--这个就比较慢了
      }
    }
  }

  // search up (from origin, and skip searched extents by hint)
  //没有找到，尝试lower_bound(hint)的前半部分（当hint不存在时，采用的是全查找，这个就慢的不可言语了）
  for (bin = orig_bin; bin < (int)free.size(); ++bin) {
    p = free[bin].begin();
    auto end = hint ? free[bin].lower_bound(hint) : free[bin].end();
    while (p != end) {
      if (aligned_len(p, alloc_unit) >= want_size) {
	goto found;
      }
      ++p;
    }
  }

  // search down (hint)
  //没有找到，尝试orig_bin之前的那一部分（采用hint)
  if (hint) {
    for (bin = orig_bin; bin >= 0; --bin) {
      p = free[bin].lower_bound(hint);
      while (p != free[bin].end()) {
	if (aligned_len(p, alloc_unit) >= alloc_unit) {
	  goto found;
	}
	++p;
      }
    }
  }

  // search down (from origin, and skip searched extents by hint)
  //还是没有找到，尝试orig_bin之前的那一部分(尝试hint之前的部分，或者没有hint时的首次查询）
  for (bin = orig_bin; bin >= 0; --bin) {
    p = free[bin].begin();
    auto end = hint ? free[bin].lower_bound(hint) : free[bin].end();
    while (p != end) {
      if (aligned_len(p, alloc_unit) >= alloc_unit) {
	goto found;
      }
      ++p;
    }
  }

  //这下终于可以死心了
  return -ENOSPC;

 found:
  uint64_t skew = p.get_start() % alloc_unit;
  if (skew)
    skew = alloc_unit - skew;
  *offset = p.get_start() + skew;//清除边角料
  *length = MIN(MAX(alloc_unit, want_size), p.get_len() - skew);//清除后的长度
  if (cct->_conf->bluestore_debug_small_allocations) {
    uint64_t max =
      alloc_unit * (rand() % cct->_conf->bluestore_debug_small_allocations);
    if (max && *length > max) {
      dout(10) << __func__ << " shortening allocation of 0x" << std::hex
	       << *length << " -> 0x"
	       << max << " due to debug_small_allocations" << std::dec << dendl;
      *length = max;
    }
  }
  dout(30) << __func__ << " got 0x" << std::hex << *offset << "~" << *length
	   << " from bin " << std::dec << bin << dendl;

  free[bin].erase(*offset, *length);//自free中删除掉 "删除掉的是不含边角料的部分“
  //如你所愿，因为上面的erase没有全部清干净，所以出现了下面恶心的处理，需要将边角料换棵树来存放
  uint64_t off, len;
  if (*offset && free[bin].contains(*offset - skew - 1, &off, &len)) {
    int newbin = _choose_bin(len);
    if (newbin != bin) {//需要换棵树 :-P
      dout(30) << __func__ << " demoting 0x" << std::hex << off << "~" << len
	       << std::dec << " to bin " << newbin << dendl;
      free[bin].erase(off, len);//删除
      _insert_free(off, len);//加上
    }
  }
  //检查剩下的那部分，是否需要换棵树
  if (free[bin].contains(*offset + *length, &off, &len)) {
    int newbin = _choose_bin(len);
    if (newbin != bin) {//需要换棵树 :-P
      dout(30) << __func__ << " demoting 0x" << std::hex << off << "~" << len
	       << std::dec << " to bin " << newbin << dendl;
      free[bin].erase(off, len);//删除
      _insert_free(off, len);//加上
    }
  }

  //让我们长长的舒一口气，终于走过了换树流程
  num_free -= *length;//减少库存
  num_reserved -= *length;//满足了清求，删除预留
  assert(num_free >= 0);
  assert(num_reserved >= 0);
  last_alloc = *offset + *length;//更新此数值，防止一会有人不传hint
  return 0;
}

int64_t StupidAllocator::allocate(
  uint64_t want_size,//需要的大小
  uint64_t alloc_unit,//块大小
  uint64_t max_alloc_size,//最大需要的大小
  int64_t hint,//最后一片的结束位置（暗示）
  mempool::bluestore_alloc::vector<AllocExtent> *extents)//出参，申请后填充
{
  uint64_t allocated_size = 0;//已申请了多少字节
  uint64_t offset = 0;
  uint32_t length = 0;
  int res = 0;

  if (max_alloc_size == 0) {
    max_alloc_size = want_size;
  }

  ExtentList block_list = ExtentList(extents, 1, max_alloc_size);

  while (allocated_size < want_size) {
    res = allocate_int(MIN(max_alloc_size, (want_size - allocated_size)),
       alloc_unit, hint, &offset, &length);
    if (res != 0) {
      /*
       * Allocation failed.
       */
      break;
    }
    block_list.add_extents(offset, length);//将刚申请的加入
    allocated_size += length;//更新已申请到的
    hint = offset + length;//更新暗示
  }

  if (allocated_size == 0) {
    return -ENOSPC;
  }
  return allocated_size;
}

//释放时，存入uncommitted的map中
void StupidAllocator::release(
  uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  _insert_free(offset, length);
  num_free += length;
}

uint64_t StupidAllocator::get_free()
{
  std::lock_guard<std::mutex> l(lock);
  return num_free;
}

void StupidAllocator::dump()
{
  std::lock_guard<std::mutex> l(lock);
  for (unsigned bin = 0; bin < free.size(); ++bin) {
    dout(0) << __func__ << " free bin " << bin << ": "
	    << free[bin].num_intervals() << " extents" << dendl;
    for (auto p = free[bin].begin();
	 p != free[bin].end();
	 ++p) {
      dout(0) << __func__ << "  0x" << std::hex << p.get_start() << "~"
	      << p.get_len() << std::dec << dendl;
    }
  }
}

//给free tree中增加offset,length
void StupidAllocator::init_add_free(uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  _insert_free(offset, length);
  num_free += length;//增加最free数量
}

//自tree中扣除offet,length 原理和申请一样，只不过这次是悄悄的拿走
void StupidAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  btree_interval_set<uint64_t> rm;
  rm.insert(offset, length);
  for (unsigned i = 0; i < free.size() && !rm.empty(); ++i) {
    btree_interval_set<uint64_t> overlap;
    overlap.intersection_of(rm, free[i]);
    if (!overlap.empty()) {
      dout(20) << __func__ << " bin " << i << " rm 0x" << std::hex << overlap
	       << std::dec << dendl;
      free[i].subtract(overlap);
      rm.subtract(overlap);
    }
  }
  assert(rm.empty());
  num_free -= length;
  assert(num_free >= 0);
}


void StupidAllocator::shutdown()
{
  dout(1) << __func__ << dendl;
}

