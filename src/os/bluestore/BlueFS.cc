// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "boost/algorithm/string.hpp" 
#include "BlueFS.h"

#include "common/debug.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "BlockDevice.h"
#include "Allocator.h"
#include "include/assert.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluefs
#undef dout_prefix
#define dout_prefix *_dout << "bluefs "

MEMPOOL_DEFINE_OBJECT_FACTORY(BlueFS::File, bluefs_file, bluefs);
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueFS::Dir, bluefs_dir, bluefs);
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueFS::FileWriter, bluefs_file_writer, bluefs);
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueFS::FileReaderBuffer,
			      bluefs_file_reader_buffer, bluefs);
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueFS::FileReader, bluefs_file_reader, bluefs);
MEMPOOL_DEFINE_OBJECT_FACTORY(BlueFS::FileLock, bluefs_file_lock, bluefs);


BlueFS::BlueFS(CephContext* cct)
  : cct(cct),
    bdev(MAX_BDEV),
    ioc(MAX_BDEV),
    block_all(MAX_BDEV),
    block_total(MAX_BDEV, 0)
{
}

BlueFS::~BlueFS()
{
  for (auto p : ioc) {
    if (p)
      p->aio_wait();
  }
  for (auto p : bdev) {
    if (p) {
      p->close();
      delete p;
    }
  }
  for (auto p : ioc) {
    delete p;
  }
}

void BlueFS::_init_logger()
{
  PerfCountersBuilder b(cct, "BlueFS",
                        l_bluefs_first, l_bluefs_last);
  b.add_u64_counter(l_bluefs_gift_bytes, "gift_bytes",
		    "Bytes gifted from BlueStore");
  b.add_u64_counter(l_bluefs_reclaim_bytes, "reclaim_bytes",
		    "Bytes reclaimed by BlueStore");
  b.add_u64(l_bluefs_db_total_bytes, "db_total_bytes",
	    "Total bytes (main db device)");
  b.add_u64(l_bluefs_db_free_bytes, "db_free_bytes",
	    "Free bytes (main db device)");
  b.add_u64(l_bluefs_wal_total_bytes, "wal_total_bytes",
	    "Total bytes (wal device)");
  b.add_u64(l_bluefs_wal_free_bytes, "wal_free_bytes",
	    "Free bytes (wal device)");
  b.add_u64(l_bluefs_slow_total_bytes, "slow_total_bytes",
	    "Total bytes (slow device)");
  b.add_u64(l_bluefs_slow_free_bytes, "slow_free_bytes",
	    "Free bytes (slow device)");
  b.add_u64(l_bluefs_num_files, "num_files", "File count");
  b.add_u64(l_bluefs_log_bytes, "log_bytes", "Size of the metadata log");
  b.add_u64_counter(l_bluefs_log_compactions, "log_compactions",
		    "Compactions of the metadata log");
  b.add_u64_counter(l_bluefs_logged_bytes, "logged_bytes",
		    "Bytes written to the metadata log");
  b.add_u64_counter(l_bluefs_files_written_wal, "files_written_wal",
		    "Files written to WAL");
  b.add_u64_counter(l_bluefs_files_written_sst, "files_written_sst",
		    "Files written to SSTs");
  b.add_u64_counter(l_bluefs_bytes_written_wal, "bytes_written_wal",
		    "Bytes written to WAL");
  b.add_u64_counter(l_bluefs_bytes_written_sst, "bytes_written_sst",
		    "Bytes written to SSTs");
  logger = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
}

void BlueFS::_shutdown_logger()
{
  cct->get_perfcounters_collection()->remove(logger);
  delete logger;
}

void BlueFS::_update_logger_stats()
{
  // we must be holding the lock
  logger->set(l_bluefs_num_files, file_map.size());
  logger->set(l_bluefs_log_bytes, log_writer->file->fnode.size);

  if (alloc[BDEV_WAL]) {
    logger->set(l_bluefs_wal_total_bytes, block_total[BDEV_WAL]);
    logger->set(l_bluefs_wal_free_bytes, alloc[BDEV_WAL]->get_free());
  }
  if (alloc[BDEV_DB]) {
    logger->set(l_bluefs_db_total_bytes, block_total[BDEV_DB]);
    logger->set(l_bluefs_db_free_bytes, alloc[BDEV_DB]->get_free());
  }
  if (alloc[BDEV_SLOW]) {
    logger->set(l_bluefs_slow_total_bytes, block_total[BDEV_SLOW]);
    logger->set(l_bluefs_slow_free_bytes, alloc[BDEV_SLOW]->get_free());
  }
}

int BlueFS::add_block_device(unsigned id, string path)
{
  dout(10) << __func__ << " bdev " << id << " path " << path << dendl;
  assert(id < bdev.size());
  assert(bdev[id] == NULL);
  BlockDevice *b = BlockDevice::create(cct, path, NULL, NULL);//不aio设置回调
  int r = b->open(path);//对kernelblock而言，打开操作句柄
  if (r < 0) {
    delete b;
    return r;
  }
  dout(1) << __func__ << " bdev " << id << " path " << path
	  << " size " << pretty_si_t(b->get_size()) << "B" << dendl;
  bdev[id] = b;
  ioc[id] = new IOContext(cct, NULL);
  return 0;
}

bool BlueFS::bdev_support_label(unsigned id)
{
  assert(id < bdev.size());
  assert(bdev[id]);
  return bdev[id]->supported_bdev_label();
}

uint64_t BlueFS::get_block_device_size(unsigned id)
{
  if (id < bdev.size() && bdev[id])
    return bdev[id]->get_size();
  return 0;
}

void BlueFS::add_block_extent(unsigned id, uint64_t offset, uint64_t length)//添加块，并设置其范围
{
  std::unique_lock<std::mutex> l(lock);
  dout(1) << __func__ << " bdev " << id
          << " 0x" << std::hex << offset << "~" << length << std::dec
	  << dendl;
  assert(id < bdev.size());
  assert(bdev[id]);
  assert(bdev[id]->get_size() >= offset + length);
  block_all[id].insert(offset, length);
  block_total[id] += length;

  if (id < alloc.size() && alloc[id]) {
    log_t.op_alloc_add(id, offset, length);
    int r = _flush_and_sync_log(l);
    assert(r == 0);
    alloc[id]->init_add_free(offset, length);
  }

  if (logger)
    logger->inc(l_bluefs_gift_bytes, length);
  dout(10) << __func__ << " done" << dendl;
}

int BlueFS::reclaim_blocks(unsigned id, uint64_t want,
			   AllocExtentVector *extents)
{
  std::unique_lock<std::mutex> l(lock);
  dout(1) << __func__ << " bdev " << id
          << " want 0x" << std::hex << want << std::dec << dendl;
  assert(id < alloc.size());
  assert(alloc[id]);
  int r = alloc[id]->reserve(want);
  assert(r == 0); // caller shouldn't ask for more than they can get
  int64_t got = alloc[id]->allocate(want, cct->_conf->bluefs_alloc_size, 0,
				    extents);
  if (got < (int64_t)want) {
    alloc[id]->unreserve(want - MAX(0, got));
  }
  if (got <= 0) {
    derr << __func__ << " failed to allocate space to return to bluestore"
	 << dendl;
    alloc[id]->dump();
    return got;
  }

  for (auto& p : *extents) {
    block_all[id].erase(p.offset, p.length);
    block_total[id] -= p.length;
    log_t.op_alloc_rm(id, p.offset, p.length);
  }

  r = _flush_and_sync_log(l);
  assert(r == 0);

  if (logger)
    logger->inc(l_bluefs_reclaim_bytes, got);
  dout(1) << __func__ << " bdev " << id << " want 0x" << std::hex << want
	  << " got " << *extents << dendl;
  return 0;
}

uint64_t BlueFS::get_fs_usage()
{
  std::lock_guard<std::mutex> l(lock);
  uint64_t total_bytes = 0;
  for (auto& p : file_map) {
    total_bytes += p.second->fnode.get_allocated();
  }
  return total_bytes;
}

uint64_t BlueFS::get_total(unsigned id)
{
  std::lock_guard<std::mutex> l(lock);
  assert(id < block_all.size());
  return block_total[id];
}

uint64_t BlueFS::get_free(unsigned id)
{
  std::lock_guard<std::mutex> l(lock);
  assert(id < alloc.size());
  return alloc[id]->get_free();
}

void BlueFS::dump_perf_counters(Formatter *f)
{
  f->open_object_section("bluefs_perf_counters");
  logger->dump_formatted(f,0);
  f->close_section();
}


void BlueFS::get_usage(vector<pair<uint64_t,uint64_t>> *usage)
{
  std::lock_guard<std::mutex> l(lock);
  usage->resize(bdev.size());
  for (unsigned id = 0; id < bdev.size(); ++id) {
    if (!bdev[id]) {
      (*usage)[id] = make_pair(0, 0);
      continue;
    }
    (*usage)[id].first = alloc[id]->get_free();
    (*usage)[id].second = block_total[id];
    uint64_t used =
      (block_total[id] - (*usage)[id].first) * 100 / block_total[id];
    dout(10) << __func__ << " bdev " << id
	     << " free " << (*usage)[id].first
	     << " (" << pretty_si_t((*usage)[id].first) << "B)"
	     << " / " << (*usage)[id].second
	     << " (" << pretty_si_t((*usage)[id].second) << "B)"
	     << ", used " << used << "%"
	     << dendl;
  }
}

int BlueFS::get_block_extents(unsigned id, interval_set<uint64_t> *extents)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " bdev " << id << dendl;
  if (id >= block_all.size())
    return -EINVAL;
  *extents = block_all[id];
  return 0;
}

//bluefs 格式化
int BlueFS::mkfs(uuid_d osd_uuid)
{
  std::unique_lock<std::mutex> l(lock);
  dout(1) << __func__
	  << " osd_uuid " << osd_uuid
	  << dendl;

  _init_alloc();
  _init_logger();

  super.version = 1;
  super.block_size = bdev[BDEV_DB]->get_block_size();
  super.osd_uuid = osd_uuid;
  super.uuid.generate_random();
  dout(1) << __func__ << " uuid " << super.uuid << dendl;

  // init log
  FileRef log_file = new File;
  log_file->fnode.ino = 1;//log文件的ino为1
  log_file->fnode.prefer_bdev = BDEV_WAL;
  int r = _allocate(
    log_file->fnode.prefer_bdev,
    cct->_conf->bluefs_max_log_runway,
    &log_file->fnode.extents);//首次为log文件申请磁盘空间
  assert(r == 0);
  log_writer = _create_writer(log_file);

  // initial txn
  log_t.op_init();
  for (unsigned bdev = 0; bdev < MAX_BDEV; ++bdev) {//填充所有块设备的可用段
    interval_set<uint64_t>& p = block_all[bdev];
    if (p.empty())
      continue;
    for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {//记录p设备可用的段
      dout(20) << __func__ << " op_alloc_add " << bdev << " 0x"
               << std::hex << q.get_start() << "~" << q.get_len() << std::dec
               << dendl;
      log_t.op_alloc_add(bdev, q.get_start(), q.get_len());
    }
  }
  _flush_and_sync_log(l);//日志落盘

  // write supers
  super.log_fnode = log_file->fnode;
  _write_super();
  flush_bdev();//数据落盘

  // clean up　//清理
  super = bluefs_super_t();
  _close_writer(log_writer);
  log_writer = NULL;
  block_all.clear();
  block_total.clear();
  _stop_alloc();
  _shutdown_logger();

  dout(10) << __func__ << " success" << dendl;
  return 0;
}

void BlueFS::_init_alloc()
{
  dout(20) << __func__ << dendl;
  alloc.resize(MAX_BDEV);
  pending_release.resize(MAX_BDEV);
  for (unsigned id = 0; id < bdev.size(); ++id) {
    if (!bdev[id]) {
      continue;
    }
    assert(bdev[id]->get_size());
    alloc[id] = Allocator::create(cct, cct->_conf->bluefs_allocator,
				  bdev[id]->get_size(),
				  cct->_conf->bluefs_alloc_size);
    interval_set<uint64_t>& p = block_all[id];
    for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
      alloc[id]->init_add_free(q.get_start(), q.get_len());
    }
  }
}

void BlueFS::_stop_alloc()
{
  dout(20) << __func__ << dendl;
  for (auto p : alloc) {
    if (p != nullptr)  {
      p->shutdown();
      delete p;
    }
  }
  alloc.clear();
}

//文件系统挂载（实现了文件各数据初始化）
int BlueFS::mount()
{
  dout(1) << __func__ << dendl;

  int r = _open_super();//读取super block,并校验super block
  if (r < 0) {
    derr << __func__ << " failed to open super: " << cpp_strerror(r) << dendl;
    goto out;
  }

  block_all.clear();
  block_all.resize(MAX_BDEV);
  block_total.clear();
  block_total.resize(MAX_BDEV, 0);
  _init_alloc();//创建allock,这时，alloc里是空的，原因是block_all里还没有数据

  r = _replay(false);//重演日志
  if (r < 0) {
    derr << __func__ << " failed to replay log: " << cpp_strerror(r) << dendl;
    _stop_alloc();
    goto out;
  }

  // init freelist
  //初始化空闲链
  for (auto& p : file_map) {//遍历每个文件
    dout(30) << __func__ << " noting alloc for " << p.second->fnode << dendl;
    for (auto& q : p.second->fnode.extents) {//针对每个文件，在alloc中移除其占用的空间
      alloc[q.bdev]->init_rm_free(q.offset, q.length);
    }
  }

  // set up the log for future writes
  log_writer = _create_writer(_get_file(1));//创建日志writer
  assert(log_writer->file->fnode.ino == 1);
  log_writer->pos = log_writer->file->fnode.size;//重定向日志位置
  dout(10) << __func__ << " log write pos set to 0x"
           << std::hex << log_writer->pos << std::dec
           << dendl;

  _init_logger();
  return 0;

 out:
  super = bluefs_super_t();
  return r;
}

void BlueFS::umount()
{
  dout(1) << __func__ << dendl;

  sync_metadata();

  _close_writer(log_writer);
  log_writer = NULL;

  _stop_alloc();
  file_map.clear();
  dir_map.clear();
  super = bluefs_super_t();
  log_t.clear();
  _shutdown_logger();
}

int BlueFS::fsck()
{
  std::lock_guard<std::mutex> l(lock);
  dout(1) << __func__ << dendl;
  // hrm, i think we check everything on mount...
  return 0;
}

int BlueFS::_write_super()//完成super块写入
{
  // build superblock
  bufferlist bl;
  ::encode(super, bl);
  uint32_t crc = bl.crc32c(-1);
  ::encode(crc, bl);//写入crc
  dout(10) << __func__ << " super block length(encoded): " << bl.length() << dendl;
  dout(10) << __func__ << " superblock " << super.version << dendl;
  dout(10) << __func__ << " log_fnode " << super.log_fnode << dendl;
  assert(bl.length() <= get_super_length());
  bl.append_zero(get_super_length() - bl.length());

  bdev[BDEV_DB]->aio_write(get_super_offset(), bl, ioc[BDEV_DB], false);//super block写入到4096开始位置
  bdev[BDEV_DB]->aio_submit(ioc[BDEV_DB]);
  ioc[BDEV_DB]->aio_wait();
  dout(20) << __func__ << " v " << super.version
           << " crc 0x" << std::hex << crc
           << " offset 0x" << get_super_offset() << std::dec
           << dendl;
  return 0;
}

int BlueFS::_open_super()
{
  dout(10) << __func__ << dendl;

  bufferlist bl;
  uint32_t expected_crc, crc;
  int r;

  // always the second block
  r = bdev[BDEV_DB]->read(get_super_offset(), get_super_length(),
			  &bl, ioc[BDEV_DB], false);//读取super block
  if (r < 0)
    return r;

  bufferlist::iterator p = bl.begin();
  ::decode(super, p);//填充super
  {
    bufferlist t;
    t.substr_of(bl, 0, p.get_off());
    crc = t.crc32c(-1);//计算crc
  }
  ::decode(expected_crc, p);
  if (crc != expected_crc) {//crc不同，读取失败
    derr << __func__ << " bad crc on superblock, expected 0x"
         << std::hex << expected_crc << " != actual 0x" << crc << std::dec
         << dendl;
    return -EIO;
  }
  dout(10) << __func__ << " superblock " << super.version << dendl;
  dout(10) << __func__ << " log_fnode " << super.log_fnode << dendl;
  return 0;
}

//按log文件，重建系统当前的目录，文件以及文件的大小
int BlueFS::_replay(bool noop)//重演日志
{
  dout(10) << __func__ << (noop ? " NO-OP" : "") << dendl;
  ino_last = 1;  // by the log
  log_seq = 0;

  FileRef log_file;
  if (noop) {
    log_file = new File;
  } else {
    log_file = _get_file(1);
  }
  log_file->fnode = super.log_fnode;//log文件的inode是硬编码
  dout(10) << __func__ << " log_fnode " << super.log_fnode << dendl;

  FileReader *log_reader = new FileReader(
    log_file, cct->_conf->bluefs_max_prefetch,
    false,  // !random
    true);  // ignore eof
  while (true) {
    assert((log_reader->buf.pos & ~super.block_mask()) == 0);
    uint64_t pos = log_reader->buf.pos;//读取头一定是按页对齐的
    uint64_t read_pos = pos;
    bufferlist bl;
    {
      int r = _read(log_reader, &log_reader->buf, read_pos, super.block_size,
		    &bl, NULL);//读取一个新的按页读取的块
      assert(r == (int)super.block_size);
      read_pos += r;
    }
    uint64_t more = 0;
    uint64_t seq;
    uuid_d uuid;
    {
      bufferlist::iterator p = bl.begin();
      __u8 a, b;
      uint32_t len;
      ::decode(a, p);
      ::decode(b, p);
      ::decode(len, p);
      ::decode(uuid, p);
      ::decode(seq, p);
      if (len + 6 > bl.length()) {//会出现不对齐问题，保证对齐
	more = ROUND_UP_TO(len + 6 - bl.length(), super.block_size);
      }
    }
    if (uuid != super.uuid) {
      dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
               << ": stop: uuid " << uuid << " != super.uuid " << super.uuid
               << dendl;
      break;
    }
    if (seq != log_seq + 1) {
      dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
               << ": stop: seq " << seq << " != expected " << log_seq + 1
               << dendl;
      break;
    }
    if (more) {
      dout(20) << __func__ << " need 0x" << std::hex << more << std::dec
               << " more bytes" << dendl;
      bufferlist t;
      int r = _read(log_reader, &log_reader->buf, read_pos, more, &t, NULL);//offset变更为４０９６以后
      if (r < (int)more) {
	dout(10) << __func__ << " 0x" << std::hex << pos
                 << ": stop: len is 0x" << bl.length() + more << std::dec
                 << ", which is past eof" << dendl;
	break;
      }
      assert(r == (int)more);
      bl.claim_append(t);
      read_pos += r;
    }
    bluefs_transaction_t t;
    try {
      bufferlist::iterator p = bl.begin();
      ::decode(t, p);
    }
    catch (buffer::error& e) {
      dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
               << ": stop: failed to decode: " << e.what()
               << dendl;
      delete log_reader;
      return -EIO;
    }
    assert(seq == t.seq);
    dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
             << ": " << t << dendl;

    bufferlist::iterator p = t.op_bl.begin();
    while (!p.end()) {
      __u8 op;
      ::decode(op, p);
      switch (op) {

      case bluefs_transaction_t::OP_INIT://init情况
	dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                 << ":  op_init" << dendl;
	assert(t.seq == 1);
	break;

      case bluefs_transaction_t::OP_JUMP://变更seq,变更offset,更新读头
        {
	  uint64_t next_seq;
	  uint64_t offset;
	  ::decode(next_seq, p);
	  ::decode(offset, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
		   << ":  op_jump seq " << next_seq
		   << " offset 0x" << std::hex << offset << std::dec << dendl;
	  assert(next_seq >= log_seq);
	  log_seq = next_seq - 1; // we will increment it below
	  uint64_t skip = offset - read_pos;
	  if (skip) {
	    bufferlist junk;
	    int r = _read(log_reader, &log_reader->buf, read_pos, skip, &junk,
			  NULL);
	    if (r != (int)skip) {
	      dout(10) << __func__ << " 0x" << std::hex << read_pos
		       << ": stop: failed to skip to " << offset
		       << std::dec << dendl;
	      assert(0 == "problem with op_jump");
	    }
	  }
	}
	break;

      case bluefs_transaction_t::OP_JUMP_SEQ://变更seq
        {
	  uint64_t next_seq;
	  ::decode(next_seq, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_jump_seq " << next_seq << dendl;
	  assert(next_seq >= log_seq);
	  log_seq = next_seq - 1; // we will increment it below
	}
	break;

      case bluefs_transaction_t::OP_ALLOC_ADD://添加空的物理范围
        {
	  __u8 id;
	  uint64_t offset, length;
	  ::decode(id, p);
	  ::decode(offset, p);
	  ::decode(length, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_alloc_add " << " " << (int)id
                   << ":0x" << std::hex << offset << "~" << length << std::dec
                   << dendl;
	  if (!noop) {
	    block_all[id].insert(offset, length);
	    block_total[id] += length;
	    alloc[id]->init_add_free(offset, length);
	  }
	}
	break;

      case bluefs_transaction_t::OP_ALLOC_RM://移除物理范围
        {
	  __u8 id;
	  uint64_t offset, length;
	  ::decode(id, p);
	  ::decode(offset, p);
	  ::decode(length, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_alloc_rm " << " " << (int)id
                   << ":0x" << std::hex << offset << "~" << length << std::dec
                   << dendl;
	  if (!noop) {
	    block_all[id].erase(offset, length);
	    block_total[id] -= length;
	    alloc[id]->init_rm_free(offset, length);
	  }
	}
	break;

      case bluefs_transaction_t::OP_DIR_LINK://增加某一文件的引用计数
        {
	  string dirname, filename;
	  uint64_t ino;
	  ::decode(dirname, p);
	  ::decode(filename, p);
	  ::decode(ino, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_dir_link " << " " << dirname << "/" << filename
                   << " to " << ino
		   << dendl;
	  if (!noop) {
	    FileRef file = _get_file(ino);
	    assert(file->fnode.ino);
	    map<string,DirRef>::iterator q = dir_map.find(dirname);
	    assert(q != dir_map.end());
	    map<string,FileRef>::iterator r = q->second->file_map.find(filename);
	    assert(r == q->second->file_map.end());
	    q->second->file_map[filename] = file;
	    ++file->refs;
	  }
	}
	break;

      case bluefs_transaction_t::OP_DIR_UNLINK://减少引用计数，移除引用文件
        {
	  string dirname, filename;
	  ::decode(dirname, p);
	  ::decode(filename, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_dir_unlink " << " " << dirname << "/" << filename
                   << dendl;
	  if (!noop) {
	    map<string,DirRef>::iterator q = dir_map.find(dirname);
	    assert(q != dir_map.end());
	    map<string,FileRef>::iterator r = q->second->file_map.find(filename);
	    assert(r != q->second->file_map.end());
            assert(r->second->refs > 0); 
	    --r->second->refs;
	    q->second->file_map.erase(r);
	  }
	}
	break;

      case bluefs_transaction_t::OP_DIR_CREATE://目录创建
        {
	  string dirname;
	  ::decode(dirname, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_dir_create " << dirname << dendl;
	  if (!noop) {
	    map<string,DirRef>::iterator q = dir_map.find(dirname);
	    assert(q == dir_map.end());
	    dir_map[dirname] = new Dir;
	  }
	}
	break;

      case bluefs_transaction_t::OP_DIR_REMOVE://目录移除
        {
	  string dirname;
	  ::decode(dirname, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_dir_remove " << dirname << dendl;
	  if (!noop) {
	    map<string,DirRef>::iterator q = dir_map.find(dirname);
	    assert(q != dir_map.end());
	    assert(q->second->file_map.empty());
	    dir_map.erase(q);
	  }
	}
	break;

      case bluefs_transaction_t::OP_FILE_UPDATE://更新元数据
        {
	  bluefs_fnode_t fnode;
	  ::decode(fnode, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_file_update " << " " << fnode << dendl;
	  if (!noop) {
	    FileRef f = _get_file(fnode.ino);
	    f->fnode = fnode;
	    if (fnode.ino > ino_last) {
	      ino_last = fnode.ino;//额外更新ino_last
	    }
	  }
	}
	break;

      case bluefs_transaction_t::OP_FILE_REMOVE://文件移除
        {
	  uint64_t ino;
	  ::decode(ino, p);
	  dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                   << ":  op_file_remove " << ino << dendl;
	  if (!noop) {
	    auto p = file_map.find(ino);
	    assert(p != file_map.end());
	    file_map.erase(p);
	  }
	}
	break;

      default://其它不认识的op
	derr << __func__ << " 0x" << std::hex << pos << std::dec
             << ": stop: unrecognized op " << (int)op << dendl;
	delete log_reader;
        return -EIO;
      }
    }
    assert(p.end());

    // we successfully replayed the transaction; bump the seq and log size
    ++log_seq;
    log_file->fnode.size = log_reader->buf.pos;
  }

  dout(10) << __func__ << " log file size was 0x"
           << std::hex << log_file->fnode.size << std::dec << dendl;
  delete log_reader;

  if (!noop) {
    // verify file link counts are all >0
    for (auto& p : file_map) {
      if (p.second->refs == 0 &&
	  p.second->fnode.ino > 1) {//校验代码，确认不会出现此情况
	derr << __func__ << " file with link count 0: " << p.second->fnode
	     << dendl;
	return -EIO;
      }
    }
  }

  dout(10) << __func__ << " done" << dendl;
  return 0;
}

BlueFS::FileRef BlueFS::_get_file(uint64_t ino)//由ino查FileRef
{
  auto p = file_map.find(ino);
  if (p == file_map.end()) {
    FileRef f = new File;
    file_map[ino] = f;
    dout(30) << __func__ << " ino " << ino << " = " << f
	     << " (new)" << dendl;
    return f;
  } else {
    dout(30) << __func__ << " ino " << ino << " = " << p->second << dendl;
    return p->second;
  }
}

void BlueFS::_drop_link(FileRef file)//文件删除
{
  dout(20) << __func__ << " had refs " << file->refs
	   << " on " << file->fnode << dendl;
  assert(file->refs > 0);
  --file->refs;//减小引用计数
  if (file->refs == 0) {
    dout(20) << __func__ << " destroying " << file->fnode << dendl;
    assert(file->num_reading.load() == 0);
    log_t.op_file_remove(file->fnode.ino);//记录删除
    for (auto& r : file->fnode.extents) {
      pending_release[r.bdev].insert(r.offset, r.length);
    }
    file_map.erase(file->fnode.ino);
    file->deleted = true;
    if (file->dirty_seq) {//如果这个文件已加入到dirty_files,则将其删除。
      assert(file->dirty_seq > log_seq_stable);
      assert(dirty_files.count(file->dirty_seq));
      auto it = dirty_files[file->dirty_seq].iterator_to(*file);
      dirty_files[file->dirty_seq].erase(it);
      file->dirty_seq = 0;
    }
  }
}

int BlueFS::_read_random(
  FileReader *h,         ///< [in] read from here
  uint64_t off,          ///< [in] offset
  size_t len,            ///< [in] this many bytes
  char *out)             ///< [out] optional: or copy it here
{
  dout(10) << __func__ << " h " << h
           << " 0x" << std::hex << off << "~" << len << std::dec
	   << " from " << h->file->fnode << dendl;

  ++h->file->num_reading;

  if (!h->ignore_eof &&
      off + len > h->file->fnode.size) {
    if (off > h->file->fnode.size)
      len = 0;
    else
      len = h->file->fnode.size - off;
    dout(20) << __func__ << " reaching (or past) eof, len clipped to 0x"
	     << std::hex << len << std::dec << dendl;
  }

  int ret = 0;
  while (len > 0) {
    uint64_t x_off = 0;
    auto p = h->file->fnode.seek(off, &x_off);
    uint64_t l = MIN(p->length - x_off, len);
    dout(20) << __func__ << " read buffered 0x"
             << std::hex << x_off << "~" << l << std::dec
             << " of " << *p << dendl;
    int r = bdev[p->bdev]->read_random(p->offset + x_off, l, out,
				       cct->_conf->bluefs_buffered_io);
    assert(r == 0);
    off += l;
    len -= l;
    ret += l;
    out += l;
  }

  dout(20) << __func__ << " got " << ret << dendl;
  --h->file->num_reading;
  return ret;
}

int BlueFS::_read(
  FileReader *h,         ///< [in] read from here
  FileReaderBuffer *buf, ///< [in] reader state
  uint64_t off,          ///< [in] offset
  size_t len,            ///< [in] this many bytes
  bufferlist *outbl,     ///< [out] optional: reference the result here
  char *out)             ///< [out] optional: or copy it here
{
  dout(10) << __func__ << " h " << h
           << " 0x" << std::hex << off << "~" << len << std::dec
	   << " from " << h->file->fnode << dendl;

  ++h->file->num_reading;//读者增加

  if (!h->ignore_eof &&
      off + len > h->file->fnode.size) {
    if (off > h->file->fnode.size)
      len = 0;
    else
      len = h->file->fnode.size - off;
    dout(20) << __func__ << " reaching (or past) eof, len clipped to 0x"
	     << std::hex << len << std::dec << dendl;
  }
  if (outbl)
    outbl->clear();

  int ret = 0;
  while (len > 0) {
    size_t left;
    if (off < buf->bl_off || off >= buf->get_buf_end()) {
      buf->bl.clear();
      buf->bl_off = off & super.block_mask();
      uint64_t x_off = 0;
      auto p = h->file->fnode.seek(buf->bl_off, &x_off);
      uint64_t want = ROUND_UP_TO(len + (off & ~super.block_mask()),
				  super.block_size);
      want = MAX(want, buf->max_prefetch);
      uint64_t l = MIN(p->length - x_off, want);
      uint64_t eof_offset = ROUND_UP_TO(h->file->fnode.size, super.block_size);
      if (!h->ignore_eof &&
	  buf->bl_off + l > eof_offset) {
	l = eof_offset - buf->bl_off;
      }
      dout(20) << __func__ << " fetching 0x"
               << std::hex << x_off << "~" << l << std::dec
               << " of " << *p << dendl;
      int r = bdev[p->bdev]->read(p->offset + x_off, l, &buf->bl, ioc[p->bdev],
				  cct->_conf->bluefs_buffered_io);
      assert(r == 0);
    }
    left = buf->get_buf_remaining(off);
    dout(20) << __func__ << " left 0x" << std::hex << left
             << " len 0x" << len << std::dec << dendl;

    int r = MIN(len, left);
    if (outbl) {
      bufferlist t;
      t.substr_of(buf->bl, off - buf->bl_off, r);
      outbl->claim_append(t);
    }
    if (out) {
      // NOTE: h->bl is normally a contiguous buffer so c_str() is free.
      memcpy(out, buf->bl.c_str() + off - buf->bl_off, r);
      out += r;
    }

    dout(30) << __func__ << " result chunk (0x"
             << std::hex << r << std::dec << " bytes):\n";
    bufferlist t;
    t.substr_of(buf->bl, off - buf->bl_off, r);
    t.hexdump(*_dout);
    *_dout << dendl;

    off += r;
    len -= r;
    ret += r;
    buf->pos += r;
  }

  dout(20) << __func__ << " got " << ret << dendl;
  assert(!outbl || (int)outbl->length() == ret);
  --h->file->num_reading;
  return ret;
}

void BlueFS::_invalidate_cache(FileRef f, uint64_t offset, uint64_t length)
{
  dout(10) << __func__ << " file " << f->fnode
	   << " 0x" << std::hex << offset << "~" << length << std::dec
           << dendl;
  if (offset & ~super.block_mask()) {
    offset &= super.block_mask();
    length = ROUND_UP_TO(length, super.block_size);
  }
  uint64_t x_off = 0;
  auto p = f->fnode.seek(offset, &x_off);
  while (length > 0 && p != f->fnode.extents.end()) {
    uint64_t x_len = MIN(p->length - x_off, length);
    bdev[p->bdev]->invalidate_cache(p->offset + x_off, x_len);
    dout(20) << __func__  << " 0x" << std::hex << x_off << "~" << x_len
             << std:: dec << " of " << *p << dendl;
    offset += x_len;
    length -= x_len;
  }
}

uint64_t BlueFS::_estimate_log_size()
{
  int avg_dir_size = 40;  // fixme
  int avg_file_size = 12;
  uint64_t size = 4096 * 2;
  size += file_map.size() * (1 + sizeof(bluefs_fnode_t));
  for (auto& p : block_all)
    size += p.num_intervals() * (1 + 1 + sizeof(uint64_t) * 2);
  size += dir_map.size() + (1 + avg_dir_size);
  size += file_map.size() * (1 + avg_dir_size + avg_file_size);
  return ROUND_UP_TO(size, super.block_size);
}

void BlueFS::compact_log()
{
  std::unique_lock<std::mutex> l(lock);
  if (cct->_conf->bluefs_compact_log_sync) {
     _compact_log_sync();
  } else {
    _compact_log_async(l);
  }
}

bool BlueFS::_should_compact_log()
{
  uint64_t current = log_writer->file->fnode.size;
  uint64_t expected = _estimate_log_size();
  float ratio = (float)current / (float)expected;
  dout(10) << __func__ << " current 0x" << std::hex << current
	   << " expected " << expected << std::dec
	   << " ratio " << ratio
	   << (new_log ? " (async compaction in progress)" : "")
	   << dendl;
  if (new_log ||
      current < cct->_conf->bluefs_log_compact_min_size ||
      ratio < cct->_conf->bluefs_log_compact_min_ratio) {
    return false;
  }
  return true;
}

void BlueFS::_compact_log_dump_metadata(bluefs_transaction_t *t)
{
  t->seq = 1;
  t->uuid = super.uuid;
  dout(20) << __func__ << " op_init" << dendl;

  t->op_init();
  for (unsigned bdev = 0; bdev < MAX_BDEV; ++bdev) {
    interval_set<uint64_t>& p = block_all[bdev];
    for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
      dout(20) << __func__ << " op_alloc_add " << bdev << " 0x"
               << std::hex << q.get_start() << "~" << q.get_len() << std::dec
               << dendl;
      t->op_alloc_add(bdev, q.get_start(), q.get_len());
    }
  }
  for (auto& p : file_map) {
    if (p.first == 1)
      continue;
    dout(20) << __func__ << " op_file_update " << p.second->fnode << dendl;
    assert(p.first > 1);
    t->op_file_update(p.second->fnode);
  }
  for (auto& p : dir_map) {
    dout(20) << __func__ << " op_dir_create " << p.first << dendl;
    t->op_dir_create(p.first);
    for (auto& q : p.second->file_map) {
      dout(20) << __func__ << " op_dir_link " << p.first << "/" << q.first
	       << " to " << q.second->fnode.ino << dendl;
      t->op_dir_link(p.first, q.first, q.second->fnode.ino);
    }
  }
}

void BlueFS::_compact_log_sync()
{
  dout(10) << __func__ << dendl;
  File *log_file = log_writer->file.get();

  // clear out log (be careful who calls us!!!)
  log_t.clear();

  bluefs_transaction_t t;
  _compact_log_dump_metadata(&t);

  dout(20) << __func__ << " op_jump_seq " << log_seq << dendl;
  t.op_jump_seq(log_seq);

  bufferlist bl;
  ::encode(t, bl);
  _pad_bl(bl);

  uint64_t need = bl.length() + cct->_conf->bluefs_max_log_runway;
  dout(20) << __func__ << " need " << need << dendl;

  mempool::bluefs::vector<bluefs_extent_t> old_extents;
  old_extents.swap(log_file->fnode.extents);
  while (log_file->fnode.get_allocated() < need) {
    int r = _allocate(log_file->fnode.prefer_bdev,
		      need - log_file->fnode.get_allocated(),
		      &log_file->fnode.extents);
    assert(r == 0);
  }

  _close_writer(log_writer);

  log_file->fnode.size = bl.length();
  log_writer = _create_writer(log_file);
  log_writer->append(bl);
  int r = _flush(log_writer, true);
  assert(r == 0);
  wait_for_aio(log_writer);

  dout(10) << __func__ << " writing super" << dendl;
  super.log_fnode = log_file->fnode;
  ++super.version;
  _write_super();
  flush_bdev();

  dout(10) << __func__ << " release old log extents " << old_extents << dendl;
  for (auto& r : old_extents) {
    pending_release[r.bdev].insert(r.offset, r.length);
  }

  logger->inc(l_bluefs_log_compactions);
}

/*
 * 1. Allocate a new extent to continue the log, and then log an event
 * that jumps the log write position to the new extent.  At this point, the
 * old extent(s) won't be written to, and reflect everything to compact.
 * New events will be written to the new region that we'll keep.
 *
 * 2. While still holding the lock, encode a bufferlist that dumps all of the
 * in-memory fnodes and names.  This will become the new beginning of the
 * log.  The last event will jump to the log continuation extent from #1.
 *
 * 3. Queue a write to a new extent for the new beginnging of the log.
 *
 * 4. Drop lock and wait
 *
 * 5. Retake the lock.
 *
 * 6. Update the log_fnode to splice in the new beginning.
 *
 * 7. Write the new superblock.
 *
 * 8. Release the old log space.  Clean up.
 */
void BlueFS::_compact_log_async(std::unique_lock<std::mutex>& l)
{
  dout(10) << __func__ << dendl;
  File *log_file = log_writer->file.get();
  assert(!new_log);
  assert(!new_log_writer);

  // 1. allocate new log space and jump to it.
  old_log_jump_to = log_file->fnode.get_allocated();
  uint64_t need = old_log_jump_to + cct->_conf->bluefs_max_log_runway;
  dout(10) << __func__ << " old_log_jump_to 0x" << std::hex << old_log_jump_to
           << " need 0x" << need << std::dec << dendl;
  while (log_file->fnode.get_allocated() < need) {//扩展到need
    int r = _allocate(log_file->fnode.prefer_bdev,
		      cct->_conf->bluefs_max_log_runway,
		      &log_file->fnode.extents);
    assert(r == 0);
  }
  dout(10) << __func__ << " log extents " << log_file->fnode.extents << dendl;

  // update the log file change and log a jump to the offset where we want to
  // write the new entries
  log_t.op_file_update(log_file->fnode);
  log_t.op_jump(log_seq, old_log_jump_to);
  _flush_and_sync_log(l, 0, old_log_jump_to);

  // 2. prepare compacted log
  bluefs_transaction_t t;
  _compact_log_dump_metadata(&t);

  // conservative estimate for final encoded size
  new_log_jump_to = ROUND_UP_TO(t.op_bl.length() + super.block_size * 2,
                                cct->_conf->bluefs_alloc_size);
  t.op_jump(log_seq, new_log_jump_to);

  bufferlist bl;
  ::encode(t, bl);
  _pad_bl(bl);

  dout(10) << __func__ << " new_log_jump_to 0x" << std::hex << new_log_jump_to
	   << std::dec << dendl;

  // create a new log [writer]
  new_log = new File;
  new_log->fnode.ino = 0;   // so that _flush_range won't try to log the fnode
  int r = _allocate(BlueFS::BDEV_DB, new_log_jump_to,
                    &new_log->fnode.extents);
  assert(r == 0);
  new_log_writer = _create_writer(new_log);
  new_log_writer->append(bl);

  // 3. flush
  r = _flush(new_log_writer, true);
  assert(r == 0);
  lock.unlock();

  // 4. wait
  dout(10) << __func__ << " waiting for compacted log to sync" << dendl;
  wait_for_aio(new_log_writer);
  flush_bdev();

  // 5. retake lock
  lock.lock();

  // 6. update our log fnode
  // discard first old_log_jump_to extents
  dout(10) << __func__ << " remove 0x" << std::hex << old_log_jump_to << std::dec
	   << " of " << log_file->fnode.extents << dendl;
  uint64_t discarded = 0;
  mempool::bluefs::vector<bluefs_extent_t> old_extents;
  while (discarded < old_log_jump_to) {
    assert(!log_file->fnode.extents.empty());
    bluefs_extent_t& e = log_file->fnode.extents.front();
    bluefs_extent_t temp = e;
    if (discarded + e.length <= old_log_jump_to) {
      dout(10) << __func__ << " remove old log extent " << e << dendl;
      discarded += e.length;
      log_file->fnode.extents.erase(log_file->fnode.extents.begin());
    } else {
      dout(10) << __func__ << " remove front of old log extent " << e << dendl;
      uint64_t drop = old_log_jump_to - discarded;
      temp.length = drop;
      e.offset += drop;
      e.length -= drop;
      discarded += drop;
      dout(10) << __func__ << "   kept " << e << " removed " << temp << dendl;
    }
    old_extents.push_back(temp);
  }
  new_log->fnode.extents.insert(new_log->fnode.extents.end(),
				log_file->fnode.extents.begin(),
				log_file->fnode.extents.end());

  // clear the extents from old log file, they are added to new log
  log_file->fnode.extents.clear();

  // swap the log files. New log file is the log file now.
  log_file->fnode.extents.swap(new_log->fnode.extents);
  log_writer->pos = log_writer->file->fnode.size =
    log_writer->pos - old_log_jump_to + new_log_jump_to;

  // 7. write the super block to reflect the changes
  dout(10) << __func__ << " writing super" << dendl;
  super.log_fnode = log_file->fnode;
  ++super.version;
  _write_super();

  lock.unlock();
  flush_bdev();
  lock.lock();

  // 8. release old space
  dout(10) << __func__ << " release old log extents " << old_extents << dendl;
  for (auto& r : old_extents) {
    pending_release[r.bdev].insert(r.offset, r.length);
  }

  // delete the new log, remove from the dirty files list
  _close_writer(new_log_writer);
  if (new_log->dirty_seq) {
    assert(dirty_files.count(new_log->dirty_seq));
    auto it = dirty_files[new_log->dirty_seq].iterator_to(*new_log);
    dirty_files[new_log->dirty_seq].erase(it);
  }
  new_log_writer = nullptr;
  new_log = nullptr;
  log_cond.notify_all();

  dout(10) << __func__ << " log extents " << log_file->fnode.extents << dendl;
  logger->inc(l_bluefs_log_compactions);
}

//采用zero补齐
void BlueFS::_pad_bl(bufferlist& bl)
{
  uint64_t partial = bl.length() % super.block_size;
  if (partial) {
    dout(10) << __func__ << " padding with 0x" << std::hex
	     << super.block_size - partial << " zeros" << std::dec << dendl;
    bl.append_zero(super.block_size - partial);
  }
}

void BlueFS::flush_log()//加锁同步日志
{
  std::unique_lock<std::mutex> l(lock);
  _flush_and_sync_log(l);
}

//日志同步
int BlueFS::_flush_and_sync_log(std::unique_lock<std::mutex>& l,
				uint64_t want_seq,
				uint64_t jump_to)//无锁同步日志
{
  while (log_flushing) {
    dout(10) << __func__ << " want_seq " << want_seq
	     << " log is currently flushing, waiting" << dendl;
    log_cond.wait(l);
  }
  if (want_seq && want_seq <= log_seq_stable) {//如果期待的seq已被刷入，则直接返回。
    dout(10) << __func__ << " want_seq " << want_seq << " <= log_seq_stable "
	     << log_seq_stable << ", done" << dendl;
    return 0;
  }
  if (log_t.empty() && dirty_files.empty()) {
    dout(10) << __func__ << " want_seq " << want_seq
	     << " " << log_t << " not dirty, dirty_files empty, no-op" << dendl;
    return 0;
  }

  uint64_t seq = log_t.seq = ++log_seq;//分配事件seq
  assert(want_seq == 0 || want_seq <= seq);//want_seq一定是已分配出来的seq
  log_t.uuid = super.uuid;

  // log dirty files
  auto lsi = dirty_files.find(seq);//检查seq对应的dirty_files
  if (lsi != dirty_files.end()) {//有dirty_files
    dout(20) << __func__ << " " << lsi->second.size() << " dirty_files" << dendl;
    for (auto &f : lsi->second) {
      dout(20) << __func__ << "   op_file_update " << f.fnode << dendl;
      log_t.op_file_update(f.fnode);//需要更新这此个文件的元数据
    }
  }

  dout(10) << __func__ << " " << log_t << dendl;
  assert(!log_t.empty());

  // allocate some more space (before we run out)?
  int64_t runway = log_writer->file->fnode.get_allocated() -
    log_writer->get_effective_write_pos();//空闲的块
  if (runway < (int64_t)cct->_conf->bluefs_min_log_runway) {
    dout(10) << __func__ << " allocating more log runway (0x"
	     << std::hex << runway << std::dec  << " remaining)" << dendl;
    while (new_log_writer) {//需要等其它人
      dout(10) << __func__ << " waiting for async compaction" << dendl;
      log_cond.wait(l);
    }
    int r = _allocate(log_writer->file->fnode.prefer_bdev,
		      cct->_conf->bluefs_max_log_runway,
		      &log_writer->file->fnode.extents);
    assert(r == 0);
    log_t.op_file_update(log_writer->file->fnode);
  }

  bufferlist bl;
  ::encode(log_t, bl);

  // pad to block boundary
  _pad_bl(bl);//添加0,保证写的内容是块对齐
  logger->inc(l_bluefs_logged_bytes, bl.length());

  log_writer->append(bl);//将log_t写入

  log_t.clear();//清空，准备复用
  log_t.seq = 0;  // just so debug output is less confusing //作者给自已解释为什么要加入seq,不认同。
  log_flushing = true;

  flush_bdev();//提前落一遍盘，防止aio完成后，未来得及调用flush_bdev
  int r = _flush(log_writer, true);//写入日志
  assert(r == 0);

  if (jump_to) {//是否缩小到指定size
    dout(10) << __func__ << " jumping log offset from 0x" << std::hex
	     << log_writer->pos << " -> 0x" << jump_to << std::dec << dendl;
    log_writer->pos = jump_to;
    log_writer->file->fnode.size = jump_to;
  }

  // drop lock while we wait for io
  list<FS::aio_t> completed_ios;
  _claim_completed_aios(log_writer, &completed_ios);
  l.unlock();
  wait_for_aio(log_writer);//等待aio完成
  completed_ios.clear();
  flush_bdev();//保证元数据落盘(防止log数据未落盘） //是否可以将日志落盘单独独立出来，这样这里就不用调两遍flush_bdev
  l.lock();

  log_flushing = false;
  log_cond.notify_all();//通知其它写日志的线程，防止别人等自已

  // clean dirty files
  if (seq > log_seq_stable) {
    log_seq_stable = seq;//更新已固化的seq
    dout(20) << __func__ << " log_seq_stable " << log_seq_stable << dendl;

    auto p = dirty_files.begin();//遍历dirty_files（这些文件已被写入log)
    while (p != dirty_files.end()) {
      if (p->first > log_seq_stable) {//遍历提前结束{小于log_seq_stable的已处理}
        dout(20) << __func__ << " done cleaning up dirty files" << dendl;
        break;
      }

      auto l = p->second.begin();//遍历dirty_files中指定seq的序列
      while (l != p->second.end()) {//删除这些文件
        File *file = &*l;
        assert(file->dirty_seq > 0);
        assert(file->dirty_seq <= log_seq_stable);
        dout(20) << __func__ << " cleaned file " << file->fnode << dendl;
        file->dirty_seq = 0;
        p->second.erase(l++);
      }

      assert(p->second.empty());
      dirty_files.erase(p++);//删除这个vector
    }
  } else {
    dout(20) << __func__ << " log_seq_stable " << log_seq_stable
             << " already >= out seq " << seq
             << ", we lost a race against another log flush, done" << dendl;
  }
  _update_logger_stats();

  return 0;
}

//将h缓冲中自offset位置开始，长度为length的数据写入{代码之所以复杂主要是处理了：
//1.buffer是list形式串起来的，写时需要遍历
//2.存在写时前向放大
//3.存在写时后向放大
//4.需要考虑更新元数据}
int BlueFS::_flush_range(FileWriter *h, uint64_t offset, uint64_t length)
{
  dout(10) << __func__ << " " << h << " pos 0x" << std::hex << h->pos
	   << " 0x" << offset << "~" << length << std::dec
	   << " to " << h->file->fnode << dendl;
  assert(!h->file->deleted);
  assert(h->file->num_readers.load() == 0);

  h->buffer_appender.flush();//将未提交到pbl中的内容，刷入pbl

  bool buffered;
  if (h->file->fnode.ino == 1)
    buffered = false;//对于ino为1的不容许采用缓存方式
  else
    buffered = cct->_conf->bluefs_buffered_io;//否则由配置决定

  if (offset + length <= h->pos)//如果h的pos表明已刷入，则不再处理
    return 0;
  if (offset < h->pos) {//offset比pos要小，需要规范
    length -= h->pos - offset;//长度需要减少
    offset = h->pos;//offset需要重置
    dout(10) << " still need 0x"
             << std::hex << offset << "~" << length << std::dec
             << dendl;
  }
  //这里lenght可以为0，需要考虑跳过。
  assert(offset <= h->file->fnode.size);//offset一定小于size

  uint64_t allocated = h->file->fnode.get_allocated();//已申请了多大的物理大小

  // do not bother to dirty the file if we are overwriting
  // previously allocated extents.
  bool must_dirty = false;
  if (allocated < offset + length) {//申请的物理大小比要写入的要小，需要扩充物理大小的申请
    // we should never run out of log space here; see the min runway check
    // in _flush_and_sync_log.
    assert(h->file->fnode.ino != 1);
    int r = _allocate(h->file->fnode.prefer_bdev,
		      offset + length - allocated,
		      &h->file->fnode.extents);
    if (r < 0) {
      derr << __func__ << " allocated: 0x" << std::hex << allocated
           << " offset: 0x" << offset << " length: 0x" << length << std::dec
           << dendl;
      return r;//申请失败，没法写，返回
    }
    if (cct->_conf->bluefs_preextend_wal_files &&
	h->writer_type == WRITER_WAL) {
      // NOTE: this *requires* that rocksdb also has log recycling
      // enabled and is therefore doing robust CRCs on the log
      // records.  otherwise, we will fail to reply the rocksdb log
      // properly due to garbage on the device.
      h->file->fnode.size = h->file->fnode.get_allocated();
      dout(10) << __func__ << " extending WAL size to 0x" << std::hex
	       << h->file->fnode.size << std::dec << " to include allocated"
	       << dendl;
    }
    must_dirty = true;
  }
  if (h->file->fnode.size < offset + length) {//文件大小未更新
    h->file->fnode.size = offset + length;//更新大小
    if (h->file->fnode.ino > 1) {
      // we do not need to dirty the log file (or it's compacting
      // replacement) when the file size changes because replay is
      // smart enough to discover it on its own.
      must_dirty = true;
    }
  }
  if (must_dirty) {//如果扩充了物理申请，或者如果更新了文件大小，则进入
    //可以看作是否需要变更metadata
    h->file->fnode.mtime = ceph_clock_now();
    assert(h->file->fnode.ino >= 1);
    if (h->file->dirty_seq == 0) {
      h->file->dirty_seq = log_seq + 1;
      dirty_files[h->file->dirty_seq].push_back(*h->file);
      dout(20) << __func__ << " dirty_seq = " << log_seq + 1
	       << " (was clean)" << dendl;
    } else {
      if (h->file->dirty_seq != log_seq + 1) {
        // need re-dirty, erase from list first
    	//dirty_seq记录的是当前文件加入到dirty_files中的索引号，由于这个值非0，故是有效值
    	//所以这个file一定处于dirty_files中
        assert(dirty_files.count(h->file->dirty_seq));
        auto it = dirty_files[h->file->dirty_seq].iterator_to(*h->file);//检查当前文件是否已加入dirty_files
        dirty_files[h->file->dirty_seq].erase(it);//在上一个seq中删除自已
        h->file->dirty_seq = log_seq + 1;
        dirty_files[h->file->dirty_seq].push_back(*h->file);//将自已加入此dirty_seq处
        dout(20) << __func__ << " dirty_seq = " << log_seq + 1
                 << " (was " << h->file->dirty_seq << ")" << dendl;
      } else {
    	//已加入，不处理。
        dout(20) << __func__ << " dirty_seq = " << log_seq + 1
                 << " (unchanged, do nothing) " << dendl;
      }
    }
  }
  dout(20) << __func__ << " file now " << h->file->fnode << dendl;

  uint64_t x_off = 0;
  auto p = h->file->fnode.seek(offset, &x_off);//求出offset在块内的实际值
  assert(p != h->file->fnode.extents.end());//断言p一定存在
  dout(20) << __func__ << " in " << *p << " x_off 0x"
           << std::hex << x_off << std::dec << dendl;

  unsigned partial = x_off & ~super.block_mask();//块内的offset可能不是块对齐的，需要将其规范化为页对齐（即写放大，放大字节partial)
  bufferlist bl;
  if (partial) {//写放大后（前向放大），需要更新x_off,offset,length
    dout(20) << __func__ << " using partial tail 0x"
             << std::hex << partial << std::dec << dendl;
    assert(h->tail_block.length() == partial);
    bl.claim_append(h->tail_block);//这个实现比较不明显，用于解决前向放大时，需要先读取，后写入问题。
    x_off -= partial;//在块内向后退（放大）
    offset -= partial;//offset向后退（文件的偏移）
    length += partial;//长度增加
    dout(20) << __func__ << " waiting for previous aio to complete" << dendl;
    for (auto p : h->iocv) {//由于与之前此h对应的写区间重叠，故有可能会与之前的aio写同一区间，为防止乱序，要求之前的aio完成后再继续
      if (p) {
	p->aio_wait();//当前的这个等待实现的不好，如果发现有重叠会导致等待所有人退出。（可以考虑hash查找）
      }
    }
  }
  if (length == partial + h->buffer.length()) {//h中的所有数据需要写入
    bl.claim_append(h->buffer);
  } else {//h中的部分数据需要写入
    bufferlist t;
    t.substr_of(h->buffer, 0, length);
    bl.claim_append(t);
    t.substr_of(h->buffer, length, h->buffer.length() - length);
    h->buffer.swap(t);
    dout(20) << " leaving 0x" << std::hex << h->buffer.length() << std::dec
             << " unflushed" << dendl;
  }
  assert(bl.length() == length);

  switch (h->writer_type) {
  case WRITER_WAL:
    logger->inc(l_bluefs_bytes_written_wal, length);
    break;
  case WRITER_SST:
    logger->inc(l_bluefs_bytes_written_sst, length);
    break;
  }

  dout(30) << "dump:\n";
  bl.hexdump(*_dout);
  *_dout << dendl;

  h->pos = offset + length;
  h->tail_block.clear();

  uint64_t bloff = 0;
  while (length > 0) {
    uint64_t x_len = MIN(p->length - x_off, length);//length是要写入的长度，p->length是这一块的长度，取两者小者
    bufferlist t;//要通过aio_write提交的缓存。
    t.substr_of(bl, bloff, x_len);//将bl中的数据，从bloff中取，取x_len长度
    unsigned tail = x_len & ~super.block_mask();//x_len可能不是块的整数倍，tail是余数
    if (tail) {//有余数（写放大，后面放大问题）
      size_t zlen = super.block_size - tail;//需要多写zlen长度
      dout(20) << __func__ << " caching tail of 0x"
               << std::hex << tail
	       << " and padding block with 0x" << zlen
	       << std::dec << dendl;
      h->tail_block.substr_of(bl, bl.length() - tail, tail);//将tail位置的数据先填充到tail_block中（提前缓存，当下次在tail后面写时，会遇到前向写放大问题，见前向放大）
      if (h->file->fnode.ino > 1) {
	// we are using the page_aligned_appender, and can safely use
	// the tail of the raw buffer.
    //由于t采用的是page_aligned_appender方式分配，故一定是block的整数倍，所以这里
    //作者认为一定能放在结尾，所以作者添加了断言。
	const bufferptr &last = t.back();
	if (last.unused_tail_length() < zlen) {
	  derr << " wtf, last is " << last << " from " << t << dendl;
	  assert(last.unused_tail_length() >= zlen);
	}
	bufferptr z = last;
	z.set_offset(last.offset() + last.length());
	z.set_length(zlen);
	z.zero();//填充0
	t.append(z, 0, zlen);//补写一组0
      } else {
	t.append_zero(zlen);
      }
    }
    //p指向申请的extends,x_off是规范后的偏移量，t是要写入的buffer
    bdev[p->bdev]->aio_write(p->offset + x_off, t, h->iocv[p->bdev], buffered);//填充aio写
    bloff += x_len;
    length -= x_len;
    ++p;
    x_off = 0;
  }
  for (unsigned i = 0; i < MAX_BDEV; ++i) {
    if (bdev[i]) {
      assert(h->iocv[i]);
      if (h->iocv[i]->has_pending_aios()) {
        bdev[i]->aio_submit(h->iocv[i]);//提交aio写
      }
    }
  }
  dout(20) << __func__ << " h " << h << " pos now 0x"
           << std::hex << h->pos << std::dec << dendl;
  return 0;
}

// we need to retire old completed aios so they don't stick around in
// memory indefinitely (along with their bufferlist refs).
void BlueFS::_claim_completed_aios(FileWriter *h, list<FS::aio_t> *ls)
{
  for (auto p : h->iocv) {
    if (p) {
      ls->splice(ls->end(), p->running_aios);
    }
  }
  dout(10) << __func__ << " got " << ls->size() << " aios" << dendl;
}

void BlueFS::wait_for_aio(FileWriter *h)//等待aio,实现为等待所有设备的aio完成
{
  // NOTE: this is safe to call without a lock, as long as our reference is
  // stable.
  dout(10) << __func__ << " " << h << dendl;
  utime_t start = ceph_clock_now();
  for (auto p : h->iocv) {
    if (p) {
      p->aio_wait();
    }
  }
  utime_t end = ceph_clock_now();
  utime_t dur = end - start;
  dout(10) << __func__ << " " << h << " done in " << dur << dendl;
}

int BlueFS::_flush(FileWriter *h, bool force)//刷新
{
  h->buffer_appender.flush();
  uint64_t length = h->buffer.length();
  uint64_t offset = h->pos;
  if (!force &&
      length < cct->_conf->bluefs_min_flush_size) {//如果非强制刷入，则等攒够到min_flush_size时，才刷入
    dout(10) << __func__ << " " << h << " ignoring, length " << length
	     << " < min_flush_size " << cct->_conf->bluefs_min_flush_size
	     << dendl;
    return 0;
  }
  if (length == 0) {//如果强制刷入，但没有数据，则返回
    dout(10) << __func__ << " " << h << " no dirty data on "
	     << h->file->fnode << dendl;
    return 0;
  }
  dout(10) << __func__ << " " << h << " 0x"
           << std::hex << offset << "~" << length << std::dec
	   << " to " << h->file->fnode << dendl;
  assert(h->pos <= h->file->fnode.size);
  return _flush_range(h, offset, length);//刷文件h的<offset,offset+length>区间的数据
}

int BlueFS::_truncate(FileWriter *h, uint64_t offset)
{
  dout(10) << __func__ << " 0x" << std::hex << offset << std::dec
           << " file " << h->file->fnode << dendl;
  if (h->file->deleted) {
    dout(10) << __func__ << "  deleted, no-op" << dendl;
    return 0;
  }

  // we never truncate internal log files
  assert(h->file->fnode.ino > 1);

  h->buffer_appender.flush();

  // truncate off unflushed data?
  if (h->pos < offset &&
      h->pos + h->buffer.length() > offset) {
    bufferlist t;
    dout(20) << __func__ << " tossing out last " << offset - h->pos
	     << " unflushed bytes" << dendl;
    t.substr_of(h->buffer, 0, offset - h->pos);
    h->buffer.swap(t);
    assert(0 == "actually this shouldn't happen");
  }
  if (h->buffer.length()) {
    int r = _flush(h, true);
    if (r < 0)
      return r;
  }
  if (offset == h->file->fnode.size) {
    return 0;  // no-op!
  }
  if (offset > h->file->fnode.size) {
    assert(0 == "truncate up not supported");
  }
  assert(h->file->fnode.size >= offset);
  h->file->fnode.size = offset;
  log_t.op_file_update(h->file->fnode);
  return 0;
}

int BlueFS::_fsync(FileWriter *h, std::unique_lock<std::mutex>& l)
{
  dout(10) << __func__ << " " << h << " " << h->file->fnode << dendl;
  int r = _flush(h, true);
  if (r < 0)
     return r;
  uint64_t old_dirty_seq = h->file->dirty_seq;
  list<FS::aio_t> completed_ios;
  _claim_completed_aios(h, &completed_ios);
  lock.unlock();
  wait_for_aio(h);
  completed_ios.clear();
  lock.lock();
  if (old_dirty_seq) {
    uint64_t s = log_seq;
    dout(20) << __func__ << " file metadata was dirty (" << old_dirty_seq
	     << ") on " << h->file->fnode << ", flushing log" << dendl;
    _flush_and_sync_log(l, old_dirty_seq);
    assert(h->file->dirty_seq == 0 ||  // cleaned
	   h->file->dirty_seq > s);    // or redirtied by someone else
  }
  return 0;
}

void BlueFS::flush_bdev()//各设备数据落盘
{
  // NOTE: this is safe to call without a lock.
  dout(20) << __func__ << dendl;
  for (auto p : bdev) {
    if (p)
      p->flush();
  }
}

//在bdev的id类型中尝试申请len长度，将申请到的物理范围，填充在ev中。
int BlueFS::_allocate(uint8_t id, uint64_t len,
		      mempool::bluefs::vector<bluefs_extent_t> *ev)
{
  dout(10) << __func__ << " len 0x" << std::hex << len << std::dec
           << " from " << (int)id << dendl;
  assert(id < alloc.size());
  uint64_t min_alloc_size = cct->_conf->bluefs_alloc_size;

  uint64_t left = ROUND_UP_TO(len, min_alloc_size);
  int r = -ENOSPC;
  if (alloc[id]) {
    r = alloc[id]->reserve(left);//预留left数量
  }
  if (r < 0) {//
    if (id != BDEV_SLOW) {//向后一个设备申请（增加id)
      if (bdev[id]) {
	dout(1) << __func__ << " failed to allocate 0x" << std::hex << left
		<< " on bdev " << (int)id
		<< ", free 0x" << alloc[id]->get_free()
		<< "; fallback to bdev " << (int)id + 1
		<< std::dec << dendl;
      }
      return _allocate(id + 1, len, ev);
    }
    //向bdev_slow申请失败
    if (bdev[id])
      derr << __func__ << " failed to allocate 0x" << std::hex << left
	   << " on bdev " << (int)id
	   << ", free 0x" << alloc[id]->get_free() << std::dec << dendl;
    else
      derr << __func__ << " failed to allocate 0x" << std::hex << left
	   << " on bdev " << (int)id << ", dne" << std::dec << dendl;
    return r;
  }

  uint64_t hint = 0;
  if (!ev->empty()) {//如果ev是有值的，则跳到最后位置
    hint = ev->back().end();
  }

  AllocExtentVector extents;
  extents.reserve(4);  // 4 should be (more than) enough for most allocations
  int64_t alloc_len = alloc[id]->allocate(left, min_alloc_size, hint,
                          &extents);
  if (alloc_len < (int64_t)left) {
    derr << __func__ << " allocate failed on 0x" << std::hex << left
	 << " min_alloc_size 0x" << min_alloc_size << std::dec << dendl;
    alloc[id]->dump();
    assert(0 == "allocate failed... wtf");
    return -ENOSPC;
  }

  for (auto& p : extents) {
    bluefs_extent_t e = bluefs_extent_t(id, p.offset, p.length);
    if (!ev->empty() &&
	ev->back().bdev == e.bdev &&
	ev->back().end() == (uint64_t) e.offset) {
      ev->back().length += e.length;//合并申请到的bluefs_extent_t
    } else {
      ev->push_back(e);//无法合并，直接加入
    }
  }
   
  return 0;
}

//扩大f的范围
int BlueFS::_preallocate(FileRef f, uint64_t off, uint64_t len)
{
  dout(10) << __func__ << " file " << f->fnode << " 0x"
	   << std::hex << off << "~" << len << std::dec << dendl;
  if (f->deleted) {
    dout(10) << __func__ << "  deleted, no-op" << dendl;
    return 0;
  }
  assert(f->fnode.ino > 1);
  uint64_t allocated = f->fnode.get_allocated();
  if (off + len > allocated) {//要写入的，比当前申请的要大。
    uint64_t want = off + len - allocated;
    int r = _allocate(f->fnode.prefer_bdev, want, &f->fnode.extents);
    if (r < 0)
      return r;
    log_t.op_file_update(f->fnode);
  }
  return 0;
}

void BlueFS::sync_metadata()//元数据同步
{
  std::unique_lock<std::mutex> l(lock);
  if (log_t.empty()) {//log_t为空，说明无log事件，直接返回即可
    dout(10) << __func__ << " - no pending log events" << dendl;
    return;
  }
  dout(10) << __func__ << dendl;
  utime_t start = ceph_clock_now();
  vector<interval_set<uint64_t>> to_release(pending_release.size());
  to_release.swap(pending_release);
  _flush_and_sync_log(l);
  for (unsigned i = 0; i < to_release.size(); ++i) {
    for (auto p = to_release[i].begin(); p != to_release[i].end(); ++p) {
      alloc[i]->release(p.get_start(), p.get_len());
    }
  }

  if (_should_compact_log()) {
    if (cct->_conf->bluefs_compact_log_sync) {
      _compact_log_sync();
    } else {
      _compact_log_async(l);
    }
  }

  //显示我们用了多长时间
  utime_t end = ceph_clock_now();
  utime_t dur = end - start;
  dout(10) << __func__ << " done in " << dur << dendl;
}

int BlueFS::open_for_write(
  const string& dirname,//目录名称
  const string& filename,//文件名称
  FileWriter **h,
  bool overwrite)
{
  std::lock_guard<std::mutex> l(lock);//读写大锁
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  DirRef dir;
  if (p == dir_map.end()) {//没有找到dirname对应的目录
    // implicitly create the dir
    dout(20) << __func__ << "  dir " << dirname
	     << " does not exist" << dendl;
    return -ENOENT;
  } else {//找到相应目录
    dir = p->second;
  }

  FileRef file;
  bool create = false;
  map<string,FileRef>::iterator q = dir->file_map.find(filename);//查找文件
  if (q == dir->file_map.end()) {//没有找到文件
    if (overwrite) {
      dout(20) << __func__ << " dir " << dirname << " (" << dir
	       << ") file " << filename
	       << " does not exist" << dendl;
      return -ENOENT;
    }
    file = new File;
    file->fnode.ino = ++ino_last;//分配inode
    file_map[ino_last] = file;//填充file_map
    dir->file_map[filename] = file;//填充相应目录下filename
    ++file->refs;//增加计数
    create = true;//标明已cache
  } else {//找到了此文件
    // overwrite existing file?
    file = q->second;
    if (overwrite) {
      dout(20) << __func__ << " dir " << dirname << " (" << dir
	       << ") file " << filename
	       << " already exists, overwrite in place" << dendl;
    } else {
      dout(20) << __func__ << " dir " << dirname << " (" << dir
	       << ") file " << filename
	       << " already exists, truncate + overwrite" << dendl;
      file->fnode.size = 0;
      for (auto& p : file->fnode.extents) {
	pending_release[p.bdev].insert(p.offset, p.length);
      }
      file->fnode.extents.clear();//清空extents
    }
  }
  assert(file->fnode.ino > 1);

  file->fnode.mtime = ceph_clock_now();//设置访问时间
  file->fnode.prefer_bdev = BlueFS::BDEV_DB;//设置dev id
  if (dirname.length() > 5) {
    // the "db.slow" and "db.wal" directory names are hard-coded at
    // match up with bluestore.  the slow device is always the second
    // one (when a dedicated block.db device is present and used at
    // bdev 0).  the wal device is always last.
    if (boost::algorithm::ends_with(filename, ".slow")) {
      file->fnode.prefer_bdev = BlueFS::BDEV_SLOW;
    } else if (boost::algorithm::ends_with(dirname, ".wal")) {
      file->fnode.prefer_bdev = BlueFS::BDEV_WAL;
    }
  }
  dout(20) << __func__ << " mapping " << dirname << "/" << filename
	   << " to bdev " << (int)file->fnode.prefer_bdev << dendl;

  log_t.op_file_update(file->fnode);
  if (create)
    log_t.op_dir_link(dirname, filename, file->fnode.ino);

  *h = _create_writer(file);

  if (boost::algorithm::ends_with(filename, ".log")) {//如果是.log
    (*h)->writer_type = BlueFS::WRITER_WAL;
    if (logger && !overwrite) {
      logger->inc(l_bluefs_files_written_wal);
    }
  } else if (boost::algorithm::ends_with(filename, ".sst")) {
    (*h)->writer_type = BlueFS::WRITER_SST;
    if (logger) {
      logger->inc(l_bluefs_files_written_sst);
    }
  }

  dout(10) << __func__ << " h " << *h << " on " << file->fnode << dendl;
  return 0;
}

BlueFS::FileWriter *BlueFS::_create_writer(FileRef f)
{
  FileWriter *w = new FileWriter(f);
  for (unsigned i = 0; i < MAX_BDEV; ++i) {
    if (bdev[i]) {//如果可以用这个块设备，则创建
      w->iocv[i] = new IOContext(cct, NULL);//创建io上下文
    } else {
      w->iocv[i] = NULL;
    }
  }
  return w;
}

void BlueFS::_close_writer(FileWriter *h)//关闭write时，iocv将被刷入
{
  dout(10) << __func__ << " " << h << " type " << h->writer_type << dendl;
  for (unsigned i=0; i<MAX_BDEV; ++i) {
    if (bdev[i]) {
      assert(h->iocv[i]);
      h->iocv[i]->aio_wait();
      bdev[i]->queue_reap_ioc(h->iocv[i]);
    }
  }
  delete h;
}

int BlueFS::open_for_read(//打开文件为读取
  const string& dirname,
  const string& filename,
  FileReader **h,
  bool random)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename
	   << (random ? " (random)":" (sequential)") << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;

  map<string,FileRef>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " (" << dir
	     << ") file " << filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  File *file = q->second.get();
  //如果目录或者文件不存在，则直接返回失败
  *h = new FileReader(file, random ? 4096 : cct->_conf->bluefs_max_prefetch,
		      random, false);
  dout(10) << __func__ << " h " << *h << " on " << file->fnode << dendl;
  return 0;
}

int BlueFS::rename(
  const string& old_dirname, const string& old_filename,
  const string& new_dirname, const string& new_filename)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << old_dirname << "/" << old_filename
	   << " -> " << new_dirname << "/" << new_filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(old_dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << old_dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef old_dir = p->second;
  map<string,FileRef>::iterator q = old_dir->file_map.find(old_filename);
  if (q == old_dir->file_map.end()) {
    dout(20) << __func__ << " dir " << old_dirname << " (" << old_dir
	     << ") file " << old_filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  //重命名时，需要保证旧文件及目录均存在。
  FileRef file = q->second;

  p = dir_map.find(new_dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << new_dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef new_dir = p->second;
  q = new_dir->file_map.find(new_filename);
  if (q != new_dir->file_map.end()) {
    dout(20) << __func__ << " dir " << new_dirname << " (" << old_dir
	     << ") file " << new_filename
	     << " already exists, unlinking" << dendl;
    assert(q->second != file);
    log_t.op_dir_unlink(new_dirname, new_filename);
    _drop_link(q->second);
  }
  //重命名文件时，需要保证新的目录存在，如果新的文件也存在的话，则删除新的文件后再重命名
  dout(10) << __func__ << " " << new_dirname << "/" << new_filename << " "
	   << " " << file->fnode << dendl;

  new_dir->file_map[new_filename] = file;//添加
  old_dir->file_map.erase(old_filename);//删除

  log_t.op_dir_link(new_dirname, new_filename, file->fnode.ino);
  log_t.op_dir_unlink(old_dirname, old_filename);
  return 0;
}

int BlueFS::mkdir(const string& dirname)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p != dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " exists" << dendl;
    return -EEXIST;
  }
  //创建新目录时，如果目录已存在，则报错
  dir_map[dirname] = new Dir;
  log_t.op_dir_create(dirname);
  return 0;
}

int BlueFS::rmdir(const string& dirname)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " does not exist" << dendl;
    return -ENOENT;
  }
  //删除目录时，如果目录不存在，则报错，如果目录不为空，则报错
  DirRef dir = p->second;
  if (!dir->file_map.empty()) {
    dout(20) << __func__ << " dir " << dirname << " not empty" << dendl;
    return -ENOTEMPTY;
  }
  dir_map.erase(dirname);//删除目录
  log_t.op_dir_remove(dirname);
  return 0;
}

bool BlueFS::dir_exists(const string& dirname)//检查目录是否已存在
{
  std::lock_guard<std::mutex> l(lock);
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  bool exists = p != dir_map.end();
  dout(10) << __func__ << " " << dirname << " = " << (int)exists << dendl;
  return exists;
}

int BlueFS::stat(const string& dirname, const string& filename,
		 uint64_t *size, utime_t *mtime)//返回文件对应的size,及修改时间
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;
  map<string,FileRef>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " (" << dir
	     << ") file " << filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  File *file = q->second.get();
  dout(10) << __func__ << " " << dirname << "/" << filename
	   << " " << file->fnode << dendl;
  if (size)
    *size = file->fnode.size;
  if (mtime)
    *mtime = file->fnode.mtime;
  return 0;
}

int BlueFS::lock_file(const string& dirname, const string& filename,
		      FileLock **plock)//锁指定文件，并返回对应的文件锁，注，如果文件不存在，将创建文件
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;
  map<string,FileRef>::iterator q = dir->file_map.find(filename);
  File *file;
  if (q == dir->file_map.end()) {//没有找到文件，则创建此文件（这里可以走创建流程）
    dout(20) << __func__ << " dir " << dirname << " (" << dir
	     << ") file " << filename
	     << " not found, creating" << dendl;
    file = new File;
    file->fnode.ino = ++ino_last;
    file->fnode.mtime = ceph_clock_now();
    file_map[ino_last] = file;
    dir->file_map[filename] = file;
    ++file->refs;
    log_t.op_file_update(file->fnode);
    log_t.op_dir_link(dirname, filename, file->fnode.ino);
  } else {
    file = q->second.get();
    if (file->locked) {//检查文件是否被锁，如果被锁，则返回busy
      dout(10) << __func__ << " already locked" << dendl;
      return -EBUSY;
    }
  }
  file->locked = true;//标记锁
  *plock = new FileLock(file);
  dout(10) << __func__ << " locked " << file->fnode
	   << " with " << *plock << dendl;
  return 0;
}

int BlueFS::unlock_file(FileLock *fl)//对文件进行解锁
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << fl << " on " << fl->file->fnode << dendl;
  assert(fl->file->locked);
  fl->file->locked = false;
  delete fl;
  return 0;
}

int BlueFS::readdir(const string& dirname, vector<string> *ls)//返回指定目录下的文件，如果dirname为空串，则返回/目录下内容。
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << dendl;
  if (dirname.empty()) {
    //列出所有根目录
    // list dirs
    ls->reserve(dir_map.size() + 2);
    for (auto& q : dir_map) {
      ls->push_back(q.first);//列出所有目录
    }
  } else {
    // list files in dir
    map<string,DirRef>::iterator p = dir_map.find(dirname);
    if (p == dir_map.end()) {
      dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
      return -ENOENT;
    }
    DirRef dir = p->second;
    ls->reserve(dir->file_map.size() + 2);
    for (auto& q : dir->file_map) {
      ls->push_back(q.first);//列出指定目录下的文件
    }
  }
  ls->push_back(".");//添加'.','..'
  ls->push_back("..");
  return 0;
}

int BlueFS::unlink(const string& dirname, const string& filename)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {//目录不存在
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;
  map<string,FileRef>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {//文件不存在
    dout(20) << __func__ << " file " << dirname << "/" << filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  FileRef file = q->second;
  if (file->locked) {//文件被锁住了
    dout(20) << __func__ << " file " << dirname << "/" << filename
             << " is locked" << dendl;
    return -EBUSY;
  }
  dir->file_map.erase(filename);//删除文件索引
  log_t.op_dir_unlink(dirname, filename);
  _drop_link(file);
  return 0;
}
