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

#include "include/unordered_map.h"

#if defined(__FreeBSD__)
#include <sys/param.h>
#endif

#include <errno.h>

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/config.h"
#include "common/debug.h"
#include "include/buffer.h"

#include "IndexManager.h"
#include "HashIndex.h"
#include "CollectionIndex.h"

#include "chain_xattr.h"

//设置path的user.cephos.collection_version的属性值，设置当前数据目录的版本号，以便后续升级
static int set_version(const char *path, uint32_t version) {
  bufferlist bl;
  encode(version, bl);
  return chain_setxattr<true, true>(
    path, "user.cephos.collection_version", bl.c_str(),
    bl.length());
}

//读取path的user.cephos.collection_version的属性值，了解当前数据版本
static int get_version(const char *path, uint32_t *version) {
  bufferptr bp(PATH_MAX);
  int r = chain_getxattr(path, "user.cephos.collection_version",
		      bp.c_str(), bp.length());
  if (r < 0) {
    if (r != -ENOENT) {
      *version = 0;
      return 0;
    } else {
      return r;
    }
  }
  bp.set_length(r);
  bufferlist bl;
  bl.push_back(bp);
  auto i = bl.cbegin();
  decode(*version, i);
  return 0;
}

//释放col_indices及其引入的index
IndexManager::~IndexManager() {

  for (ceph::unordered_map<coll_t, CollectionIndex* > ::iterator it = col_indices.begin();
       it != col_indices.end(); ++it) {

    delete it->second;//调用index的析构函数
    it->second = NULL;
  }
  col_indices.clear();
}


//初始化index
int IndexManager::init_index(coll_t c, const char *path, uint32_t version) {
  RWLock::WLocker l(lock);
  int r = set_version(path, version);//设置path对应的version
  if (r < 0)
    return r;
  //构造coll_t对应的index
  HashIndex index(cct, c, path, cct->_conf->filestore_merge_threshold,
		  cct->_conf->filestore_split_multiple,
		  version,
		  cct->_conf->filestore_index_retry_probability);
  r = index.init();
  if (r < 0)
    return r;
  return index.read_settings();
}

int IndexManager::build_index(coll_t c, const char *path, CollectionIndex **index) {//构造index

  //升级时进入
  if (upgrade) {
    // Need to check the collection generation
    int r;
    uint32_t version = 0;
    r = get_version(path, &version);//取数据目录的版本号
    if (r < 0)
      return r;

    switch (version) {
    case CollectionIndex::FLAT_INDEX_TAG:
    case CollectionIndex::HASH_INDEX_TAG: // fall through
    case CollectionIndex::HASH_INDEX_TAG_2: // fall through
    case CollectionIndex::HOBJECT_WITH_POOL: {
      // Must be a HashIndex
      *index = new HashIndex(cct, c, path,
			     cct->_conf->filestore_merge_threshold,
			     cct->_conf->filestore_split_multiple,
			     version);
      //自index中读取setting的配置
      return (*index)->read_settings();
    }
    default: ceph_abort();
    }

  } else {
    // No need to check
	//非升级情况下，直接创建HashIndex
    *index = new HashIndex(cct, c, path, cct->_conf->filestore_merge_threshold,
			   cct->_conf->filestore_split_multiple,
			   CollectionIndex::HOBJECT_WITH_POOL,
			   cct->_conf->filestore_index_retry_probability);
    return (*index)->read_settings();
  }
}

//查找index,看其在col_indices中是否存在
bool IndexManager::get_index_optimistic(coll_t c, Index *index) {
  RWLock::RLocker l(lock);
  ceph::unordered_map<coll_t, CollectionIndex* > ::iterator it = col_indices.find(c);
  if (it == col_indices.end()) 
    return false;
  index->index = it->second;
  return true;
}

//获取,如果不存在,则创建
int IndexManager::get_index(coll_t c, const string& baseDir, Index *index) {
  //先在col_indices中查一遍，如果命中设置后直接返回
  if (get_index_optimistic(c, index))
    return 0;
  //再检查一遍，此时肯定大概率情况下仍然不存在
  RWLock::WLocker l(lock);
  ceph::unordered_map<coll_t, CollectionIndex* > ::iterator it = col_indices.find(c);
  if (it == col_indices.end()) {
    //仍然查询不出来，需要构造一个index
    //先构造出collection对应的目录名称（绝对路径）
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/current/%s", baseDir.c_str(), c.to_str().c_str());
    CollectionIndex* colIndex = NULL;
    int r = build_index(c, path, &colIndex);//再构造HashIndex类型
    if (r < 0)
      return r;
    col_indices[c] = colIndex;//加入缓存
    index->index = colIndex;//填充index的index数据结构,用于返回出参
  } else {
    index->index = it->second;
  }
  return 0;
}
