// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include "PluginRegistry.h"
#include "ceph_ver.h"
#include "common/ceph_context.h"
#include "common/errno.h"
#include "common/debug.h"

#include <dlfcn.h>

#define PLUGIN_PREFIX "libceph_"
#ifdef __APPLE__
#define PLUGIN_SUFFIX ".dylib"
#else
#define PLUGIN_SUFFIX ".so"
#endif
#define PLUGIN_INIT_FUNCTION "__ceph_plugin_init"
#define PLUGIN_VERSION_FUNCTION "__ceph_plugin_version"

#define dout_subsys ceph_subsys_context

PluginRegistry::PluginRegistry(CephContext *cct) :
  cct(cct),
  lock("PluginRegistry::lock"),
  loading(false),
  disable_dlclose(false)
{
}

PluginRegistry::~PluginRegistry()
{
  if (disable_dlclose)
    return;

  for (std::map<std::string,std::map<std::string, Plugin*> >::iterator i =
	 plugins.begin();
       i != plugins.end();
       ++i) {
    for (std::map<std::string,Plugin*>::iterator j = i->second.begin();
	 j != i->second.end(); ++j) {
      void *library = j->second->library;
      delete j->second;
      dlclose(library);
    }
  }
}

int PluginRegistry::remove(const std::string& type, const std::string& name)
{
  ceph_assert(lock.is_locked());

  std::map<std::string,std::map<std::string,Plugin*> >::iterator i =
    plugins.find(type);
  if (i == plugins.end())
    return -ENOENT;
  std::map<std::string,Plugin*>::iterator j = i->second.find(name);
  if (j == i->second.end())
    return -ENOENT;

  ldout(cct, 1) << __func__ << " " << type << " " << name << dendl;
  void *library = j->second->library;
  delete j->second;
  dlclose(library);
  i->second.erase(j);
  if (i->second.empty())
    plugins.erase(i);

  return 0;
}

int PluginRegistry::add(const std::string& type,
			const std::string& name,
			Plugin* plugin)
{
  ceph_assert(lock.is_locked());
  if (plugins.count(type) &&
      plugins[type].count(name)) {
    return -EEXIST;
  }
  ldout(cct, 1) << __func__ << " " << type << " " << name
		<< " " << plugin << dendl;
  plugins[type][name] = plugin;
  return 0;
}

//找到指定插件，并将其载入
Plugin *PluginRegistry::get_with_load(const std::string& type,
          const std::string& name)
{
  std::lock_guard<Mutex> l(lock);
  Plugin* ret = get(type, name);//查找是否存在这个插件
  if (!ret) {
    int err = load(type, name);
    if (err == 0)
      ret = get(type, name);//本来直接返回ret即可，为什么需要再查一次？
  } 
  return ret;
}

//插件被按照类型划分放置在plugins变量中，类型是key,插件是value（map<std::string,Plugin*>类型）
Plugin *PluginRegistry::get(const std::string& type,
			    const std::string& name)
{
  ceph_assert(lock.is_locked());
  Plugin *ret = 0;

  std::map<std::string,Plugin*>::iterator j;
  std::map<std::string,map<std::string,Plugin*> >::iterator i =
    plugins.find(type);
  if (i == plugins.end()) 
    goto out;
  j = i->second.find(name);
  if (j == i->second.end()) 
    goto out;
  ret = j->second;

 out:
  ldout(cct, 1) << __func__ << " " << type << " " << name
		<< " = " << ret << dendl;
  return ret;
}

//装载指定类型，指定名称的plugin
int PluginRegistry::load(const std::string &type,
			 const std::string &name)
{
  ceph_assert(lock.is_locked());
  ldout(cct, 1) << __func__ << " " << type << " " << name << dendl;

  // std::string fname = cct->_conf->plugin_dir + "/" + type + "/" PLUGIN_PREFIX
  //  + name + PLUGIN_SUFFIX;
  //插件目录及名称格式
  std::string fname = cct->_conf.get_val<std::string>("plugin_dir") + "/" + type + "/" + PLUGIN_PREFIX
      + name + PLUGIN_SUFFIX;
  void *library = dlopen(fname.c_str(), RTLD_NOW);
  if (!library) {
    string err1(dlerror());
    // fall back to plugin_dir
    std::string fname2 = cct->_conf.get_val<std::string>("plugin_dir") + "/" + PLUGIN_PREFIX +
      name + PLUGIN_SUFFIX;
    library = dlopen(fname2.c_str(), RTLD_NOW);
    if (!library) {
      lderr(cct) << __func__
		 << " failed dlopen(): \""	<< err1.c_str() 
		 << "\" or \"" << dlerror() << "\""
		 << dendl;
      return -EIO;
    }
  }

  const char * (*code_version)() =
    (const char *(*)())dlsym(library, PLUGIN_VERSION_FUNCTION);
  if (code_version == NULL) {
    lderr(cct) << __func__ << " code_version == NULL" << dlerror() << dendl;
    return -EXDEV;
  }
  if (code_version() != string(CEPH_GIT_NICE_VER)) {//检查版本
    lderr(cct) << __func__ << " plugin " << fname << " version "
	       << code_version() << " != expected "
	       << CEPH_GIT_NICE_VER << dendl;
    dlclose(library);
    return -EXDEV;
  }

  int (*code_init)(CephContext *,
		   const std::string& type,
		   const std::string& name) =
    (int (*)(CephContext *,
	     const std::string& type,
	     const std::string& name))dlsym(library, PLUGIN_INIT_FUNCTION);
  if (code_init) {//作初始化
    int r = code_init(cct, type, name);
    if (r != 0) {
      lderr(cct) << __func__ << " " << fname << " "
		 << PLUGIN_INIT_FUNCTION << "(" << cct
		 << "," << type << "," << name << "): " << cpp_strerror(r)
		 << dendl;
      dlclose(library);
      return r;
    }
  } else {
    lderr(cct) << __func__ << " " << fname << " dlsym(" << PLUGIN_INIT_FUNCTION
	       << "): " << dlerror() << dendl;
    dlclose(library);
    return -ENOENT;
  }

  Plugin *plugin = get(type, name);
  if (plugin == 0) {//没有注册的插件
    lderr(cct) << __func__ << " " << fname << " "
	       << PLUGIN_INIT_FUNCTION << "()"
	       << "did not register plugin type " << type << " name " << name
	       << dendl;
    dlclose(library);
    return -EBADF;
  }

  plugin->library = library;//设置so句柄

  ldout(cct, 1) << __func__ << ": " << type << " " << name
		<< " loaded and registered" << dendl;
  return 0;
}

/*
int ErasureCodePluginRegistry::preload(const std::string &plugins,
				       const std::string &directory,
				       ostream &ss)
{
  std::lock_guard<Mutex> l(lock);
  list<string> plugins_list;
  get_str_list(plugins, plugins_list);
  for (list<string>::iterator i = plugins_list.begin();
       i != plugins_list.end();
       ++i) {
    ErasureCodePlugin *plugin;
    int r = load(*i, directory, &plugin, ss);
    if (r)
      return r;
  }
  return 0;
}
*/
