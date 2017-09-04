// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/pick_address.h"
#include "include/ipaddr.h"
#include "include/str_list.h"
#include "common/debug.h"
#include "common/errno.h"

#include <netdb.h>

#define dout_subsys ceph_subsys_

//在ifa中找一个属于networks网络中的ip{netowrks可能有多个子网，用';,= \t'隔开
static const struct sockaddr *find_ip_in_subnet_list(CephContext *cct,
						     const struct ifaddrs *ifa,
						     const std::string &networks)
{
  std::list<string> nets;
  get_str_list(networks, nets);//split string by ',;'

  for(std::list<string>::iterator s = nets.begin(); s != nets.end(); ++s) {
      struct sockaddr_storage net;
      unsigned int prefix_len;

      if (!parse_network(s->c_str(), &net, &prefix_len)) {//解释格式a.a.a.a/32
	lderr(cct) << "unable to parse network: " << *s << dendl;
	exit(1);
      }

      const struct ifaddrs *found = find_ip_in_subnet(ifa,
                                      (struct sockaddr *) &net, prefix_len);
      if (found)
	return found->ifa_addr;
    }

  return NULL;
}

// observe this change
struct Observer : public md_config_obs_t {
  const char *keys[2];
  explicit Observer(const char *c) {
    keys[0] = c;
    keys[1] = NULL;
  }

  const char** get_tracked_conf_keys() const override {
    return (const char **)keys;
  }
  void handle_conf_change(const struct md_config_t *conf,
			  const std::set <std::string> &changed) override {
    // do nothing.
  }
};

static void fill_in_one_address(CephContext *cct,
				const struct ifaddrs *ifa,
				const string networks,
				const char *conf_var)
{
  const struct sockaddr *found = find_ip_in_subnet_list(cct, ifa, networks);
  if (!found) {//如果没有找到，退出
    lderr(cct) << "unable to find any IP address in networks: " << networks << dendl;
    exit(1);
  }

  char buf[INET6_ADDRSTRLEN];
  int err;

  err = getnameinfo(found,
		    (found->sa_family == AF_INET)
		    ? sizeof(struct sockaddr_in)
		    : sizeof(struct sockaddr_in6),

		    buf, sizeof(buf),
		    NULL, 0,
		    NI_NUMERICHOST);
  if (err != 0) {
    lderr(cct) << "unable to convert chosen address to string: " << gai_strerror(err) << dendl;
    exit(1);
  }

  Observer obs(conf_var);

  cct->_conf->add_observer(&obs);

  cct->_conf->set_val_or_die(conf_var, buf);
  cct->_conf->apply_changes(NULL);

  cct->_conf->remove_observer(&obs);
}

//选择公网ip及集群Ip
void pick_addresses(CephContext *cct, int needs)
{
  struct ifaddrs *ifa;
  int r = getifaddrs(&ifa);//拿到所有接口
  if (r<0) {
    string err = cpp_strerror(errno);
    lderr(cct) << "unable to fetch interfaces and addresses: " << err << dendl;
    exit(1);
  }


  if ((needs & CEPH_PICK_ADDRESS_PUBLIC)
		  //如果要选public地址，且没有给出public_addr,给出了public_network
      && cct->_conf->public_addr.is_blank_ip()
      && !cct->_conf->public_network.empty()) {
    fill_in_one_address(cct, ifa, cct->_conf->public_network, "public_addr");
  }

  if ((needs & CEPH_PICK_ADDRESS_CLUSTER)
      //如果要选cluster地址，且没有给出cluster_addr,给出了cluster_network
      && cct->_conf->cluster_addr.is_blank_ip()) {
    if (!cct->_conf->cluster_network.empty()) {
      fill_in_one_address(cct, ifa, cct->_conf->cluster_network, "cluster_addr");
    } else {
      if (!cct->_conf->public_network.empty()) {
        lderr(cct) << "Public network was set, but cluster network was not set " << dendl;
        lderr(cct) << "    Using public network also for cluster network" << dendl;
        fill_in_one_address(cct, ifa, cct->_conf->public_network, "cluster_addr");
      }
    }
  }

  freeifaddrs(ifa);
}


std::string pick_iface(CephContext *cct, const struct sockaddr_storage &network)
{
  struct ifaddrs *ifa;
  int r = getifaddrs(&ifa);
  if (r < 0) {
    string err = cpp_strerror(errno);
    lderr(cct) << "unable to fetch interfaces and addresses: " << err << dendl;
    return {};
  }

  unsigned int prefix_len = 0;
  const struct ifaddrs *found = find_ip_in_subnet(ifa,
                                  (const struct sockaddr *) &network, prefix_len);

  std::string result;
  if (found) {
    result = found->ifa_name;
  }

  freeifaddrs(ifa);

  return result;
}


bool have_local_addr(CephContext *cct, const list<entity_addr_t>& ls, entity_addr_t *match)
{
  struct ifaddrs *ifa;
  int r = getifaddrs(&ifa);
  if (r < 0) {
    lderr(cct) << "unable to fetch interfaces and addresses: " << cpp_strerror(errno) << dendl;
    exit(1);
  }

  bool found = false;
  for (struct ifaddrs *addrs = ifa; addrs != NULL; addrs = addrs->ifa_next) {
    if (addrs->ifa_addr) {
      entity_addr_t a;
      a.set_sockaddr(addrs->ifa_addr);
      for (list<entity_addr_t>::const_iterator p = ls.begin(); p != ls.end(); ++p) {
        if (a.is_same_host(*p)) {
          *match = *p;
          found = true;
          goto out;
        }
      }
    }
  }

 out:
  freeifaddrs(ifa);
  return found;
}
