// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/str_list.h"

using std::string;
using std::vector;
using std::set;
using std::list;
using ceph::for_each_substr;

//将str依delims进行划分,划分结果存入str_list
void get_str_list(const string& str, const char *delims, list<string>& str_list)
{
  str_list.clear();
  for_each_substr(str, delims, [&str_list] (auto token) {
      str_list.emplace_back(token.begin(), token.end());
    });
}

//按";,= \t"划分str,并将结果存入str_list
void get_str_list(const string& str, list<string>& str_list)
{
  const char *delims = ";,= \t";
  get_str_list(str, delims, str_list);
}

list<string> get_str_list(const string& str, const char *delims)
{
  list<string> result;
  get_str_list(str, delims, result);
  return result;
}

//用delims划分str,并将划分结果存入到str_vec中.(同get_str_list一样,但集合类型不同)
void get_str_vec(const string& str, const char *delims, vector<string>& str_vec)
{
  str_vec.clear();
  for_each_substr(str, delims, [&str_vec] (auto token) {
      str_vec.emplace_back(token.begin(), token.end());
    });
}

//用";,= \t"进行划分
void get_str_vec(const string& str, vector<string>& str_vec)
{
  const char *delims = ";,= \t";
  get_str_vec(str, delims, str_vec);
}

//另一种类型
vector<string> get_str_vec(const string& str, const char *delims)
{
  vector<string> result;
  get_str_vec(str, delims, result);
  return result;
}

void get_str_set(const string& str, const char *delims, set<string>& str_set)
{
  str_set.clear();
  for_each_substr(str, delims, [&str_set] (auto token) {
      str_set.emplace(token.begin(), token.end());
    });
}

//set类型的划分.
void get_str_set(const string& str, set<string>& str_set)
{
  const char *delims = ";,= \t";
  get_str_set(str, delims, str_set);
}

set<string> get_str_set(const string& str, const char *delims)
{
  set<string> result;
  get_str_set(str, delims, result);
  return result;
}
