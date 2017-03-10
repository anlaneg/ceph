// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <algorithm>
#include <errno.h>
#include <list>
#include <map>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>

#include "include/buffer.h"
#include "common/errno.h"
#include "common/utf8.h"
#include "common/ConfUtils.h"

using std::cerr;
using std::ostringstream;
using std::pair;
using std::string;

#define MAX_CONFIG_FILE_SZ 0x40000000

////////////////////////////// ConfLine //////////////////////////////
ConfLine::
ConfLine(const std::string &key_, const std::string &val_,
      const std::string &newsection_, const std::string &comment_, int line_no_)
  : key(key_), val(val_), newsection(newsection_)
{
  // If you want to implement writable ConfFile support, you'll need to save
  // the comment and line_no arguments here.
}

bool ConfLine::
operator<(const ConfLine &rhs) const
{
  // We only compare keys.
  // If you have more than one line with the same key in a given section, the
  // last one wins.
  if (key < rhs.key)
    return true;
  else
    return false;
}

std::ostream &operator<<(std::ostream& oss, const ConfLine &l)
{
  oss << "ConfLine(key = '" << l.key << "', val='"
      << l.val << "', newsection='" << l.newsection << "')";
  return oss;
}
///////////////////////// ConfFile //////////////////////////
ConfFile::
ConfFile()
{
}

ConfFile::
~ConfFile()
{
}

void ConfFile::
clear()
{
  sections.clear();
}

/* We load the whole file into memory and then parse it.  Although this is not
 * the optimal approach, it does mean that most of this code can be shared with
 * the bufferlist loading function. Since bufferlists are always in-memory, the
 * load_from_buffer interface works well for them.
 * In general, configuration files should be a few kilobytes at maximum, so
 * loading the whole configuration into memory shouldn't be a problem.
 */
//加载文件fname到内存,然后一行行的进行解析,解析结果存入ConfFile
int ConfFile::
parse_file(const std::string &fname, std::deque<std::string> *errors,
	   std::ostream *warnings)
{
  clear();

  int ret = 0;
  size_t sz;
  char *buf = NULL;
  FILE *fp = fopen(fname.c_str(), "r");
  if (!fp) {
    ret = -errno;
    return ret;
  }

  struct stat st_buf;
  if (fstat(fileno(fp), &st_buf)) {
    ret = -errno;
    ostringstream oss;
    oss << "read_conf: failed to fstat '" << fname << "': " << cpp_strerror(ret);
    errors->push_back(oss.str());
    goto done;
  }

  if (st_buf.st_size > MAX_CONFIG_FILE_SZ) {
    ostringstream oss;
    oss << "read_conf: config file '" << fname << "' is " << st_buf.st_size
	<< " bytes, but the maximum is " << MAX_CONFIG_FILE_SZ;
    errors->push_back(oss.str());
    ret = -EINVAL;
    goto done;
  }

  sz = (size_t)st_buf.st_size;
  buf = (char*)malloc(sz);
  if (!buf) {
    ret = -ENOMEM;
    goto done;
  }

  if (fread(buf, 1, sz, fp) != sz) {
    if (ferror(fp)) {
      ret = -errno;
      ostringstream oss;
      oss << "read_conf: fread error while reading '" << fname << "': "
	  << cpp_strerror(ret);
      errors->push_back(oss.str());
      goto done;
    }
    else {
      ostringstream oss;
      oss << "read_conf: unexpected EOF while reading '" << fname << "': "
	  << "possible concurrent modification?";
      errors->push_back(oss.str());
      ret = -EIO;
      goto done;
    }
  }

  load_from_buffer(buf, sz, errors, warnings);
  ret = 0;

done:
  free(buf);
  fclose(fp);
  return ret;
}

int ConfFile::
parse_bufferlist(ceph::bufferlist *bl, std::deque<std::string> *errors,
		 std::ostream *warnings)
{
  clear();

  load_from_buffer(bl->c_str(), bl->length(), errors, warnings);
  return 0;
}

//获取section段中配置项key的配置值,配置值采用val返回,如果操作成功,返回0,否则操作失败,返回非0
int ConfFile::
read(const std::string &section, const std::string &key, std::string &val) const
{
  string k(normalize_key_name(key));

  const_section_iter_t s = sections.find(section);
  if (s == sections.end())
    return -ENOENT;
  ConfLine exemplar(k, "", "", "", 0);
  ConfSection::const_line_iter_t l = s->second.lines.find(exemplar);
  if (l == s->second.lines.end())
    return -ENOENT;
  val = l->val;
  return 0;
}

ConfFile::const_section_iter_t ConfFile::
sections_begin() const
{
  return sections.begin();
}

ConfFile::const_section_iter_t ConfFile::
sections_end() const
{
  return sections.end();
}

//跳过前后空白字符用.
void ConfFile::
trim_whitespace(std::string &str, bool strip_internal)
{
  // strip preceding
  const char *in = str.c_str();
  while (true) {
    char c = *in;
    if ((!c) || (!isspace(c)))
      break;
    ++in;
  }
  char output[strlen(in) + 1];
  strcpy(output, in);

  // strip trailing
  char *o = output + strlen(output);
  while (true) {
    if (o == output)
      break;
    --o;
    if (!isspace(*o)) {
      ++o;
      *o = '\0';
      break;
    }
  }

  if (!strip_internal) {
    str.assign(output);
    return;
  }

  // strip internal
  char output2[strlen(output) + 1];
  char *out2 = output2;
  bool prev_was_space = false;
  for (char *u = output; *u; ++u) {
    char c = *u;
    if (isspace(c)) {
      if (!prev_was_space)
	*out2++ = c;
      prev_was_space = true;
    }
    else {
      *out2++ = c;
      prev_was_space = false;
    }
  }
  *out2++ = '\0';
  str.assign(output2);
}

/* Normalize a key name.
 *
 * Normalized key names have no leading or trailing whitespace, and all
 * whitespace is stored as underscores.  The main reason for selecting this
 * normal form is so that in common/config.cc, we can use a macro to stringify
 * the field names of md_config_t and get a key in normal form.
 */
//跳前后空白符,将其它空白符,转化为"_"
std::string ConfFile::
normalize_key_name(const std::string &key)
{
  string k(key);
  ConfFile::trim_whitespace(k, true);//跳前后空白符
  std::replace(k.begin(), k.end(), ' ', '_');//将空白符,转化为"_"
  return k;
}

std::ostream &operator<<(std::ostream &oss, const ConfFile &cf)
{
  for (ConfFile::const_section_iter_t s = cf.sections_begin();
       s != cf.sections_end(); ++s) {
    oss << "[" << s->first << "]\n";
    for (ConfSection::const_line_iter_t l = s->second.lines.begin();
	 l != s->second.lines.end(); ++l) {
      if (!l->key.empty()) {
	oss << "\t" << l->key << " = \"" << l->val << "\"\n";
      }
    }
  }
  return oss;
}

//负责填充sections
void ConfFile::
load_from_buffer(const char *buf, size_t sz, std::deque<std::string> *errors,
		 std::ostream *warnings)
{
  errors->clear();

  section_iter_t::value_type vt("global", ConfSection());
  pair < section_iter_t, bool > vr(sections.insert(vt));//默认是global段
  assert(vr.second);
  section_iter_t cur_section = vr.first;
  std::string acc;

  const char *b = buf;
  int line_no = 0;
  size_t line_len = -1;
  size_t rem = sz;
  while (1) {
    b += line_len + 1;//当前分析位置
    if ((line_len + 1) > rem)
      break;
    rem -= line_len + 1;//剩余字节数
    if (rem == 0)
      break;
    line_no++;//行号

    // look for the next newline
    const char *end = (const char*)memchr(b, '\n', rem);
    if (!end) {//这一行说明,配置文件需要以\n结束
      ostringstream oss;
      oss << "read_conf: ignoring line " << line_no << " because it doesn't "
	  << "end with a newline! Please end the config file with a newline.";
      errors->push_back(oss.str());
      break;
    }

    // find length of line, and search for NULLs
    //计算这一行字符串的长度
    line_len = 0;
    bool found_null = false;
    for (const char *tmp = b; tmp != end; ++tmp) {
      line_len++;
      if (*tmp == '\0') {
	found_null = true;
      }
    }

    //如果在一行中发现了'\0',此行需要被忽略
    if (found_null) {
      ostringstream oss;
      oss << "read_conf: ignoring line " << line_no << " because it has "
	  << "an embedded null.";
      errors->push_back(oss.str());
      acc.clear();
      continue;
    }

    //utf8有效检查
    if (check_utf8(b, line_len)) {
      ostringstream oss;
      oss << "read_conf: ignoring line " << line_no << " because it is not "
	  << "valid UTF8.";
      errors->push_back(oss.str());
      acc.clear();
      continue;
    }

    //检查是否有续行符
    if ((line_len >= 1) && (b[line_len-1] == '\\')) {
      // A backslash at the end of a line serves as a line continuation marker.
      // Combine the next line with this one.
      // Remove the backslash itself from the text.
      acc.append(b, line_len - 1);
      continue;
    }

    acc.append(b, line_len);

    //cerr << "acc = '" << acc << "'" << std::endl;
    ConfLine *cline = process_line(line_no, acc.c_str(), errors);
    acc.clear();
    if (!cline)
      continue;
    const std::string &csection(cline->newsection);
    if (!csection.empty()) {
      //发现新的段,加入新段,并更新cur_section
      std::map <std::string, ConfSection>::value_type nt(csection, ConfSection());
      pair < section_iter_t, bool > nr(sections.insert(nt));
      cur_section = nr.first;
    }
    else {
      if (cur_section->second.lines.count(*cline)) {
	// replace an existing key/line in this section, so that
	//  [mysection]
	//    foo = 1
	//    foo = 2
	// will result in foo = 2.
	cur_section->second.lines.erase(*cline);
	if (cline->key.length() && warnings)
	  *warnings << "warning: line " << line_no << ": '" << cline->key << "' in section '"
		    << cur_section->first << "' redefined " << std::endl;
      }
      // add line to current section
      //std::cerr << "cur_section = " << cur_section->first << ", " << *cline << std::endl;
      cur_section->second.lines.insert(*cline);
    }
    delete cline;
  }

  if (!acc.empty()) {
    ostringstream oss;
    oss << "read_conf: don't end with lines that end in backslashes!";
    errors->push_back(oss.str());
  }
}

/*
 * A simple state-machine based parser.
 * This probably could/should be rewritten with something like boost::spirit
 * or yacc if the grammar ever gets more complex.
 */
//采用状态机样式解析inf文件格式(感觉写的有点复杂了,唯一和inf基于流不同的时,要求[section]之后要有注释)
//返回的是这一行的信息
ConfLine* ConfFile::
process_line(int line_no, const char *line, std::deque<std::string> *errors)
{
  enum acceptor_state_t {
    ACCEPT_INIT,//首状态
    ACCEPT_SECTION_NAME,//尝试解析seciton
    ACCEPT_KEY,//尝试解析key
    ACCEPT_VAL_START,//尝试开始解释val
    ACCEPT_UNQUOTED_VAL,//解析非引号引起来的值
    ACCEPT_QUOTED_VAL,//解析引号引起来的值
    ACCEPT_COMMENT_START,//注释开始
    ACCEPT_COMMENT_TEXT,//注释字符串状态
  };
  const char *l = line;
  acceptor_state_t state = ACCEPT_INIT;
  string key, val, newsection, comment;
  bool escaping = false;
  while (true) {
    char c = *l++;
    switch (state) {
      case ACCEPT_INIT:
	if (c == '\0')
	  return NULL; // blank line. Not an error, but not interesting either.
	else if (c == '[')
	  state = ACCEPT_SECTION_NAME;
	else if ((c == '#') || (c == ';'))
	  state = ACCEPT_COMMENT_TEXT;
	else if (c == ']') {//accept_init时,不应以']'开头
	  ostringstream oss;
	  oss << "unexpected right bracket at char " << (l - line)
	      << ", line " << line_no;
	  errors->push_back(oss.str());
	  return NULL;
	}
	else if (isspace(c)) {//忽略前导到空格
	  // ignore whitespace here
	}
	else {
	  // try to accept this character as a key
	  //说明是一个key的起始
	  state = ACCEPT_KEY;
	  --l;
	}
	break;
      case ACCEPT_SECTION_NAME:
	if (c == '\0') {//[后面不能直接结束
	  ostringstream oss;
	  oss << "error parsing new section name: expected right bracket "
	      << "at char " << (l - line) << ", line " << line_no;
	  errors->push_back(oss.str());
	  return NULL;
	}
	else if ((c == ']') && (!escaping)) {//
	  trim_whitespace(newsection, true);//清楚前导空格
	  if (newsection.empty()) {//如果空格,认为无效.
	    ostringstream oss;
	    oss << "error parsing new section name: no section name found? "
	        << "at char " << (l - line) << ", line " << line_no;
	    errors->push_back(oss.str());
	    return NULL;
	  }
	  state = ACCEPT_COMMENT_START;//到达comment状态
	}
	else if (((c == '#') || (c == ';')) && (!escaping)) {//[ 之后不接受注释
	  ostringstream oss;
	  oss << "unexpected comment marker while parsing new section name, at "
	      << "char " << (l - line) << ", line " << line_no;
	  errors->push_back(oss.str());
	  return NULL;
	}
	else if ((c == '\\') && (!escaping)) {//转义处理
	  escaping = true;
	}
	else {
	  escaping = false;
	  newsection += c; //加入到newsection中
	}
	break;
      case ACCEPT_KEY://处理key
	if ((((c == '#') || (c == ';')) && (!escaping)) || (c == '\0')) {//无效处理
	  ostringstream oss;
	  if (c == '\0') {
	    oss << "end of key=val line " << line_no
	        << " reached, no \"=val\" found...missing =?";
	  } else {
	    oss << "unexpected character while parsing putative key value, "
		<< "at char " << (l - line) << ", line " << line_no;
	  }
	  errors->push_back(oss.str());
	  return NULL;
	}
	else if ((c == '=') && (!escaping)) {//发现'='号
	  key = normalize_key_name(key);
	  if (key.empty()) {
	    ostringstream oss;
	    oss << "error parsing key name: no key name found? "
	        << "at char " << (l - line) << ", line " << line_no;
	    errors->push_back(oss.str());
	    return NULL;
	  }
	  state = ACCEPT_VAL_START;//走值处理流程
	}
	else if ((c == '\\') && (!escaping)) {
	  escaping = true;
	}
	else {
	  escaping = false;
	  key += c;
	}
	break;
      case ACCEPT_VAL_START:
	if (c == '\0')
	  return new ConfLine(key, val, newsection, comment, line_no);//没有给值情况
	else if ((c == '#') || (c == ';'))
	  state = ACCEPT_COMMENT_TEXT;
	else if (c == '"')
	  state = ACCEPT_QUOTED_VAL;
	else if (isspace(c)) {
	  // ignore whitespace
	}
	else {
	  // try to accept character as a val
	  state = ACCEPT_UNQUOTED_VAL;
	  --l;
	}
	break;
      case ACCEPT_UNQUOTED_VAL:
	if (c == '\0') {
	  if (escaping) {
	    ostringstream oss;
	    oss << "error parsing value name: unterminated escape sequence "
	        << "at char " << (l - line) << ", line " << line_no;
	    errors->push_back(oss.str());
	    return NULL;
	  }
	  trim_whitespace(val, false);
	  return new ConfLine(key, val, newsection, comment, line_no);//仅key_value情况(非"号值)
	}
	else if (((c == '#') || (c == ';')) && (!escaping)) {
	  trim_whitespace(val, false);
	  state = ACCEPT_COMMENT_TEXT;
	}
	else if ((c == '\\') && (!escaping)) {
	  escaping = true;
	}
	else {
	  escaping = false;
	  val += c;
	}
	break;
      case ACCEPT_QUOTED_VAL:
	if (c == '\0') {
	  ostringstream oss;
	  oss << "found opening quote for value, but not the closing quote. "
	      << "line " << line_no;
	  errors->push_back(oss.str());
	  return NULL;
	}
	else if ((c == '"') && (!escaping)) {
	  state = ACCEPT_COMMENT_START;
	}
	else if ((c == '\\') && (!escaping)) {
	  escaping = true;
	}
	else {
	  escaping = false;
	  // Add anything, including whitespace.
	  val += c;
	}
	break;
      case ACCEPT_COMMENT_START:
	if (c == '\0') {
	  return new ConfLine(key, val, newsection, comment, line_no);//section情况,key-value=""情况,
	}
	else if ((c == '#') || (c == ';')) {
	  state = ACCEPT_COMMENT_TEXT;
	}
	else if (isspace(c)) {
	  // ignore whitespace
	}
	else {
	  ostringstream oss;
	  oss << "unexpected character at char " << (l - line) << " of line "
	      << line_no;
	  errors->push_back(oss.str());
	  return NULL;
	}
	break;
      case ACCEPT_COMMENT_TEXT:
	if (c == '\0')
	  return new ConfLine(key, val, newsection, comment, line_no);//仅注释情况
	else
	  comment += c;
	break;
      default:
	ceph_abort();
	break;
    }
    assert(c != '\0'); // We better not go past the end of the input string.
  }
}
