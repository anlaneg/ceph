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

#include "auth/Auth.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/version.h"
#include "include/str_list.h"

/*
 * Ceph argument parsing library
 *
 * We probably should eventually replace this with something standard like popt.
 * Until we do that, though, this file is the place for argv parsing
 * stuff to live.
 */

#undef dout
#undef pdout
#undef derr
#undef generic_dout
#undef dendl

struct strict_str_convert {
  const char *str;
  std::string *err;
  strict_str_convert(const char *str,  std::string *err)
    : str(str), err(err) {}

  inline operator float() const
  {
    return strict_strtof(str, err);
  }
  inline operator int() const
  {
    return strict_strtol(str, 10, err);
  }
  inline operator long long() const
  {
    return  strict_strtoll(str, 10, err);
  }
};

void string_to_vec(std::vector<std::string>& args, std::string argstr)
{
  istringstream iss(argstr);
  while(iss) {
    string sub;
    iss >> sub;
    if (sub == "") break;
    args.push_back(sub);
  }
}

//拆分args,将‘--’之前的放入到arguments中,将‘--’之后的放入到options中
bool split_dashdash(const std::vector<const char*>& args,
		    std::vector<const char*>& options,
		    std::vector<const char*>& arguments) {
  bool dashdash = false;
  for (std::vector<const char*>::const_iterator i = args.begin();
       i != args.end();
       ++i) {
    if (dashdash) {
      arguments.push_back(*i);
    } else {
      if (strcmp(*i, "--") == 0)
    	  //找到了'--'
	dashdash = true;
      else
	options.push_back(*i);
    }
  }
  return dashdash;
}

//传入时args中包含了option和argemnt(用--划分),这个函数负责将name环境
//变量中的option,argment进行区分(用--划分),然后再将args与env中的option,argment进行合并.(仍以--划分)
void env_to_vec(std::vector<const char*>& args, const char *name)
{
  if (!name)
    name = "CEPH_ARGS";
  char *p = getenv(name);
  if (!p)
	//不存在'CEPH_ARGS‘环境变量，跳出
    return;

  bool dashdash = false;
  std::vector<const char*> options;
  std::vector<const char*> arguments;
  //在args中查找'--'，将args划分成两部分，options,arguments
  if (split_dashdash(args, options, arguments))
    dashdash = true;

  std::vector<const char*> env_options;
  std::vector<const char*> env_arguments;
  static vector<string> str_vec;
  std::vector<const char*> env;
  str_vec.clear();
  //按空格划分p对应的字符串，将划分结果存入vector中
  get_str_vec(p, " ", str_vec);
  for (vector<string>::iterator i = str_vec.begin();
       i != str_vec.end();
       ++i)
	//向env中加入str_vec集中的字符串指针.
    env.push_back(i->c_str());
  //在env中查找'--'，将env分解为options及arguments
  if (split_dashdash(env, env_options, env_arguments))
    dashdash = true;

  //将参数与环境变量合并，并用'--'划分成option及arguments两部分
  args.clear();
  args.insert(args.end(), options.begin(), options.end());
  args.insert(args.end(), env_options.begin(), env_options.end());//将args变更为options(含env的options)
  if (dashdash)
    args.push_back("--");
  args.insert(args.end(), arguments.begin(), arguments.end());
  args.insert(args.end(), env_arguments.begin(), env_arguments.end());//将args中合入argments
}

//将argv加入到args指定的vector中
void argv_to_vec(int argc, const char **argv,
                 std::vector<const char*>& args)
{
  args.insert(args.end(), argv + 1, argv + argc);
}

void vec_to_argv(const char *argv0, std::vector<const char*>& args,
                 int *argc, const char ***argv)
{
  *argv = (const char**)malloc(sizeof(char*) * (args.size() + 1));
  if (!*argv)
    throw bad_alloc();
  *argc = 1;
  (*argv)[0] = argv0;

  for (unsigned i=0; i<args.size(); i++)
    (*argv)[(*argc)++] = args[i];
}

void ceph_arg_value_type(const char * nextargstr, bool *bool_option, bool *bool_numeric)
{
  bool is_numeric = true;
  bool is_float = false;
  bool is_option;

  if (nextargstr == NULL) {
    return;
  }

  if (strlen(nextargstr) < 2) {
    is_option = false;
  } else {
    is_option = (nextargstr[0] == '-') && (nextargstr[1] == '-');
  }

  for (unsigned int i = 0; i < strlen(nextargstr); i++) {
    if (!(nextargstr[i] >= '0' && nextargstr[i] <= '9')) {
      // May be negative numeral value
      if ((i == 0) && (strlen(nextargstr) >= 2))  {
	if (nextargstr[0] == '-')
	  continue;
      }
      if ( (nextargstr[i] == '.') && (is_float == false) ) {
        is_float = true;
        continue;
      }
        
      is_numeric = false;
      break;
    }
  }

  // -<option>
  if (nextargstr[0] == '-' && is_numeric == false) {
    is_option = true;
  }

  *bool_option = is_option;
  *bool_numeric = is_numeric;

  return;
}

bool parse_ip_port_vec(const char *s, vector<entity_addr_t>& vec)
{
  const char *p = s;
  const char *end = p + strlen(p);
  while (p < end) {
    entity_addr_t a;
    //cout << " parse at '" << p << "'" << std::endl;
    if (!a.parse(p, &p)) {
      //dout(0) << " failed to parse address '" << p << "'" << dendl;
      return false;
    }
    //cout << " got " << a << ", rest is '" << p << "'" << std::endl;
    vec.push_back(a);
    while (*p == ',' || *p == ' ' || *p == ';')
      p++;
  }
  return true;
}

// The defaults for CephInitParameters
CephInitParameters::CephInitParameters(uint32_t module_type_)
  : module_type(module_type_)
{
  name.set(module_type, "admin");
}

//前两个input原样copy到output,如果在'='号之前,遇到'-',则转换为'_'.否则不转换,将结果copy到output
static void dashes_to_underscores(const char *input, char *output)
{
  char c = 0;
  char *o = output;
  const char *i = input;
  // first two characters are copied as-is
  // 前两个符号不进行转换.
  *o = *i++;
  if (*o++ == '\0')
    return;
  *o = *i++;
  if (*o++ == '\0')
    return;
  for (; ((c = *i)); ++i) {
    if (c == '=') {
      strcpy(o, i);//'='号之后的不进行转换
      return;
    }
    if (c == '-')
      *o++ = '_';//'-'号转换为'_'
    else
      *o++ = c;
  }
  *o++ = '\0';
}

/** Once we see a standalone double dash, '--', we should remove it and stop
 * looking for any other options and flags. */
bool ceph_argparse_double_dash(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i)//如果遇到"--"返回True
{
  if (strcmp(*i, "--") == 0) {
    i = args.erase(i);
    return true;
  }
  return false;
}

//检查第i个位置的args,如果第i的数据,与...中的任一个相等,返回TRUE,并删除args中第i个元素.
//如果一直不相等(结束符用NULL表示),则返回False
bool ceph_argparse_flag(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, ...)
{
  const char *first = *i;
  char tmp[strlen(first)+1];
  dashes_to_underscores(first, tmp);
  first = tmp;
  va_list ap;

  va_start(ap, i);
  while (1) {
    const char *a = va_arg(ap, char*);
    if (a == NULL) {
      va_end(ap);
      return false;
    }
    char a2[strlen(a)+1];
    dashes_to_underscores(a, a2);//转换a2
    if (strcmp(a2, first) == 0) {
      i = args.erase(i);//如果a2与first相等,自args中删除first.
      va_end(ap);
      return true;
    }
  }
}

static bool va_ceph_argparse_binary_flag(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, int *ret,
	std::ostream *oss, va_list ap)
{
  const char *first = *i;
  char tmp[strlen(first)+1];
  dashes_to_underscores(first, tmp);
  first = tmp;

  // does this argument match any of the possibilities?
  while (1) {
    const char *a = va_arg(ap, char*);
    if (a == NULL)
      return false;
    int strlen_a = strlen(a);
    char a2[strlen_a+1];
    dashes_to_underscores(a, a2);
    if (strncmp(a2, first, strlen(a2)) == 0) {
      if (first[strlen_a] == '=') {
	i = args.erase(i);
	const char *val = first + strlen_a + 1;
	if ((strcmp(val, "true") == 0) || (strcmp(val, "1") == 0)) {
	  *ret = 1;
	  return true;
	}
	else if ((strcmp(val, "false") == 0) || (strcmp(val, "0") == 0)) {
	  *ret = 0;
	  return true;
	}
	if (oss) {
	  (*oss) << "Parse error parsing binary flag  " << a
	         << ". Expected true or false, but got '" << val << "'\n";
	}
	*ret = -EINVAL;
	return true;
      }
      else if (first[strlen_a] == '\0') {
	i = args.erase(i);
	*ret = 1;
	return true;
      }
    }
  }
}

//同ceph_argparse_flag类似，但要求i位置的参数后面必须有'='号指定的参数
//这个参数需要是二义性（真或假）
bool ceph_argparse_binary_flag(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, int *ret,
	std::ostream *oss, ...)
{
  bool r;
  va_list ap;
  va_start(ap, oss);
  r = va_ceph_argparse_binary_flag(args, i, ret, oss, ap);
  va_end(ap);
  return r;
}

//同ceph_argparse_flag类似,但要求i位置的参数后面必须有'='号指定的参数
//这个参数,将由ret进行返回.
static int va_ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret,
	std::ostream &oss, va_list ap)
{
  const char *first = *i;
  char tmp[strlen(first)+1];
  dashes_to_underscores(first, tmp);
  first = tmp;

  // does this argument match any of the possibilities?
  while (1) {
    const char *a = va_arg(ap, char*);
    if (a == NULL)
      return 0;
    int strlen_a = strlen(a);
    char a2[strlen_a+1];
    dashes_to_underscores(a, a2);
    if (strncmp(a2, first, strlen(a2)) == 0) {
      if (first[strlen_a] == '=') {
	*ret = first + strlen_a + 1;
	i = args.erase(i);
	return 1;
      }
      else if (first[strlen_a] == '\0') {
	// find second part (or not)
	if (i+1 == args.end()) {
	  oss << "Option " << *i << " requires an argument." << std::endl;
	  i = args.erase(i);
	  return -EINVAL;
	}
	i = args.erase(i);
	*ret = *i;
	i = args.erase(i);
	return 1;
      }
    }
  }
}

template<class T>
bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, T *ret,
	std::ostream &oss, ...)
{
  int r;
  va_list ap;
  bool is_option = false;
  bool is_numeric = true;
  std::string str;
  va_start(ap, oss);
  r = va_ceph_argparse_witharg(args, i, &str, oss, ap);
  va_end(ap);
  if (r == 0) {
    return false;
  } else if (r < 0) {
    return true;
  }

  ceph_arg_value_type(str.c_str(), &is_option, &is_numeric);
  if ((is_option == true) || (is_numeric == false)) {
    *ret = EXIT_FAILURE;
    if (is_option == true) {
      oss << "Missing option value";
    } else {
      oss << "The option value '" << str << "' is invalid";
    }
    return true;
  }

  std::string err;
  T myret = strict_str_convert(str.c_str(), &err);
  *ret = myret;
  if (!err.empty()) {
    oss << err;
  }
  return true;
}

template bool ceph_argparse_witharg<int>(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, int *ret,
	std::ostream &oss, ...);

template bool ceph_argparse_witharg<long long>(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, long long *ret,
	std::ostream &oss, ...);

template bool ceph_argparse_witharg<float>(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, float *ret,
	std::ostream &oss, ...);

bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret,
	std::ostream &oss, ...)
{
  int r;
  va_list ap;
  va_start(ap, oss);
  r = va_ceph_argparse_witharg(args, i, ret, oss, ap);
  va_end(ap);
  return r != 0;
}

//对va_ceph_argparse_witharg进行ap整理
bool ceph_argparse_witharg(std::vector<const char*> &args,
	std::vector<const char*>::iterator &i, std::string *ret, ...)
{
  int r;
  va_list ap;
  va_start(ap, ret);
  r = va_ceph_argparse_witharg(args, i, ret, cerr, ap);
  va_end(ap);
  if (r < 0)
    _exit(1);
  return r != 0;
}

//此函数用于识别args中必要数据（属于哪个集群，读那个配置文件)
//进程的名称是什么，通过参数识别后返回。
//conf_file_list为配置文件列表 由-c,--conf参数指定
//cluster设置参数中的cluster 由--cluster参数指定
CephInitParameters ceph_argparse_early_args
	  (std::vector<const char*>& args, uint32_t module_type,
	   std::string *cluster, std::string *conf_file_list)
{
  CephInitParameters iparams(module_type);
  std::string val;

  vector<const char *> orig_args = args;

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
	//不处理parameter,仅处理option
    if (strcmp(*i, "--") == 0) {
      /* Normally we would use ceph_argparse_double_dash. However, in this
       * function we *don't* want to remove the double dash, because later
       * argument parses will still need to see it. */
      break;
    }
    else if (ceph_argparse_flag(args, i, "--version", "-v", (char*)NULL)) {
      //如果有显示版本的选项,则显示版本,并退出.
      cout << pretty_version_to_str() << std::endl;
      _exit(0);
    }
    else if (ceph_argparse_witharg(args, i, &val, "--conf", "-c", (char*)NULL)) {
      //如果指明配置,则设置配置
      *conf_file_list = val;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--cluster", (char*)NULL)) {
      //如果指明cluster,则设置cluster
      *cluster = val;
    }
    else if ((module_type != CEPH_ENTITY_TYPE_CLIENT) &&
	     (ceph_argparse_witharg(args, i, &val, "-i", (char*)NULL))) {
      //如果当前非clinet模块，则设置id
      iparams.name.set_id(val);
    }
    else if (ceph_argparse_witharg(args, i, &val, "--id", "--user", (char*)NULL)) {
      //设置id
      iparams.name.set_id(val);
    }
    else if (ceph_argparse_witharg(args, i, &val, "--name", "-n", (char*)NULL)) {
      //通过字符串设置iparams(type.id格式）
      if (!iparams.name.from_str(val)) {
	cerr << "error parsing '" << val << "': expected string of the form TYPE.ID, "
	     << "valid types are: " << EntityName::get_valid_types_as_str()
	     << std::endl;
	_exit(1);
      }
    }
    else if (ceph_argparse_flag(args, i, "--show_args", (char*)NULL)) {
      //显示相应的参数
      cout << "args: ";
      for (std::vector<const char *>::iterator ci = orig_args.begin(); ci != orig_args.end(); ++ci) {
        if (ci != orig_args.begin())
          cout << " ";
        cout << *ci;
      }
      cout << std::endl;
    }
    else {
      // ignore
    	//忽略掉不认识的.
      ++i;
    }
  }
  return iparams;
}

//显示命令行帮助信息
static void generic_usage(bool is_server)
{
  cout << "\
  --conf/-c FILE    read configuration from the given configuration file\n\
  --id/-i ID        set ID portion of my name\n\
  --name/-n TYPE.ID set name\n\
  --cluster NAME    set cluster name (default: ceph)\n\
  --setuser USER    set uid to user or uid (and gid to user's gid)\n\
  --setgroup GROUP  set gid to group or gid\n\
  --version         show version and quit\n\
" << std::endl;

  if (is_server) {
    cout << "\
  -d                run in foreground, log to stderr.\n\
  -f                run in foreground, log to usual location.\n";
    cout << "\
  --debug_ms N      set message debug level (e.g. 1)\n";
  }

  cout.flush();
}

void generic_server_usage()
{
  generic_usage(true);
  exit(1);
}
void generic_client_usage()
{
  generic_usage(false);
  exit(1);
}
