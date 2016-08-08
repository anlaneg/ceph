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

#ifndef CEPH_INTARITH_H
#define CEPH_INTARITH_H

#ifndef MIN
#define MIN(a,b) ((a) < (b) ? (a):(b))
#endif

#ifndef MAX
#define MAX(a,b) ((a) > (b) ? (a):(b))
#endif

#ifndef DIV_ROUND_UP
#define DIV_ROUND_UP(n, d)  (((n) + (d) - 1) / (d))
#endif

#ifndef ROUND_UP_TO
#define ROUND_UP_TO(n, d) ((n)%(d) ? ((n)+(d)-(n)%(d)) : (n))
#endif

#ifndef SHIFT_ROUND_UP
#define SHIFT_ROUND_UP(x,y) (((x)+(1<<(y))-1) >> (y))
#endif

/*
 * Macro to determine if value is a power of 2
 */
#define ISP2(x)		(((x) & ((x) - 1)) == 0)

/*
 * Macros for various sorts of alignment and rounding.  The "align" must
 * be a power of 2.  Often times it is a block, sector, or page.
 */

/*
 * return x rounded down to an align boundary
 * eg, P2ALIGN(1200, 1024) == 1024 (1*align)
 * eg, P2ALIGN(1024, 1024) == 1024 (1*align)
 * eg, P2ALIGN(0x1234, 0x100) == 0x1200 (0x12*align)
 * eg, P2ALIGN(0x5600, 0x100) == 0x5600 (0x56*align)
 */
#define P2ALIGN(x, align)		((x) & -(align))

/*
 * return x % (mod) align
 * eg, P2PHASE(0x1234, 0x100) == 0x34 (x-0x12*align)
 * eg, P2PHASE(0x5600, 0x100) == 0x00 (x-0x56*align)
 */
#define P2PHASE(x, align)		((x) & ((align) - 1))

/*
 * return how much space is left in this block (but if it's perfectly
 * aligned, return 0).
 * eg, P2NPHASE(0x1234, 0x100) == 0xcc (0x13*align-x)
 * eg, P2NPHASE(0x5600, 0x100) == 0x00 (0x56*align-x)
 */
#define P2NPHASE(x, align)		(-(x) & ((align) - 1))

/*
 * return x rounded up to an align boundary
 * eg, P2ROUNDUP(0x1234, 0x100) == 0x1300 (0x13*align)
 * eg, P2ROUNDUP(0x5600, 0x100) == 0x5600 (0x56*align)
 */
#define P2ROUNDUP(x, align)		(-(-(x) & -(align)))

// count trailing zeros.
// NOTE: the builtin is nondeterministic on 0 input
static inline unsigned ctz(unsigned v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_ctz(v);
}
static inline unsigned ctzl(unsigned long v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_ctzl(v);
}
static inline unsigned ctzll(unsigned long long v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_ctzll(v);
}

// count leading zeros
// NOTE: the builtin is nondeterministic on 0 input
//返回前导0的数目
static inline unsigned clz(unsigned v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_clz(v);
}
static inline unsigned clzl(unsigned long v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_clzl(v);
}
static inline unsigned clzll(unsigned long long v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_clzll(v);
}

// count bits (set + any 0's that follow)
//采用__builtin_clz来获得前导0的数目,返回来被size(v)*8来减
//于是用来表示v值最高位,占用的位序号.
static inline unsigned cbits(unsigned v) {
  if (v == 0)
    return 0;
  return (sizeof(v) * 8) - __builtin_clz(v);
}
static inline unsigned cbitsl(unsigned long v) {
  if (v == 0)
    return 0;
  return (sizeof(v) * 8) - __builtin_clzl(v);
}
static inline unsigned cbitsll(unsigned long long v) {
  if (v == 0)
    return 0;
  return (sizeof(v) * 8) - __builtin_clzll(v);
}

#endif
