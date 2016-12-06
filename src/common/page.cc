#include <unistd.h>

namespace ceph {

  // page size crap, see page.h
  int _get_bits_of(int v) {//求v占用的bit位数
    int n = 0;
    while (v) {
      n++;
      v = v >> 1;
    }
    return n;
  }

  unsigned _page_size = sysconf(_SC_PAGESIZE);//页大小
  unsigned long _page_mask = ~(unsigned long)(_page_size - 1);//页大小的反掩码（可与后得出页倍数）
  unsigned _page_shift = _get_bits_of(_page_size - 1);//页占用的最大位数

}
