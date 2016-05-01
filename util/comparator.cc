// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <stdint.h>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"

namespace leveldb {

Comparator::~Comparator() { }

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const {
    return "leveldb.BytewiseComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }

  //这里首先找出start和limit中最小串的长度（用于比较的临界，或者判断是否是完全前缀），之后找出共同串的长度（diff_index），
  //取出diff_index位置start对应的byte，将这个byte进行+1，然后截断start字符串到diff_index。
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());//先取start和limit的最小长度
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }//比较获取相同前缀

    if (diff_index >= min_length) {//这里最多是个等于，如果等于说明一个字符串完全是另一个字符串的前缀
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);//diff_byte就是第一个不一样的byte数据
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;//对应的start的diff_index存储的数据，需要加1
        start->resize(diff_index + 1);//最后resize start为diff_index+1
        assert(Compare(*start, limit) < 0);//确保start比limit小
      }
    }
  }

  //找出距离最近的successor，这东西用于确定key的下一个直接后继的key是多少，即比当前key大一点的key。
  virtual void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

static port::OnceType once = LEVELDB_ONCE_INIT;
static const Comparator* bytewise;

static void InitModule() {
  bytewise = new BytewiseComparatorImpl;
}

const Comparator* BytewiseComparator() {
  port::InitOnce(&once, InitModule);
  return bytewise;
}

}  // namespace leveldb
