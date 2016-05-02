// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.txt for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;//最开始的1byte用于存放lg(base)，如果base是2kb，那面这个值是11
static const size_t kFilterBase = 1 << kFilterBaseLg;//这个是实际的最大值2048

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {
}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {//开始block，对应的参数是block的偏移量（即第几个block）
  uint64_t filter_index = (block_offset / kFilterBase);//由于一个filter可能对应多个block，因此，这里是计算filter index的地址
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());//先将key的大小放入start
  keys_.append(k.data(), k.size());//在将key的内容放到keys里面
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();//result中存放的是filter的结果
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();//keys的数量
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation，将keys的总长度放在了start的最后
  tmp_keys_.resize(num_keys);//临时keys的大小为先前start的大小，还原整个keys
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i+1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);//将计算结果放到了result中

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

//读取filter相关信息
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,//filter名字
                                     const Slice& contents)//contents中包含filter信息
    : policy_(policy),
      data_(NULL),
      offset_(NULL),
      num_(0),
      base_lg_(0) {
  size_t n = contents.size();
  //至少要有1byte 存放基数，4byte存放存放
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n-1];//base 基数和起始的offset array
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);//最后一个word，就是起始array的偏移量
  if (last_word > n - 5) return;//这个应该表示的是一个空
  data_ = contents.data();//数据等于当前的数据
  offset_ = data_ + last_word;//偏移量等于第一个filter<0>出现的位置
  num_ = (n - 5 - last_word) / 4;//filter的数量
}

//判断key是否在过滤器中
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;//这个是单位，默认是2KB
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index*4);//对应filter的起始index
    uint32_t limit = DecodeFixed32(offset_ + index*4 + 4);//对应filter的下一个index
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}
