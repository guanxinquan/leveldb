// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.txt for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;//表示2kb（偏移11位）
static const size_t kFilterBase = 1 << kFilterBaseLg;//表示2kb


FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {
}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {//开始一个新的block
  uint64_t filter_index = (block_offset / kFilterBase);//计算当前的block对应的filter_index
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {//计算当前filter之前的所有filter
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());//放入key size
  keys_.append(k.data(), k.size());//key数据追加到keys中
}

Slice FilterBlockBuilder::Finish() {//完成当前builder
  if (!start_.empty()) {//生成最后的filter
    GenerateFilter();//result

  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {//将filter的offsets放入到数据中（固定32长度）
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset);//存放长度（array offset）
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result 存放基数，默认是11（相当于2kb有一个filter）
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();//keys数量
  if (num_keys == 0) {//如果当前filter没有key
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());//直接将result的大小放入filter offsets
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation 将keys的大小放入到start中（这个用于计算最后一个key长度使用，在下面的循环中）
  tmp_keys_.resize(num_keys);//将tmp keys调整到实际key数量的大小
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];//base是当前key的起始位置
    size_t length = start_[i+1] - start_[i];//获取到当前key的长度
    tmp_keys_[i] = Slice(base, length);//构建出第i个key
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size());//将先前result的size放入filter_offsets中
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);//将当前keys生成的filter存放到result中

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

//��ȡfilter�����Ϣ
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,//filter����
                                     const Slice& contents)//contents�а���filter��Ϣ
    : policy_(policy),
      data_(NULL),
      offset_(NULL),
      num_(0),
      base_lg_(0) {
  size_t n = contents.size();
  //����Ҫ��1byte ��Ż�����4byte��Ŵ��
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n-1];//base ��������ʼ��offset array
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);//���һ��word��������ʼarray��ƫ����
  if (last_word > n - 5) return;//���Ӧ�ñ�ʾ����һ����
  data_ = contents.data();//���ݵ��ڵ�ǰ������
  offset_ = data_ + last_word;//ƫ�������ڵ�һ��filter<0>���ֵ�λ��
  num_ = (n - 5 - last_word) / 4;//filter������
}

//过滤器
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;//计算filter的起始位置
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index*4);//filter的起始位置，每个index由4bytes固定长度构成
    uint32_t limit = DecodeFixed32(offset_ + index*4 + 4);//终止位置就是下一个filter的起始位置
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);//获取filter
      return policy_->KeyMayMatch(key, filter);//判断key是否在filter中
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches 无法从过滤器中判断，那面直接返回true，需要读取对应块判断
}

}
