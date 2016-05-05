// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {
}

namespace {

// LRU cache implementation

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle {//entry是在heap-allocated分配的可变长度的结构。entries按照访问时间有序存放在一个环形双向链表中
  void* value;//值内容
  void (*deleter)(const Slice&, void* value);//删除函数指针
  LRUHandle* next_hash;//hash table中的单向链表使用的下一个handle指针
  LRUHandle* next;//环形双向链表使用的下一个handle
  LRUHandle* prev;//环形双向链表使用的上一个handle
  size_t charge;      // TODO(opt): Only allow uint32_t? 这个是预估空间，表示当前的元素占用的空间
  size_t key_length;//key的长度，注意，key_data是一个字符的数组，实际是一个指针，这个指针指向key，而key_length是key的长度，you这两个值可以判断实际key是什么
  uint32_t refs;//引用数，使用引用计数方式实现垃圾回收
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons ，用于sharding或者比对
  char key_data[1];   // Beginning of key key的第一个字符指针

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {//如果next与当前相同，那么value表示key
      return *(reinterpret_cast<Slice*>(value));
    } else {//否者获取key
      return Slice(key_data, key_length);
    }
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {//自己实现的hashtable
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);//找出对应的handle的前一个handle指针的指针
    LRUHandle* old = *ptr;//old存放找出的handle指针
    h->next_hash = (old == NULL ? NULL : old->next_hash);//新handle的next_hash指向old的next_hash
    *ptr = h;//变更old前面的指针内容到当前指针
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {//只要elements的数量大于bucket的数量就扩展map。（因此，每个bucket的平均element数量小于1
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;//这个应当是bucket的个数
  uint32_t elems_;//当前广义表的元素个数
  LRUHandle** list_;//这是一个广义表，第一维是一个数组，数组的每个element是一个bucket，每个bucket是一个链表，链表中存放的是hash投递到同一个bucket上的所有entry

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {//查找元素
    LRUHandle** ptr = &list_[hash & (length_ - 1)];//获取bucket
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;//初始为4
    while (new_length < elems_) {//如果长度小于elems的个数，就成倍扩展
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];//创建新的bucket数组
    memset(new_list, 0, sizeof(new_list[0]) * new_length);//初始化数组的指针都是0
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {//将先前的bocket数组中的元素拷贝到新数组中
      LRUHandle* h = list_[i];
      while (h != NULL) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;//清空先前的bucket释放空间
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  void Unref(LRUHandle* e);

  // Initialized before use.
  size_t capacity_;//cache空间大小

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;
  size_t usage_;//已经使用的空间

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  LRUHandle lru_;//虚假的链表头，这个指针一直指向一个头部，这个lru的prev是最新的entry，next是最老的entry

  HandleTable table_;//一个hash table
};

LRUCache::LRUCache()
    : usage_(0) {
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

LRUCache::~LRUCache() {
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->refs == 1);  // Error if caller has an unreleased handle
    Unref(e);
    e = next;
  }
}

void LRUCache::Unref(LRUHandle* e) {//取消引用
  assert(e->refs > 0);
  e->refs--;
  if (e->refs <= 0) {//如果引用小于等于0，释放handle
    usage_ -= e->charge;
    (*e->deleter)(e->key(), e->value);
    free(e);
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {//删掉元素
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* e) {//每次元素都追加在头部
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);//通过hash map获取到handle
  if (e != NULL) {//如果handle不为空
    e->refs++;//引用计数加1
    LRU_Remove(e);//从双向链表中删除handle
    LRU_Append(e);//将handle添加到双向链表的头部
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {//release不一定释放空间，如果release时，ref为0，那么会释放空间
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {//每次插入，如果table中有旧的元素，需要先删除
  MutexLock l(&mutex_);

  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));//那个1是key_data数组的第一个字符，key的剩余部分放在后面
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 2;  // One from LRUCache, one for the returned handle 这里的引用计数是2，一个是返回值使用，一个是LRUCache使用
  memcpy(e->key_data, key.data(), key.size());//将key拷贝到handle的后面
  LRU_Append(e);//追加元素
  usage_ += charge;//

  LRUHandle* old = table_.Insert(e);
  if (old != NULL) {
    LRU_Remove(old);
    Unref(old);
  }

  while (usage_ > capacity_ && lru_.next != &lru_) {//如果使用的量已经超过容量，并且不是空，删除一部分老的，未使用的element
    LRUHandle* old = lru_.next;
    LRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    Unref(old);
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Remove(key, hash);
  if (e != NULL) {
    LRU_Remove(e);
    Unref(e);
  }
}

void LRUCache::Prune() {//清除所有ref为1的handle（ref为1表示只有cache持有对应handle的引用）
  MutexLock l(&mutex_);
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    if (e->refs == 1) {
      table_.Remove(e->key(), e->hash);
      LRU_Remove(e);
      Unref(e);
    }
    e = next;
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];//默认是16个shard
  port::Mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);//计算hash值
  }

  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);//计算hash index
  }

 public:
  explicit ShardedLRUCache(size_t capacity)//总空间要平均分配
      : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache() { }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  virtual void Prune() {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  virtual size_t TotalCharge() const {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

}  // namespace leveldb
