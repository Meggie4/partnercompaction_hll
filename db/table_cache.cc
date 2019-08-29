// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"
#include "db/partner_meta.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options& options,
                       int entries, 
                       const std::string& dbname_nvm)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)), 
      dbname_nvm_(dbname_nvm) {
}

TableCache::~TableCache() {
  delete cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle, bool isPartnerTable) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      //////////////meggie
      if(isPartnerTable) {
        s = Table::OpenPartnerTable(options_, file, &table);
      } else {
        s = Table::Open(options_, file, file_size, &table);
      }
      //////////////meggie
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle);
  }
  return s;
}

//////////////meggie
static void UnrefPartnerMeta(void* arg1, void* arg2) {
  PartnerMeta* pm = reinterpret_cast<PartnerMeta*>(arg1);
  pm->Unref();
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&), 
                       uint64_t block_offset, 
                       uint64_t block_size) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, 0, &handle, true);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver, block_offset, block_size);
    cache_->Release(handle);
  }
  return s;
}

Iterator* TableCache::NewPartnerIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t meta_number, 
                                  uint64_t meta_size,
                                  Table** tableptr) {                              
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  std::string metaFile = MapFileName(dbname_nvm_, meta_number);  
  ArenaNVM* arena = new ArenaNVM(meta_size, &metaFile, true);
  arena->nvmarena_ = true;
  DEBUG_T("after get arena nvm\n");
  PartnerMeta* pm = new PartnerMeta(*(reinterpret_cast<const InternalKeyComparator *>(options_.comparator)), arena, true);
  Iterator* meta_iter = pm->NewIterator();
  meta_iter->RegisterCleanup(&UnrefPartnerMeta, pm, nullptr);
  pm->Ref();

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, 0, &handle, true);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewPartnerIterator(options, meta_iter);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}
/////////////meggie

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
