#include "db/partner_table.h"
#include "db/partner_index.h"
#include "leveldb/env.h"
#include "db/dbformat.h"

namespace leveldb {
    PartnerTable::PartnerTable(WritableFile* file, PartnerIndex* index, 
                               uint64_t offset, int64_t num_entries)
    : file_(file), 
      index_(index),
      num_entries_(num_entries), 
      offset_(offset) {
    }

    void PartnerTable::Add(const Slice& key, const Slice& value) {
        index_->Add(key, offset_);
        size_t key_size = key.size();
        size_t value_size = value.size();
        const size_t encoded_len = VarintLength(key_size) + key_size +
            VarintLength(value_size) + value_size;
        
        PutVarint32(&buffer_, key_size);
        buffer_.append(key.data(), key.size());
        PutVarint32(&buffer_, value_size);
        buffer_.append(value.data(), value.size());
        
        offset_ += encoded_len;
        num_entries_++;
    }

    void PartnerTable::Finish() {
        file_->Append(Slice(buffer_));
    }
}
