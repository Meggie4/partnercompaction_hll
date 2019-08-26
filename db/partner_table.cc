#include "db/partner_table.h"
#include "db/partner_index.h"
#include "leveldb/env.h"
#include "db/dbformat.h"

/*
    partner table中包含多个partner number, vector<uint64> partnerNumbers 以及一个nvm index,
    get时，首先得到partner在partnerNumbers中的编号，从而获得文件编号
*/

namespace leveldb {
    PartnerTable::PartnerTable(WritableFile* file, PartnerIndex* index, 
                               uint64_t offset, int64_t num_entries)
    : file_(file), 
      index_(index),
      num_entries_(num_entries), 
      offset_(offset){
    }

    void PartnerTable::Add(const Slice& key, uint32_t partner_number, const Slice& value) {
        index_->Add(key, partner_number, offset_);
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

    // bool PartnerTable::Get(const LookupKey& lkey,  std::string* value, Status* s)  {
    //     uint64_t offset;
    //     bool find = index_->Get(lkey, &offset, s);
    //     if(!find) {
    //         return false;
    //     }
    //     return true;
    // }

    void PartnerTable::Finish() {
        file_->Append(Slice(buffer_));
    }
}
