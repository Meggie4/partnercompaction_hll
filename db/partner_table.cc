#include "db/partner_table.h"
#include "db/partner_meta.h"
#include "leveldb/env.h"
#include "db/dbformat.h"

/*
    partner table中包含多个partner number, vector<uint64> partnerNumbers 以及一个nvm index,
    get时，首先得到partner在partnerNumbers中的编号，从而获得文件编号，然后根据table cache获取所对应的data block以及在data block中的偏移
    add时，也是根据key的前面一个前缀，来获取相应的编号，首先写到相应的文件中，并获取相应的偏移，然后再写到index中
*/

namespace leveldb {
    PartnerTable::PartnerTable(WritableFile* file, PartnerMeta* meta, 
                               std::vector<uint64_t>& partner_numbers)
    : file_(file), 
      meta_(meta),
      partner_numbers_(partner_numbers){
    }

    void PartnerTable::Add(const Slice& key, const Slice& value) {
        //首先根据前缀写入到相应文件中，获取partner编号，以及在相应data block中的偏移量
        int index = GetKeyIndex(key);
        //怎么能够快速定位呢
        uint64_t block_offset, block_size;
        meta_->Add(key, index, block_offset, block_size);
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
    //     bool find = meta_->Get(lkey, &offset, s);
    //     if(!find) {
    //         return false;
    //     }
    //     return true;
    // }

    void PartnerTable::Finish() {
        file_->Append(Slice(buffer_));
    }
}
