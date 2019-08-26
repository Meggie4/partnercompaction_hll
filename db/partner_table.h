/**
 *used for kv in ssd, and index in nvm
 *
 *
 */
#ifndef STORAGE_DB_PARTNER_TABLE_H_
#define STORAGE_DB_PARTNER_TABLE_H_ 
#include <cstdio>
#include "leveldb/slice.h"
#include <string>
#include "db/partner_meta.h"
namespace leveldb {
    class PartnerMeta;
    class WritableFile;
    class PartnerTable{
        public:
            PartnerTable(WritableFile* file, PartnerMeta* meta, 
                         std::vector<uint64_t>& partner_numbers);
            PartnerTable(const PartnerTable&) = delete;
            void operator=(const PartnerTable&) = delete;
            
            void Add(const Slice& key, const Slice& value);
            //bool Get(const LookupKey& lkey,  std::string* value, Status* s);
            void Finish();
            
            uint64_t NumEntries() const {return num_entries_;}
            uint64_t FileSize() const {return offset_;}
            int GetKeyIndex(const Slice& key) {
                char start = key.data()[4];
                int index = start - '0' - 1;
                return index;
            }
            void Ref() {
                meta_->Ref();
            } 

            void Unref() {
                meta_->Unref();
            }
        private:
            WritableFile* file_;
            PartnerMeta* meta_;
            int64_t num_entries_;
            std::vector<uint64_t> partner_numbers_;
            uint64_t offset_;
            std::string buffer_;
    };
}

#endif 
