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
#include "db/partner_index.h"
namespace leveldb {
    class PartnerIndex;
    class WritableFile;
    class PartnerTable{
        public:
            PartnerTable(WritableFile* file, PartnerIndex* index, 
                         uint64_t offset, int64_t num_entries);
            PartnerTable(const PartnerTable&) = delete;
            void operator=(const PartnerTable&) = delete;
            
            void Add(const Slice& key, uint32_t partner_number, const Slice& value);
            //bool Get(const LookupKey& lkey,  std::string* value, Status* s);
            void Finish();
            
            uint64_t NumEntries() const {return num_entries_;}
            uint64_t FileSize() const {return offset_;}
            void Ref() {
                index_->Ref();
            } 

            void Unref() {
                index_->Unref();
            }
        private:
            WritableFile* file_;
            PartnerIndex* index_;
            int64_t num_entries_;
            uint64_t offset_;
            std::string buffer_;
    };
}

#endif 
