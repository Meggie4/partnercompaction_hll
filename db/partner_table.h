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
namespace leveldb {
    class PartnerIndex;
    class WritableFile;
    class PartnerTable{
        public:
            PartnerTable(WritableFile* file, PartnerIndex* index, 
                         uint64_t offset, int64_t num_entries);
            PartnerTable(const PartnerTable&) = delete;
            void operator=(const PartnerTable&) = delete;
            
            void Add(const Slice& key, const Slice& value);
            void Finish();
            
            uint64_t NumEntries() const {return num_entries_;}
            uint64_t FileSize() const {return offset_;}
        private:
            WritableFile* file_;
            PartnerIndex* index_;
            int64_t num_entries_;
            uint64_t offset_;
            std::string buffer_;
    };
}

#endif 
