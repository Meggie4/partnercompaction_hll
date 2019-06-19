/**
 *used for kv in ssd, and index in nvm
 *
 *
 */
#ifndef STORAGE_DB_PARTNER_TABLE_H_
#define STORAGE_DB_PARTNER_TABLE_H_ 
#include "db/dbformat.h"

namespace leveldb {
    class PartnerIndex;
    class PartnerTable{
        public:
            PartnerTable(WritableFile* file, PartnerIndex* pindex);
            PartnerTable(const PartnerTable&) = delete;
            void operator=(const PartnerTable&) = delete;
            
            void Add(const Slice& key, const Slice& value);
            
            Status Finish();
            uint64_t NumEntries() const;
            uint64_t FileSize() const;
        private:
            int64_t num_entries;
            uint64_t offset;
    };
}

#endif 
