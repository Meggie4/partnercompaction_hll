#ifndef STORAGE_DB_SINGLE_PARTNER_TABLE_H_
#define STORAGE_DB_SINGLE_PARTNER_TABLE_H_ 
#include <cstdio>
#include <map>
#include <string>
#include "leveldb/slice.h"
#include "db/partner_meta.h"
namespace leveldb {
    class PartnerMeta;
    class TableBuilder;
    class SinglePartnerTable{
        public:
            SinglePartnerTable(TableBuilder* builder, PartnerMeta* meta);
            SinglePartnerTable(const SinglePartnerTable&) = delete;
            void operator=(const SinglePartnerTable&) = delete;

            void Add(const Slice& key, const Slice& value);
            //bool Get(const LookupKey& lkey,  std::string* value, Status* s);
            Status Finish();
            void Abandon();
            uint64_t FileSize();
            size_t NVMSize();
            ~SinglePartnerTable();
            
        private:
            void insertMeta();
            TableBuilder* builder_;
            PartnerMeta* meta_;
            uint64_t curr_blockoffset_;
            uint64_t curr_blocksize_;
            std::vector<std::string> queue_;
    };
}

#endif 
