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
            SinglePartnerTable(uint64_t partner_number, TableBuilder* builder, PartnerMeta* meta);
            SinglePartnerTable(const SinglePartnerTable&) = delete;
            void operator=(const SinglePartnerTable&) = delete;
            ~SinglePartnerTable();

            void Add(const Slice& key, const Slice& value);
            //bool Get(const LookupKey& lkey,  std::string* value, Status* s);
            void Finish();
            
        private:
            void insertMeta();
            TableBuilder* builder_;
            uint64_t partner_number_;
            PartnerMeta* meta_;
            uint64_t curr_blockoffset_;
            uint64_t curr_blocksize_;
            std::vector<Slice> queue_;
    };
}

#endif 
