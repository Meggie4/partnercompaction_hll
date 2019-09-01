/*
 *
 *used for metaing PartnerTable 
 *
 */
#ifndef STORAGE_DB_PARTNER_META_H_
#define STORAGE_DB_PARTNER_META_H_
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "util/arena.h"

namespace leveldb {
    class PartnerMetaIterator;
    class PartnerMeta {
        public:
            void* operator new(std::size_t sz);
            void operator delete(void* ptr);
            void *operator new[](std::size_t sz);
            PartnerMeta(const InternalKeyComparator& comparator, ArenaNVM* arena, bool recovery);
            ~PartnerMeta();
            
            void Add(const Slice& key, uint64_t block_offset_, uint64_t block_size);
            bool Get(const LookupKey& key, uint64_t* block_offset, uint64_t* block_size, Status* s);
            size_t ApproximateMemoryUsage();
            Iterator* NewIterator();
           
            Arena* arena_;
            
        private:
            struct KeyComparator {
                const InternalKeyComparator comparator;
                explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {  }
                int operator() (const char* a, const char* b) const;    
            };

            friend class PartnerMetaIterator;
            typedef SkipList<const char*, KeyComparator> Meta;
            
            KeyComparator comparator_;
            Meta meta_;

            PartnerMeta(const PartnerMeta&);
            void operator=(PartnerMeta);
    };
}



#endif 
