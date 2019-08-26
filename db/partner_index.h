/*
 *
 *used for indexing PartnerTable 
 *
 */
#ifndef STORAGE_DB_PARTNER_INDEX_H_
#define STORAGE_DB_PARTNER_INDEX_H_
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "util/arena.h"

namespace leveldb {
    class PartnerIndexIterator;
    class PartnerIndex {
        public:
            void* operator new(std::size_t sz);
            void operator delete(void* ptr);
            void *operator new[](std::size_t sz);
            PartnerIndex(const InternalKeyComparator& comparator, ArenaNVM* arena, bool recovery);
            
            void Ref() {
                ++refs_;
            } 

            void Unref() {
                --refs_;
                assert(refs_ >= 0);
                if (refs_ <= 0) {
                    DEBUG_T("to delete partner index\n");
                    delete this;
                    DEBUG_T("after delete partner index\n");
                } 
            }
            
            void Add(const Slice& key, uint32_t partner_number, uint64_t offset);
            bool Get(const LookupKey& key, uint32_t* partner_number, uint64_t* offset, Status* s);
            size_t ApproximateMemoryUsage();
            Iterator* NewIterator();
           
            Arena* arena_;
            
        private:
            ~PartnerIndex();
            struct KeyComparator {
                const InternalKeyComparator comparator;
                explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {  }
                int operator() (const char* a, const char* b) const;    
            };

            friend class PartnerIndexIterator;
            typedef SkipList<const char*, KeyComparator> Index;
            
            KeyComparator comparator_;
            int refs_;
            Index index_;

            PartnerIndex(const PartnerIndex&);
            void operator=(PartnerIndex);
    };
}



#endif 
