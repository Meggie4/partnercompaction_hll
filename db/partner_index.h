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
    class PartnerIndex {
        public:
            PartnerIndex(const InternalKeyComparator& comparator);
            
    };
}



#endif 
