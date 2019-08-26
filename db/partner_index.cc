#include "db/partner_index.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "db/skiplist.h"
#include "port/cache_flush.h"
#include <cstdio>
#include <gnuwrapper.h>
#include <string>
#include "util/debug.h"

namespace leveldb {
    static Slice GetLengthPrefixedSlice(const char* data) {
        uint32_t len;
        const char* p = data;
        p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
        return Slice(p, len);
    }
    
    void* PartnerIndex::operator new(std::size_t sz) {
        return malloc(sz);
    } 

    void* PartnerIndex::operator new[](std::size_t sz) {
        return malloc(sz);
    }

    void PartnerIndex::operator delete(void* ptr) {
        free(ptr);
    }

    PartnerIndex::PartnerIndex(const InternalKeyComparator& cmp, 
        ArenaNVM* arena, bool recovery) 
        : comparator_(cmp), 
          refs_(0), 
          arena_(arena), 
          index_(comparator_, arena, recovery){          
    }

    PartnerIndex::~PartnerIndex() {
        assert(refs_ == 0);
        if(arena_){
            DEBUG_T("before delete arena\n");
            delete arena_;
            DEBUG_T("after delete arena\n");
        }
    }

    size_t PartnerIndex::ApproximateMemoryUsage() {
        return arena_->MemoryUsage();
    }

    int PartnerIndex::KeyComparator::operator()(const char* aptr, 
            const char* bptr) const {
        Slice a = GetLengthPrefixedSlice(aptr);
        Slice b = GetLengthPrefixedSlice(bptr);
        return comparator.Compare(a, b);
    }

    static const char* EncodeKey(std::string* scratch, const Slice& target) {
        scratch->clear();
        PutVarint32(scratch, target.size());
        scratch->append(target.data(), target.size());
        return scratch->data();
    }

    class PartnerIndexIterator: public Iterator {
    public:
        explicit PartnerIndexIterator(PartnerIndex::Index* index) : iter_(index) { }

        virtual bool Valid() const { return iter_.Valid(); }
        virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
        virtual void SeekToFirst() { iter_.SeekToFirst(); }
        virtual void SeekToLast() { iter_.SeekToLast(); }
        virtual void Next() { iter_.Next(); }
        virtual void Prev() { iter_.Prev(); }

#ifdef USE_OFFSETS
        virtual char *GetNodeKey(){return reinterpret_cast<char *>((intptr_t)iter_.node_ - (intptr_t)iter_.key_offset()); }
#else
        virtual char *GetNodeKey(){return iter_.key(); }
#endif

#if defined(USE_OFFSETS)
        virtual Slice key() const { return GetLengthPrefixedSlice(reinterpret_cast<const char *>((intptr_t)iter_.node_ - (intptr_t)iter_.key_offset())); }
#else
        virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
#endif
        virtual Slice value() const {
#if defined(USE_OFFSETS)
            Slice key_slice = GetLengthPrefixedSlice(reinterpret_cast<const char *>((intptr_t)iter_.node_ - (intptr_t)iter_.key_offset()));
#else
            Slice key_slice = GetLengthPrefixedSlice(iter_.key());
#endif
            return Slice(key_slice.data() + key_slice.size(), 8);
        }
        //NoveLSM
        //virtual void SetHead(void *ptr) { iter_.SetHead(ptr); }
        void* operator new(std::size_t sz) {
            return malloc(sz);
        }

        void* operator new[](std::size_t sz) {
            return malloc(sz);
        }
        void operator delete(void* ptr)
        {
            free(ptr);
        }
        virtual Status status() const { return Status::OK(); }

    private:
        PartnerIndex::Index::Iterator iter_;
        std::string tmp_;       // For passing to EncodeKey

        // No copying allowed
        PartnerIndexIterator(const PartnerIndexIterator&);
        void operator=(const PartnerIndexIterator&);
    };

    Iterator* PartnerIndex::NewIterator() {
        return new PartnerIndexIterator(&index_);
    }

    //这里的partner number指的是0~9中
    void PartnerIndex::Add(const Slice& key, uint32_t partner_number, uint64_t offset) {
       ///存储的形式是：key_size + key + partner_number(varint 32) + offset(64)
       size_t key_size = key.size();
       const size_t encoded_len = VarintLength(key_size) + key_size + VarintLength(partner_number) + 8;
       char* buf = nullptr;
       
       buf = ((ArenaNVM*)arena_)->AllocateAlignedNVM(encoded_len);
       
       if(!buf) {
           perror("Memory allocation failed");
           exit(-1);
       }

       char* p = EncodeVarint32(buf, key_size);
       
       DEBUG_T("add key is %s\n", key.ToString().c_str());
       memcpy_persist(p, key.data(), key_size);
        
       p += key_size;
       
       p = EncodeVarint32(p, partner_number);
       DEBUG_T("add offset is %d\n", offset);
       EncodeFixed64(p, offset);
       p += 8;

       assert(p - buf == encoded_len);

       index_.Insert(buf);
    }

    bool PartnerIndex::Get(const LookupKey& key, uint32_t* partner_number,  uint64_t* offset, Status* s) {
        Slice memkey = key.memtable_key();
        Index::Iterator iter(&index_);
        DEBUG_T("memtable key is %s\n", memkey.ToString().c_str());
        iter.Seek(memkey.data());
        
        if(iter.Valid()) {
            DEBUG_T("find!\n");
#if defined(USE_OFFSETS)
            const char* entry = reinterpret_cast<const char*>((intptr_t)iter.node_ - 
                                                (intptr_t)iter.key_offset());
#else 
            const char* entry = iter.key();
#endif
            uint32_t key_length;
            const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
            if(comparator_.comparator.user_comparator()->Compare(
                        Slice(key_ptr, key_length - 8), 
                        key.user_key()) == 0) {
                uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
                switch (static_cast<ValueType>(tag & 0xff)) {
                case kTypeValue: {
                    const char* number_ptr = GetVarint32Ptr(key_ptr + key_length, key_ptr + key_length + 5, partner_number);
                    *offset = DecodeFixed64(number_ptr);
                    DEBUG_T("get partner number is:%d, offset, is %d\n", *partner_number, *offset);
                    return true;
                }
                case kTypeDeletion:
                    *s = Status::NotFound(Slice());
                    return true;
                }
            }
        }
        DEBUG_T("not find!\n");
        return false;
    }
                
}
