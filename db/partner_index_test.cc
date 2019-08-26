//test partner index
#include "db/partner_index.h"
#include "db/filename.h"
#include "util/testharness.h"
#include "util/arena.h"
#include "util/debug.h"
#include "leveldb/env.h"
#include <stdint.h>

namespace leveldb {
    class PartnerIndexTest{};
    TEST(PartnerIndexTest, AddAndGet) {
        //获取比较器
        //fprintf(stderr, "before get mapfilename\n");
        DEBUG_T("before get mapfilename\n");
        InternalKeyComparator cmp(BytewiseComparator());
        Env* env = Env::Default();
        std::string nvm_path;
        env->GetMEMDirectory(&nvm_path);
        std::string indexFile = MapFileName(nvm_path, 1);
        size_t indexFileSize = (4 << 10) << 10;
        DEBUG_T("after get mapfilename:%s\n", indexFile.c_str());
        ArenaNVM* arena = new ArenaNVM(indexFileSize, &indexFile, false);
        arena->nvmarena_ = true;
        DEBUG_T("after get arena nvm\n");
        PartnerIndex* pi = new PartnerIndex(cmp, arena, false);
        DEBUG_T("after get partner index\n");
        pi->Ref();
        LookupKey lkey(Slice("00000000mykey"), 0);
        pi->Add(lkey.internal_key(), 1, 0);
        DEBUG_T("after add key\n");
        uint64_t offset = 0;
        uint32_t partner_number;
        Status s;
        LookupKey lkey2(Slice("00000000mykey"), 1);
        bool find = pi->Get(lkey, &partner_number, &offset, &s);
        if(find) {
            DEBUG_T("partner_number is %d, offset is %llu\n", partner_number, offset);
        }
        pi->Unref();
    }
}

int main() {
    leveldb::test::RunAllTests();
}