//test for partner table

#include "db/partner_table.h"
#include "util/testharness.h"
#include "db/partner_index.h"
#include "db/filename.h"
#include "util/arena.h"
#include "util/debug.h"
#include "leveldb/env.h"
#include <stdint.h>

namespace leveldb {
    class PartnerTableTest {};
    TEST(PartnerTableTest, AddAndGet) {
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
    }
}

int main() {
    leveldb::test::RunAllTests();
}