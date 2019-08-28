#include "db/single_partner_table.h"
#include "util/testharness.h"
#include "db/partner_meta.h"
#include "db/filename.h"
#include "util/arena.h"
#include "util/debug.h"
#include "leveldb/env.h"
#include <stdint.h>
#include "leveldb/db.h"
#include "db/db_impl.h"
#include "db/table_cache.h"
#include "leveldb/slice.h"

namespace leveldb {
    class SinglePartnerTableTest {};
    enum SaverState {
        kNotFound,
        kFound,
        kDeleted,
        kCorrupt,
    };
    struct Saver {
        SaverState state;
        const Comparator* ucmp;
        Slice user_key;
        std::string* value;
    };
   
    static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
        Saver* s = reinterpret_cast<Saver*>(arg);
        ParsedInternalKey parsed_key;
        if (!ParseInternalKey(ikey, &parsed_key)) {
            s->state = kCorrupt;
        } else {
            if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
                s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
                if (s->state == kFound) {
                    s->value->assign(v.data(), v.size());
                }
            }
        }
    }
    TEST(SinglePartnerTableTest, AddSimple) {
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
        PartnerMeta* pm = new PartnerMeta(cmp, arena, false);
        pm->Ref();
        std::string dbname;
        env->GetTestDirectory(&dbname);
        std::string fname = TableFileName(dbname, 1);
        WritableFile* file;
        Status s = env->NewWritableFile(fname, &file);
        if (!s.ok()) {
            DEBUG_T("new writable file failed\n");
        }
        Options option = leveldb::Options();
        TableBuilder* builder = new TableBuilder(option, file, 1);
        SinglePartnerTable* spt = new SinglePartnerTable(1, builder, pm);
        Slice value("this is my key");
        LookupKey lkey(Slice("abcdmykey"), 0);
        spt->Add(lkey.internal_key(), value);
        spt->Finish();
        DEBUG_T("finish add single partner table\n");

        //读取数据
        uint64_t block_offset, block_size;
        bool find = pm->Get(lkey, &block_offset, &block_size, &s);
        TableCache* table_cache = new TableCache(dbname, option, option.max_open_files);
        ReadOptions roptions;
        std::string resValue;
        Saver saver;
        saver.state = kNotFound;
        saver.ucmp = cmp.user_comparator();
        saver.user_key = lkey.user_key();
        saver.value = &resValue;
        if(find) {
            DEBUG_T("offset is %llu, block size:%llu\n", block_offset, block_size);
            s = table_cache->Get(roptions, 1, lkey.internal_key(), &saver, SaveValue, block_offset, block_size);
            DEBUG_T("get value %s\n", (*saver.value).c_str());
        } else {
            DEBUG_T("cannot find key from nvm skiplist\n");
        }
        delete spt;
    }
}

int main() {
    leveldb::test::RunAllTests();
}