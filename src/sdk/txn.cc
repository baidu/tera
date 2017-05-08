// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <boost/bind.hpp>

#include "common/thread_pool.h"
#include "common/base/string_format.h"

#include "io/coding.h"
#include "sdk/read_impl.h"
#include "sdk/txn.h"
#include "sdk/single_row_txn.h"
#include "sdk/table_impl.h"
#include "types.h"
#include "utils/timer.h"

#include "tso/tso.h"

DEFINE_bool(crash0, false, "for test");
DEFINE_bool(crash1, false, "for test");
DEFINE_bool(crash2, false, "for test");
DEFINE_bool(use_real_tso, true, "for test");

namespace tera {

static pthread_once_t timestamporacle_once_control = PTHREAD_ONCE_INIT;
static tera::tso::TimestampOracle* g_tso = NULL;
void InitTimestmpOracle() {
    // TODO we need check error!
    g_tso = new tera::tso::TimestampOracle;
}

//----- for test
class TimeOracle {
public:
    TimeOracle() {}
    static int64_t GetTimestamp() {
        MutexLock l(&mutex_);
        if (FLAGS_use_real_tso) {
            pthread_once(&timestamporacle_once_control, InitTimestmpOracle);
            return g_tso->GetTimestamp();
        } else {
            return t_++;
        }
    }
    static int64_t t_;
private:
    static Mutex mutex_;
};
int64_t TimeOracle::t_ = 42;
Mutex TimeOracle::mutex_;
//----- for test

Transaction* NewTransaction(int64_t ts) {
    // XXX for test
    {
        if (ts != -1) {
            TimeOracle::t_ = ts;
        }
    }
    return MultiRowTxn::NewMultiRowTxn();
}

Transaction* MultiRowTxn::NewMultiRowTxn() {
    int64_t start_ts = TimeOracle::GetTimestamp();
    if (start_ts > 0) {
        return new MultiRowTxn(start_ts);
    } else {
        return NULL;
    }
}

MultiRowTxn::MultiRowTxn(int64_t start_ts)
   : start_ts_(start_ts) {}

MultiRowTxn::~MultiRowTxn() {}

static std::string LockColumnName(const std::string& c) {
    return c + "__l__"; // lock
}

static std::string WriteColumnName(const std::string& c) {
    return c + "__w__"; // write
}

std::string Int64EncodedString(int64_t n) {
    char buf[sizeof(int64_t)];
    io::EncodeBigEndian(buf, n);
    std::string s;
    s.assign(buf, sizeof(int64_t));
    return s;
}

int64_t EncodedStringToInt64(const std::string& s) {
    return io::DecodeBigEndain(s.c_str());
}

void MultiRowTxn::BuildReaderForPrewrite(RowMutation* w, RowReader* reader) {
    // reader->SetTimeRange(0, kLatestTimestamp);
    for (size_t i = 0; i < w->MutationNum(); i++) {
        const RowMutation::Mutation& mu = w->GetMutation(i); // 1 colomn
        if (mu.type == RowMutation::kPut) {
            reader->AddColumn(mu.family, mu.qualifier);
            reader->AddColumn(mu.family, WriteColumnName(mu.qualifier));
            reader->AddColumn(mu.family, LockColumnName(mu.qualifier));
        } else if (mu.type == RowMutation::kDeleteColumn) {

        } else {

        }
    }
}

void GetTxnPrimaryKeyInfo(RowMutation* w, std::string* info) {
    assert (w && w->MutationNum() > 0);
    assert (info != NULL);
    const RowMutation::Mutation& mu = w->GetMutation(0); // one cell
    MultiRowTxnPrimaryLockInfo the_info;
    the_info.set_table_name(""); // TODO
    the_info.set_row(w->RowKey());
    the_info.set_family(mu.family);
    the_info.set_qualifier(mu.qualifier);
    the_info.set_timestamp(get_micros());
    the_info.SerializeToString(info);
}

void MultiRowTxn::BuildRowMutationForPrewrite(RowMutation* user_mu,
                                              RowMutation* txn_mu,
                                              const std::string& primary_info) {
    for (size_t i = 0; i < user_mu->MutationNum(); i++) {
        const RowMutation::Mutation& mu = user_mu->GetMutation(i); // 1 colomn
        if (mu.type == RowMutation::kPut) {
            txn_mu->Put(mu.family, LockColumnName(mu.qualifier), primary_info, (int64_t)start_ts_);
            txn_mu->Put(mu.family, mu.qualifier, mu.value, (int64_t)start_ts_);
            //LOG(ERROR) << "prewrite family:" << mu.family << ", qualifier:" << mu.qualifier
            //    << ", value:" << mu.value;
        } else if (mu.type == RowMutation::kDeleteColumn) {

        } else {

        }
    }
}

// TODO merge IsWritingByOthers() & IsLockedByOthers()
// return true: this row is been writing by others
bool MultiRowTxn::IsWritingByOthers(RowMutation* row_mu, RowReader* reader) {
    if (reader->GetError().GetType() == tera::ErrorCode::kNotFound) {
        LOG(ERROR) << "IsWritingByOthers not found";
        return false;
    }
    assert(reader->GetError().GetType() == tera::ErrorCode::kOK);
    RowReader::TRow row;
    reader->ToMap(&row);

    // check every cell
    for (size_t i = 0; i < row_mu->MutationNum(); i++) {
        const RowMutation::Mutation& mu = row_mu->GetMutation(i); // 1 cell
        if (row.find(mu.family) == row.end()) {
            LOG(ERROR) << "not found columnfamily";
            return false;
        }
        if (row[mu.family].find(WriteColumnName(mu.qualifier)) == row[mu.family].end()) {
            LOG(ERROR) << "not found qualifier";
            return false;
        }
        for (RowReader::TColumn::reverse_iterator k = row[mu.family][WriteColumnName(mu.qualifier)].rbegin();
             k != row[mu.family][WriteColumnName(mu.qualifier)].rend();
             ++k) {
            LOG(ERROR) << "write pointer:" <<  k->first << ":" << EncodedStringToInt64(k->second);
            if (k->first >= start_ts_) {
                return true;
            }
        }
    }
    return false;
}

// return true: this row is locked by others
bool MultiRowTxn::IsLockedByOthers(RowMutation* row_mu, RowReader* reader) {
    if (reader->GetError().GetType() == tera::ErrorCode::kNotFound) {
        LOG(ERROR) << "IsLockedByOthers not found";
        return false;
    }
    assert(reader->GetError().GetType() == tera::ErrorCode::kOK);
    RowReader::TRow row;
    reader->ToMap(&row);

    // check ever cell
    for (size_t i = 0; i < row_mu->MutationNum(); i++) {
        const RowMutation::Mutation& mu = row_mu->GetMutation(i); // 1 cell
        if (row.find(mu.family) == row.end()) {
            LOG(ERROR) << "not found cfamily";
            return false;
        }
        if (row[mu.family].find(LockColumnName(mu.qualifier)) == row[mu.family].end()) {
            LOG(ERROR) << "not found qualifier";
            return false;
        }
        for (RowReader::TColumn::reverse_iterator k = row[mu.family][LockColumnName(mu.qualifier)].rbegin();
             k != row[mu.family][LockColumnName(mu.qualifier)].rend();
             ++k) {
            LOG(ERROR) << k->first << ":" << k->second;
            return true;
        }
    }
    return false;
}

ErrorCode MultiRowTxn::Prewrite(RowMutation* w, RowMutation* primary) {
    ErrorCode status;
    Table* table = w->GetTable();

    tera::Transaction* single_row_txn = table->StartRowTransaction(w->RowKey());
    RowReader* reader = table->NewRowReader(w->RowKey());
    BuildReaderForPrewrite(w, reader);
    single_row_txn->Get(reader);
    // TODO check error
    if (reader->GetError().GetType() == tera::ErrorCode::kNotFound) {
        LOG(ERROR) << "prewrite read notfound:" << reader->GetError().GetReason();
    } else if (reader->GetError().GetType() != tera::ErrorCode::kOK) {
        status.SetFailed(ErrorCode::kSystem, "fail to read in prewrite");
        return status;
    }

    if (IsWritingByOthers(w, reader)) {
        status.SetFailed(ErrorCode::kSystem, "writing by others");
        return status;
    }
    if (IsLockedByOthers(w, reader)) {
        status.SetFailed(ErrorCode::kSystem, "locked by others");
        return status;
    }
    delete reader;

    RowMutation* txn_mu = w->GetTable()->NewRowMutation(w->RowKey());
    std::string info;
    GetTxnPrimaryKeyInfo(primary, &info);
    BuildRowMutationForPrewrite(w, txn_mu, info);
    single_row_txn->ApplyMutation(txn_mu);
    table->CommitRowTransaction(single_row_txn);
    status = single_row_txn->GetError();

    delete txn_mu;
    delete single_row_txn;
    return status;
}

void MultiRowTxn::BuildRowMutationForCommit(RowMutation* user_mu, RowMutation* txn_mu, int64_t commit_ts) {
    for (size_t i = 0; i < user_mu->MutationNum(); i++) {
        const RowMutation::Mutation& mu = user_mu->GetMutation(i); // 1 cell
        if (mu.type == RowMutation::kPut) {
            txn_mu->Put(mu.family,
                        WriteColumnName(mu.qualifier),
                        Int64EncodedString(start_ts_),  // value
                        commit_ts);   // cell timestamp
            txn_mu->DeleteColumn(mu.family,
                                 LockColumnName(mu.qualifier),
                                 commit_ts);
        } else if (mu.type == RowMutation::kDeleteColumn) {

        } else {

        }
    }
}

void MultiRowTxn::BuildRowReaderForCommit(RowMutation* user_mu, RowReader* reader) {
    assert(user_mu->MutationNum() > 0);
    const RowMutation::Mutation& mu = user_mu->GetMutation(0); // 1 cell
    reader->AddColumn(mu.family, LockColumnName(mu.qualifier));
}

// return true: there is a txn-lock
bool MultiRowTxn::LockExists(tera::Transaction* single_row_txn, RowMutation* row_mu) {
    RowReader* reader = row_mu->GetTable()->NewRowReader(row_mu->RowKey());
    BuildRowReaderForCommit(row_mu, reader);
    single_row_txn->Get(reader);

    if (reader->GetError().GetType() == tera::ErrorCode::kNotFound) {
        LOG(ERROR) << "not found";
        return false;
    }
    assert(reader->GetError().GetType() == tera::ErrorCode::kOK);
    RowReader::TRow row;
    reader->ToMap(&row);

    const RowMutation::Mutation& mu = row_mu->GetMutation(0); // 1 cell
    if (row.find(mu.family) == row.end()) {
        LOG(ERROR) << "not found cfamily";
        return false;
    }
    for (RowReader::TColumn::reverse_iterator k = row[mu.family][LockColumnName(mu.qualifier)].rbegin();
         k != row[mu.family][LockColumnName(mu.qualifier)].rend();
         ++k) {
        LOG(ERROR) << "LockExists " << k->first << ":" << k->second;
        if (k->first != start_ts_) {
            return false;
        } else {
            return true;
        }
    }

    delete reader;
    return false;
}

ErrorCode MultiRowTxn::Commit() {
    assert(writes_.size() > 0);

    ErrorCode status;
    RowMutation* primary = writes_[0];
    std::vector<RowMutation*> secondaries(writes_.begin() + 1, writes_.end());
    status = Prewrite(primary, primary);
    if (status.GetType() != tera::ErrorCode::kOK) {
        LOG(ERROR) << status.GetReason();
        return status;
    }
    LOG(ERROR) << "prewrite primary done";
    if (FLAGS_crash0) {
        exit(66);
    }

    for (size_t i = 0; i < secondaries.size(); i++) {
        status = Prewrite(secondaries[i], primary);
        if (status.GetType() != tera::ErrorCode::kOK) {
            LOG(ERROR) << status.GetReason() << " | key: " <<  secondaries[i]->RowKey();
            return status;
        }
    }
    if (FLAGS_crash1) {
        exit(66);
    }

    LOG(ERROR) << "prewrite done";
    int64_t commit_ts = TimeOracle::GetTimestamp();


    // commit primary first
    Table* table = primary->GetTable();
    tera::Transaction* single_row_txn = table->StartRowTransaction(primary->RowKey());
    if (!LockExists(single_row_txn, primary)) {
        status.SetFailed(ErrorCode::kSystem, "maybe lock already erased by others");
        return status;
    }
    RowMutation* txn_primary = primary->GetTable()->NewRowMutation(primary->RowKey());
    BuildRowMutationForCommit(primary, txn_primary, commit_ts);

    single_row_txn->ApplyMutation(txn_primary);
    table->CommitRowTransaction(single_row_txn);
    status = single_row_txn->GetError();
    delete txn_primary;
    delete single_row_txn;
    if (status.GetType() != tera::ErrorCode::kOK) {
        LOG(ERROR) << "commit primary fail";
        return status;
    }
    LOG(ERROR) << "commit primary ok";
    if (FLAGS_crash2) {
        exit(66);
    }

    for (size_t i = 0; i < secondaries.size(); i++) {
        RowMutation* user_mu = secondaries[i];
        RowMutation* txn_mu = user_mu->GetTable()->NewRowMutation(user_mu->RowKey());
        BuildRowMutationForCommit(user_mu, txn_mu, commit_ts);
        txn_mu->GetTable()->ApplyMutation(txn_mu);
        delete txn_mu;
    }
    return status;
}

/// 提交一个修改操作
void MultiRowTxn::ApplyMutation(RowMutation* row_mu) {
    assert(row_mu != NULL);
    writes_.push_back(row_mu);
}

// return true: primary lock already timeout
bool MultiRowTxn::MaybePrimaryLockTimeout(int64_t ts) {
    int64_t now = get_micros();
    return (now - ts) > 10000000; // timeout in us
}

// return true: clean lock & data ok
//        false: fail to clean or timeout
bool MultiRowTxn::CleanupLockAndData(Transaction* single_row_txn,
                                     RowReader* reader,
                                     const std::string& row,
                                     const std::string& cf,
                                     const std::string& qu,
                                     int64_t start_ts) {
    Table* table = reader->GetTable();
    RowMutation *mu = table->NewRowMutation(row);
    mu->DeleteColumn(cf, qu, start_ts);
    mu->DeleteColumn(cf, LockColumnName(qu), start_ts);
    single_row_txn->ApplyMutation(mu);
    table->CommitRowTransaction(single_row_txn);
    ErrorCode status = single_row_txn->GetError();
    if (status.GetType() != tera::ErrorCode::kOK) {
        LOG(ERROR) << "cleanup primary lock failed:"
            << row << ":" << cf << ":" << qu << " for:" << status.GetReason();
        delete mu;
        return false;
    }
    delete mu;
    return true;
}

void MultiRowTxn::RollForwardThisCell(tera::Transaction* target_row_txn, RowReader* reader,
                                      const std::string& cf, const std::string qu,
                                      int64_t start_ts, int64_t commit_ts) {
    RowMutation* mu = reader->GetTable()->NewRowMutation(reader->RowKey());
    mu->Put(cf, WriteColumnName(qu), Int64EncodedString(start_ts), commit_ts);
    mu->DeleteColumn(cf, LockColumnName(qu), commit_ts);
    target_row_txn->ApplyMutation(mu);
    reader->GetTable()->CommitRowTransaction(target_row_txn);
    if (target_row_txn->GetError().GetType() != tera::ErrorCode::kOK) {
        LOG(ERROR) << "RollForwardThisCell failed";
    }
    delete mu;
}

void MultiRowTxn::CheckPrimaryLockAndTimestamp(RowReader* reader, const std::string& cf, const std::string& qu,
                                                     bool* lock_exists, int64_t* lock_timestamp) {
    RowReader::TRow row;
    reader->ToMap(&row);
    if (row.find(cf) == row.end()) {
        LOG(ERROR) << "lock not found";
        *lock_exists = false;
    } else if (row[cf].find(LockColumnName(qu)) == row[cf].end()) {
        LOG(ERROR) << "lock not found";
        *lock_exists = false;
    } else {
        LOG(ERROR) << "lock exists";
        *lock_exists = true;
    }
    if (row.find(cf) != row.end()) {
        if (row[cf].find(LockColumnName(qu)) != row[cf].end()) {
            for (RowReader::TColumn::reverse_iterator k = row[cf][LockColumnName(qu)].rbegin();
                 k != row[cf][LockColumnName(qu)].rend(); ++k) {
                *lock_timestamp = k->first;
                LOG(ERROR) << "lock_timestamp:" <<  k->first;
            }
        } else {
            LOG(ERROR) << "lock-qu not found";
        }
    } else {
        LOG(ERROR) << "lock-cf not found";
    }
}

bool IsSameCell(const std::string& r0, const std::string& c0, const std::string& q0,
                const std::string& r1, const std::string& c1, const std::string& q1) {
    return (r0 == r1) && (c0 == c1) && (q0 == q1);
}

// return false: IO Error, result is unknown
bool IfPrimaryCommittedThenGetCommitTimestamp(RowReader* primary_reader, const std::string& family, const std::string& qualifier,
                                              int64_t last_txn_start_ts, bool* is_commited, int64_t* commit_ts) {
    Table* table = primary_reader->GetTable();
    RowReader* reader = table->NewRowReader(primary_reader->RowKey()); // TODO delete reader
    reader->SetTimeRange(0, std::numeric_limits<int64_t>::max()); // XXX too many?
    reader->AddColumn(family, qualifier);
    reader->AddColumn(family, WriteColumnName(qualifier));
    table->Get(reader);
    if (reader->GetError().GetType() == tera::ErrorCode::kNotFound) {
        *is_commited = false;
        return true;
    }
    if (reader->GetError().GetType() != tera::ErrorCode::kOK) {
        return false;
    }

    assert (reader->GetError().GetType() == tera::ErrorCode::kOK);
    RowReader::TRow row;
    reader->ToMap(&row);

    // TODO delete ?
    if (row.find(family) == row.end()) {
        *is_commited = false;
        return true;
    }
    if (row[family].find(qualifier) == row[family].end()) {
        *is_commited = false;
        return true;
    }
    if (row[family].find(WriteColumnName(qualifier)) == row[family].end()) {
        *is_commited = false;
        return true;
    }
    if (row[family][qualifier].find(last_txn_start_ts) == row[family][qualifier].end()) {
        *is_commited = false;
        return true;
    }
    for (RowReader::TColumn::reverse_iterator k = row[family][WriteColumnName(qualifier)].rbegin();
         k != row[family][WriteColumnName(qualifier)].rend(); ++k) {
        int64_t tmp_commit_ts = k->first;
        int64_t tmp_start_ts = EncodedStringToInt64(k->second);
        LOG(ERROR) << "someone txn commit_ts:" << tmp_commit_ts << ", start_ts:" << tmp_start_ts;
        if (tmp_start_ts == last_txn_start_ts) {
            *commit_ts = tmp_commit_ts;
            *is_commited = true;
            return true;
        }
    }
    *is_commited = false;
    return true;
}

void MultiRowTxn::BackoffAndMaybeCleanupLock(tera::Transaction* target_row_txn, RowReader* user_reader,
                                             const std::string& primary_info, const std::string& cf,
                                             const std::string& qu, int64_t last_txn_start_ts) {
    assert(user_reader != NULL);

    MultiRowTxnPrimaryLockInfo primary;
    primary.ParseFromString(primary_info);
    LOG(ERROR) << "primary: " << primary.row() << ":" << primary.family() << ":"
        << primary.qualifier();
    Table* table = user_reader->GetTable(); // TODO cross-table
    tera::Transaction* primary_row_txn = table->StartRowTransaction(primary.row());

    RowReader* primary_reader = table->NewRowReader(primary.row());
    primary_reader->AddColumn(primary.family(), LockColumnName(primary.qualifier()));
    primary_reader->AddColumn(primary.family(), WriteColumnName(primary.qualifier()));
    primary_row_txn->Get(primary_reader);
    ErrorCode status = primary_reader->GetError();
    if ((status.GetType() != tera::ErrorCode::kOK)
        && (status.GetType() != tera::ErrorCode::kNotFound)) {
        LOG(ERROR) << "IO ERROR: fail to read primary lock info";
        delete primary_row_txn;
        return;
    }
    while (!primary_reader->Done()) {
        LOG(ERROR) << "primary reader:" << primary_reader->RowKey() << ":" << primary_reader->Family()
            << ":" << primary_reader->Qualifier() << ":" << primary_reader->Timestamp() << ":" << primary_reader->Value();
        primary_reader->Next();
    }
    bool lock_exists = true;
    int64_t lock_timestamp = -1;
    CheckPrimaryLockAndTimestamp(primary_reader, primary.family(), primary.qualifier(), &lock_exists, &lock_timestamp);
    if (lock_exists && (lock_timestamp == last_txn_start_ts)) {
        if (MaybePrimaryLockTimeout(primary.timestamp())) {
            LOG(ERROR) << "clean legacy lock & data";
            if (CleanupLockAndData(primary_row_txn, primary_reader,
                                   primary.row(), primary.family(), primary.qualifier(), lock_timestamp)) {
                if (!IsSameCell(primary.row(), primary.family(), primary.qualifier(),
                                user_reader->RowKey(), cf, qu)) {
                    LOG(ERROR) << "clean primary done, then clean this cell";
                    CleanupLockAndData(target_row_txn, user_reader,
                                       user_reader->RowKey(), cf, qu, lock_timestamp);
                }
            }
        } else {
            // primary txn is live, wait
            LOG(ERROR) << "wait primary lock";
            sleep(2);
        }
    } else {
        int64_t commit_ts = -1;
        bool is_primary_commited = false;
        if (!IfPrimaryCommittedThenGetCommitTimestamp(primary_reader, primary.family(), primary.qualifier(),
                                                      last_txn_start_ts, &is_primary_commited, &commit_ts)) {
            LOG(ERROR) << "IO ERROR, retry later";
        } else {
            if (is_primary_commited) {
                // primary committed, so roll forward this cell
                LOG(ERROR) << "primary has commited, so rollforward this cell, star_ts:"
                    << last_txn_start_ts << ", commit_ts:"<< commit_ts;
                RollForwardThisCell(target_row_txn, user_reader, cf, qu, last_txn_start_ts, commit_ts);
            } else {
                // primary lock has beed cleaned, so clean this cell
                LOG(ERROR) << "primary has been cleaned, so clean this cell";
                CleanupLockAndData(target_row_txn, user_reader, user_reader->RowKey(),
                                   cf, qu, last_txn_start_ts);
            }
        }
    }
    delete primary_row_txn;
}

// TODO read columnfamily/single-row/multi-rows
bool MultiRowTxn::IsLockedBeforeMe(RowReader* user_reader, RowReader* txn_reader, std::string* primary,
                                   std::string* cf, std::string* qu, int64_t* last_txn_start_ts) {
    RowReader::TRow row;
    txn_reader->ToMap(&row);

    const RowReader::ReadColumnList& cf_map = user_reader->GetReadColumnList();
    RowReader::ReadColumnList::const_iterator cf_it;
    for (cf_it = cf_map.begin(); cf_it != cf_map.end(); ++cf_it) {
        std::set<std::string>::const_iterator qu_it;
        std::string cf_name = cf_it->first;
        for (qu_it = cf_it->second.begin(); qu_it != cf_it->second.end(); ++qu_it) {
            std::string qu_name = *qu_it;
            LOG(ERROR) << "IsLockedBeforeMe: found:" << cf_name << ":" << qu_name;

            // check every cell
            if (row.find(cf_name) == row.end()) {
                LOG(ERROR) << "IsLockedBeforeMe cf not found:" << cf_name;
                continue;
            }
            if (row[cf_name].find(LockColumnName(qu_name)) == row[cf_name].end()) {
                LOG(ERROR) << "IsLockedBeforeMe  qu not found:" << LockColumnName(qu_name);
                continue;
            }
            for (RowReader::TColumn::reverse_iterator k = row[cf_name][LockColumnName(qu_name)].rbegin();
                 k != row[cf_name][LockColumnName(qu_name)].rend();
                 ++k) {
                LOG(ERROR) << "IsLockedBeforeMe found:" <<  k->first << ":" << k->second;
                if (k->first <= start_ts_) {
                    *primary = row[cf_name][LockColumnName(qu_name)][k->first];
                    *cf = cf_name;
                    *qu = qu_name;
                    *last_txn_start_ts = k->first;
                    MultiRowTxnPrimaryLockInfo info;
                    info.ParseFromString(*primary);
                    LOG(ERROR) << "LockedBeforeMe, oops, lock:" << info.DebugString();
                    return true;
                }
            }
        }
    }
    return false;
}

void BuildTxnRowReaderForGet(RowReader* user_reader, RowReader* txn_reader) {
    //txn_reader->SetTimeRange(0, std::numeric_limits<int64_t>::max());

    const RowReader::ReadColumnList& cf_map = user_reader->GetReadColumnList();
    RowReader::ReadColumnList::const_iterator cf_it;
    for (cf_it = cf_map.begin(); cf_it != cf_map.end(); ++cf_it) {
        std::set<std::string>::const_iterator qu_it;
        std::string cf_name = cf_it->first;
        for (qu_it = cf_it->second.begin(); qu_it != cf_it->second.end(); ++qu_it) {
            txn_reader->AddColumn(cf_name, *qu_it);
            txn_reader->AddColumn(cf_name, WriteColumnName(*qu_it));
            txn_reader->AddColumn(cf_name, LockColumnName(*qu_it));
        }
    }
}

void MultiRowTxn::FillReadResult(RowReader* txn_reader, RowReader* user_reader) {
    RowResult result;

    RowReader::TRow row;    // result
    txn_reader->ToMap(&row);
    ((RowReaderImpl*)user_reader)->SetError(tera::ErrorCode::kNotFound, "not found any data in row");

    const RowReader::ReadColumnList& cf_map = user_reader->GetReadColumnList();
    RowReader::ReadColumnList::const_iterator cf_it;
    for (cf_it = cf_map.begin(); cf_it != cf_map.end(); ++cf_it) {
        std::set<std::string>::const_iterator qu_it;
        std::string cf_name = cf_it->first;
        for (qu_it = cf_it->second.begin(); qu_it != cf_it->second.end(); ++qu_it) {
            std::string qu_name = *qu_it;
            // user want to read cf_name:qu_name cell
            LOG(ERROR) << "user want read " << cf_name << ":" << qu_name;

            if (row.find(cf_name) == row.end()) {
                LOG(ERROR) << "cf not found:" << cf_name;
                continue;
            }
            if (row[cf_name].find(qu_name) == row[cf_name].end()) {
                LOG(ERROR) << "qu not found:" << qu_name;
                continue;
            }
            if (row[cf_name].find(WriteColumnName(qu_name)) == row[cf_name].end()) {
                LOG(ERROR) << "write-pointer not found:" << WriteColumnName(qu_name);
                continue;
            }
            for (RowReader::TColumn::reverse_iterator k = row[cf_name][WriteColumnName(qu_name)].rbegin();
                 k != row[cf_name][WriteColumnName(qu_name)].rend(); ++k) {
                int64_t start_ts = EncodedStringToInt64(k->second); // value is a pointer
                LOG(ERROR) << "FillReadResult: " << cf_name << ":" << qu_name << ":" << k->first << ":" << start_ts;
                if (start_ts_ <= k->first) {
                    LOG(ERROR) << "FillReadResult: there is new data, use newer txn try again";
                    return;
                }
                if (row[cf_name][qu_name].find(start_ts) != row[cf_name][qu_name].end()) {
                    KeyValuePair* cell = result.add_key_values();
                    cell->set_key(txn_reader->RowKey());
                    cell->set_column_family(cf_name);
                    cell->set_qualifier(qu_name);
                    cell->set_timestamp(start_ts);
                    cell->set_value(row[cf_name][qu_name][start_ts]);
                    break; // next cell
                } else {
                    LOG(ERROR) << "found write-pointer but data not found";
                    return; // error
                }
            }
        }
    }

    ((RowReaderImpl*)user_reader)->SetResult(result);
    if (result.key_values_size() == 0) {
        ((RowReaderImpl*)user_reader)->SetError(tera::ErrorCode::kNotFound, "not found any data in row");
    } else {
        ((RowReaderImpl*)user_reader)->SetError(tera::ErrorCode::kOK);
    }
}

/// 读取操作
void MultiRowTxn::Get(RowReader* user_reader) {
    assert(user_reader != NULL);
    Table* table = user_reader->GetTable();

    RowReader* txn_reader = NULL;
    while (true) {
        tera::Transaction* target_row_txn = table->StartRowTransaction(user_reader->RowKey());
        txn_reader = table->NewRowReader(user_reader->RowKey());
        BuildTxnRowReaderForGet(user_reader, txn_reader);
        target_row_txn->Get(txn_reader);
        if ((txn_reader->GetError().GetType() != tera::ErrorCode::kOK)
            && (txn_reader->GetError().GetType() != tera::ErrorCode::kNotFound)) {
            LOG(ERROR) << "fail to read:" << txn_reader->GetError().GetReason();
            delete txn_reader;
            return;
        }
        while (!txn_reader->Done()) {
            LOG(ERROR) << "txn reader:" << txn_reader->RowKey() << ":" << txn_reader->Family()
                << ":" << txn_reader->Qualifier() << ":" << txn_reader->Timestamp() << ":" << txn_reader->Value();
            txn_reader->Next();
        }
        std::string primary;
        std::string cf;
        std::string qu;
        int64_t last_txn_start_ts;
        if (IsLockedBeforeMe(user_reader, txn_reader, &primary, &cf, &qu, &last_txn_start_ts)) {
            //LOG(ERROR) << "read a locked cell";
            BackoffAndMaybeCleanupLock(target_row_txn, user_reader, primary, cf, qu, last_txn_start_ts);
            delete txn_reader;

        } else {
            break;
        }
    }

    FillReadResult(txn_reader, user_reader);
    delete txn_reader;
}

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
