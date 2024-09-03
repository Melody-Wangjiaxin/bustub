//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { 
    // throw NotImplementedException("InsertExecutor is not implemented"); 
    child_executor_->Init();
    is_called_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    Tuple child_tuple{};
    int sz = 0;
    ExecutorContext *ctx = GetExecutorContext();
    Catalog *catalog = ctx->GetCatalog();
    TableInfo *table_info = catalog->GetTable(plan_->GetTableOid());
    std::vector<IndexInfo*> indexes = catalog->GetTableIndexes(table_info->name_);
    while(child_executor_->Next(&child_tuple, rid)) {
        auto rid_optional = table_info->table_->InsertTuple(TupleMeta{0, false}, child_tuple, ctx->GetLockManager(), nullptr, plan_->GetTableOid());
        if(rid_optional != std::nullopt) {
            *rid = rid_optional.value();
        }
        for(IndexInfo *idx_info : indexes) {
            idx_info->index_->InsertEntry(
                child_tuple.KeyFromTuple(table_info->schema_, idx_info->key_schema_, 
                                    idx_info->index_->GetKeyAttrs()), *rid, nullptr
            );
        }
        ++sz;
    }
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    values.emplace_back(INTEGER, sz);
    *tuple = Tuple(values, &GetOutputSchema());
    if(sz == 0 && !is_called_) {
        is_called_ = true;
        return true;
    }
    is_called_ = true;
    return sz != 0; 
}

}  // namespace bustub
