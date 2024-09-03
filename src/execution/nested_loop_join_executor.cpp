//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { 
  // throw NotImplementedException("NestedLoopJoinExecutor is not implemented"); 
  left_executor_->Init();
  right_executor_->Init();
  RID left_rid;
  left_ret_ = left_executor_->Next(&left_tuple_, &left_rid);
  is_find_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  Tuple right_tuple;
  RID right_rid;
  while(true) {
    if(!left_ret_) return false;
    if(!right_executor_->Next(&right_tuple, &right_rid)) {
      if(!is_find_ && plan_->GetJoinType() == JoinType::LEFT) {
        *tuple = LeftJoinTuple(&left_tuple_);
        *rid = tuple->GetRid();
        is_find_ = true;
        return true;
      }
      RID left_rid;
      left_ret_ = left_executor_->Next(&left_tuple_, &left_rid);
      right_executor_->Init();
      is_find_ = false;
      continue;
    }

    Value is_true = plan_->predicate_->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &(right_tuple), right_executor_->GetOutputSchema());
    if(is_true.GetAs<bool>()) {
      is_find_ = true;
      *tuple = InnerJoinTuple(&left_tuple_, &right_tuple);
      *rid = tuple->GetRid();
      return true;
    }
  }
}

auto NestedLoopJoinExecutor::InnerJoinTuple(const Tuple *left_tuple, const Tuple *right_tuple) -> Tuple {
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  for(uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(left_tuple->GetValue(&(left_executor_->GetOutputSchema()), i));
  }
  for(uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(right_tuple->GetValue(&(right_executor_->GetOutputSchema()), i));
  }
  return {values, &GetOutputSchema()};
}

auto NestedLoopJoinExecutor::LeftJoinTuple(const Tuple *left_tuple) -> Tuple {
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  for(uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(left_tuple->GetValue(&(left_executor_->GetOutputSchema()), i));
  }
  for(uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
  }
  return {values, &GetOutputSchema()};
}

}  // namespace bustub
