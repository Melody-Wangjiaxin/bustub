#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept 
	: bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
	that.bpm_ = nullptr;
	that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
	if (page_ != nullptr && bpm_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
	if (this == &that) {
    return *this;
  }
	this->Drop();
	bpm_ = that.bpm_;
	that.bpm_ = nullptr;
	page_ = that.page_;
	that.page_ = nullptr;
	is_dirty_ = that.is_dirty_;
	return *this; 
}

BasicPageGuard::~BasicPageGuard(){
	this->Drop();
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard { 
	if (page_ != nullptr) {
    page_->RLatch();
  }
  auto read_page_guard = ReadPageGuard(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  return read_page_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { 
	if (page_ != nullptr) {
    page_->WLatch();
  }
  auto write_page_guard = WritePageGuard(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  return write_page_guard;
}

auto BasicPageGuard::IsEmpty() -> bool { return page_ == nullptr; }

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
	guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
	if (this == &that) {
    return *this;
  }
	if(guard_.page_) guard_.page_->RUnlatch();
	guard_ = std::move(that.guard_);
	return *this; 
}

void ReadPageGuard::Drop() {
	if(guard_.page_) guard_.page_->RUnlatch();
	guard_.Drop();
}

auto ReadPageGuard::IsEmpty() -> bool { return guard_.IsEmpty(); }

ReadPageGuard::~ReadPageGuard() {
	this->Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
	guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
	if (this != &that) {
    this->Drop();
    guard_ = std::move(that.guard_);
  }
  return *this; 
}

void WritePageGuard::Drop() {
	if(guard_.page_) guard_.page_->WUnlatch();
	guard_.is_dirty_ = true;
	guard_.Drop();
}

auto WritePageGuard::IsEmpty() -> bool { return guard_.IsEmpty(); }

WritePageGuard::~WritePageGuard() {
	this->Drop();
}  // NOLINT

}  // namespace bustub
