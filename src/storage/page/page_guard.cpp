#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
	bpm_ = that.bpm_;
	that.bpm_ = nullptr;
	page_ = that.page_;
	that.page_ = nullptr;
	is_dirty_ = that.is_dirty_;
}

void BasicPageGuard::Drop() {
	if(page_ && !(bpm_->UnpinPage(page_->GetPageId(), is_dirty_))) {
		throw Exception("Fail to unpin page!");
	} 
	bpm_ = nullptr;
	page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
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
	ReadPageGuard r_guard(bpm_, page_);
	bpm_ = nullptr;
	page_ = nullptr;
	return r_guard;
	// return std::move(r_guard);
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { 
	WritePageGuard w_guard(bpm_, page_);
	bpm_ = nullptr;
	page_ = nullptr;
	return w_guard;
	// return std::move(w_guard);
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {
	if(page) guard_.page_->RLatch();
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
	guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
	if(guard_.page_) guard_.page_->RUnlatch();
	guard_ = std::move(that.guard_);
	return *this; 
}

void ReadPageGuard::Drop() {
	if(guard_.page_) guard_.page_->RUnlatch();
	guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
	this->Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {
	if(page) guard_.page_->WLatch();
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
	guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
	if(guard_.page_) guard_.page_->WUnlatch();
	guard_ = std::move(that.guard_);
	return *this; 
}

void WritePageGuard::Drop() {
	if(guard_.page_) guard_.page_->WUnlatch();
	guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
	this->Drop();
}  // NOLINT

}  // namespace bustub
