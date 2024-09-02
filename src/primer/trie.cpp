#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if(!root_) return nullptr;

  auto cur_node = root_;
  for(auto &k : key) {
    if(!(cur_node->HasChild(k))) return nullptr;
    cur_node = cur_node->GetChildNode(k);
    if(!cur_node) return nullptr;
  }

  if(key.empty()) {
    cur_node = cur_node->GetChildNode(*key.begin());
  }

  if(!cur_node->is_value_node_) return nullptr;

  auto value_node = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(cur_node);
  if(!value_node) return nullptr;
  return value_node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<TrieNode> new_root;
  if(root_) {
    new_root = root_->Clone();
  } else {
    new_root = std::make_shared<TrieNode>();
  }

  auto parent_node = new_root;
  std::shared_ptr<TrieNode> new_node;
  auto iter = key.begin();
  for(; iter != key.end() && std::next(iter) != key.end(); iter++) {
    if(!parent_node->HasChild(*iter)) new_node = std::make_shared<TrieNode>();
    else new_node = parent_node->GetChildNode(*iter)->Clone();

    parent_node->children_[*iter] = new_node;
    parent_node = new_node;
  }

  auto value_ptr = std::make_shared<T>(std::move(value));

  if(key.empty() || !parent_node->HasChild(*iter)) {
    parent_node->children_[*iter] = std::make_shared<TrieNodeWithValue<T>>(value_ptr);
  } else {
    auto child_node = parent_node->GetChildNode(*iter);
    parent_node->children_[*iter] = std::make_shared<TrieNodeWithValue<T>>(child_node->children_, value_ptr);
  }

  auto new_trie = std::make_shared<Trie>();
  new_trie->root_ = new_root;
  return *new_trie;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if(!root_) return {};

  std::stack<std::shared_ptr<const TrieNode>> st;
  auto cur_node = root_;
  for(auto &k: key) {
    st.push(cur_node);
    if(!(cur_node->HasChild(k))) return Trie(root_);

    cur_node = cur_node->GetChildNode(k);
    if(!cur_node) return Trie(root_);
  }

  if(key.empty()) cur_node = cur_node->GetChildNode(*key.begin());

  if(!cur_node->is_value_node_) return Trie(root_);

  auto new_node = std::make_shared<const TrieNode>(cur_node->children_);
  for(auto riter = key.rbegin(); riter != key.rend(); riter++) {
    auto new_parent = st.top()->Clone();
    st.pop();

    if(new_node->children_.empty() && !new_node->is_value_node_) {
      new_parent->children_.erase(*riter);
    } else {
      new_parent->children_[*riter] = new_node;
    }
    new_node = std::move(new_parent);
  }

  if(new_node->children_.empty()) return {};
  return Trie(new_node);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
