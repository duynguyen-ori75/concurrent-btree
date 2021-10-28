/*
Copyright (C) 2021 Duy Nguyen
All rights reserved.
Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:
The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
*/

#pragma once

#include <cassert>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest_prod.h>

#include "common/constants.h"
#include "common/macros.h"
#include "tree/definitions.h"

namespace btree::implementation {

/**
 * @brief NodeMetadata definition
 *        It provides some useful utilities, such as latch per node
 */
class NodeMetadata {
public:
  /** Size of current node */
  int size_;

  /** Latch for each node */
  std::shared_mutex latch_;

  /** Constructor */
  NodeMetadata() : size_(0) {}

  constexpr std::shared_mutex *SharedLatchPtr() { return &(this->latch_); }
};

/**
 * @brief QueryContext to provide Lock Crabbing protocol
 *          with the Depth-First Search operation
 *          on top of the in-memory B+Tree index
 */
class QueryContext {
public:
  /**
   * @brief All latches in this QueryContext should have the same type
   *        Either all are SHARE or EXCLUSIVE latches
   *        At any moment, the latches vector should not hold a mix of both
   * SHARE and EXCLUSIVE latches
   */
  short smallest_unlk_idx_;
  std::vector<std::shared_mutex *> latches_;
#ifdef NDEBUG
  short expected_unlk_depth_;
  bool have_released_all_;
#endif

  QueryContext() : smallest_unlk_idx_(0), latches_(0) {
#ifdef NDEBUG
    expected_unlk_depth_ = -1;
    have_released_all_ = false;
#endif
  }

  bool IsEmpty() const { return latches_.empty(); }

  void Clear() {
#ifdef NDEBUG
    assert(have_released_all_);
#endif
    this->latches_.clear();
    smallest_unlk_idx_ = 0;
#ifdef NDEBUG
    expected_unlk_depth_ = -1;
    have_released_all_ = false;
#endif
  }

  /**
   * @brief Acquire a latch on either SHARE or EXCLUSIVE mode, determined by
   * latch_type
   *
   * @param latch
   * @param latch_type
   * @return int  the current latch index of the caller
   */
  int AcquireLatch(std::shared_mutex *latch, common::Constants::SharedLockType latch_type) {
#ifdef NDEBUG
    have_released_all_ = false;
#endif
    switch (latch_type) {
      case common::Constants::SHARE:
        latch->lock_shared();
        break;
      case common::Constants::EXCLUSIVE:
        latch->lock();
        break;
      default:
        // user can specify common::Constants::NONE in case he/she simply wants to
        // append and manage the lock
        //  i.e. no lock acquisition required
        break;
    }
    this->latches_.push_back(latch);
    return this->latches_.size() - 1;
  }

  void ReplaceLatch(short idx, std::shared_mutex *latch, common::Constants::SharedLockType latch_type) {
    switch (latch_type) {
      case common::Constants::SHARE:
        this->latches_[idx]->unlock_shared();
        break;
      case common::Constants::EXCLUSIVE:
        this->latches_[idx]->unlock();
        break;
      default:
        throw std::invalid_argument("Should not release NONE latch");
    }
    this->latches_[idx] = latch;
  }

  void ReleaseLatch(short upto_idx, common::Constants::SharedLockType latch_type) {
    if (smallest_unlk_idx_ >= upto_idx) return;
    assert(upto_idx <= static_cast<int>(this->latches_.size()));
#ifdef NDEBUG
    if (upto_idx >= static_cast<int>(this->latches_.size())) {
      have_released_all_ = true;
    }
#endif
    for (auto idx = smallest_unlk_idx_; idx < upto_idx; ++idx) {
      switch (latch_type) {
        case common::Constants::SHARE:
          this->latches_[idx]->unlock_shared();
          break;
        case common::Constants::EXCLUSIVE:
          this->latches_[idx]->unlock();
          break;
        default:
          throw std::invalid_argument("Should not release NONE latch");
      }
    }
    smallest_unlk_idx_ = upto_idx;
  }

  void ReleaseLatchFromParent(short parent_depth, common::Constants::SharedLockType latch_type) {
    assert(smallest_unlk_idx_ == parent_depth);
#ifdef NDEBUG
    have_released_all_ = true;
    if (expected_unlk_depth_ >= 0) {
      assert(expected_unlk_depth_ == parent_depth);
    }
#endif
    for (auto idx = parent_depth; idx < static_cast<int>(this->latches_.size()); ++idx) {
      switch (latch_type) {
        case common::Constants::SHARE:
          this->latches_[idx]->unlock_shared();
          break;
        case common::Constants::EXCLUSIVE:
          this->latches_[idx]->unlock();
          break;
        default:
          throw std::invalid_argument("Should not release NONE latch");
      }
    }
    smallest_unlk_idx_ = this->latches_.size();
  }
};

/**
 * @brief LeafNode class definition
 */
template <typename KeyType, typename ValueType, int Capacity>
class LeafNode : public Node<KeyType, ValueType, QueryContext, NodeMetadata> {
private:
  KeyType keys_[Capacity];
  ValueType values_[Capacity];
  LeafNode<KeyType, ValueType, Capacity> *right_sibl_;
  FRIEND_TEST(LeafNode, BalanceBorrowing);
  FRIEND_TEST(LeafNode, BalanceMerge);

  bool SearchKeyIndex(const KeyType &key, int &index) {
    index = std::lower_bound(this->keys_, this->keys_ + this->Size(), key) - this->keys_;
    return (index < this->Size() && this->keys_[index] == key);
  }

  void DeleteIndex(int index, bool &underflow) {
    std::move(this->keys_ + index + 1, this->keys_ + this->Size(), this->keys_ + index);
    std::move(this->values_ + index + 1, this->values_ + this->Size(), this->values_ + index);
    underflow = (--this->Size() < UNDERFLOW_BOUND(Capacity));
  }

  void ShiftAndInsert(const KeyType &key, const ValueType &val, int insert_pos) {
    // shift the entries to the right in backward manner to prevent overwrite
    // correct data
    std::move_backward(this->keys_ + insert_pos, this->keys_ + this->Size(), this->keys_ + this->Size() + 1);
    std::move_backward(this->values_ + insert_pos, this->values_ + this->Size(), this->values_ + this->Size() + 1);
    // insert new entry
    this->keys_[insert_pos] = key;
    this->values_[insert_pos] = val;
    this->Size()++;
  }

public:
  LeafNode() : right_sibl_(nullptr) {}
#ifdef ENABLE_TESTING
  // special constructor just for testing purpose
  LeafNode(KeyType infinity_key, ValueType infinity_val) : LeafNode() {
    this->keys_[0] = infinity_key;
    this->values_[0] = infinity_val;
    this->Size() = 1;
  }
#endif
  ~LeafNode() = default;

  /**
   * Right sibling constructor
   * @param keys        An array of keys from current LeafNode
   * @param values      An array of values from current LeafNode
   * @param start_idx   The new sibling should clone keys in the range of
   * [start_idx, end)
   * @param right_sibling
   */
  LeafNode(KeyType (&keys)[Capacity], ValueType (&values)[Capacity], int start_idx,
           LeafNode<KeyType, ValueType, Capacity> *right_sibling) {
    this->right_sibl_ = right_sibling;
    this->Size() = Capacity - start_idx;
    std::copy(keys + start_idx, keys + Capacity, this->keys_);
    std::copy(values + start_idx, values + Capacity, this->values_);
  }

  constexpr NodeType Type() { return LEAF; };
  constexpr NodeMetadata &Metadata() { return this->meta_; }
  int &Size() { return this->Metadata().size_; };
  LeafNode<KeyType, ValueType, Capacity> *RightSibling() const { return this->right_sibl_; }

  std::string String() {
    std::stringstream ss;
    ss << "[LEAF: ";
    for (int idx = 0; idx < this->Size(); idx++) {
      ss << "(" << this->keys_[idx] << "," << this->values_[idx] << ")";
      if (idx < this->Size() - 1) ss << " ";
    }
    ss << "]";
    return ss.str();
  }

  constexpr KeyType &GetKey(int offset) {
    assert(offset >= 0 && offset < Capacity);
    return this->keys_[offset];
  }

  /**
   * Get an entry of a Leaf node given the index of the entry
   * Require the caller to already have shared-lock on the leaf
   * @param offset
   * @param key
   * @param val
   * @return false if offset is out of range, true otherwise
   */
  bool GetEntry(int offset, KeyType &key, ValueType &value) {
    if (offset < 0 || offset >= this->Size()) return false;
    key = this->keys_[offset];
    value = this->values_[offset];
    return true;
  }

  /**************************************************************************************
   * @brief Core utilities are placed below, and all are thread-safe, except
   *Balance    *
   **************************************************************************************/

  bool Insert(const KeyType &key, const ValueType &val, Split<KeyType, ValueType, QueryContext, NodeMetadata> &split,
              QueryContext *context) {
    int insert_pos;

    // lock crabbing - acquire the exclusive latch on current node first
    // after that, if this node is safe, unlock all exclusive latches on its
    // ancestors
    auto depth = context->AcquireLatch(this->Metadata().SharedLatchPtr(), common::Constants::EXCLUSIVE);
    if (this->Size() < Capacity) {
      context->ReleaseLatch(depth, common::Constants::EXCLUSIVE);
    }
    assert(this->Metadata().SharedLatchPtr()->try_lock_shared() == false);

    bool found = this->SearchKeyIndex(key, insert_pos);
    if (found) {
      this->values_[insert_pos] = val;
      context->ReleaseLatch(depth + 1, common::Constants::EXCLUSIVE);
      return false;
    }

    if (this->Size() < Capacity) {
      this->ShiftAndInsert(key, val, insert_pos);
      context->ReleaseLatch(depth + 1, common::Constants::EXCLUSIVE);
      return false;
    }

    // come here means that we have to split current node into two
    int boundary_idx = UNDERFLOW_BOUND(this->Size());

    // initialize new right sibling
    auto new_sibling =
        new LeafNode<KeyType, ValueType, Capacity>(this->keys_, this->values_, boundary_idx, this->right_sibl_);

    // modify in-memory content of this node
    this->Size() = boundary_idx;
    this->right_sibl_ = new_sibling;

    // insert the new (key, val) pair
    if (insert_pos < boundary_idx) {
      this->ShiftAndInsert(key, val, insert_pos);
    } else {
      new_sibling->ShiftAndInsert(key, val, insert_pos - boundary_idx);
    }

    // populate split data structure
    split.left = this;
    split.right = new_sibling;
    split.boundary_key = RIGHTMOST_KEY(this);

    // because this insertion causes a split, its exclusive latch will be
    // unlocked by its parent
    return true;
  }

  bool Search(const KeyType &key, ValueType &value, QueryContext *context) {
    int index;
    // lock crabbing: acquire share latch on current node and unlock all share
    // latches on its ancestors
    auto depth = context->AcquireLatch(this->Metadata().SharedLatchPtr(), common::Constants::SHARE);
    context->ReleaseLatch(depth, common::Constants::SHARE);
    assert(this->Metadata().SharedLatchPtr()->try_lock() == false);

    bool found = this->SearchKeyIndex(key, index);
    if (found) {
      value = this->values_[index];
    }

    // unlock latch on current node after completing the search
    context->ReleaseLatch(depth + 1, common::Constants::SHARE);
    return found;
  }

  bool Update(const KeyType &key, const ValueType &value, QueryContext *context) {
    int index;
    // lock crabbing: acquire share latch on current node and unlock all share
    // latches on its ancestors
    auto depth = context->AcquireLatch(this->Metadata().SharedLatchPtr(), common::Constants::EXCLUSIVE);
    context->ReleaseLatch(depth, common::Constants::EXCLUSIVE);

    bool found = this->SearchKeyIndex(key, index);
    if (found) {
      this->values_[index] = value;
    }

    // unlock latch on current node after completing the search
    context->ReleaseLatch(depth + 1, common::Constants::SHARE);
    return found;
  }

  bool Delete(const KeyType &key, bool &underflow, QueryContext *context) {
    int index;
    // the unnecessity below is to mute warning
    underflow = false;
    // lock crabbing: acquire exclusive latch on current node
    auto depth = context->AcquireLatch(this->Metadata().SharedLatchPtr(), common::Constants::EXCLUSIVE);
    // if this node is likely to be safe, we should unlock all the exclusive
    // latches on its ancestors ASAP
    if (this->Size() - 1 >= UNDERFLOW_BOUND(Capacity)) {
      context->ReleaseLatch(depth, common::Constants::EXCLUSIVE);
    }

    bool found = this->SearchKeyIndex(key, index);
    if (found) {
      this->DeleteIndex(index, underflow);
    }

    /**
     * lock crabbing, only unlock all latches if it is not underflow
     *    otherwise, the exclusive latch on this node has to be unlocked by
     *      one of its latched ancestor (the lowest safe one)
     * also unlock all latches if key is not found
     */
    if (!underflow || !found) {
      context->ReleaseLatch(depth + 1, common::Constants::EXCLUSIVE);
    }
    return found;
  }

  bool LocateKey(const KeyType &key, Node<KeyType, ValueType, QueryContext, NodeMetadata> *&child, int &position,
                 QueryContext *context) {
    auto depth = context->AcquireLatch(this->Metadata().SharedLatchPtr(), common::Constants::SHARE);
    context->ReleaseLatch(depth, common::Constants::SHARE);
    child = this;
    auto found = this->SearchKeyIndex(key, position);

    // for LocateKey operation, we only release latches on Internal Nodes, and
    // keep the latch on the leaf node
    //  note that even if key is not found, we should always lock that leaf node
    context->ReleaseLatch(depth, common::Constants::SHARE);
    return found;
  }

  bool Balance(Node<KeyType, ValueType, QueryContext, NodeMetadata> *right, KeyType &boundary) {
    auto right_sibling = static_cast<LeafNode<KeyType, ValueType, Capacity> *>(right);
    if (this->Size() < UNDERFLOW_BOUND(Capacity) && right_sibling->Size() > UNDERFLOW_BOUND(Capacity)) {
      assert(this->Size() == UNDERFLOW_BOUND(Capacity) - 1);
      /**
       * this node is underflow, while its right sibling is not
       * start borrowing process - borrow 1 item from its right sibling
       */
      boundary = LEFTMOST_KEY(right_sibling);
      this->ShiftAndInsert(right_sibling->keys_[0], right_sibling->values_[0], this->Size());
      bool unused_underflow = false;
      right_sibling->DeleteIndex(0, unused_underflow);
      assert(unused_underflow == false);
      return false;
    }
    if (this->Size() > UNDERFLOW_BOUND(Capacity) && right_sibling->Size() < UNDERFLOW_BOUND(Capacity)) {
      assert(right_sibling->Size() == UNDERFLOW_BOUND(Capacity) - 1);
      /**
       * right sibling is underflow, while the current node is not
       * start borrowing process
       */
      right_sibling->ShiftAndInsert(this->keys_[this->Size() - 1], this->values_[this->Size() - 1], 0);
      bool unused_underflow = false;
      this->DeleteIndex(this->Size() - 1, unused_underflow);
      assert(unused_underflow == false);
      boundary = RIGHTMOST_KEY(this);
      return false;
    }
    /**
     * both siblings are underflow, merge two siblings into one, update
     * `boundary`, and delete right sibling
     */
    std::copy(right_sibling->keys_, right_sibling->keys_ + right_sibling->Size(), this->keys_ + this->Size());
    std::copy(right_sibling->values_, right_sibling->values_ + right_sibling->Size(), this->values_ + this->Size());
    this->Size() += right_sibling->Size();
    this->right_sibl_ = right_sibling->right_sibl_;
    boundary = RIGHTMOST_KEY(this);

    return true;
  }
};

/**
 * @brief InternalNode class definition
 */
template <typename KeyType, typename ValueType, int Capacity>
class InternalNode : public Node<KeyType, ValueType, QueryContext, NodeMetadata> {
private:
  /**
   * The internal node maintains N-1 keys and N child pointers (N == Capacity)
   *    and child[I] contains all <Key, Value> pairs whose key <= keys[I]
   *    in other words, the stored format of an internal node is as follow:
   *      child[0] - keys[0] - child[1] - keys[1] - ...... - child[N-1] -
   * keys[N-1] - child[N] - keys[N]
   *
   * We also allow extra OVERFLOW_SIZE entries to easily implement Internal Node
   * split operation these OVERFLOW_SIZE entries are in-memory only, which
   * means, these entries won't be persisted in non-volatile media
   */
  KeyType keys_[Capacity + common::Constants::OVERFLOW_SIZE];
  Node<KeyType, ValueType, QueryContext, NodeMetadata> *child_[Capacity + common::Constants::OVERFLOW_SIZE];

  InternalNode<KeyType, ValueType, Capacity> *right_sibl_;

  void ShiftAndInsert(const KeyType &key, Node<KeyType, ValueType, QueryContext, NodeMetadata> *child, int insert_pos) {
    // only move backward key array if new child is not supposed to be new
    // right-most child
    if (insert_pos <= this->Size()) {
      std::move_backward(this->keys_ + insert_pos, this->keys_ + this->Size(),
                         this->keys_ + this->Size() + common::Constants::OVERFLOW_SIZE);
      this->keys_[insert_pos] = key;
    }
    // insert new child
    std::move_backward(this->child_ + insert_pos, this->child_ + this->Size() + 1,
                       this->child_ + this->Size() + common::Constants::OVERFLOW_SIZE + 1);
    this->child_[insert_pos] = child;
    this->Size()++;
  }

  void DeleteIndex(int index, bool &underflow) {
    if (index < this->Size()) {
      std::move(this->keys_ + index + 1, this->keys_ + this->Size(), this->keys_ + index);
      std::move(this->child_ + index + 1, this->child_ + this->Size() + 1, this->child_ + index);
    }
    underflow = (--this->Size() < UNDERFLOW_BOUND(Capacity)) ? true : false;
  }

public:
  InternalNode() = default;

  InternalNode(Node<KeyType, ValueType, QueryContext, NodeMetadata> *left_chld,
               Node<KeyType, ValueType, QueryContext, NodeMetadata> *right_chld, KeyType boundary_key)
      : keys_{boundary_key, RIGHTMOST_KEY(right_chld)},
        child_{static_cast<Node<KeyType, ValueType, QueryContext, NodeMetadata> *>(left_chld),
               static_cast<Node<KeyType, ValueType, QueryContext, NodeMetadata> *>(right_chld)},
        right_sibl_(nullptr) {
    this->Size() = 2;
  }

  InternalNode(Split<KeyType, ValueType, QueryContext, NodeMetadata> &split)
      : InternalNode(split.left, split.right, split.boundary_key) {}

  /**
   * Right sibling constructor, should only executed only when this is overflow
   * @param keys        An array of keys from current InternalNode
   * @param children    An array of child pointers from current InternalNode
   * @param start_idx   The new sibling should clone keys in the range of
   * [start_idx, end)
   * @param right_sibling
   */
  InternalNode(
      KeyType (&keys)[Capacity + common::Constants::OVERFLOW_SIZE],
      Node<KeyType, ValueType, QueryContext, NodeMetadata> *(&children)[Capacity + common::Constants::OVERFLOW_SIZE],
      int start_idx, InternalNode<KeyType, ValueType, Capacity> *right_sibling) {
    this->right_sibl_ = right_sibling;
    this->Size() = Capacity - start_idx + 1;
    std::copy(keys + start_idx, keys + Capacity + common::Constants::OVERFLOW_SIZE, this->keys_);
    std::copy(children + start_idx, children + Capacity + common::Constants::OVERFLOW_SIZE, this->child_);
  }

  ~InternalNode() {
    for (int idx = 0; idx < this->Size(); idx++) {
      delete this->child_[idx];
    }
  }

  int SearchChildIndex(const KeyType &key) {
    return std::lower_bound(this->keys_, this->keys_ + this->Size() - 1, key) - this->keys_;
  }

  constexpr NodeType Type() { return INTERNAL; };
  constexpr NodeMetadata &Metadata() { return this->meta_; }

  std::string String() {
    std::stringstream ss;
    ss << "[INTERNAL: ";
    for (int idx = 0; idx < this->Size() - 1; idx++) {
      ss << this->child_[idx]->String() << " | " << this->keys_[idx] << " | ";
    }
    ss << this->child_[this->Size() - 1]->String() << "]";
    return ss.str();
  }

  constexpr KeyType &GetKey(int offset) {
    assert(offset >= 0 && offset < Capacity);
    return this->keys_[offset];
  }

  /**
   * Get the child at index `idx`
   * Require the caller to already have shared-lock on the internal node
   * @param idx
   * @return
   */
  Node<KeyType, ValueType, QueryContext, NodeMetadata> *GetChild(int idx) {
    assert(idx < this->meta_.size_ + 1);
    return this->child_[idx];
  }

  /**
   * Clear the child ptrs of the internal node
   * It should only be triggered before cleaning up an invalid internal node for
   * memory reclaimation
   */
  void ClearChildArray() { std::fill_n(this->child_, std::size(this->child_), nullptr); }

  int &Size() { return this->Metadata().size_; };
  InternalNode<KeyType, ValueType, Capacity> *RightSibling() const { return this->right_sibl_; }

  /**************************************************************************************
   * @brief Core utilities are placed below, and all are thread-safe, except
   *Balance    *
   **************************************************************************************/

  bool Insert(const KeyType &key, const ValueType &val, Split<KeyType, ValueType, QueryContext, NodeMetadata> &split,
              QueryContext *context) {
    // lock crabbing - acquire the exclusive latch on current node first
    // after that, if this node is 100% safe, unlock all exclusive latches on
    // its ancestors
    auto depth = context->AcquireLatch(this->Metadata().SharedLatchPtr(), common::Constants::EXCLUSIVE);
    if (this->Size() < Capacity) {
      context->ReleaseLatch(depth, common::Constants::EXCLUSIVE);
    }

    int target_idx = this->SearchChildIndex(key);
    auto target = this->child_[target_idx];

    bool is_split = target->Insert(key, val, split, context);
    /**
     * the target child did not cause a split, hence no new child is created,
     *  and this node won't create a split
     */
    if (!is_split) {
      context->ReleaseLatch(depth + 1, common::Constants::EXCLUSIVE);
      return false;
    }

    // new sibling should be inserted next to `target`
    int insert_pos = target_idx + 1;

    /**
     * the `target` child is split into two, it and its new right sibling
     * in this case, the key boundary for `target` should be
     * `split.boundary_key` while `target`'s old boundary will be used for its
     * new right sibling if `target` is not the right-most child
     */
    std::swap(this->keys_[target_idx], split.boundary_key);
    this->ShiftAndInsert(split.boundary_key,
                         static_cast<Node<KeyType, ValueType, QueryContext, NodeMetadata> *>(split.right), insert_pos);

    // no need to split this current internal node, release all latches from
    // this safe internal node to its lowest leaf
    if (this->Size() <= Capacity) {
      context->ReleaseLatchFromParent(depth, common::Constants::EXCLUSIVE);
      return false;
    }

    // come here means that we have to split current node into two
    int boundary_idx = UNDERFLOW_BOUND(this->Size());

    // initialize new right sibling
    auto new_sibling =
        new InternalNode<KeyType, ValueType, Capacity>(this->keys_, this->child_, boundary_idx, this->right_sibl_);

    // modify in-memory content of this node
    this->Size() = boundary_idx;
    this->right_sibl_ = new_sibling;

    // populate split data structure
    split.left = this;
    split.right = new_sibling;
    split.boundary_key = this->keys_[boundary_idx - 1];

    // because the current node splits, the unlocking responsibility belongs to
    // its lowest safe ancestor
    return true;
  }

  bool Search(const KeyType &key, ValueType &value, QueryContext *context) {
    // lock crabbing: acquire share latch on current node and unlock all share
    // latches on its ancestors
    auto depth = context->AcquireLatch(this->Metadata().SharedLatchPtr(), common::Constants::SHARE);
    context->ReleaseLatch(depth, common::Constants::SHARE);

    auto target = this->child_[this->SearchChildIndex(key)];

    // latch on this node will be unlocked by its child, we dont have to worry
    // about unlock it
    return target->Search(key, value, context);
  }

  bool Update(const KeyType &key, const ValueType &value, QueryContext *context) {
    // lock crabbing: acquire share latch on current node and unlock all share
    // latches on its ancestors
    auto depth = context->AcquireLatch(this->Metadata().SharedLatchPtr(), common::Constants::EXCLUSIVE);
    context->ReleaseLatch(depth, common::Constants::EXCLUSIVE);

    auto target = this->child_[this->SearchChildIndex(key)];

    // latch on this node will be unlocked by its child, we dont have to worry
    // about unlock it
    return target->Update(key, value, context);
  }

  bool LocateKey(const KeyType &key, Node<KeyType, ValueType, QueryContext, NodeMetadata> *&child, int &position,
                 QueryContext *context) {
    // lock crabbing: acquire share latch on current node and unlock all share
    // latches on its ancestors
    auto depth = context->AcquireLatch(this->Metadata().SharedLatchPtr(), common::Constants::SHARE);
    context->ReleaseLatch(depth, common::Constants::SHARE);

    auto target = this->child_[this->SearchChildIndex(key)];

    // latch on this node will be unlocked by its child, we dont have to worry
    // about unlock it
    return target->LocateKey(key, child, position, context);
  }

  bool Delete(const KeyType &key, bool &underflow, QueryContext *context) {
    // lock crabbing: acquire exclusive latch on current node
    auto depth = context->AcquireLatch(this->Metadata().SharedLatchPtr(), common::Constants::EXCLUSIVE);
    // if this node is likely to be safe, we should unlock all the exclusive
    // latches on its ancestors ASAP
    if (this->Size() - 1 >= UNDERFLOW_BOUND(Capacity)) {
      context->ReleaseLatch(depth, common::Constants::EXCLUSIVE);
    }

    int target_idx = this->SearchChildIndex(key);
    auto target = this->child_[target_idx];

    bool deleted = target->Delete(key, underflow, context);

    /**
     * key is not found || the target is not underflow
     *  -> tree structure is the same -> do nothing
     */
    if (!deleted || !underflow) {
      context->ReleaseLatch(depth + 1, common::Constants::EXCLUSIVE);
      return deleted;
    }

    assert(underflow == true);
    /**
     * If the current node doesn't have enough sibling to re-balance, we simply
     * return true (key is deleted) The unlocking responsibility now lays on its
     * parent
     */
    if (this->Size() <= 1) return true;

    /**
     * @brief execute re-balance operation, always prefer the left sibling of
     * current node such deterministic behavior does not affect perf
     * significantly, while it offers easier testing + impl
     */
    int boundary_idx, sibl_index;
    Node<KeyType, ValueType, QueryContext, NodeMetadata> *left_child, *right_child;
    if (target_idx >= 1) {
      boundary_idx = sibl_index = target_idx - 1;
      left_child = this->child_[target_idx - 1];
      right_child = target;
    } else {
      boundary_idx = target_idx;
      sibl_index = target_idx + 1;
      left_child = target;
      right_child = this->child_[target_idx + 1];
    }
    auto target_latch = target->Metadata().SharedLatchPtr();
    auto sibl_latch = this->child_[sibl_index]->Metadata().SharedLatchPtr();
    sibl_latch->lock();
    KeyType boundary = this->keys_[boundary_idx];

    auto merged = left_child->Balance(right_child, boundary);
    // note that, for ease of the impl, we should assume that only latch of
    // left_child is hold after the Balance operation
    //  the main reason for that is, if a merge op is executed, right_child will
    //  be de-allocated if right child's latch is still locked, SEGFAULT will
    //  arise -> we have to prevent this
    if (right_child == target) {
      assert(target_latch == context->latches_[depth + 1]);
      context->ReplaceLatch(depth + 1, sibl_latch, common::Constants::EXCLUSIVE);
    } else {
      sibl_latch->unlock();
    }

    // if a borrow op was executed, update the boundary and return
    if (!merged) {
      /**
       * this node is safe, unlock all latches on its ancestors, as well as all
       * its descendants note that it is important to release latches on its
       * ancestors first, because of:
       *  - other threads can acquire locks asap
       *  - maintain the semantic that only the lowest safe internal can release
       * latches on its children
       */
      context->ReleaseLatch(depth, common::Constants::EXCLUSIVE);
      this->keys_[boundary_idx] = boundary;
      underflow = false;
      context->ReleaseLatchFromParent(depth, common::Constants::EXCLUSIVE);
      return true;
    }

    // if a merge op was executed, update correct split key and delete the
    // right_child
    std::swap(this->keys_[boundary_idx], this->keys_[boundary_idx + 1]);
    this->DeleteIndex(boundary_idx + 1, underflow);
    if (right_child->Type() == INTERNAL) {
      static_cast<InternalNode<KeyType, ValueType, Capacity> *>(right_child)->ClearChildArray();
    }
    delete right_child;

    /**
     * lock crabbing, only unlock all latches if this node is not underflow
     *    otherwise, the exclusive latch on this node has to be unlocked by
     *      one of its latched ancestor (the lowest safe one)
     */
    if (!underflow) {
      context->ReleaseLatch(depth, common::Constants::EXCLUSIVE);
      context->ReleaseLatchFromParent(depth, common::Constants::EXCLUSIVE);
    }
    return true;
  }

  bool Balance(Node<KeyType, ValueType, QueryContext, NodeMetadata> *right, KeyType &boundary) {
    auto right_sibling = static_cast<InternalNode<KeyType, ValueType, Capacity> *>(right);
    // The right-sibling pointer is wrong (very rarely), just reset it for
    // safety
    this->right_sibl_ = right_sibling;
    if (this->Size() < UNDERFLOW_BOUND(Capacity) && right_sibling->Size() > UNDERFLOW_BOUND(Capacity)) {
      assert(this->Size() == UNDERFLOW_BOUND(Capacity) - 1);
      /**
       * this node is underflow, while its right sibling is not
       * start borrowing process:
       * - borrow the key from parent + left most child of right sibl
       * - retrieve new `boundary` value - old left most key of right sibl
       * - delete <key[0], child[0]> from right sibl
       */
      this->child_[this->Size()] = right_sibling->child_[0];
      this->keys_[this->Size()] = RIGHTMOST_KEY(this->child_[this->Size()]);
      boundary = this->keys_[this->Size()++];
      bool unused_underflow = false;
      right_sibling->DeleteIndex(0, unused_underflow);
      assert(unused_underflow == false);
      return false;
    }
    if (this->Size() > UNDERFLOW_BOUND(Capacity) && right_sibling->Size() < UNDERFLOW_BOUND(Capacity)) {
      assert(right_sibling->Size() == UNDERFLOW_BOUND(Capacity) - 1);
      /**
       * right sibling is underflow, while the current node is not
       * start borrowing process for right sibling:
       * - borrow the key from parent + right most child of current node
       * - retrieve new `boundary` value - old right most key of current node
       * - delete last item of current node (by simply decreasing the size)
       */
      right_sibling->ShiftAndInsert(boundary, this->child_[this->Size() - 1], 0);
      --this->Size();
      boundary = RIGHTMOST_KEY(this);
      return false;
    }
    /**
     * both siblings are underflow, merge two siblings into one and delete right
     * sibling `boundary` parameter would become the new boundary of the merged
     * node the caller should be responsible for this value
     */
    this->keys_[this->Size()] = boundary;
    std::copy(right_sibling->keys_, right_sibling->keys_ + right_sibling->Size(), this->keys_ + this->Size());
    std::copy(right_sibling->child_, right_sibling->child_ + right_sibling->Size(), this->child_ + this->Size());
    this->Size() += right_sibling->Size();
    this->right_sibl_ = right_sibling->right_sibl_;

    return true;
  }
};

/**
 * A memory B+Tree implementation, distinguished by the KeyType, ValueType and
 * the capacity of Leaf and Internal nodes
 */
template <typename KeyType, typename ValueType, int LeafCapacity, int InternalCapacity>
class MemoryBTree : public BTreeInterface<KeyType, ValueType, QueryContext> {
private:
  std::unique_ptr<Node<KeyType, ValueType, QueryContext, NodeMetadata>> root_;
  std::shared_mutex tree_latch_;

  constexpr std::shared_mutex *LatchPtr() { return &this->tree_latch_; }

public:
  MemoryBTree() { root_ = std::make_unique<LeafNode<KeyType, ValueType, LeafCapacity>>(); }

  std::string String() const { return this->root_->String(); }

  bool Search(const KeyType &key, ValueType &val, QueryContext *context) {
    context->AcquireLatch(this->LatchPtr(), common::Constants::SHARE);
    auto found = this->root_->Search(key, val, context);
    context->Clear();
    return found;
  }

  void Insert(const KeyType &key, const ValueType &val, QueryContext *context) {
    context->AcquireLatch(this->LatchPtr(), common::Constants::EXCLUSIVE);
    Split<KeyType, ValueType, QueryContext, NodeMetadata> split;
    bool required_split = this->root_->Insert(key, val, split, context);
    if (required_split) {
      /**
       * we only manage ownership of the root node,
       *  and this root node will maintain the ownership of all child nodes
       * because the old root will become a child of the new root, so we dont
       * have to deallocate it
       */
      this->root_.release();
      this->root_ = std::make_unique<InternalNode<KeyType, ValueType, InternalCapacity>>(split);
      context->ReleaseLatchFromParent(0, common::Constants::EXCLUSIVE);
    }
#ifdef NDEBUG
    else {
      assert(context->smallest_unlk_idx_ == context->latches_.size());
    }
#endif
    context->Clear();
  }

  bool Delete(const KeyType &key, QueryContext *context) {
    context->AcquireLatch(this->LatchPtr(), common::Constants::EXCLUSIVE);
    bool underflow = false;
    auto ret = this->root_->Delete(key, underflow, context);
    // if key is not found, then all latches should be unlocked already
    if (!ret) {
#ifdef NDEBUG
      assert(context->smallest_unlk_idx_ == context->latches_.size());
      assert(!underflow);
#endif
      return ret;
    }
    /**
     * When the current root only has 1 child left,
     *  we have to push its only child and make that child the new root node
     */
    if (this->root_->Type() == NodeType::INTERNAL && this->root_->Size() == 1) {
      assert(underflow == true);
      auto old_root = static_cast<InternalNode<KeyType, ValueType, InternalCapacity> *>(this->root_.release());
      this->root_.reset(old_root->GetChild(0));
      old_root->ClearChildArray();
      delete old_root;
    }
    if (underflow) {
      context->ReleaseLatchFromParent(0, common::Constants::EXCLUSIVE);
    }
#ifdef NDEBUG
    else {
      assert(context->smallest_unlk_idx_ == context->latches_.size());
    }
#endif
    context->Clear();
    return true;
  }

  void Clear() { this->root_.reset(new LeafNode<KeyType, ValueType, LeafCapacity>()); };

  class MemoryIterator : public BTreeInterface<KeyType, ValueType, QueryContext>::Iterator {
  private:
    int offset_;
    KeyType key_high_;
    bool upper_bound_;
    LeafNode<KeyType, ValueType, LeafCapacity> *current_;
    QueryContext *ctx_;

  public:
    MemoryIterator(LeafNode<KeyType, ValueType, LeafCapacity> *node, int offset, const KeyType &key_high,
                   QueryContext *context)
        : offset_(offset), key_high_(key_high), upper_bound_(true), current_(node), ctx_(context) {}
    MemoryIterator(LeafNode<KeyType, ValueType, LeafCapacity> *node, int offset, QueryContext *context)
        : offset_(offset), upper_bound_(false), current_(node), ctx_(context) {}
    MemoryIterator(LeafNode<KeyType, ValueType, LeafCapacity> *node, QueryContext *context)
        : offset_(0), upper_bound_(false), current_(node), ctx_(context) {}
    ~MemoryIterator() {}
    bool Next(KeyType &key, ValueType &val) {
      // current node is an invalid one
      if (!this->current_) return false;
      if (this->current_->GetEntry(this->offset_, key, val)) {
        this->offset_++;
        return !upper_bound_ || key <= this->key_high_;
      } else {
        this->offset_ = 0;
        auto right_sibl = this->current_->RightSibling();
        if (right_sibl) {
          auto lock_success = right_sibl->Metadata().SharedLatchPtr()->try_lock_shared();
          if (!lock_success) {
            throw std::range_error("Can't acquire SHARE lock on next sibling");
          }
          auto sibl_depth = ctx_->AcquireLatch(right_sibl->Metadata().SharedLatchPtr(), common::Constants::NONE);
          ctx_->ReleaseLatch(sibl_depth, common::Constants::SHARE);
        }
        this->current_ = right_sibl;
        return this->Next(key, val);
      }
    }
  };
  MemoryIterator *RangeQuery(const KeyType &key_low, const KeyType &key_high, QueryContext *context) {
    context->AcquireLatch(this->LatchPtr(), common::Constants::SHARE);
    Node<KeyType, ValueType, QueryContext, NodeMetadata> *leaf;
    int offset;
    this->root_->LocateKey(key_low, leaf, offset, context);
    assert(context->smallest_unlk_idx_ == static_cast<int>(context->latches_.size()) - 1);
    return new MemoryIterator(dynamic_cast<LeafNode<KeyType, ValueType, LeafCapacity> *>(leaf), offset, key_high,
                              context);
  }
  MemoryIterator *TreeScan(QueryContext *context) {
    context->AcquireLatch(this->LatchPtr(), common::Constants::SHARE);
    auto current = static_cast<Node<KeyType, ValueType, QueryContext, NodeMetadata> *>(this->root_.get());
    context->AcquireLatch(current->Metadata().SharedLatchPtr(), common::Constants::SHARE);
    while (current->Type() == NodeType::INTERNAL) {
      auto child = (static_cast<InternalNode<KeyType, ValueType, InternalCapacity> *>(current))->GetChild(0);
      auto child_depth = context->AcquireLatch(child->Metadata().SharedLatchPtr(), common::Constants::SHARE);
      context->ReleaseLatch(child_depth, common::Constants::SHARE);
      current = child;
    }
    return new MemoryIterator(dynamic_cast<LeafNode<KeyType, ValueType, LeafCapacity> *>(current), context);
  }
};

}  // namespace btree::implementation
