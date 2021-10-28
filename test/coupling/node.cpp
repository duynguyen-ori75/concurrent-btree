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

#include <gtest/gtest.h>

#include "common/constants.h"
#include "tree/lock_crabbing.h"

using namespace btree::implementation;
using namespace btree::common;

namespace btree::implementation {

/**
 * @brief Most of the Insert and Split ops dont require testing `context`
 * carefully Therefore, only tests related to Delete & Merge validate the state
 * of `context` variable
 */
TEST(LeafNode, InsertionWithoutSplit) {
  LeafNode<int, int, 5> leaf;
  QueryContext context;
  Split<int, int, QueryContext, NodeMetadata> split;

  leaf.Insert(1, 1, split, &context);
  leaf.Insert(3, 2, split, &context);
  leaf.Insert(2, 5, split, &context);
  leaf.Insert(-1, 5, split, &context);
  leaf.Insert(6, 2, split, &context);
  EXPECT_EQ("[LEAF: (-1,5) (1,1) (2,5) (3,2) (6,2)]", leaf.String());
}

TEST(LeafNode, InsertionWithoutSplitDouble) {
  LeafNode<double, int, 5> leaf;
  QueryContext context;
  Split<double, int, QueryContext, NodeMetadata> split;

  leaf.Insert(1.3, 1, split, &context);
  leaf.Insert(3.2, 2, split, &context);
  leaf.Insert(2.5, 5, split, &context);
  leaf.Insert(-1.0, 5, split, &context);
  leaf.Insert(-6.4, 2, split, &context);
  EXPECT_EQ("[LEAF: (-6.4,2) (-1,5) (1.3,1) (2.5,5) (3.2,2)]", leaf.String());
}

TEST(LeafNode, InsertionAndUpdate) {
  LeafNode<int, int, 5> leaf_node;
  QueryContext context;
  Split<int, int, QueryContext, NodeMetadata> split;

  leaf_node.Insert(1, 2, split, &context);
  leaf_node.Insert(1, 3, split, &context);
  EXPECT_EQ("[LEAF: (1,3)]", leaf_node.String());
}

TEST(LeafNode, Search) {
  LeafNode<int, int, 10> leaf_node;
  QueryContext context;
  Split<int, int, QueryContext, NodeMetadata> split;

  leaf_node.Insert(1, 5, split, &context);
  leaf_node.Insert(5, 8, split, &context);
  leaf_node.Insert(-1, 222, split, &context);
  int result;
  EXPECT_TRUE(leaf_node.Search(1, result, &context));
  EXPECT_EQ(5, result);

  EXPECT_TRUE(leaf_node.Search(5, result, &context));
  EXPECT_EQ(8, result);

  EXPECT_TRUE(leaf_node.Search(-1, result, &context));
  EXPECT_EQ(222, result);
}

TEST(LeafNode, Update) {
  LeafNode<int, int, 10> leaf_node;
  QueryContext context;
  Split<int, int, QueryContext, NodeMetadata> split;

  leaf_node.Insert(5, 5, split, &context);
  leaf_node.Insert(9, 10, split, &context);
  leaf_node.Insert(3, 100, split, &context);

  int value;

  EXPECT_TRUE(leaf_node.Update(5, 100, &context));
  leaf_node.Search(5, value, &context);
  EXPECT_EQ(100, value);

  EXPECT_TRUE(leaf_node.Update(3, 20, &context));
  leaf_node.Search(3, value, &context);
  EXPECT_EQ(20, value);

  EXPECT_TRUE(leaf_node.Update(9, 44, &context));
  leaf_node.Search(9, value, &context);
  EXPECT_EQ(44, value);
}

TEST(LeafNode, Delete) {
  LeafNode<int, int, 5> leaf_node;
  QueryContext context;
  Split<int, int, QueryContext, NodeMetadata> split;

  leaf_node.Insert(5, 5, split, &context);
  leaf_node.Insert(6, 6, split, &context);
  leaf_node.Insert(7, 7, split, &context);
  leaf_node.Insert(3, 3, split, &context);

  bool underflow;

  EXPECT_TRUE(leaf_node.Delete(6, underflow, &context));
  EXPECT_FALSE(underflow);
  EXPECT_EQ("[LEAF: (3,3) (5,5) (7,7)]", leaf_node.String());

  EXPECT_FALSE(leaf_node.Delete(6, underflow, &context));

  // clean lock crabbing context before deletion
  context.Clear();
  EXPECT_TRUE(leaf_node.Delete(3, underflow, &context));
  EXPECT_EQ("[LEAF: (5,5) (7,7)]", leaf_node.String());
  EXPECT_TRUE(underflow);

  // clean lock crabbing context
  context.ReleaseLatchFromParent(0, Constants::EXCLUSIVE);
  context.Clear();

  EXPECT_TRUE(leaf_node.Delete(7, underflow, &context));
  EXPECT_EQ("[LEAF: (5,5)]", leaf_node.String());
  EXPECT_TRUE(underflow);

  // clean lock crabbing context
  context.ReleaseLatchFromParent(0, Constants::EXCLUSIVE);
  context.Clear();

  EXPECT_TRUE(leaf_node.Delete(5, underflow, &context));
  EXPECT_EQ("[LEAF: ]", leaf_node.String());
  EXPECT_TRUE(underflow);

  // clean lock crabbing context
  context.ReleaseLatchFromParent(0, Constants::EXCLUSIVE);
  context.Clear();
}

TEST(LeafNode, SplitInsertLeft) {
  QueryContext context;
  auto leaf = new LeafNode<int, int, 4>();
  Split<int, int, QueryContext, NodeMetadata> split;

  EXPECT_FALSE(leaf->Insert(3, 3, split, &context));
  EXPECT_FALSE(leaf->Insert(4, 4, split, &context));
  EXPECT_FALSE(leaf->Insert(6, 6, split, &context));
  EXPECT_FALSE(leaf->Insert(5, 5, split, &context));

  EXPECT_TRUE(leaf->Insert(1, 1, split, &context));
  EXPECT_EQ(4, split.boundary_key);
  EXPECT_EQ("[LEAF: (1,1) (3,3) (4,4)]", split.left->String());
  EXPECT_EQ("[LEAF: (5,5) (6,6)]", split.right->String());

  delete split.left;
  delete split.right;
}

TEST(LeafNode, SplitInsertRight) {
  QueryContext context;
  auto leaf = new LeafNode<int, int, 4>();
  Split<int, int, QueryContext, NodeMetadata> split;
  EXPECT_FALSE(leaf->Insert(3, 3, split, &context));
  EXPECT_FALSE(leaf->Insert(4, 4, split, &context));
  EXPECT_FALSE(leaf->Insert(7, 7, split, &context));
  EXPECT_FALSE(leaf->Insert(5, 5, split, &context));

  EXPECT_TRUE(leaf->Insert(6, 6, split, &context));
  EXPECT_EQ(4, split.boundary_key);
  EXPECT_EQ("[LEAF: (3,3) (4,4)]", split.left->String());
  EXPECT_EQ("[LEAF: (5,5) (6,6) (7,7)]", split.right->String());

  delete split.left;
  delete split.right;
}

TEST(LeafNode, BalanceBorrowing) {
  QueryContext context;
  LeafNode<int, int, 3> leaf, *right_sibling;
  Split<int, int, QueryContext, NodeMetadata> split;

  EXPECT_FALSE(leaf.Insert(3, 2, split, &context));

  context.ReleaseLatch(1, Constants::EXCLUSIVE);
  context.Clear();
  EXPECT_FALSE(leaf.Insert(2, 1, split, &context));
  EXPECT_EQ(context.latches_.size(), 1);

  context.ReleaseLatch(1, Constants::EXCLUSIVE);
  context.Clear();
  EXPECT_FALSE(leaf.Insert(-1, 5, split, &context));
  EXPECT_EQ(context.latches_.size(), 1);

  context.ReleaseLatch(1, Constants::EXCLUSIVE);
  context.Clear();
  EXPECT_TRUE(leaf.Insert(6, 6, split, &context));
  EXPECT_EQ(context.latches_.size(), 1);
  EXPECT_EQ(2, split.boundary_key);
  EXPECT_EQ("[LEAF: (-1,5) (2,1)]", split.left->String());
  EXPECT_EQ("[LEAF: (3,2) (6,6)]", split.right->String());

  context.ReleaseLatch(1, Constants::EXCLUSIVE);
  context.Clear();

  bool underflow;
  EXPECT_TRUE(leaf.Delete(2, underflow, &context));
  EXPECT_EQ(context.latches_.size(), 1);
  EXPECT_TRUE(underflow);
  EXPECT_EQ("[LEAF: (-1,5)]", leaf.String());

  context.ReleaseLatchFromParent(0, Constants::EXCLUSIVE);
  context.Clear();

  right_sibling = static_cast<LeafNode<int, int, 3> *>(split.right);

  EXPECT_FALSE(right_sibling->Insert(1, 7, split, &context));
  EXPECT_EQ("[LEAF: (1,7) (3,2) (6,6)]", right_sibling->String());

  EXPECT_EQ(context.smallest_unlk_idx_, 1);
  EXPECT_EQ(context.latches_.size(), 1);
  context.Clear();

  /* we have to acquire latches before executing Balance */
  context.AcquireLatch(leaf.meta_.SharedLatchPtr(), Constants::EXCLUSIVE);
  context.AcquireLatch(right_sibling->meta_.SharedLatchPtr(), Constants::EXCLUSIVE);
  EXPECT_FALSE(leaf.Balance(right_sibling, split.boundary_key));
  EXPECT_EQ(context.latches_.size(), 2);
  EXPECT_EQ(context.smallest_unlk_idx_, 0);
  EXPECT_EQ(1, split.boundary_key);
  EXPECT_EQ("[LEAF: (-1,5) (1,7)]", leaf.String());
  EXPECT_EQ("[LEAF: (3,2) (6,6)]", right_sibling->String());

  context.ReleaseLatchFromParent(0, Constants::EXCLUSIVE);

  delete right_sibling;
}

TEST(LeafNode, BalanceMerge) {
  QueryContext context;
  LeafNode<int, int, 3> leaf, *right_sibling;
  Split<int, int, QueryContext, NodeMetadata> split;

  // these three insert ops dont cause split, there is no need to test `context`
  // these cases
  EXPECT_FALSE(leaf.Insert(3, 2, split, &context));
  EXPECT_FALSE(leaf.Insert(2, 1, split, &context));
  EXPECT_FALSE(leaf.Insert(-1, 5, split, &context));

  EXPECT_EQ(context.smallest_unlk_idx_, 3);
  EXPECT_EQ(context.latches_.size(), 3);
  context.Clear();

  // a split requires the caller to release latches
  EXPECT_TRUE(leaf.Insert(6, 6, split, &context));
  EXPECT_EQ(2, split.boundary_key);
  EXPECT_EQ("[LEAF: (-1,5) (2,1)]", split.left->String());
  EXPECT_EQ("[LEAF: (3,2) (6,6)]", split.right->String());

  EXPECT_EQ(context.smallest_unlk_idx_, 0);
  EXPECT_EQ(context.latches_.size(), 1);
  context.ReleaseLatchFromParent(0, Constants::EXCLUSIVE);
  context.Clear();

  bool underflow;
  EXPECT_TRUE(leaf.Delete(2, underflow, &context));
  EXPECT_TRUE(underflow);
  EXPECT_EQ("[LEAF: (-1,5)]", leaf.String());
  EXPECT_EQ(context.smallest_unlk_idx_, 0);
  EXPECT_EQ(context.latches_.size(), 1);

  right_sibling = static_cast<LeafNode<int, int, 3> *>(split.right);
  context.AcquireLatch(right_sibling->meta_.SharedLatchPtr(), Constants::EXCLUSIVE);

  EXPECT_TRUE(leaf.Balance(right_sibling, split.boundary_key));
  EXPECT_EQ(6, split.boundary_key);
  EXPECT_EQ("[LEAF: (-1,5) (3,2) (6,6)]", leaf.String());

  EXPECT_EQ(context.latches_.size(), 2);
  EXPECT_EQ(context.smallest_unlk_idx_, 0);
  context.ReleaseLatchFromParent(0, Constants::EXCLUSIVE);

  delete right_sibling;
}

TEST(InternalNode, InsertWithoutSplit) {
  QueryContext context;
  Split<int, int, QueryContext, NodeMetadata> split;

  auto left_leaf = new LeafNode<int, int, 2>();
  auto right_leaf = new LeafNode<int, int, 2>();

  left_leaf->Insert(1, 1, split, &context);
  right_leaf->Insert(3, 3, split, &context);

  split = Split<int, int, QueryContext, NodeMetadata>{left_leaf, right_leaf, 2};
  InternalNode<int, int, 4> internal(split);
  EXPECT_EQ("[INTERNAL: [LEAF: (1,1)] | 2 | [LEAF: (3,3)]]", internal.String());

  EXPECT_FALSE(internal.Insert(2, 2, split, &context));
  EXPECT_EQ("[INTERNAL: [LEAF: (1,1) (2,2)] | 2 | [LEAF: (3,3)]]", internal.String());

  EXPECT_FALSE(internal.Insert(4, 4, split, &context));
  EXPECT_EQ("[INTERNAL: [LEAF: (1,1) (2,2)] | 2 | [LEAF: (3,3) (4,4)]]", internal.String());
}

TEST(InternalNode, InsertSplitSupportWithoutSplit) {
  QueryContext context;
  Split<int, int, QueryContext, NodeMetadata> split;

  auto left_leaf = new LeafNode<int, int, 2>();
  auto right_leaf = new LeafNode<int, int, 2>();

  left_leaf->Insert(1, 1, split, &context);
  right_leaf->Insert(3, 3, split, &context);

  split = Split<int, int, QueryContext, NodeMetadata>{left_leaf, right_leaf, 2};
  InternalNode<int, int, 4> internal(split);
  EXPECT_EQ("[INTERNAL: [LEAF: (1,1)] | 2 | [LEAF: (3,3)]]", internal.String());

  EXPECT_FALSE(internal.Insert(2, 2, split, &context));
  EXPECT_EQ("[INTERNAL: [LEAF: (1,1) (2,2)] | 2 | [LEAF: (3,3)]]", internal.String());

  EXPECT_FALSE(internal.Insert(4, 4, split, &context));
  EXPECT_EQ("[INTERNAL: [LEAF: (1,1) (2,2)] | 2 | [LEAF: (3,3) (4,4)]]", internal.String());
}

TEST(InternalNode, InsertionSplitInternalLeftAndBorrowLeft) {
  QueryContext context;
  auto leaf = new LeafNode<int, int, 2>();
  Split<int, int, QueryContext, NodeMetadata> split, split_2nd;

  EXPECT_FALSE(leaf->Insert(3, 3, split, &context));
  EXPECT_FALSE(leaf->Insert(5, 5, split, &context));
  EXPECT_TRUE(leaf->Insert(6, 6, split, &context));

  auto internal = new InternalNode<int, int, 3>(split);
  EXPECT_EQ("[INTERNAL: [LEAF: (3,3)] | 3 | [LEAF: (5,5) (6,6)]]", internal->String());

  EXPECT_FALSE(internal->Insert(4, 4, split, &context));
  EXPECT_EQ("[INTERNAL: [LEAF: (3,3)] | 3 | [LEAF: (4,4) (5,5)] | 5 | [LEAF: (6,6)]]", internal->String());

  EXPECT_FALSE(internal->Insert(2, 2, split, &context));
  EXPECT_EQ(
      "[INTERNAL: [LEAF: (2,2) (3,3)] | 3 | [LEAF: (4,4) (5,5)] | 5 | "
      "[LEAF: (6,6)]]",
      internal->String());

  EXPECT_TRUE(internal->Insert(1, 1, split_2nd, &context));

  auto root = std::make_unique<InternalNode<int, int, 3>>(split_2nd);
  EXPECT_EQ(
      "[INTERNAL: "
      "[INTERNAL: [LEAF: (1,1) (2,2)] | 2 | [LEAF: (3,3)]] "
      "| 3 | "
      "[INTERNAL: [LEAF: (4,4) (5,5)] | 5 | [LEAF: (6,6)]]"
      "]",
      root->String());

  EXPECT_FALSE(root->Insert(7, 7, split, &context));
  EXPECT_FALSE(root->Insert(8, 8, split, &context));
  EXPECT_EQ(
      "[INTERNAL: "
      "[INTERNAL: [LEAF: (1,1) (2,2)] | 2 | [LEAF: (3,3)]] "
      "| 3 | "
      "[INTERNAL: [LEAF: (4,4) (5,5)] | 5 | [LEAF: (6,6)] | 6 | [LEAF: "
      "(7,7) (8,8)]]"
      "]",
      root->String());

  bool underflow = true;
  EXPECT_TRUE(root->Delete(3, underflow, &context));
  EXPECT_FALSE(underflow);

  EXPECT_TRUE(root->Delete(2, underflow, &context));
  EXPECT_FALSE(underflow);
  EXPECT_EQ(
      "[INTERNAL: "
      "[INTERNAL: [LEAF: (1,1)] | 3 | [LEAF: (4,4) (5,5)]] "
      "| 5 | "
      "[INTERNAL: [LEAF: (6,6)] | 6 | [LEAF: (7,7) (8,8)]]"
      "]",
      root->String());
}

TEST(InternalNode, InsertionSplitInternalRightAndBorrowRight) {
  auto leaf = new LeafNode<int, int, 2>();
  Split<int, int, QueryContext, NodeMetadata> split, split_2nd;
  QueryContext context;

  EXPECT_FALSE(leaf->Insert(3, 3, split, &context));
  EXPECT_FALSE(leaf->Insert(5, 5, split, &context));
  EXPECT_TRUE(leaf->Insert(8, 8, split, &context));

  auto internal = new InternalNode<int, int, 3>(split);
  EXPECT_EQ("[INTERNAL: [LEAF: (3,3)] | 3 | [LEAF: (5,5) (8,8)]]", internal->String());

  EXPECT_FALSE(internal->Insert(4, 4, split, &context));
  EXPECT_EQ("[INTERNAL: [LEAF: (3,3)] | 3 | [LEAF: (4,4) (5,5)] | 5 | [LEAF: (8,8)]]", internal->String());

  EXPECT_FALSE(internal->Insert(2, 2, split, &context));
  EXPECT_EQ(
      "[INTERNAL: [LEAF: (2,2) (3,3)] | 3 | [LEAF: (4,4) (5,5)] | 5 | "
      "[LEAF: (8,8)]]",
      internal->String());

  EXPECT_FALSE(internal->Insert(6, 6, split, &context));
  EXPECT_EQ(
      "[INTERNAL: [LEAF: (2,2) (3,3)] | 3 | [LEAF: (4,4) (5,5)] | 5 | "
      "[LEAF: (6,6) (8,8)]]",
      internal->String());

  EXPECT_TRUE(internal->Insert(7, 7, split_2nd, &context));

  auto root = std::make_unique<InternalNode<int, int, 3>>(split_2nd);
  EXPECT_EQ(
      "[INTERNAL: "
      "[INTERNAL: [LEAF: (2,2) (3,3)] | 3 | [LEAF: (4,4) (5,5)]] "
      "| 5 | "
      "[INTERNAL: [LEAF: (6,6)] | 6 | [LEAF: (7,7) (8,8)]]"
      "]",
      root->String());

  EXPECT_FALSE(root->Insert(1, 1, split, &context));
  EXPECT_EQ(
      "[INTERNAL: "
      "[INTERNAL: [LEAF: (1,1) (2,2)] | 2 | [LEAF: (3,3)] | 3 | [LEAF: "
      "(4,4) (5,5)]] "
      "| 5 | "
      "[INTERNAL: [LEAF: (6,6)] | 6 | [LEAF: (7,7) (8,8)]]"
      "]",
      root->String());

  bool underflow = true;
  EXPECT_TRUE(root->Delete(6, underflow, &context));
  EXPECT_FALSE(underflow);
  EXPECT_EQ(
      "[INTERNAL: "
      "[INTERNAL: [LEAF: (1,1) (2,2)] | 2 | [LEAF: (3,3)] | 3 | [LEAF: "
      "(4,4) (5,5)]] "
      "| 5 | "
      "[INTERNAL: [LEAF: (7,7)] | 7 | [LEAF: (8,8)]]"
      "]",
      root->String());

  EXPECT_TRUE(root->Delete(7, underflow, &context));
  EXPECT_FALSE(underflow);
  EXPECT_EQ(
      "[INTERNAL: "
      "[INTERNAL: [LEAF: (1,1) (2,2)] | 2 | [LEAF: (3,3)]] "
      "| 3 | "
      "[INTERNAL: [LEAF: (4,4) (5,5)] | 5 | [LEAF: (8,8)]]"
      "]",
      root->String());
}

TEST(InternalNode, DeleteAndBalance) {
  bool underflow;
  auto leaf = new LeafNode<int, int, 2>();
  Split<int, int, QueryContext, NodeMetadata> split, split_2nd;
  QueryContext context;

  EXPECT_FALSE(leaf->Insert(3, 3, split, &context));
  EXPECT_FALSE(leaf->Insert(5, 5, split, &context));

  // dont validate context for non-split insert - not necessary
  EXPECT_EQ(context.smallest_unlk_idx_, 2);
  EXPECT_EQ(context.latches_.size(), 2);
  context.Clear();

  EXPECT_TRUE(leaf->Insert(6, 6, split, &context));
  EXPECT_EQ(context.smallest_unlk_idx_, 0);
  EXPECT_EQ(context.latches_.size(), 1);

  auto internal = new InternalNode<int, int, 3>(split);
  // lock crabbing context should be clear after constructing + setting the new
  // root node
  context.ReleaseLatchFromParent(0, Constants::EXCLUSIVE);
  context.Clear();

  // a non-split insert from an internal node will release latches on all nodes
  EXPECT_FALSE(internal->Insert(4, 4, split, &context));
  EXPECT_EQ(context.smallest_unlk_idx_, 2);
  EXPECT_EQ(context.latches_.size(), 2);
  EXPECT_EQ("[INTERNAL: [LEAF: (3,3)] | 3 | [LEAF: (4,4) (5,5)] | 5 | [LEAF: (6,6)]]", internal->String());
  context.Clear();

  EXPECT_FALSE(internal->Insert(2, 2, split, &context));
  EXPECT_EQ(context.smallest_unlk_idx_, 2);
  EXPECT_EQ(context.latches_.size(), 2);
  context.Clear();

  // a split insert will maintain exclusive latches on all unsafe nodes in the
  // traversal path
  EXPECT_TRUE(internal->Insert(1, 1, split_2nd, &context));
  EXPECT_EQ(context.smallest_unlk_idx_, 0);
  EXPECT_EQ(context.latches_.size(), 2);

  auto root = std::make_unique<InternalNode<int, int, 3>>(split_2nd);
  context.ReleaseLatchFromParent(0, Constants::EXCLUSIVE);
  context.Clear();
  EXPECT_EQ(
      "[INTERNAL: "
      "[INTERNAL: [LEAF: (1,1) (2,2)] | 2 | [LEAF: (3,3)]] "
      "| 3 | "
      "[INTERNAL: [LEAF: (4,4) (5,5)] | 5 | [LEAF: (6,6)]]"
      "]",
      root->String());

  /**
   * acquire latches on 3 nodes in the traversal path
   *  1 latch on sibling will be locked & unlocked in the Balance() op -> not
   * counted here all 3 latches should be unlocked at second depth (aka depth =
   * 1)
   */
  EXPECT_TRUE(root->Delete(3, underflow, &context));
  EXPECT_FALSE(underflow);
  ASSERT_EQ(context.smallest_unlk_idx_, 3);
  EXPECT_EQ(context.latches_.size(), 3);
  context.Clear();
  EXPECT_EQ(
      "[INTERNAL: "
      "[INTERNAL: [LEAF: (1,1)] | 1 | [LEAF: (2,2)]] "
      "| 3 | "
      "[INTERNAL: [LEAF: (4,4) (5,5)] | 5 | [LEAF: (6,6)]]"
      "]",
      root->String());

  /**
   * after deleting 1, root node + its two child node -> 3 latches
   *  two leaf are locked in the delete op, so wont be counted here
   *  the latched wont be released because the root node requires some
   * modification i.e. expected_unlk_depth_ will be 0
   */
  EXPECT_TRUE(root->Delete(1, underflow, &context));
  EXPECT_TRUE(underflow);
  EXPECT_EQ(context.smallest_unlk_idx_, 0);
  EXPECT_EQ(context.latches_.size(), 3);
  ASSERT_EQ(
      "[INTERNAL: [INTERNAL: [LEAF: (2,2)] | 3 | [LEAF: (4,4) (5,5)] | 5 "
      "| [LEAF: (6,6)]]]",
      root->String());
  context.ReleaseLatchFromParent(0, Constants::EXCLUSIVE);
  context.Clear();

  // update new root
  auto old_root = static_cast<InternalNode<int, int, 3> *>(root.release());
  root.reset(static_cast<InternalNode<int, int, 3> *>(old_root->GetChild(0)));
  old_root->ClearChildArray();
  delete old_root;

  EXPECT_TRUE(root->Delete(2, underflow, &context));
  EXPECT_FALSE(underflow);
  EXPECT_EQ(context.smallest_unlk_idx_, 2);
  EXPECT_EQ(context.latches_.size(), 2);
  context.Clear();
  EXPECT_EQ("[INTERNAL: [LEAF: (4,4)] | 4 | [LEAF: (5,5)] | 5 | [LEAF: (6,6)]]", root->String());

  EXPECT_TRUE(root->Delete(5, underflow, &context));
  EXPECT_FALSE(underflow);
  EXPECT_EQ(context.smallest_unlk_idx_, 2);
  EXPECT_EQ(context.latches_.size(), 2);
  context.Clear();
  EXPECT_EQ("[INTERNAL: [LEAF: (4,4)] | 5 | [LEAF: (6,6)]]", root->String());

  EXPECT_TRUE(root->Delete(6, underflow, &context));
  EXPECT_TRUE(underflow);
  EXPECT_EQ(context.smallest_unlk_idx_, 0);
  EXPECT_EQ(context.latches_.size(), 2);
  context.ReleaseLatchFromParent(0, Constants::EXCLUSIVE);
  context.Clear();
  EXPECT_EQ("[INTERNAL: [LEAF: (4,4)]]", root->String());

  int test_offset;
  Node<int, int, QueryContext, NodeMetadata> *test_node;
  EXPECT_FALSE(root->LocateKey(6, test_node, test_offset, &context));
  EXPECT_EQ(context.smallest_unlk_idx_, 1);
  EXPECT_EQ(context.latches_.size(), 2);
  context.ReleaseLatch(2, Constants::SHARE);
  context.Clear();

  EXPECT_TRUE(root->LocateKey(4, test_node, test_offset, &context));

  // this new_root node should be SHARE locked for now, we should unlock it
  EXPECT_EQ(context.smallest_unlk_idx_, 1);
  EXPECT_EQ(context.latches_.size(), 2);
  context.ReleaseLatch(2, Constants::SHARE);
  context.Clear();

  auto new_root = static_cast<LeafNode<int, int, 2> *>(test_node);
  EXPECT_EQ("[LEAF: (4,4)]", new_root->String());

  EXPECT_TRUE(new_root->Delete(4, underflow, &context));
  EXPECT_TRUE(underflow);
  EXPECT_EQ(context.smallest_unlk_idx_, 0);
  EXPECT_EQ(context.latches_.size(), 1);
  EXPECT_EQ("[LEAF: ]", new_root->String());
}

}  // namespace btree::implementation