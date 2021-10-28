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

#include <climits>
#include <iostream>
#include <unordered_set>

#include <gtest/gtest.h>

#include "tree/lock_crabbing.h"

namespace btree::implementation {

TEST(BPlusTree, InsertAndQueryTest) {
  MemoryBTree<int, int, 5, 5> tree;
  QueryContext context;

  tree.Insert(1, 1, &context);
  tree.Insert(3, 3, &context);
  tree.Insert(6, 6, &context);
  tree.Insert(2, 2, &context);
  tree.Insert(7, 7, &context);
  tree.Insert(10, 10, &context);
  tree.Insert(9, 9, &context);
  tree.Insert(8, 8, &context);
  tree.Insert(11, 11, &context);
  tree.Insert(4, 4, &context);
  tree.Insert(5, 5, &context);
  tree.Insert(12, 12, &context);

  for (int i = 1; i <= 12; ++i) {
    int value;
    EXPECT_EQ(true, tree.Search(i, value, &context));
    EXPECT_EQ(i, value);
  }
}

TEST(BPlusTree, InsertReverseOrderAndQueryTest) {
  MemoryBTree<int, int, 2, 2> tree;
  QueryContext context;

  tree.Insert(11, 11, &context);
  tree.Insert(12, 12, &context);
  tree.Insert(10, 10, &context);
  tree.Insert(9, 9, &context);
  tree.Insert(8, 8, &context);
  tree.Insert(7, 7, &context);
  tree.Insert(5, 5, &context);
  tree.Insert(4, 4, &context);
  tree.Insert(6, 6, &context);
  tree.Insert(3, 3, &context);
  tree.Insert(2, 2, &context);
  tree.Insert(1, 1, &context);

  for (int i = 1; i <= 12; ++i) {
    int value;
    EXPECT_EQ(true, tree.Search(i, value, &context));
    EXPECT_EQ(i, value);
  }
}

TEST(BPlusTree, Search) {
  MemoryBTree<int, int, 2, 2> tree;
  int value;
  QueryContext context;

  EXPECT_FALSE(tree.Search(100, value, &context));

  tree.Insert(100, 100, &context);

  EXPECT_FALSE(tree.Search(0, value, &context));
  EXPECT_FALSE(tree.Search(200, value, &context));

  tree.Insert(101, 100, &context);
  tree.Insert(110, 110, &context);
  tree.Insert(150, 150, &context);
  tree.Insert(170, 170, &context);

  EXPECT_FALSE(tree.Search(0, value, &context));
  EXPECT_FALSE(tree.Search(105, value, &context));
  EXPECT_FALSE(tree.Search(120, value, &context));
  EXPECT_FALSE(tree.Search(160, value, &context));
  EXPECT_FALSE(tree.Search(180, value, &context));
}

TEST(BPlusTree, DeleteWithoutMergeTest) {
  MemoryBTree<int, int, 4, 4> tree;
  QueryContext context;

  tree.Insert(1, 1, &context);
  tree.Insert(2, 2, &context);
  tree.Insert(3, 3, &context);
  tree.Insert(4, 4, &context);
  tree.Insert(5, 5, &context);
  tree.Insert(6, 6, &context);
  EXPECT_EQ("[INTERNAL: [LEAF: (1,1) (2,2)] | 2 | [LEAF: (3,3) (4,4) (5,5) (6,6)]]", tree.String());

  tree.Delete(4, &context);
  tree.Delete(3, &context);

  EXPECT_EQ("[INTERNAL: [LEAF: (1,1) (2,2)] | 2 | [LEAF: (5,5) (6,6)]]", tree.String());
}

TEST(BPlusTree, DeleteWithLeafNodeRebalancedAndMerged) {
  MemoryBTree<int, int, 4, 4> tree;
  QueryContext context;

  tree.Insert(1, 1, &context);
  tree.Insert(2, 2, &context);
  tree.Insert(3, 3, &context);
  tree.Insert(4, 4, &context);
  tree.Insert(5, 5, &context);
  tree.Insert(6, 6, &context);
  tree.Insert(7, 7, &context);
  tree.Insert(8, 8, &context);

  tree.Delete(4, &context);
  EXPECT_EQ(
      "[INTERNAL: [LEAF: (1,1) (2,2) (3,3)] | 4 | [LEAF: (5,5) (6,6) "
      "(7,7) (8,8)]]",
      tree.String());

  EXPECT_FALSE(tree.Delete(4, &context));
  EXPECT_FALSE(tree.Delete(0, &context));
  tree.Delete(1, &context);
  tree.Delete(3, &context);
  EXPECT_EQ("[INTERNAL: [LEAF: (2,2) (5,5)] | 5 | [LEAF: (6,6) (7,7) (8,8)]]", tree.String());

  tree.Delete(5, &context);
  tree.Delete(6, &context);

  tree.Delete(2, &context);
  EXPECT_EQ("[LEAF: (7,7) (8,8)]", tree.String());

  tree.Delete(7, &context);
  tree.Delete(8, &context);

  EXPECT_EQ("[LEAF: ]", tree.String());
}

TEST(BPlusTree, DeleteWithInternalNodeRebalancedAndMerged) {
  MemoryBTree<int, int, 4, 4> tree;
  QueryContext context;

  tree.Insert(1, 1, &context);
  tree.Insert(2, 2, &context);
  tree.Insert(3, 3, &context);
  tree.Insert(4, 4, &context);
  tree.Insert(5, 5, &context);
  tree.Insert(6, 6, &context);
  tree.Insert(7, 7, &context);
  tree.Insert(8, 8, &context);
  tree.Insert(9, 9, &context);
  tree.Insert(10, 10, &context);
  tree.Insert(11, 11, &context);
  tree.Insert(12, 12, &context);
  tree.Insert(13, 13, &context);
  tree.Insert(14, 14, &context);
  tree.Insert(15, 15, &context);
  tree.Insert(16, 16, &context);

  tree.Delete(16, &context);
  tree.Delete(15, &context);
  tree.Delete(14, &context);
  tree.Delete(13, &context);
  tree.Delete(12, &context);

  tree.Delete(11, &context);
  tree.Delete(10, &context);
  tree.Delete(9, &context);
  tree.Delete(8, &context);
  tree.Delete(7, &context);
  tree.Delete(6, &context);
  tree.Delete(5, &context);
  tree.Delete(4, &context);
  tree.Delete(3, &context);
  tree.Delete(2, &context);
  tree.Delete(1, &context);

  EXPECT_EQ("[LEAF: ]", tree.String());
}

TEST(BPlusTree, IteratorFullScanTest) {
  MemoryBTree<int, int, 4, 4> tree;
  QueryContext context;

  const int number_of_tuples = 10000;
  std::vector<int> tuples;
  for (int i = 0; i < number_of_tuples; ++i) {
    tuples.push_back(i);
  }
  std::random_shuffle(tuples.begin(), tuples.end());

  for (auto it = tuples.begin(); it != tuples.end(); ++it) {
    tree.Insert(*it, *it, &context);
  }

  std::unique_ptr<MemoryBTree<int, int, 4, 4>::MemoryIterator> it;
  it.reset(tree.TreeScan(&context));

  int key, value;
  int i = 0;
  while (it->Next(key, value)) {
    EXPECT_EQ(i, key);
    EXPECT_EQ(i, value);
    i++;
  }
}

TEST(BPlusTree, IteratorRangeScanOnEmptyTreeTest) {
  MemoryBTree<int, int, 4, 4> tree;
  QueryContext context;

  std::unique_ptr<MemoryBTree<int, int, 4, 4>::MemoryIterator> it;
  it.reset(tree.RangeQuery(INT_MIN, INT_MAX, &context));
  int k, v;
  EXPECT_EQ(false, it->Next(k, v));
}

TEST(BPlusTree, IteratorRangeScanTest) {
  MemoryBTree<int, int, 4, 4> tree;
  QueryContext context;

  const int number_of_tuples = 10000;
  std::vector<int> tuples;
  for (int i = 0; i < number_of_tuples; ++i) {
    tuples.push_back(i);
  }
  std::random_shuffle(tuples.begin(), tuples.end());

  for (auto it = tuples.begin(); it != tuples.end(); ++it) {
    tree.Insert(*it, *it, &context);
  }

  std::unique_ptr<MemoryBTree<int, int, 4, 4>::MemoryIterator> it;

  const int runs = 10;
  for (int i = 0; i < runs; i++) {
    int start = rand() % number_of_tuples;
    int end = rand() % number_of_tuples;
    it.reset(tree.RangeQuery(start, end, &context));
    int key, value;
    int founds = 0;
    while (it->Next(key, value)) {
      EXPECT_EQ(founds + start, key);
      EXPECT_EQ(founds + start, value);
      founds++;
    }
    EXPECT_EQ(start <= end ? end - start + 1 : 0, founds);
  }
}

TEST(BPlusTree, MassiveRandomInsertionAndQuery) {
  std::unordered_set<int> s;
  MemoryBTree<int, int, 4, 4> tree;
  QueryContext context;
  constexpr int tuples = 100000;
  const int range = tuples * 10;

  for (int i = 0; i < tuples; ++i) {
    const int r = std::rand() % range;
    s.insert(r);
    tree.Insert(r, r, &context);
  }

  for (int i = 0; i < range; ++i) {
    int value = -1;
    if (s.find(i) != s.end()) {
      EXPECT_EQ(true, tree.Search(i, value, &context));
      EXPECT_EQ(i, value);
    } else {
      EXPECT_EQ(false, tree.Search(i, value, &context));
    }
  }
}

TEST(BPlusTree, KeysInsertedAndDeletedInRandomOrder) {
  MemoryBTree<int, int, 4, 4> tree;
  QueryContext context;

  constexpr int number_of_tuples = 100000;
  std::array<int, number_of_tuples> tuples{number_of_tuples};
  for (int i = 0; i < number_of_tuples; ++i) {
    tuples[i] = i;
  }
  std::random_shuffle(tuples.begin(), tuples.end());

  for (int i = 0; i < number_of_tuples; ++i) {
    tree.Insert(tuples[i], tuples[i], &context);
  }

  std::random_shuffle(tuples.begin(), tuples.end());
  for (auto it = tuples.begin(); it != tuples.end(); ++it) {
    tree.Delete(*it, &context);
  }

  EXPECT_EQ("[LEAF: ]", tree.String());
}

}  // namespace btree::implementation