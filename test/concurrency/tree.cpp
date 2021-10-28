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

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <queue>
#include <thread>

#include <gtest/gtest.h>

#include "common/constants.h"
#include "tree/lock_crabbing.h"

using namespace btree::implementation;
using namespace btree::common;

enum OperationType { INSERT, SEARCH, DELETE };

#define NO_THREADS 10
#define MAX_KEY 100000
#define NODE_CAPACITY 10
#define MAX_OPERATION 200000

TEST(ConcurrentTreeTest, InsertAndSearch) {
  MemoryBTree<int, int, NODE_CAPACITY, NODE_CAPACITY> tree;
  std::array<std::atomic<bool>, MAX_KEY + 1> inserted = {false, MAX_KEY + 1};

  std::atomic<int> current_op = 0, next_key = 1;
  std::array<OperationType, MAX_OPERATION> ops;
  // at least 1 of all test threads has to be an insert one
  //  to prevent the case where all initial threads are doing SEARCH
  //  which cause the program to be in livelock at line 83-85
  ops[0] = INSERT;
  for (int idx = 1; idx < MAX_OPERATION; ++idx) {
    int r = rand() % 2;
    ops[idx] = static_cast<OperationType>(r);
  }

  std::thread threads[NO_THREADS];
  for (int tidx = 0; tidx < NO_THREADS; ++tidx) {
    threads[tidx] = std::thread([&]() {
      int op_idx;
      QueryContext context;
      while (true) {
        op_idx = current_op++;
        if (op_idx > MAX_OPERATION) break;
        switch (ops[op_idx]) {
          case INSERT: {
            int key = next_key++;
            // ensure that key is always in [1, MAX_KEY]
            if (key > MAX_KEY) {
              key = rand() % MAX_KEY + 1;
              next_key = MAX_KEY;
            }
            tree.Insert(key, key, &context);
            inserted[key] = true;
          } break;
          case SEARCH: {
            int key, value;
            do {
              key = rand() % next_key;
            } while (key > MAX_KEY || !inserted[key]);
            auto found = tree.Search(key, value, &context);
            ASSERT_TRUE(found);
            EXPECT_EQ(key, value);
          } break;
          default: {
          }
        }
        context.Clear();
      }
    });
  }

  for (int tidx = 0; tidx < NO_THREADS; ++tidx) {
    threads[tidx].join();
  }

  int value;
  QueryContext context;
  for (int key = 1; key <= MAX_KEY; ++key) {
    auto found = tree.Search(key, value, &context);
    ASSERT_TRUE(found);
    EXPECT_EQ(key, value);
    context.Clear();
  }
}

TEST(ConcurrentTreeTest, DeleteAndSearch) {
  MemoryBTree<int, int, NODE_CAPACITY, NODE_CAPACITY> tree;
  std::array<std::atomic<bool>, MAX_KEY + 1> exists;
  std::array<std::atomic_flag, MAX_KEY + 1> locked;

  std::atomic<int> current_op = 0;
  std::array<OperationType, MAX_OPERATION> ops;
  ops[0] = DELETE;
  for (int idx = 1; idx < MAX_OPERATION; ++idx) {
    int r = rand() % MAX_OPERATION;
    // Should not remove all the keys
    ops[idx] = (r <= MAX_KEY / 2) ? DELETE : SEARCH;
  }
  for (int key = 1; key <= MAX_KEY; ++key) {
    QueryContext context;
    tree.Insert(key, key, &context);
    context.Clear();
    exists[key] = true;
    locked[key].clear();
  }

  std::thread threads[NO_THREADS];
  for (int tidx = 0; tidx < NO_THREADS; ++tidx) {
    threads[tidx] = std::thread([&]() {
      int op_idx;
      QueryContext context;
      while (true) {
        op_idx = current_op++;
        if (op_idx > MAX_OPERATION) break;
        switch (ops[op_idx]) {
          case DELETE: {
            int key;
            while (true) {
              key = rand() % MAX_KEY + 1;
              auto is_locked = locked[key].test_and_set(std::memory_order_acquire);
              if (!is_locked) break;
            }
            ASSERT_TRUE(exists[key]);
            // ensure that key is always in [1, MAX_KEY]
            auto deleted = tree.Delete(key, &context);
            exists[key] = false;
            ASSERT_TRUE(deleted);
          } break;
          case SEARCH: {
            int key, value;
            do {
              key = rand() % MAX_KEY + 1;
            } while (exists[key]);
            auto found = tree.Search(key, value, &context);
            ASSERT_FALSE(found);
          } break;
          // mute the excessive warning
          default: {
          }
        }
        context.Clear();
      }
    });
  }

  for (int tidx = 0; tidx < NO_THREADS; ++tidx) {
    threads[tidx].join();
  }

  int value;
  QueryContext context;
  for (int key = 1; key <= MAX_KEY; ++key) {
    auto found = tree.Search(key, value, &context);
    if (exists[key]) {
      ASSERT_TRUE(found);
      EXPECT_EQ(key, value);
    } else {
      ASSERT_FALSE(found);
    }
    context.Clear();
  }
}