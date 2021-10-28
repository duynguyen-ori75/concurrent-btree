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

#include <cmath>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "common/constants.h"
#include "common/macros.h"

namespace btree::implementation {

enum NodeType { LEAF, INTERNAL };

// Forward declaration
template <typename KeyType, typename ValueType, typename ContextType, typename MetadataClass>
class Node;

// data structure that represents the information associated with node split
template <typename KeyType, typename ValueType, typename ContextType, typename MetadataClass>
struct Split {
  Node<KeyType, ValueType, ContextType, MetadataClass> *left;
  Node<KeyType, ValueType, ContextType, MetadataClass> *right;
  KeyType boundary_key;
};

template <typename KeyType, typename ValueType, typename ContextType, typename MetadataClass>
class Node {
protected:
  MetadataClass meta_;

public:
  virtual ~Node(){};

  /**********************************************************************
   * @brief All utilities are placed below                              *
   **********************************************************************/

  /**
   * @return Type of current node
   */
  virtual constexpr NodeType Type() = 0;

  /**
   * @return Metadata of this node
   */
  virtual constexpr MetadataClass &Metadata() = 0;

  /**
   * @return Number of entries in the leaf node or the number of children in the
   * internal node
   */
  virtual int &Size() = 0;

  /**
   * @return The current right sibling of this node
   */
  virtual Node<KeyType, ValueType, ContextType, MetadataClass> *RightSibling() const = 0;

  /**
   * @return String representation of this node.
   */
  virtual std::string String() = 0;

  /**
   * @return Get key at specific index
   */
  virtual constexpr KeyType &GetKey(int offset) = 0;

  /**************************************************************************************
   * @brief Core utilities are placed below, and all are thread-safe, except
   *Balance    *
   **************************************************************************************/

  /**
   * Insert an entry (key, val) into the node with split support
   * @param key
   * @param val
   * @param split boundary information of current node with its new sibling, if
   * a split is required
   * @return false if no split caused, true otherwise
   */
  virtual bool Insert(const KeyType &key, const ValueType &val,
                      Split<KeyType, ValueType, ContextType, MetadataClass> &split, ContextType *context) = 0;

  /**
   * Search for the value associated with the given key
   * @param key
   * @param value
   * @return true/false whether is key is found. If found, value is stored in
   * value variable
   */
  virtual bool Search(const KeyType &key, ValueType &value, ContextType *context) = 0;

  /**
   * Update the value associated with the given key
   * @param key
   * @param value
   * @return true if a key is found, false otherwise
   */
  virtual bool Update(const KeyType &key, const ValueType &value, ContextType *context) = 0;

  /**
   * Delete the entry of the given key.
   * @param key
   * @param underflow If the node is under-flowed, the flag is set to true
   * @return True if the searched key is found and deleted, otherwise return
   * false
   */
  virtual bool Delete(const KeyType &key, bool &underflow, ContextType *context) = 0;

  /**
   * This function is called when either this node or its right sibling node
   * overflows The underflow is solved by borrowing one entry from one node to
   * the other If both nodes are under-flowed, merge operation is triggered Note
   * that the caller has to lock both current node and its parent first before
   * triggering this function In other words, this func is not responsible for
   * latching/unlocking latch on any B-tree node Moreover, the caller should
   * also be responsible for de-allocating right_sibling_node in case a merge op
   * is executed
   * @param right_sibling_node
   * @param boundary    This should store the old `boundary` of the two
   * siblings, and be updated if necessary The old `boundary` is only useful for
   * Internal node re-balancing operation
   * @return False if entries redistribution is executed between two nodes, with
   * the boundary info stored in `boundary` True if two nodes are merged, and
   * the right sibling node is deleted
   */
  virtual bool Balance(Node<KeyType, ValueType, ContextType, MetadataClass> *right_sibling_node, KeyType &boundary) = 0;

  /**
   * Locate the leaf node which contains the given key, and SHARE lock it
   *  Even if key is not there, the SHARE latch will still holds
   * @param key
   * @param child The leaf node
   * @param position Offset of the searched key in child
   * @return true/false whether the search is completed successfully or not
   */
  virtual bool LocateKey(const KeyType &key, Node<KeyType, ValueType, ContextType, MetadataClass> *&child,
                         int &position, ContextType *context) = 0;
};

template <typename KeyType, typename ValueType, typename Context>
class BTreeInterface {
public:
  virtual void Insert(const KeyType &key, const ValueType &val, Context *context) = 0;
  virtual bool Delete(const KeyType &key, Context *context) = 0;
  virtual bool Search(const KeyType &key, ValueType &val, Context *context) = 0;
  // the caller must makes sure that there is no other thread using the tree
  // before running Clear()
  virtual void Clear() = 0;
  virtual std::string String() const = 0;
  class Iterator {
  public:
    virtual ~Iterator(){};
    virtual bool Next(KeyType &key, ValueType &val) = 0;
  };
  virtual Iterator *RangeQuery(const KeyType &key_low, const KeyType &key_high, Context *context) = 0;
  virtual Iterator *TreeScan(Context *context) = 0;
};

}  // namespace btree::implementation