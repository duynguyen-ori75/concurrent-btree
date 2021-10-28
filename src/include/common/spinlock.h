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

#include <atomic>

#include "common/macros.h"

namespace btree::common {

class Spinlock {
public:
  Spinlock() : flag_(ATOMIC_FLAG_INIT) {}

  // non-copyable/non-movable
  Spinlock(const Spinlock &) = delete;
  Spinlock(Spinlock &&) = delete;
  Spinlock &operator=(const Spinlock &) = delete;

  inline void Lock() {
    while (flag_.test_and_set(std::memory_order_acquire)) NOP_PAUSE;
  }

  inline void Unlock() { flag_.clear(std::memory_order_release); }

  inline bool TryLock() { return !flag_.test_and_set(std::memory_order_acquire); }

private:
  std::atomic_flag flag_;
};

}  // namespace btree::common