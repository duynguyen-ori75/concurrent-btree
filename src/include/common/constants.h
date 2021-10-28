/*
Copyright (C) 2021 Duy Nguyen
All rights reserved.
Last modification: Aug 31, 2021
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

#include <cstdint>

namespace btree::common {
/**
 * Declare all system-level constants here.
 */

struct Constants {
  /**
   * @brief Max supported tree height
   */
  static const int MAX_HEIGHT = 20;

  /**
   * @brief Max support overflow size for internal node
   */
  static const int OVERFLOW_SIZE = 1;

  /**
   * @brief Various types of shared lock
   */
  enum SharedLockType { NONE, SHARE, EXCLUSIVE };
};
}  // namespace btree::common