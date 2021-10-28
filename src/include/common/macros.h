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

/**
 * Assembly Wait
 */
#define NOP_PAUSE ({ __asm__ volatile("pause" ::); })

//===----------------------------------------------------------------------===//
// Helper macros for B-Tree
//===----------------------------------------------------------------------===//
#define UNDERFLOW_BOUND(N) (((N) + 1) / 2)
#define LEFTMOST_KEY(NODE)      \
  ({                            \
    assert((NODE)->Size() > 0); \
    (NODE)->GetKey(0);          \
  })
#define RIGHTMOST_KEY(NODE)             \
  ({                                    \
    assert((NODE)->Size() > 0);         \
    (NODE)->GetKey((NODE)->Size() - 1); \
  })

//===----------------------------------------------------------------------===//
// Helper macros for Shadowing B-Tree
//===----------------------------------------------------------------------===//
#define LATEST_VERSION(NODE) \
  (((NODE)->SiblingVersion()->VersionInfo() <= (NODE)->VersionInfo()) ? (NODE) : (NODE)->SiblingVersion())

#define POSSIBLE_NEGATIVE_VERSION(NODE) \
  (((NODE)->SiblingVersion()->VersionInfo() < 0) ? (NODE)->SiblingVersion() : (NODE))

#define MODIFIED_VERSION(NODE) \
  ((POSSIBLE_NEGATIVE_VERSION(NODE)->VersionInfo() < 0) ? POSSIBLE_NEGATIVE_VERSION(NODE) : LATEST_VERSION(NODE))
