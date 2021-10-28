# Copyright 2021 Duy Nguyen. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if(ENABLE_TESTING)
    set(TEST_REQUIRED_LIBS ${GTEST_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
    set(TEST_REQUIRED_HEADERS ${BTREE_INCLUDE_DIRECTORIES} ${GTEST_INCLUDE_DIRS})

    function(TREE_ADD_TEST TEST_NAME)
      add_executable(${ARGV})
      target_link_libraries(${TEST_NAME} PRIVATE ${TEST_REQUIRED_LIBS})
      target_include_directories(${TEST_NAME} PRIVATE ${TEST_REQUIRED_HEADERS})
      add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
    endfunction()

    TREE_ADD_TEST(coupling_node coupling/node.cpp main.cpp)
    TREE_ADD_TEST(coupling_tree coupling/tree.cpp main.cpp)
    TREE_ADD_TEST(concurrency_node concurrency/node.cpp main.cpp)
    TREE_ADD_TEST(concurrency_tree concurrency/tree.cpp main.cpp)
endif()