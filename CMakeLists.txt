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

cmake_minimum_required(VERSION 3.18)

project(btree VERSION 0.0.1 LANGUAGES CXX)

set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_C_FLAGS "-g -fPIC -Wall -Wextra")
set(CMAKE_CXX_FLAGS "-g -fPIC -Wall -Wextra")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

#######################################################################################################################
# global variables
#######################################################################################################################
set(BTREE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/src/include)

#######################################################################################################################
# options & requirements
#######################################################################################################################
find_package(Threads REQUIRED)

option(ENABLE_TESTING "Build tests" OFF)
if(ENABLE_TESTING)
    find_package(GTest REQUIRED)
    enable_testing()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DENABLE_TESTING")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENABLE_TESTING")
endif()

#######################################################################################################################
# format            :   Reformat the codebase according to standards.
# check-format      :   Check if the codebase is formatted according to standards.
#######################################################################################################################

find_program(CLANG_FORMAT_BIN NAMES clang-format HINTS ${CLANG_TOOLS_SEARCH_PATH})
if ("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
    message(STATUS "[MISSING] clang-format not found, no format and no check-format.")
else ()
    # The directories to be formatted. Note that we modified the format script to take in multiple arguments.
    string(CONCAT FORMAT_DIRS
            "${CMAKE_CURRENT_SOURCE_DIR}/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/test"
            )

    # Run clang-format and update files in place.
    add_custom_target(format
            ${BUILD_SUPPORT_DIR}/run-clang-format.py
            ${CLANG_FORMAT_BIN}
            --source_dirs
            ${FORMAT_DIRS}
            --fix
            USES_TERMINAL
            )

    message(STATUS "[ADDED] clang-format and check-clang-format (${CLANG_FORMAT_BIN})")

    unset(FORMAT_DIRS)
endif ()
unset(CLANG_FORMAT_BIN)

#######################################################################################################################
# main
#######################################################################################################################
add_subdirectory(src)
add_subdirectory(test)