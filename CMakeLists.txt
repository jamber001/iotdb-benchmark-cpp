# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

CMAKE_MINIMUM_REQUIRED(VERSION 3.7)

PROJECT(benchmark-cpp VERSION 1.0.0)

SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
SET(CMAKE_POSITION_INDEPENDENT_CODE ON)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g")

SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin )
MESSAGE( " Build executable file: ${EXECUTABLE_OUTPUT_PATH}/${PROJECT_NAME} \n")

configure_file(src/cpp/benchmark_version.h.in "${PROJECT_SOURCE_DIR}/src/cpp/benchmark_version.h")
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/iotdb_sdk/include)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/src/cpp)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/src/cpp/common)
LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/iotdb_sdk/lib)

AUX_SOURCE_DIRECTORY(./src/cpp SRC_LIST)
AUX_SOURCE_DIRECTORY(./src/cpp/common SRC_LIST2)

ADD_EXECUTABLE(${PROJECT_NAME} ${SRC_LIST} ${SRC_LIST2})
TARGET_LINK_LIBRARIES(${PROJECT_NAME} iotdb_session pthread)


# Install
set(CMAKE_INSTALL_PREFIX "benchmark_cpp")
install(TARGETS ${PROJECT_NAME}
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        PUBLIC_HEADER DESTINATION include)
install(PROGRAMS src/benchmark-cpp.sh DESTINATION ./)
install(FILES iotdb_sdk/lib/libiotdb_session.so DESTINATION lib)
install(FILES conf/main.conf DESTINATION conf)

# Package
set(CPACK_GENERATOR "ZIP")
set(CPACK_SET_DESTDIR ON)
include(CPack)








