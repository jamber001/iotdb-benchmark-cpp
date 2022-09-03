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

PROJECT(benchmark-cpp)
CMAKE_MINIMUM_REQUIRED(VERSION 3.7)

SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
SET(CMAKE_POSITION_INDEPENDENT_CODE ON)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g ")
#add_definitions("-Wall -g -O2")

SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/sbin )

#FIND_PACKAGE(Boost REQUIRED )
#IF (DEFINED BOOST_INCLUDEDIR)
#    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
#ENDIF()


MESSAGE( " Build executable file: ${EXECUTABLE_OUTPUT_PATH}/${PROJECT_NAME} \n")

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/sdk/clientIncLib/include)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/src/common)
LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/sdk/clientIncLib/lib)

AUX_SOURCE_DIRECTORY(./src SRC_LIST)
AUX_SOURCE_DIRECTORY(./src/common SRC_LIST2)

ADD_EXECUTABLE(${PROJECT_NAME} ${SRC_LIST} ${SRC_LIST2})
TARGET_LINK_LIBRARIES(${PROJECT_NAME} iotdb_session pthread)

