PROJECT(worker_pool)

CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake/Modules/")

FIND_PACKAGE(ZMQ REQUIRED)

LINK_DIRECTORIES(${ZMQ_LIBRARY_DIRS})
INCLUDE_DIRECTORIES(${ZMQ_INCLUDE_DIRS})

IF(CMAKE_COMPILER_IS_GNUCC)
ENDIF(CMAKE_COMPILER_IS_GNUCC)

IF ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  IF(CMAKE_BUILD_TYPE MATCHES Release)
    ADD_DEFINITIONS(-O3)
  ENDIF(CMAKE_BUILD_TYPE MATCHES Release)

  SET(CMAKE_CXX_FLAGS "-W -Wall -pedantic --std=c++0x")
ELSEIF ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  IF(CMAKE_BUILD_TYPE MATCHES Release)
    ADD_DEFINITIONS(-O3 -fexpensive-optimizations)
  ENDIF(CMAKE_BUILD_TYPE MATCHES Release)

  SET(CMAKE_CXX_FLAGS "-W -Wall -pedantic --std=c++0x")
ENDIF ()

INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR})

ADD_SUBDIRECTORY(0mq)
ADD_SUBDIRECTORY(0mqmap)
ADD_SUBDIRECTORY(src)
