INCLUDE_DIRECTORIES("${CMAKE_CURRENT_SOURCE_DIR}")

FILE(GLOB lib_srcs *.cc)

SET(the_target "0mq")

ADD_LIBRARY(${the_target} STATIC ${lib_srcs})
TARGET_LINK_LIBRARIES(${the_target} zmq)
