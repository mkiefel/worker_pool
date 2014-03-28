ADD_LIBRARY(0mqmap STATIC worker.cc client.cc)

ADD_EXECUTABLE(broker broker.cc)
TARGET_LINK_LIBRARIES(broker 0mq zmq)
