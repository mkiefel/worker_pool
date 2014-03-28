ADD_EXECUTABLE(ppworker ppworker.cc)
TARGET_LINK_LIBRARIES(ppworker 0mqmap 0mq zmq)

ADD_EXECUTABLE(ppclient ppclient.cc)
TARGET_LINK_LIBRARIES(ppclient 0mqmap 0mq zmq)
