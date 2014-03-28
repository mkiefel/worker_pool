ADD_EXECUTABLE(ppworker ppworker.cc)
TARGET_LINK_LIBRARIES(ppworker zmq 0mq)

ADD_EXECUTABLE(ppclient ppclient.cc)
TARGET_LINK_LIBRARIES(ppclient zmq 0mq)

ADD_EXECUTABLE(ppqueue ppqueue.cc)
TARGET_LINK_LIBRARIES(ppqueue zmq 0mq)
