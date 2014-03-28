ADD_LIBRARY(zmqmap STATIC zmq_worker.cc)

ADD_EXECUTABLE(ppworker ppworker.cc)
TARGET_LINK_LIBRARIES(ppworker zmqmap 0mq zmq)

ADD_EXECUTABLE(ppclient ppclient.cc)
TARGET_LINK_LIBRARIES(ppclient zmq 0mq)

ADD_EXECUTABLE(ppqueue ppqueue.cc)
TARGET_LINK_LIBRARIES(ppqueue zmq 0mq)
