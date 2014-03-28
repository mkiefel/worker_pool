#include "zmq_worker.h"

#include <0mq/message.h>

#include <thread>

zmq::Message job(const zmq::Message& data) {
  std::this_thread::sleep_for(std::chrono::seconds(10));
  return data;
}

int main(int /*argc*/, const char** /*argv*/) {
  ZmqWorker worker(job);
  worker.init();

  worker.go();

  return 0;
}
