#include <0mq/message.h>

#include <0mqmap/worker.h>

#include <thread>

zmq::Message job(const zmq::Message& data) {
  std::this_thread::sleep_for(std::chrono::seconds(2));
  return data;
}

int main(int /*argc*/, const char** /*argv*/) {
  zmqmap::Worker worker(job);
  worker.init();

  worker.go();

  return 0;
}
