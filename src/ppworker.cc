#include <0mq/message.h>

#include <0mqmap/worker.h>

#include <thread>
#include <string>
#include <iostream>

zmq::Message job(const zmq::Message& data) {
  std::this_thread::sleep_for(std::chrono::seconds(2));
  return data;
}

int main(int argc, const char** argv) {
  if (argc < 2) {
    std::cout << "Usage: " << argv[0] << " <broker-address>" << std::endl;
    return 1;
  }

  std::string brokerAddress = argv[1];

  zmqmap::Worker worker(job);
  worker.init(brokerAddress);

  worker.go();

  return 0;
}
